#!/usr/bin/env python3
"""
Daily Emergency Transcript Processor 
"""

import os
import json
import logging
import time
import datetime as dt
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import spacy
import usaddress
from tqdm import tqdm
import runpod
import paramiko
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from dotenv import load_dotenv
import colorlog

# ----------------------------
# Load environment variables
# ----------------------------
load_dotenv()
RUNPOD_API_KEY = os.getenv("RUNPOD_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
GOOGLE_SHEET_CREDS = os.getenv("GOOGLE_SHEET_CREDS")  # Path to JSON file
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "output")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH", "~/.ssh/id_ed25519")

# ----------------------------
# Colorful Logging Configuration
# ----------------------------
formatter = colorlog.ColoredFormatter(
    fmt="%(log_color)s%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    log_colors={
        'DEBUG':    'cyan',
        'INFO':     'green',
        'WARNING':  'yellow',
        'ERROR':    'red',
        'CRITICAL': 'bold_red',
    }
)

handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger = logging.getLogger("EmergencyProcessor")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False

# ----------------------------
# Custom Google Sheet Logging Handler
# ----------------------------
class GoogleSheetHandler(logging.Handler):
    def __init__(self, sheet_url, worksheet_name, creds_path):
        super().__init__()
        self.sheet_url = sheet_url
        self.worksheet_name = worksheet_name
        self.creds_path = creds_path

        # Initialize Google Sheet client
        scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        self.client = gspread.authorize(creds)
        self.sheet = self.client.open_by_url(sheet_url)

        # Ensure worksheet exists
        try:
            self.worksheet = self.sheet.worksheet(self.worksheet_name)
        except gspread.WorksheetNotFound:
            self.worksheet = self.sheet.add_worksheet(title=self.worksheet_name, rows="100", cols="10")
            # Create headers
            self.worksheet.append_row(["timestamp", "level", "message"])

    def emit(self, record):
        try:
            log_entry = [
                dt.datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S"),
                record.levelname,
                record.getMessage()
            ]
            self.worksheet.append_row(log_entry, value_input_option="RAW")
        except Exception as e:
            print(f"Failed to log to Google Sheet: {e}")

# Attach Google Sheet logger
if GOOGLE_SHEET_CREDS and SPREADSHEET_URL:
    gs_handler = GoogleSheetHandler(SPREADSHEET_URL, "log", GOOGLE_SHEET_CREDS)
    gs_handler.setLevel(logging.INFO)
    gs_handler.setFormatter(formatter)
    logger.addHandler(gs_handler)

# ----------------------------
# Constants & Regex
# ----------------------------
KEYWORD_LIST = [
    "Fire","Explosion","Collapse","House fire","structure fire",
    "residential fire","working structure fire","fire damage",
    "Fatal fire","Arson","Suspicious fire","Roof collapse",
    "Building collapse","structure collapse","Major Water Damage",
    "Flooded home","Severe water damage","Forced Vacate",
    "Unsafe structure","Red-tagged building","Condemned property",
    "Code enforcement closure","Uninhabitable dwelling","Homicide",
    "Death investigation","Fatal accident","Meth lab",
    "Drug lab contamination","Hazmat cleanup"
]

ADDRESS_REGEX = re.compile(
    r'\d{1,5}\s[\w\s]+(?:Street|St|Avenue|Ave|Boulevard|Blvd|Road|Rd|Drive|Dr|Court|Ct|Lane|Ln|Way|Terrace|Ter|Place|Pl)\b',
    re.IGNORECASE
)

# ----------------------------
# Initialize RunPod
# ----------------------------
runpod.api_key = RUNPOD_API_KEY
ssh_key_path = os.path.expanduser(SSH_KEY_PATH)

# ----------------------------
# Helper Functions
# ----------------------------
def fetch_pod_records(pod):
    """Fetch JSON records from a single RunPod pod via SSH."""
    records = []
    for port in pod['runtime']['ports']:
        if port['isIpPublic'] and port['type'] == 'tcp' and port['privatePort'] == 22:
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(
                    hostname=port['ip'],
                    port=port['publicPort'],
                    username='root',
                    key_filename=ssh_key_path
                )

                stdin, stdout, stderr = ssh.exec_command('ls /workspace/outputs/*.json')
                files = stdout.read().decode().splitlines()

                for file in files:
                    stdin, stdout, stderr = ssh.exec_command(f'cat {file}')
                    output = stdout.read().decode().strip()
                    if output:
                        try:
                            file_records = json.loads(output)
                            records.extend(file_records)
                        except json.JSONDecodeError as e:
                            logger.warning(f"JSON decode error in pod {pod['name']} file {file}: {e}")
                ssh.close()
            except Exception as e:
                logger.error(f"Failed to read from pod {pod['name']} at {port['ip']}:{port['publicPort']}: {e}")
    return records

def filter_yesterday(df, date_col="timestamp"):
    """Filter dataframe to only rows from yesterday's date."""
    df[date_col] = pd.to_datetime(df[date_col], format="%Y%m%d_%H%M%S", errors="coerce")
    yesterday = (dt.datetime.now() - dt.timedelta(days=1)).date()
    return df[df[date_col].dt.date == yesterday].copy()

def extract_address_regex(text):
    match = ADDRESS_REGEX.search(text)
    return match.group(0) if match else ""

def extract_address_spacy(text, nlp_model):
    doc = nlp_model(text)
    candidates = [ent.text for ent in doc.ents if ent.label_ in ("GPE","FAC","LOC")]
    return max(candidates, key=len, default="")

def standardize_usaddress(raw_text):
    if not raw_text:
        return ""
    try:
        parsed, _ = usaddress.tag(raw_text)
        parts = []
        for key in [
            "AddressNumber","StreetNamePreDirectional","StreetName",
            "StreetNamePostType","OccupancyType","OccupancyIdentifier",
            "PlaceName","StateName","ZipCode"
        ]:
            if key in parsed:
                parts.append(parsed[key])
        return " ".join(parts)
    except usaddress.RepeatedLabelError:
        return raw_text.strip()

def extract_address_prefilter(text, nlp_model):
    candidate = extract_address_regex(text)
    if not candidate:
        candidate = extract_address_spacy(text, nlp_model)
    return standardize_usaddress(candidate)

def extract_location_and_keywords(text):
    """Use LLM to extract standardized address and incident keywords."""
    prompt = (
        "You are an assistant that extracts information from emergency transcripts.\n\n"
        "Tasks:\n"
        "1. Extract the full location if present (street number, name, type, city, state, zip).\n"
        "   - Convert spelled-out numbers to digits.\n"
        "   - Standardize to USPS-style formatting.\n"
        "2. Identify ALL relevant incident keywords from this list:\n"
        f"{', '.join(KEYWORD_LIST)}\n"
        "3. Return ONLY valid JSON with two keys:\n"
        "   - 'location': string\n"
        "   - 'keywords': array of matching keywords\n\n"
        f"Transcript:\n{text}\n"
    )

    payload = {
        "model": "gpt-oss-20b",
        "messages": [
            {"role":"system","content":"You are a strict JSON extraction assistant."},
            {"role":"user","content":prompt}
        ],
        "temperature": 0
    }

    try:
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json"
            },
            json=payload
        )
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        cleaned = re.sub(r"```(?:json)?|```","",content).strip()
        parsed = json.loads(cleaned)

        raw_keywords = parsed.get("keywords", [])
        if isinstance(raw_keywords, str):
            raw_keywords = [raw_keywords]
        valid_keywords = [kw for kw in raw_keywords if kw in KEYWORD_LIST]

        return parsed.get("location",""), ", ".join(valid_keywords)
    except Exception as e:
        logger.error(f"LLM extraction error: {e}")
        return "",""

def append_to_google_sheet(df, sheet_url, worksheet_name, creds_path):
    """Append DataFrame rows to a Google Sheet."""
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(sheet_url).worksheet(worksheet_name)
    values = df.astype(str).values.tolist()
    sheet.append_rows(values, value_input_option="RAW")
    logger.info("✅ Appended data to Google Sheet.")

# ----------------------------
# Main workflow
# ----------------------------
def main():
    logger.info("🚀 Starting main.py ...")

    start_time = time.time()
    pods = runpod.get_pods()
    logger.info(f"Found {len(pods)} pods.")

    # Parallel fetch
    all_records = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_pod_records, pod): pod for pod in pods}
        for future in as_completed(futures):
            records = future.result()
            all_records.extend(records)
            logger.debug(f"Fetched {len(records)} records from a pod.")

    if not all_records:
        logger.warning("⚠️ No JSON records found in any pods.")
        return

    df = pd.DataFrame(all_records)
    expected_cols = ['url', 'transcription', 'timestamp', 'location', 'keywords', 'count']
    df = df[[c for c in expected_cols if c in df.columns]]
    logger.info(f"📊 Total records loaded: {len(df)}")

    # Filter yesterday
    df = filter_yesterday(df, "timestamp")
    logger.info(f"🗓 {len(df)} rows from yesterday after filtering.")

    # Load spaCy model
    logger.info("Loading spaCy model 'en_core_web_sm'...")
    nlp = spacy.load("en_core_web_sm")

    # Prefilter addresses with progress bar
    tqdm.pandas(desc="Prefilter addresses")
    df["prefilter_address"] = df["transcription"].progress_apply(lambda x: extract_address_prefilter(x, nlp))
    df_prefiltered = df[
        (df["prefilter_address"] != "") &
        (df["keywords"].notna()) &
        (df["keywords"].astype(str) != "[]") &
        (df["keywords"].astype(str) != "")
    ].copy()
    logger.info(f"✅ {len(df_prefiltered)} rows passed prefilter and keyword check.")

    # LLM extraction with progress bar
    tqdm.pandas(desc="LLM extracting address + keywords")
    results = df_prefiltered["transcription"].progress_apply(extract_location_and_keywords)
    df_prefiltered["address"] = results.apply(lambda x: x[0])
    df_prefiltered["extracted_keywords"] = results.apply(lambda x: x[1])

    # Append to Google Sheet
    append_to_google_sheet(df_prefiltered, SPREADSHEET_URL, WORKSHEET_NAME, GOOGLE_SHEET_CREDS)

    elapsed = time.time() - start_time
    logger.info(f"⏱ Total runtime: {elapsed:.2f} seconds")

if __name__ == "__main__":
    main()
