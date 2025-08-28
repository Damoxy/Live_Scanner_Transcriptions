# Daily Emergency Transcript Processor

**A Python-based tool for fetching, processing, and analyzing emergency transcript data from RunPod pods, with automated extraction of locations and incident keywords, and integration with Google Sheets.**

---

## Features

* Fetch JSON transcript outputs from multiple RunPod pods via SSH.
* Filter transcripts for **yesterday's data**.
* Extract addresses and incident keywords using:

  * Regex for common address patterns
  * **spaCy** NLP model
  * LLM (OpenRouter GPT-OSS) for advanced extraction
* Standardize addresses using **USPS-style formatting** (`usaddress`).
* Append processed results to a **Google Sheet** for easy access.
* **Colorized logging** with `colorlog` for clear monitoring.
* Progress bars via `tqdm` for prefiltering and LLM extraction.
* Handles SSH connection, JSON parsing, and error logging robustly.

---

## Requirements

* Python 3.10+
* Packages:

```bash
pip install pandas requests spacy usaddress tqdm runpod paramiko gspread oauth2client python-dotenv colorlog
python -m spacy download en_core_web_sm
```

* Access to a **Google Service Account JSON** for Google Sheets API.
* **RunPod API Key** and pod SSH access.
* **OpenRouter API Key** for LLM-based extraction.

---

## Setup

1. Clone the repository:

```bash
git clone https://github.com/yourusername/daily-emergency-transcripts.git
cd daily-emergency-transcripts
```

2. Create a `.env` file in the project root with the following variables:

```dotenv
RUNPOD_API_KEY=your_runpod_api_key
OPENROUTER_API_KEY=your_openrouter_api_key
GOOGLE_SHEET_CREDS=/path/to/google_service_account.json
SPREADSHEET_URL=https://docs.google.com/spreadsheets/d/your_sheet_id
WORKSHEET_NAME=output
SSH_KEY_PATH=~/.ssh/id_ed25519
```

3. Ensure your SSH key has access to the RunPod pods.

---

## Usage

Run the script:

```bash
python main.py
```

Example output (colorized logs):

```
2025-08-28 10:15:22 [INFO] 🚀 Starting main.py with colorful logging...
2025-08-28 10:15:23 [INFO] Found 3 pods.
2025-08-28 10:15:24 [INFO] 📊 Total records loaded: 150
2025-08-28 10:15:24 [INFO] 🗓 75 rows from yesterday after filtering.
2025-08-28 10:15:30 [INFO] ✅ 60 rows passed prefilter and keyword check.
2025-08-28 10:16:05 [INFO] ✅ Appended data to Google Sheet.
2025-08-28 10:16:05 [INFO] ⏱ Total runtime: 43.21 seconds
```

---

## Project Structure

```
daily-emergency-transcripts/
│
├── main.py               # Main script with logging and workflow
├── README.md             # Project documentation
├── .env                  # Environment variables (API keys, paths)
├── requirements.txt      # Python dependencies
└── ...                   # Other supporting scripts or modules
```

---

## Workflow Overview

```
       +----------------+
       |  RunPod Pods   |
       |  JSON Outputs  |
       +-------+--------+
               |
               v
       +----------------+
       |   Fetch via    |
       |     SSH        |
       +-------+--------+
               |
               v
       +----------------+
       | Prefilter &    |
       | Regex / spaCy  |
       +-------+--------+
               |
               v
       +----------------+
       | LLM Extraction |
       |  Address &     |
       |  Keywords      |
       +-------+--------+
               |
               v
       +----------------+
       | Google Sheets  |
       |  Append Data   |
       +----------------+
```

---

## Quick Notes

* Only keywords from **KEYWORD\_LIST** are included in the Google Sheet.
* Errors from SSH, JSON, or LLM calls are logged without stopping execution.
* Filtered automatically to **yesterday's transcripts**.

---


