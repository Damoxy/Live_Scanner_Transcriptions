function sendYesterdayEmailAsPDF() {
  const sheetName = "output";
  const emailRecipient = " ";

  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  const data = sheet.getDataRange().getValues();

  if (data.length < 2) return;

  // Headers
  const headers = data[0];
  const addressIndex = headers.indexOf("extracted_address");
  const keywordIndex = headers.indexOf("extracted_keyword");
  const timestampIndex = headers.indexOf("timestamp");

  if (addressIndex === -1 || keywordIndex === -1 || timestampIndex === -1) {
    Logger.log("Required columns not found.");
    return;
  }

  // Yesterday's date range
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);
  yesterday.setHours(0, 0, 0, 0);
  const yesterdayEnd = new Date(today);
  yesterdayEnd.setDate(today.getDate() - 1);
  yesterdayEnd.setHours(23, 59, 59, 999);

  // Filter rows for yesterday AND non-empty keyword
  const rowsForYesterday = data.slice(1).filter(row => {
    const ts = new Date(row[timestampIndex]);
    const address = row[addressIndex];
    const keyword = row[keywordIndex];
    return ts >= yesterday &&
           ts <= yesterdayEnd &&
           address && address.toString().trim() !== "" &&
           keyword && keyword.toString().trim() !== "";
  });

  if (rowsForYesterday.length === 0) {
    Logger.log("No valid data for yesterday.");
    return;
  }

  // Styled HTML template
  let htmlBody = `
    <div style="font-family:Arial, sans-serif; padding:20px; color:#333;">
      <div style="text-align:center; margin-bottom:20px;">
        <h1 style="color:#2C3E50;">Scanner Report</h1>
        <p style="font-size:14px; color:#555;">Data extracted for <strong>${yesterday.toDateString()}</strong></p>
      </div>
      <table style="width:100%; border-collapse:collapse; box-shadow:0 2px 6px rgba(0,0,0,0.1);">
        <thead>
          <tr style="background-color:#2C3E50; color:#fff; text-align:left;">
            <th style="padding:10px;">#</th>
            <th style="padding:10px;">Timestamp</th>
            <th style="padding:10px;">Keyword</th>
            <th style="padding:10px;">Address</th>
          </tr>
        </thead>
        <tbody>`;

  rowsForYesterday.forEach((row, i) => {
    const keyword = row[keywordIndex].toString().replace(/[\[\]']+/g, "");
    // Alternating calm colors: soft blue and soft gray
    const bgColor = i % 2 === 0 ? "#e8f0fe" : "#f5f5f5";
    htmlBody += `
      <tr style="background-color:${bgColor};">
        <td style="padding:10px; border-bottom:1px solid #ddd;">${i + 1}</td>
        <td style="padding:10px; border-bottom:1px solid #ddd;">${row[timestampIndex]}</td>
        <td style="padding:10px; border-bottom:1px solid #ddd;">${keyword}</td>
        <td style="padding:10px; border-bottom:1px solid #ddd;">${row[addressIndex]}</td>
      </tr>`;
  });

  htmlBody += `
        </tbody>
      </table>
      <p style="margin-top:30px; font-size:12px; text-align:center; color:#777;">
        © ${new Date().getFullYear()} solvinghomesales.com.
      </p>
    </div>`;

  // Convert HTML to PDF
  const blob = Utilities.newBlob(htmlBody, "text/html", "report.html")
                        .getAs("application/pdf")
                        .setName("Scanner_Report_" + yesterday.toDateString() + ".pdf");

  // Send email with PDF
  MailApp.sendEmail({
    to: emailRecipient,
    subject: "Scanner Report - " + yesterday.toDateString(),
    htmlBody: "Hi,<br><br>Please find attached the scanner report for <b>" + yesterday.toDateString() + "</b>.<br><br>Regards,<br>Scanner Auto Extractor",
    name: "Scanner Auto Extractor",
    attachments: [blob]
  });

  Logger.log("PDF sent to " + emailRecipient);
}
