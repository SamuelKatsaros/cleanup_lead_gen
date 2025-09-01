import os, gspread, datetime as dt
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

def _client():
    sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
    creds = Credentials.from_service_account_file(sa_file, scopes=SCOPES)
    return gspread.authorize(creds)

def _open_sheet():
    gc = _client()
    sh = gc.open_by_key(os.getenv("GOOGLE_SHEET_ID"))
    title = os.getenv("GOOGLE_SHEET_TAB", "Leads")
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=100, cols=8)
        ws.append_row(["timestamp_iso","crime_type","address","city_state","confidence","snippet"])
    return ws

def append_lead(crime_type: str, address: str, city_state: str, confidence: int, snippet: str):
    ws = _open_sheet()
    ts = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    ws.append_row([ts, crime_type, address, city_state, str(confidence), snippet[:900]])
