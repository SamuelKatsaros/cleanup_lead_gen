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
        ws = sh.add_worksheet(title=title, rows=200, cols=16)
        ws.append_row([
            "timestamp_iso",
            "stream_name",
            "stream_type",
            "stream_id",
            "city",
            "county",
            "state",
            "crime_type",
            "address",
            "confidence",
            "snippet"
        ])
    return ws

def append_lead(stream_name: str, stream_type: str, stream_id: str, city: str, county: str, state: str,
                crime_type: str, address: str, confidence: int, snippet: str):
    ws = _open_sheet()
    ts = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    ws.append_row([
        ts,
        stream_name or "",
        stream_type or "",
        stream_id or "",
        city or "",
        county or "",
        state or "",
        crime_type or "",
        address or "",
        str(confidence),
        (snippet or "")[:900]
    ])
