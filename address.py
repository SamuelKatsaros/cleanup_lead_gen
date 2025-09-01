import re
from typing import Optional

STREET_TYPES = r"(st|street|ave|avenue|rd|road|blvd|boulevard|ln|lane|dr|drive|ct|court|trl|trail|pkwy|parkway|hwy|highway|way|pl|place)"
DIR = r"(n|s|e|w|ne|nw|se|sw)"
NUM = r"\d{1,6}"
UNIT = r"(apt|unit|#)\s*\w+"

ADDRESS_RE = re.compile(
    rf"\b({NUM})\s+([A-Za-z0-9'.\- ]+?)\s+({STREET_TYPES})\b(?:\s+({DIR})\b)?(?:\s*(?:{UNIT}))?",
    re.IGNORECASE,
)

def extract_address(text: str) -> Optional[str]:
    m = ADDRESS_RE.search(text)
    if not m:
        return None
    return " ".join([p for p in m.groups() if p]).strip()
