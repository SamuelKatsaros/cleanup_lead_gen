import os, re
from tenacity import retry, wait_fixed, stop_after_attempt
from openai import OpenAI

# strict prefilter: only proceed if strong biohazard terms appear
KEYWORDS = [
    r"\b(homicide|murder|suicide|doa|deceased|pronounced)\b",
    r"\b(unattended\s+death|body\s+found|human\s+remains)\b",
    r"\b(gsw|gunshot\s+wound|shot\s+in|multiple\s+shots)\b",
    r"\b(stab|stabbing|knife\s+wound)\b",
    r"\b(heavy\s+bleeding|bleeding\s+badly|blood\s+everywhere|pool\s+of\s+blood|biohazard)\b",
]

def passes_prefilter(text: str) -> bool:
    t = text.lower()
    return any(re.search(p, t) for p in KEYWORDS)

@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def classify(text: str) -> dict:
    """
    returns {"lead": bool, "crime_type": str, "confidence": int, "address_hint": str}
    """
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    system = (
        "you are a cautious dispatcher analyst. output strict json with keys: "
        "lead(boolean), crime_type(string one of [homicide,suicide,unattended_death,stabbing,shooting,other_biohazard,not_a_lead]), "
        "confidence(integer 0-100), address_hint(string).\n"
        "flag lead=true only if cleanup is nearly certain (blood/body at scene or pronounced deceased). "
        "include stabbings with visible bleeding. exclude mere arrests or medicals without blood."
    )
    prompt = (
        "transcript:\n"
        f"{text}\n\n"
        "extract: was this a cleanup lead? what type? any address phrase?"
    )
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        response_format={"type":"json_object"},
        messages=[{"role":"system","content":system},
                  {"role":"user","content":prompt}],
    )
    j = resp.choices[0].message.content
    import json
    out = json.loads(j)
    # normalize
    out["crime_type"] = out.get("crime_type","").strip().lower()
    out["confidence"] = int(out.get("confidence", 0))
    out["address_hint"] = out.get("address_hint","").strip()
    out["lead"] = bool(out.get("lead", False))
    return out
