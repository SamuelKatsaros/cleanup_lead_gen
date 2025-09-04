import os, re
from tenacity import retry, wait_fixed, stop_after_attempt
from openai import OpenAI

# strict prefilter: only proceed if strong biohazard terms appear
# include close synonyms per business requirements
KEYWORDS = [
    # homicide/suicide/murder/unattended death
    r"\b(homicide|murder|suicide|doa|deceased|pronounced)\b",
    r"\b(unattended\s+death|body\s+found|human\s+remains)\b",
    # shootings
    r"\b(gsw|gunshot\s+wound|shot\s+in|multiple\s+shots|shooting|shots\s+fired)\b",
    # stabbings
    r"\b(stab|stabbing|knife\s+wound)\b",
    # blood/bleeding
    r"\b(heavy\s+bleeding|bleeding\s+badly|blood\s+everywhere|pool\s+of\s+blood|biohazard|hemorrhag(e|ing|ed))\b",
    # decomposition
    r"\b(decomp|decomposition|decomposed|decomposing|odor\s+of\s+decomposition|smell\s+of\s+decomp)\b",
    # tear gas
    r"\b(tear\s+gas|cs\s+gas)\b",
    # fire/large fire-related (structure/wildland, smoke, burn victims)
    r"\b(structure\s+fire|house\s+fire|apartment\s+fire|building\s+fire|commercial\s+fire|garage\s+fire|warehouse\s+fire)\b",
    r"\b(wild\s*fire|wildland\s+fire|brush\s+fire|grass\s+fire|forest\s+fire|wildland)\b",
    r"\b(working\s+fire|second\s+alarm|third\s+alarm|multiple\s+alarm|defensive\s+attack|offensive\s+attack)\b",
    r"\b(heavy\s+smoke|smoke\s+condition|smoke\s+showing|flames\s+visible|fully\s+involved|engulfed)\b",
    r"\b(burn\s+victim|thermal\s+burns|severe\s+burns|burn\s+injury|burns\s+to)\b",
    r"\b(lines?\s+down|power\s+lines?\s+down|downed\s+lines?|downed\s+power\s+lines?)\b",
    r"\b(evacuation\s+order|evacuate\s+area|evacuations?)\b",
    r"\b(red\s+flag\s+warning|fire\s+weather)\b",
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
