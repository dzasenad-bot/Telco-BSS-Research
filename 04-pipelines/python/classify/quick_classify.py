# 04-pipelines/python/classify/quick_classify.py
import json, pathlib, re, datetime, os, html

ROOT = pathlib.Path(__file__).resolve().parents[3]
raw_dir = ROOT / "03-data" / "raw" / datetime.date.today().strftime("%Y/%m/%d")
interim_dir = ROOT / "03-data" / "interim"
interim_dir.mkdir(parents=True, exist_ok=True)

# ======= KONFIG =======
# Ako želiš strogo "operator-centric" filtriranje, postavi env var:
#   setx OPERATOR_CENTRIC 1   (Windows trajno)  ili u sesiji:  $env:OPERATOR_CENTRIC="1"
OPERATOR_CENTRIC = os.getenv("OPERATOR_CENTRIC", "0") == "1"

# Operateri koje trenutno pratiš (case-insensitive regex; dodaj/izmijeni po potrebi)
operator_patterns = [
    r"\be[ -]?and\b", r"\be\&\b",              # e& varijante
    r"\bdeutsche[\s-]?telekom\b", r"\btelekom\b\b(?! slovenije)", r"\bdt\b",
    r"\borange\b",
    r"\bvodafone\b",
    r"\btelef[oó]nica\b",
    r"\booredoo\b",
    r"\bstc\b", r"\bdtac\b", r"\bmt\s?n\b", r"\betisalat\b", r"\bzain\b",
    r"\btelekom\s+srpske\b|\bm:tel\b", r"\bht\b|\bhrvatski telekom\b",
]

# Glavni BSS moduli i ključne riječi (prošireno)
keywords = {
    "charging": r"\b(ccs|ocs|converged\s+charging|converged\s+charging\s+system|converged\s*charging\s*solution|charging|policy\s*control|pcf|ocf|5g\s*sa\s*charging|monetiz|rate\s*plan|usage\s*based)\b",
    "billing":  r"\b(billing|invoice|invoic(ing|e)|bill\s*run|mediation|revenue\s*assurance|revenue-assurance|\bra\b|dunning|collections)\b",
    "crm":      r"\b(crm|customer\s+(experience|care|management)|cx|care\s+(platform|system)|csat|nps)\b",
    "catalog":  r"\b(product\s*catalog|offer\s*catalog|catalog\s*mgmt|tmf620|tm\s*forum\s*620)\b",
    "order":    r"\b(order\s*(management|manager)|som|tom|orchestrat(ion|e)|order-to-cash|otc)\b",
    "partner":  r"\b(partner\s*(management|portal)|ecosystem|marketplace|b2b2x|wholesale|re\s*seller)\b",
    "policy":   r"\b(policy\s*(control|manager)|pcf|pcrf)\b",
    "ra_fms":   r"\b(revenue\s*assurance|fraud\s*management|fms|leakage)\b",
    "ai_bss":   r"\b(genai|agentic\s*ai|predictive|recommendation\s*engine|copilot)\b",
    "saas_bss": r"\b(saas\s+bss|cloud[-\s]*native|kubernetes|containerized|microservices|hyperscaler)\b",
    "tmf_oda":  r"\b(tmf\s*oda|open\s*digital\s*architecture|tm\s*forum\s*oda)\b"
}

# Vendor “watch” (pomaže za konkurenciju)
vendor_patterns = [
    r"\bamdocs\b", r"\bnetcracker\b", r"\bericsson\b", r"\bcsg\b", r"\boracle\b",
    r"\bcomarch\b", r"\btecnotree\b", r"\bcerillion\b", r"\boptiva\b", r"\bmatrixx\b",
    r"\bhansen\b|\binfonova\b", r"\btotogi\b", r"\bredknee\b"
]

# ======= UTIL =======
def clean_text(s: str) -> str:
    if not s: return ""
    s = html.unescape(s)
    s = re.sub(r"<[^>]+>", " ", s)  # skini HTML tagove
    return re.sub(r"\s+", " ", s).strip().lower()

def match_any(patterns, text: str) -> bool:
    return any(re.search(rx, text, flags=re.IGNORECASE) for rx in patterns)

def classify_modules(text: str):
    tags = [tag for tag, rx in keywords.items() if re.search(rx, text, flags=re.IGNORECASE)]
    return tags or ["general"]

def find_entities(text: str):
    ops = sorted({m.group(0) for rx in operator_patterns for m in re.finditer(rx, text, flags=re.IGNORECASE)})
    vds = sorted({m.group(0) for rx in vendor_patterns   for m in re.finditer(rx, text, flags=re.IGNORECASE)})
    return ops, vds

def relevance_score(tags, ops, vds):
    score = 0
    # moduli nose poen
    core = {"charging","billing","crm","catalog","order","partner","policy","ra_fms"}
    score += sum(1 for t in tags if t in core)
    # operator ili vendor spomenut = +2
    if ops: score += 2
    if vds: score += 2
    # TMF/ODA, SaaS, AI u BSS = dodatni +1
    if "tmf_oda" in tags: score += 1
    if "saas_bss" in tags: score += 1
    if "ai_bss" in tags: score += 1
    return score

# ======= MAIN =======
seen = set()
out = []

for jf in raw_dir.glob("*.jsonl"):
    with open(jf, encoding="utf-8") as fh:
        for line in fh:
            try:
                item = json.loads(line)
            except Exception:
                continue
            iid = item.get("id") or item.get("link")
            if not iid or iid in seen: 
                continue
            seen.add(iid)

            title = clean_text(item.get("title",""))
            summary = clean_text(item.get("summary",""))
            text = f"{title} {summary}"

            # Modul tagovi
            tags = classify_modules(text)

            # Entiteti
            ops, vds = find_entities(text)

            # Ako je uključen striktni operator-centric mod, zadrži samo vijesti gdje se spominje neki operator
            if OPERATOR_CENTRIC and not ops:
                continue

            # Relevancy prag: minimum = bar 1 modul ili vendor/operator spomen
            if not tags and not (ops or vds):
                continue

            item["bss_module_tags"] = tags
            item["entities"] = {"operators": ops, "vendors": vds}
            item["relevance_score"] = relevance_score(tags, ops, vds)

            out.append(item)

# Sortiraj po score ↓, pa po datumu ↓
def dt_key(x):
    # YYYY-MM-DD očekujemo; ako nema, stavi nulu
    return x.get("published_date") or ""

out = sorted(out, key=lambda x: (x.get("relevance_score",0), dt_key(x)), reverse=True)

# Zapiši
with open(interim_dir / "today.jsonl", "w", encoding="utf-8") as f:
    for it in out:
        f.write(json.dumps(it, ensure_ascii=False) + "\n")

print("Interim saved:", interim_dir / "today.jsonl", "| items:", len(out), "| operator_centric:", OPERATOR_CENTRIC)
