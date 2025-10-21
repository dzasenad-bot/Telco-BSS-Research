# 04-pipelines/python/synth/make_market_panorama_weekly.py
import json, pathlib, datetime, collections, html, re, os
from urllib.parse import urlparse
from jinja2 import Template

ROOT = pathlib.Path(__file__).resolve().parents[3]
RAW_ROOT = ROOT / "03-data" / "raw"

# ---- mini classifier (isti duh kao quick_classify) ----
KEYWORDS = {
    "charging": r"\b(charging|ccs|ocs|converged charging|monetiz)\b",
    "billing": r"\b(billing|invoice|revenue assurance|ra)\b",
    "crm": r"\b(crm|customer experience|cx|care)\b",
    "catalog": r"\b(catalog|offer|product catalog)\b",
    "order": r"\b(order management|som|tom)\b",
    "partner": r"\b(partner|marketplace|b2b2x)\b",
    "ai_bss": r"\b(ai|genai|ml)\b",
    "saas_bss": r"\b(saas|cloud-native|kubernetes)\b",
    "tmf_oda": r"\b(oda|tm forum|open api)\b",
}
def classify(title, summary):
    text = f"{title} {summary}".lower()
    tags = [k for k, rx in KEYWORDS.items() if re.search(rx, text)]
    return tags or ["general"]

# ---- helpers ----
def strip_html(s):
    if not s: return ""
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()

def domain_of(link: str) -> str:
    try:
        d = urlparse(link).netloc.lower()
        return d[4:] if d.startswith("www.") else d
    except: return ""

def mk_why(tags):
    t = set(tags or [])
    if "charging" in t: return "Monetizacija/CCS – 5G SA, usage-based, B2B2X."
    if "billing" in t: return "Billing/RA – utjecaj na cashflow i tačnost."
    if "crm" in t: return "CX/CRM – churn/NPS i digitalna prodaja."
    if "catalog" in t or "order" in t: return "T2M/Order-to-Cash – brže lansiranje."
    if "saas_bss" in t: return "SaaS/cloud-native – agilnost i TCO."
    if "tmf_oda" in t: return "TMF ODA/standardi – interoperabilnost."
    if "ai_bss" in t: return "AI u BSS – automatizacija/personalizacija."
    return "Širi BSS/industrijski signal."

# ---- anti-dominance toggles (po difoltu OFF) ----
ANTI_DOM = os.getenv("PANORAMA_ANTI_DOM", "0") == "1"
MAX_PER_DOMAIN = int(os.getenv("PANORAMA_MAX_PER_DOMAIN", "3"))
MIN_SCORE = int(os.getenv("PANORAMA_MIN_SCORE", "0"))  # nemamo score, ostaje 0

def apply_anti_dominance(items):
    if not ANTI_DOM: return items
    by_dom = collections.defaultdict(list)
    for it in items: by_dom[domain_of(it.get("link",""))].append(it)
    limited = []
    for dom, lst in by_dom.items():
        lst_sorted = sorted(lst, key=lambda x: (x.get("published_date") or ""), reverse=True)
        limited.extend(lst_sorted[:MAX_PER_DOMAIN])
    limited = sorted(limited, key=lambda x: (x.get("published_date") or ""), reverse=True)
    print(f"[WEEKLY] anti-dominance ON | max_per_domain={MAX_PER_DOMAIN} | kept={len(limited)} of {len(items)}")
    return limited

# ---- collect last 7 days from raw/*.jsonl ----
today = datetime.date.today()
dates = [today - datetime.timedelta(days=i) for i in range(7)]
items, seen = [], set()
for d in dates:
    p = RAW_ROOT / d.strftime("%Y/%m/%d")
    for jf in p.glob("*.jsonl"):
        with open(jf, encoding="utf-8") as fh:
            for line in fh:
                it = json.loads(line)
                iid = it.get("id") or (it.get("link") or it.get("title"))
                if not iid or iid in seen: continue
                seen.add(iid)
                it["title_clean"] = strip_html(it.get("title",""))
                # (re)classify to ensure tags exist
                it["bss_module_tags"] = classify(it.get("title",""), it.get("summary",""))
                it["why"] = mk_why(it["bss_module_tags"])
                items.append(it)

# agregati
cnt_tags = collections.Counter()
cnt_v = collections.Counter()  # vendors (ako quick_classify nije trčao, ovo će ostati prazno)
cnt_o = collections.Counter()  # operators
for it in items:
    for t in it.get("bss_module_tags",[]): cnt_tags[t] += 1
    for v in it.get("entities",{}).get("vendors",[]): cnt_v[v.lower()] += 1
    for o in it.get("entities",{}).get("operators",[]): cnt_o[o.lower()] += 1

# sort po datumu (novije prvo). MIN_SCORE zadržan za kompat., ali nemamo score pa ne filtrira.
sorted_all = sorted(items, key=lambda x: (x.get("published_date") or ""), reverse=True)
sorted_all = apply_anti_dominance(sorted_all)
top10 = sorted_all[:10]

# render
ISO_WEEK = f"{today.isocalendar().year}-W{today.isocalendar().week:02d}"
TPL = Template("""
# Market Panorama — BSS (Weekly {{week}})
**Period:** last 7 days ({{start}} → {{end}}).  
**Scope:** Charging, Billing/RA, CRM/CX, Catalog/Order, Partner/Policy; + SaaS/ODA/AI.

## 1) Top trendovi
{% for tag, cnt in top_tags %}
- **{{tag}}** — {{cnt}}
{% endfor %}

## 2) Najspominjaniji vendori
{% for v,c in top_vendors %}
- **{{v}}** — {{c}}
{% endfor %}

## 3) Najspominjaniji operatori
{% for o,c in top_ops %}
- **{{o}}** — {{c}}
{% endfor %}

## 4) Top 10 naslova (sa “why it matters”)
{% for it in top_items %}
- **[{{it.title_clean}}]({{it.link}})** — _{{it.source_name}}_ ({{it.published_date or 'N/A'}})  
  Why it matters: {{it.why}}
{% endfor %}

---
*Generated from {{total}} unique items across 7 days.*
""".strip())

outdir = ROOT / "06-delivery" / "briefs" / "industry"
outdir.mkdir(parents=True, exist_ok=True)
fname = outdir / f"market-panorama_{ISO_WEEK}.md"
with open(fname, "w", encoding="utf-8-sig") as f:
    f.write(TPL.render(
        week=ISO_WEEK,
        start=min(dates).isoformat(),
        end=max(dates).isoformat(),
        top_tags=cnt_tags.most_common(10),
        top_vendors=cnt_v.most_common(10),
        top_ops=cnt_o.most_common(10),
        top_items=top10,
        total=len(items),
    ))
print("Wrote:", fname)
