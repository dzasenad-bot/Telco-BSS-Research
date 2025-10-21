import json, pathlib, datetime, collections, html, re, os
from urllib.parse import urlparse
from jinja2 import Template

ROOT = pathlib.Path(__file__).resolve().parents[3]
INTERIM = ROOT / "03-data" / "interim" / "today.jsonl"

def strip_html(s):
    if not s: return ""
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()

TPL = Template("""
# Market Panorama — BSS ({{date}})
**Scope:** Charging, Billing/RA, CRM/CX, Catalog/Order, Partner/Policy; plus SaaS/ODA/AI signali.

## 1) Top trendovi (po broju spominjanja)
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

## 4) Top 10 naslova (sa kratkim “why it matters”)
{% for it in top_items %}
- **[{{it.title_clean}}]({{it.link}})** — _{{it.source_name}}_ ({{it.published_date or 'N/A'}})  
  Why it matters: {{it.why}}
{% endfor %}

---

*Generated from today's corpus ({{total}} items).*
""".strip())

def mk_why(tags):
    t = set(tags or [])
    if "charging" in t: return "Signal monetizacije/CCS – relevantno za 5G SA, usage-based i B2B2X."
    if "billing" in t or "ra_fms" in t: return "FinOps/naplata/RA – utjecaj na cashflow i tačnost."
    if "crm" in t: return "CX/CRM – churn/NPS i digitalna prodaja."
    if "catalog" in t or "order" in t: return "T2M/Order-to-Cash – ubrzava lansiranje ponuda."
    if "saas_bss" in t: return "SaaS BSS/cloud-native – TCO i agilnost."
    if "tmf_oda" in t: return "TMF ODA/standardi – interoperabilnost i vendor lock-in rizik."
    if "ai_bss" in t: return "AI u BSS – automatizacija i personalizacija."
    return "Širi BSS/industrijski signal."

# ---------- Anti-dominance (ISKLJUČENO po difoltu) ----------
# Uključi testno postavljanjem varijabli okruženja prije pokretanja:
#   PowerShell (samo u tekućoj sesiji):
#     $env:PANORAMA_ANTI_DOM="1"
#     $env:PANORAMA_MAX_PER_DOMAIN="3"      # max linkova po domenu (default 3)
#     $env:PANORAMA_MIN_SCORE="2"           # minimalni relevance_score za Top sekciju (default 0)
#
# Isključi:
#     Remove-Item Env:PANORAMA_ANTI_DOM
# ------------------------------------------------------------
ANTI_DOM = os.getenv("PANORAMA_ANTI_DOM", "0") == "1"
MAX_PER_DOMAIN = int(os.getenv("PANORAMA_MAX_PER_DOMAIN", "3"))
MIN_SCORE = int(os.getenv("PANORAMA_MIN_SCORE", "0"))

def domain_of(link: str) -> str:
    try:
        netloc = urlparse(link).netloc.lower()
        # ukloni www.
        return netloc[4:] if netloc.startswith("www.") else netloc
    except Exception:
        return ""

def apply_anti_dominance(items):
    """Zadrži najviše N stavki po domenu, preferiraj veći relevance_score i novije."""
    if not ANTI_DOM:
        return items
    by_dom = collections.defaultdict(list)
    for it in items:
        by_dom[domain_of(it.get("link",""))].append(it)
    limited = []
    for dom, lst in by_dom.items():
        lst_sorted = sorted(lst, key=lambda x: (x.get("relevance_score",0), x.get("published_date") or ""), reverse=True)
        limited.extend(lst_sorted[:MAX_PER_DOMAIN])
    # ponovo sortiraj cijelu listu
    limited = sorted(limited, key=lambda x: (x.get("relevance_score",0), x.get("published_date") or ""), reverse=True)
    print(f"[PANORAMA] anti-dominance ON | max_per_domain={MAX_PER_DOMAIN} | min_score={MIN_SCORE} | kept={len(limited)} of {len(items)}")
    return limited

# ------------------------------------------------------------

items = []
with open(INTERIM, encoding="utf-8") as fh:
    for line in fh:
        it = json.loads(line)
        it["title_clean"] = strip_html(it.get("title",""))
        it["why"] = mk_why(it.get("bss_module_tags",[]))
        items.append(it)

# agregati
cnt_tags = collections.Counter()
cnt_v = collections.Counter()
cnt_o = collections.Counter()
for it in items:
    for t in it.get("bss_module_tags",[]):
        cnt_tags[t] += 1
    for v in it.get("entities",{}).get("vendors",[]):
        cnt_v[v.lower()] += 1
    for o in it.get("entities",{}).get("operators",[]):
        cnt_o[o.lower()] += 1

# bazna lista za Top: sort po (score, date)
sorted_all = sorted(items, key=lambda x: (x.get("relevance_score",0), x.get("published_date") or ""), reverse=True)

# (opcionalno) minimalni score prag
if MIN_SCORE > 0:
    sorted_all = [it for it in sorted_all if (it.get("relevance_score",0) >= MIN_SCORE)]

# (opcionalno) anti-dominance
sorted_all = apply_anti_dominance(sorted_all)

# uzmi Top 10
top10 = sorted_all[:10]

outdir = ROOT / "06-delivery" / "briefs" / "industry"
outdir.mkdir(parents=True, exist_ok=True)
fname = outdir / f"market-panorama_{datetime.date.today().isoformat()}.md"
with open(fname, "w", encoding="utf-8-sig") as f:
    f.write(TPL.render(
        date=datetime.date.today().isoformat(),
        top_tags=cnt_tags.most_common(8),
        top_vendors=cnt_v.most_common(8),
        top_ops=cnt_o.most_common(8),
        top_items=top10,
        total=len(items)
    ))
print("Wrote:", fname)
