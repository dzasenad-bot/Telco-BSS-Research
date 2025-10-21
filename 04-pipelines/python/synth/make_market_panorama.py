import json, pathlib, datetime, collections, html, re
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

# top 10 po “relevance_score” (ako nema polja, tretiraj kao 0)
items_sorted = sorted(items, key=lambda x: (x.get("relevance_score",0), x.get("published_date") or ""), reverse=True)[:10]

outdir = ROOT / "06-delivery" / "briefs" / "industry"
outdir.mkdir(parents=True, exist_ok=True)
fname = outdir / f"market-panorama_{datetime.date.today().isoformat()}.md"
with open(fname, "w", encoding="utf-8-sig") as f:
    f.write(TPL.render(
        date=datetime.date.today().isoformat(),
        top_tags=cnt_tags.most_common(8),
        top_vendors=cnt_v.most_common(8),
        top_ops=cnt_o.most_common(8),
        top_items=items_sorted,
        total=len(items)
    ))
print("Wrote:", fname)
