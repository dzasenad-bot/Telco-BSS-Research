import json, pathlib, datetime, random, re, html
from jinja2 import Template

ROOT = pathlib.Path(__file__).resolve().parents[3]
INTERIM = ROOT / "03-data" / "interim" / "today.jsonl"
OPERATORS = ["e-and", "Deutsche-Telekom", "Orange"]  # update-ano

def strip_html(s: str) -> str:
    if not s: return ""
    # odstrani HTML tagove i dekodiraj HTML entitete
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()

tpl = Template("""# Daily Brief
{% for it in items -%}
- **[{{it.title_clean}}]({{it.link}})** — _{{it.source_name}}_ ({{it.published_date or 'N/A'}})
  - Why it matters: {{it.why}}
  - Impact: Sales={{it.impact_sales}}, Product={{it.impact_product}}, GRC={{it.impact_compliance}}
  - Sources: {{it.link}}
{% endfor -%}
""")

def mk_why(it):
    tags = it.get("bss_module_tags", [])
    if "charging" in tags:
        return "Charging/monetizacija signal — potencijalno relevantno za 5G SA/B2B2X ponude."
    if "billing" in tags:
        return "Billing/RA pomak — utječe na cashflow i tačnost naplate."
    if "crm" in tags:
        return "CRM/CX signal — utječe na churn i NPS."
    return "Širi BSS/industrijski signal."

def impact_stub(tags):
    return ("med" if "charging" in tags or "billing" in tags else "low",
            "med" if "catalog" in tags or "order" in tags else "low",
            "low")

items = []
for line in open(INTERIM, encoding="utf-8"):
    it = json.loads(line)
    it["title_clean"] = strip_html(it.get("title",""))
    it["why"] = mk_why(it)
    s,p,c = impact_stub(it.get("bss_module_tags",[]))
    it["impact_sales"], it["impact_product"], it["impact_compliance"] = s,p,c
    items.append(it)

# uzmi 5 najnovijih
items = sorted(items, key=lambda x: x.get("published_date") or "", reverse=True)[:5] or random.sample(items, min(5,len(items)))

week = f"{datetime.date.today().isocalendar().year}-W{datetime.date.today().isocalendar().week:02d}"
for op in OPERATORS:
    outdir = ROOT / "06-delivery" / "briefs" / "operator-centric" / op / week
    outdir.mkdir(parents=True, exist_ok=True)
    # piši kao UTF-8 s BOM da PowerShell sigurno prikaže ispravno
    with open(outdir / "daily.md", "w", encoding="utf-8-sig") as f:
        f.write(tpl.render(items=items))
    print("Wrote:", outdir / "daily.md")
