import os, csv, json, pathlib, datetime, hashlib, re
import feedparser
from dateutil import parser as dp

ROOT = pathlib.Path(__file__).resolve().parents[3]
rss_file = ROOT / "02-sources" / "seed_lists" / "news_rss.csv"
out_dir = ROOT / "03-data" / "raw" / datetime.date.today().strftime("%Y/%m/%d")
out_dir.mkdir(parents=True, exist_ok=True)

rows = [r for r in csv.DictReader(open(rss_file, newline='', encoding="utf-8"))]

def norm_dt(e):
    dt = e.get("published") or e.get("updated") or ""
    try: return dp.parse(dt).date().isoformat()
    except: return ""

def fid(url, title):
    m = hashlib.md5(); m.update((url or title or "").encode("utf-8", errors="ignore"))
    return m.hexdigest()

def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")

total = 0
for r in rows:
    kind, name, url = r["source"], r["name"], r["url"]
    feed = feedparser.parse(url)
    entries = getattr(feed, "entries", []) or []
    print(f"[INGEST] {name}: {len(entries)} items from {url}")
    if not entries:
        continue
    items = []
    for e in entries[:50]:
        url_e = getattr(e, "link", "")
        title = getattr(e, "title", "")
        items.append({
            "id": fid(url_e, title),
            "source_kind": kind, "source_name": name, "source_url": url,
            "title": title, "link": url_e, "summary": getattr(e, "summary", ""),
            "published_date": norm_dt(e)
        })
    total += len(items)
    with open(out_dir / (safe_name(name) + ".jsonl"), "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

print("Saved raw feeds to:", out_dir, "| total items:", total)
