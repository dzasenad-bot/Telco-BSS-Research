# 04-pipelines/python/etl/ingest_rss.py
import csv, json, pathlib, datetime, hashlib, re, time
import requests, feedparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dateutil import parser as dp

ROOT = pathlib.Path(__file__).resolve().parents[3]
RSS_FILE = ROOT / "02-sources" / "seed_lists" / "news_rss.csv"
OUT_DIR  = ROOT / "03-data" / "raw" / datetime.date.today().strftime("%Y/%m/%d")
LOG_DIR  = ROOT / "03-data" / "logs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"ingest_{datetime.date.today().strftime('%Y%m%d')}.jsonl"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

# neki sajtovi traže i Referer
HOST_REF = {
    "telecoms.com": "https://telecoms.com/",
    "ericsson.com": "https://www.ericsson.com/",
    "blogs.oracle.com": "https://blogs.oracle.com/",
    "ooredoo.com": "https://www.ooredoo.com/",
}

def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")

def fid(url, title):
    m = hashlib.md5()
    m.update((url or title or "").encode("utf-8", errors="ignore"))
    return m.hexdigest()

def norm_dt(entry):
    dt = entry.get("published") or entry.get("updated") or ""
    try:
        return dp.parse(dt).date().isoformat()
    except Exception:
        return ""

def parse_notes(notes: str) -> dict:
    """Parsiraj 'notes' polje: key=value parovi (npr. 'limit=100;allow_insecure=1')."""
    cfg = {}
    if not notes:
        return cfg
    for part in re.split(r"[;,]\s*", notes.strip()):
        if "=" in part:
            k, v = part.split("=", 1)
            cfg[k.strip().lower()] = v.strip()
        elif part:
            cfg[part.strip().lower()] = "1"
    return cfg

def read_seed_rows(path: pathlib.Path):
    # podrži utf-8, utf-8-sig, cp1250/cp1252, latin-1
    encodings = ["utf-8", "utf-8-sig", "cp1250", "cp1252", "latin-1"]
    last_exc = None
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                rdr = csv.DictReader(f)
                rows = []
                for r in rdr:
                    if not r:
                        continue
                    r = { (k or "").strip().lower(): (v or "").strip() for k, v in r.items() }
                    url = r.get("url")
                    if not url:
                        continue
                    rows.append({
                        "source": r.get("source","news") or "news",
                        "name":   r.get("name") or r.get("source") or "unknown",
                        "url":    url,
                        "notes":  r.get("notes","")
                    })
                if rows:
                    return rows
        except Exception as ex:
            last_exc = ex
            continue
    raise last_exc or RuntimeError(f"Cannot read {path} with known encodings")

def new_session():
    s = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3, backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

def fetch(sess: requests.Session, url: str, referer: str | None, insecure: bool, timeout: int = 25) -> bytes:
    headers = {
        "User-Agent": UA,
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }
    if referer:
        headers["Referer"] = referer
    r = sess.get(url, headers=headers, timeout=timeout, verify=not insecure)
    r.raise_for_status()
    return r.content

def parse_feed(raw_bytes: bytes):
    return feedparser.parse(raw_bytes)

def log_status(name, url, status, count=0, error=None):
    rec = {
        "date": datetime.date.today().isoformat(),
        "source_name": name,
        "url": url,
        "status": status,         # OK / EMPTY / ERROR
        "count": count,
        "error": str(error) if error else None,
        "host": requests.utils.urlparse(url).hostname,
    }
    with open(LOG_FILE, "a", encoding="utf-8") as lf:
        lf.write(json.dumps(rec, ensure_ascii=False) + "\n")

def main():
    rows = read_seed_rows(RSS_FILE)
    sess = new_session()
    total = 0

    for r in rows:
        kind, name, url = r["source"], r["name"], r["url"]
        cfg = parse_notes(r.get("notes",""))
        # flags iz notes
        if cfg.get("disabled", "0") in ("1", "true", "yes"):
            print(f"[INGEST] {name}: SKIP (disabled)")
            log_status(name, url, "DISABLED", 0, None)
            continue
        
        limit = int(cfg.get("limit", "50")) if str(cfg.get("limit","")).isdigit() else 50
        insecure = cfg.get("allow_insecure", "0") in ("1", "true", "yes")
        host = requests.utils.urlparse(url).hostname or ""
        referer = HOST_REF.get(host)

        fname = OUT_DIR / f"{safe_name(name)}.jsonl"
        try:
            raw = fetch(sess, url, referer=referer, insecure=insecure)
            feed = parse_feed(raw)
            entries = getattr(feed, "entries", []) or []
            print(f"[INGEST] {name}: parsed entries={len(entries)} | {url}")
        except Exception as ex:
            print(f"[INGEST] {name}: ERROR fetching {url} -> {ex}")
            log_status(name, url, "ERROR", 0, ex)
            entries = []

        if not entries:
            log_status(name, url, "EMPTY", 0, None)
            time.sleep(0.25)
            continue

        # Pišemo samo ako ima stavki
        wrote = 0
        with open(fname, "w", encoding="utf-8") as f:
            for e in entries[:limit]:
                link = getattr(e, "link", "")
                title = getattr(e, "title", "")
                item = {
                    "id": fid(link, title),
                    "source_kind": kind,
                    "source_name": name,
                    "source_url": url,
                    "title": title,
                    "link": link,
                    "summary": getattr(e, "summary", ""),
                    "published_date": norm_dt(e),
                }
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                total += 1
                wrote += 1

        log_status(name, url, "OK", wrote, None)
        time.sleep(0.4)  # budi fin prema izvorima

    print("Saved raw feeds to:", OUT_DIR, "| total items:", total)

if __name__ == "__main__":
    main()
