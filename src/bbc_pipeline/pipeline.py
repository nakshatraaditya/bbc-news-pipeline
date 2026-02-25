import argparse
import re
import time
import sqlite3
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NakshWebDataBot/1.0; +learning-project)"
}

BBC_HOSTS = {"www.bbc.com", "bbc.com"}
NEWS_PATH_RE = re.compile(r"^/news(/|/articles/)")
REL_RE = re.compile(r"^\s*(\d+)\s+(minute|minutes|hour|hours|day|days|week|weeks)\s+ago\s*$", re.I)


def safe_get_html(url: str, timeout: int = 10, retries: int = 2, backoff: float = 2.0) -> str | None:
  
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            last_err = e
            wait = backoff * (attempt + 1)
            print(f"[WARN] GET failed ({attempt+1}/{retries+1}) {url} -> {e} | sleep {wait:.1f}s")
            time.sleep(wait)
    print(f"[ERROR] All retries failed: {url} -> {last_err}")
    return None


def extract_homepage_article_urls(html: str, base: str) -> list[str]:
   
    soup = BeautifulSoup(html, "lxml")
    found: list[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue

        full = urljoin(base, href)
        parsed = urlparse(full)

        if parsed.netloc.lower() not in BBC_HOSTS:
            continue
        if not NEWS_PATH_RE.match(parsed.path):
            continue

        normalized = parsed._replace(fragment="").geturl()
        found.append(normalized)

    # de-dupe preserve order
    seen = set()
    out = []
    for u in found:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def extract_article_fields(html: str, url: str) -> dict:
   
    soup = BeautifulSoup(html, "lxml")

    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else ""

    t = soup.find("time")
    published_raw = ""
    if t:
        published_raw = (t.get("datetime") or t.get_text(" ", strip=True) or "").strip()

    first_para = ""
    for p in soup.find_all("p"):
        txt = p.get_text(" ", strip=True)
        if txt and len(txt) > 60:
            first_para = txt
            break

    return {
        "url": url,
        "title": title,
        "published_raw": published_raw,
        "first_paragraph": first_para,
    }


def parse_published_to_iso(published_raw: str, run_ts_utc: str) -> str:
 
    if not published_raw:
        return ""

    p = published_raw.strip()

    # ISO with Z
    try:
        if p.endswith("Z"):
            dt = datetime.fromisoformat(p.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).isoformat()
        # ISO with timezone offset
        if "T" in p and ("+" in p or p.endswith("00:00")):
            dt = datetime.fromisoformat(p)
            return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass

    # Relative time (if your source ever uses it)
    m = REL_RE.match(p)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()

        try:
            base = datetime.fromisoformat(run_ts_utc.replace("Z", "+00:00"))
            if base.tzinfo is None:
                base = base.replace(tzinfo=timezone.utc)
            base = base.astimezone(timezone.utc)
        except Exception:
            base = datetime.now(timezone.utc)

        if "minute" in unit:
            dt = base - timedelta(minutes=n)
        elif "hour" in unit:
            dt = base - timedelta(hours=n)
        elif "day" in unit:
            dt = base - timedelta(days=n)
        elif "week" in unit:
            dt = base - timedelta(weeks=n)
        else:
            return ""

        return dt.isoformat()

    return ""


def init_db(db_file: str) -> None:
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS bbc_articles (
        url TEXT PRIMARY KEY,
        run_ts_utc TEXT,
        title TEXT,
        published_raw TEXT,
        published_iso TEXT,
        first_paragraph TEXT
    )
    """)
    conn.commit()
    conn.close()


def get_existing_urls(db_file: str) -> set[str]:
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute("SELECT url FROM bbc_articles")
    rows = cur.fetchall()
    conn.close()
    return {r[0] for r in rows}


def upsert_rows(db_file: str, rows: list[dict]) -> int:
    if not rows:
        return 0

    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    n = 0

    for row in rows:
        cur.execute("""
        INSERT OR REPLACE INTO bbc_articles
        (url, run_ts_utc, title, published_raw, published_iso, first_paragraph)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row.get("url", ""),
            row.get("run_ts_utc", ""),
            row.get("title", ""),
            row.get("published_raw", ""),
            row.get("published_iso", ""),
            row.get("first_paragraph", ""),
        ))
        n += 1

    conn.commit()
    conn.close()
    return n


def print_newest(db_file: str, n: int = 10) -> None:
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    rows = list(cur.execute("""
        SELECT published_iso, title, url
        FROM bbc_articles
        WHERE published_iso != ''
        ORDER BY published_iso DESC
        LIMIT ?
    """, (n,)))
    conn.close()

    print(f"\nNewest {n} by published_iso:")
    for pub, title, url in rows:
        print(f"- {pub} | {title}")
        print(f"  {url}")


def main():
    parser = argparse.ArgumentParser(description="BBC homepage -> enrich -> SQLite pipeline")
    parser.add_argument("--base", default="https://www.bbc.com")
    parser.add_argument("--path", default="/")
    parser.add_argument("--db", default="bbc_articles.db")
    parser.add_argument("--limit-links", type=int, default=60)
    parser.add_argument("--sleep", type=float, default=1.0)
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--newest", type=int, default=10)
    args = parser.parse_args()

    init_db(args.db)

    run_ts_utc = datetime.now(timezone.utc).isoformat()
    homepage_url = urljoin(args.base, args.path)

    print("[INFO] Fetching homepage:", homepage_url)
    home_html = safe_get_html(homepage_url, timeout=args.timeout, retries=args.retries)
    if not home_html:
        print("[ERROR] Could not fetch homepage.")
        return

    links = extract_homepage_article_urls(home_html, args.base)[: args.limit_links]
    print("[INFO] Links found:", len(links))

    existing = get_existing_urls(args.db)
    to_fetch = [u for u in links if u not in existing]
    print("[INFO] New links to enrich:", len(to_fetch))

    rows_out = []
    ok = 0
    fail = 0

    for i, url in enumerate(to_fetch, start=1):
        print(f"[INFO] ({i}/{len(to_fetch)}) {url}")
        html = safe_get_html(url, timeout=args.timeout, retries=args.retries)
        if not html:
            fail += 1
            continue

        data = extract_article_fields(html, url)
        data["run_ts_utc"] = run_ts_utc
        data["published_iso"] = parse_published_to_iso(data.get("published_raw", ""), run_ts_utc)

        rows_out.append(data)
        ok += 1
        print(f"[OK] {data.get('title','')[:80]}")
        time.sleep(args.sleep)

    inserted = upsert_rows(args.db, rows_out)

    print("\n[SUMMARY]")
    print("Run timestamp (UTC):", run_ts_utc)
    print("Homepage links scanned:", len(links))
    print("New attempted:", len(to_fetch))
    print("Enriched OK:", ok)
    print("Failed:", fail)
    print("Upserted rows:", inserted)
    print("DB:", args.db)

    print_newest(args.db, n=args.newest)

