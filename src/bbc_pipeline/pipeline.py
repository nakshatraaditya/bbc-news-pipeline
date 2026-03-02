from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from .cleaner import clean_record
from .db import get_existing_urls, init_db, newest_articles, upsert_articles
from .fetcher import safe_get_html
from .logger import get_logger
from .parser import extract_article_fields, parse_published_to_iso
from .quality import filter_valid_records
from .scraper import extract_bbc_links


def main(
    limit: int = 60,
    sleep_seconds: float = 1.0,
    db_file: str = "data/bbc_articles.db",
    base_url: str = "https://www.bbc.com/",
    log_file: str = "logs/pipeline.log",
) -> None:
    Path(db_file).parent.mkdir(parents=True, exist_ok=True)
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger = get_logger(log_file)
    run_ts_utc = datetime.now(timezone.utc).isoformat()

    init_db(db_file)

    logger.info("[INFO] Fetching homepage: %s", base_url)
    home_html = safe_get_html(base_url)
    if not home_html:
        logger.error("[ERROR] Could not fetch homepage.")
        return

    links = extract_bbc_links(home_html, base_url, limit=limit)
    logger.info("[INFO] Links found: %d", len(links))

    existing = get_existing_urls(db_file, links)
    logger.info("[INFO] Already in DB: %d", len(existing))

    new_links = [u for u in links if u not in existing]
    logger.info("[INFO] New links to enrich: %d", len(new_links))

    ok_count = 0
    fail_count = 0
    parsed_rows: list[dict] = []

    for i, url in enumerate(new_links, start=1):
        logger.info("[INFO] (%d/%d) %s", i, len(new_links), url)

        html = safe_get_html(url)
        if not html:
            fail_count += 1
            logger.error("[ERROR] Failed to fetch: %s", url)
            time.sleep(sleep_seconds)
            continue

        try:
            rec = extract_article_fields(html, url)
            rec["run_ts_utc"] = run_ts_utc
            rec["published_iso"] = parse_published_to_iso(rec.get("published_raw", ""), run_ts_utc)

            rec = clean_record(rec)
            parsed_rows.append(rec)

            ok_count += 1
            logger.info("[OK] %s", (rec.get("title") or "")[:85])
        except Exception as e:
            fail_count += 1
            logger.error("[ERROR] Parse/clean failed for %s -> %s", url, e)

        time.sleep(sleep_seconds)

    valid_rows: list[dict] = []
    invalid_rows: list[dict] = []

    if parsed_rows:
        valid_rows, invalid_rows = filter_valid_records(parsed_rows)
        logger.info("[INFO] Valid records: %d", len(valid_rows))
        logger.info("[INFO] Invalid records: %d", len(invalid_rows))

        if invalid_rows:
            invalid_path = Path(log_file).parent / "invalid_records.jsonl"
            with invalid_path.open("a", encoding="utf-8") as f:
                for r in invalid_rows:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
            logger.warning("[WARN] Wrote invalid records to: %s", str(invalid_path))

    upserted = upsert_articles(db_file, valid_rows)

    logger.info("")
    logger.info("[SUMMARY]")
    logger.info("Run timestamp (UTC): %s", run_ts_utc)
    logger.info("Homepage links scanned: %d", len(links))
    logger.info("New attempted: %d", len(new_links))
    logger.info("Enriched OK: %d", ok_count)
    logger.info("Failed: %d", fail_count)
    logger.info("Upserted rows: %d", upserted)
    logger.info("DB: %s", db_file)
    logger.info("")
    logger.info("Newest 10 by published_iso:")

    newest = newest_articles(db_file, 10)
    for row in newest:
        if isinstance(row, dict):
            logger.info("- %s | %s\n  %s", row.get("published_iso"), row.get("title"), row.get("url"))
        else:
            try:
                published_iso, title, url = row[0], row[1], row[2]
                logger.info("- %s | %s\n  %s", published_iso, title, url)
            except Exception:
                logger.info("- %s", str(row))
