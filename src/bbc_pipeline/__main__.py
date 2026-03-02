from __future__ import annotations

import argparse
from pathlib import Path

from .pipeline import main


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="bbc_pipeline", description="BBC News pipeline (fetch -> parse -> store)")
    p.add_argument("--base-url", default="https://www.bbc.com/", help="Homepage URL to scan")
    p.add_argument("--limit", type=int, default=60, help="Max links to scan from homepage")
    p.add_argument("--sleep", type=float, default=1.0, help="Sleep seconds between article fetches")
    p.add_argument("--db", default="data/bbc_articles.db", help="SQLite DB file path")
    p.add_argument("--log", default="logs/pipeline.log", help="Log file path")
    return p.parse_args()


def ensure_parent(path_str: str) -> str:
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def main_entry() -> None:
    args = parse_args()
    db_file = ensure_parent(args.db)
    log_file = ensure_parent(args.log)
    main(limit=args.limit, sleep_seconds=args.sleep, db_file=db_file, base_url=args.base_url, log_file=log_file)


if __name__ == "__main__":
    main_entry()
