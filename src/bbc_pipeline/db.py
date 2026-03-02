import sqlite3


def init_db(db_file: str) -> None:
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            url TEXT PRIMARY KEY,
            title TEXT,
            published_raw TEXT,
            published_iso TEXT,
            first_paragraph TEXT,
            run_ts_utc TEXT,
            author TEXT,
            section TEXT,
            description TEXT,
            content_type TEXT
        )
        """
    )

    conn.commit()
    conn.close()


def get_existing_urls(db_file: str, urls: list[str]) -> set[str]:
    if not urls:
        return set()

    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    existing: set[str] = set()
    chunk_size = 900

    for i in range(0, len(urls), chunk_size):
        chunk = urls[i:i + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))

        rows = cur.execute(
            f"SELECT url FROM articles WHERE url IN ({placeholders})",
            chunk
        ).fetchall()

        existing.update(r[0] for r in rows)

    conn.close()
    return existing


def upsert_articles(db_file: str, rows: list[dict]) -> int:
    if not rows:
        return 0

    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    cur.executemany(
        """
        INSERT INTO articles (
            url, title, published_raw, published_iso, first_paragraph, run_ts_utc,
            author, section, description, content_type
        )
        VALUES (
            :url, :title, :published_raw, :published_iso, :first_paragraph, :run_ts_utc,
            :author, :section, :description, :content_type
        )
        ON CONFLICT(url) DO UPDATE SET
            title=excluded.title,
            published_raw=excluded.published_raw,
            published_iso=excluded.published_iso,
            first_paragraph=excluded.first_paragraph,
            run_ts_utc=excluded.run_ts_utc,
            author=excluded.author,
            section=excluded.section,
            description=excluded.description,
            content_type=excluded.content_type
        """,
        rows,
    )

    conn.commit()
    affected = cur.rowcount
    conn.close()
    return affected


def newest_articles(db_file: str, n: int = 10):
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    rows = cur.execute(
        """
        SELECT published_iso, title, url
        FROM articles
        ORDER BY published_iso DESC
        LIMIT ?
        """,
        (n,),
    ).fetchall()

    conn.close()
    return rows
