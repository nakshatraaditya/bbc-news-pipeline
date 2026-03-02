from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urlparse


def parse_published_to_iso(published_raw: str, run_ts_utc: str) -> str:
    """
    Convert BBC published datetime into ISO-8601 with timezone.
    If parsing fails, fallback to run timestamp.
    """
    if not published_raw:
        return run_ts_utc

    s = published_raw.strip()

    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.isoformat()

    except Exception:
        return run_ts_utc


def detect_content_type(url: str) -> tuple[str, str]:
    """
    Determine section + content_type from URL path
    """
    path = urlparse(url).path

    if "/videos/" in path:
        return "videos", "video"
    elif "/live/" in path:
        return "live", "live"
    else:
        return "articles", "article"


def extract_author(soup: BeautifulSoup) -> str:
    """
    Try multiple BBC patterns for author extraction
    """
    # BBC often uses meta tag
    meta_author = soup.find("meta", {"name": "byl"})
    if meta_author and meta_author.get("content"):
        return meta_author["content"].replace("By ", "").strip()

    # sometimes span with class
    span_author = soup.find("span", {"class": "byline__name"})
    if span_author:
        return span_author.get_text(strip=True).replace("By ", "")

    return ""


def extract_description(soup: BeautifulSoup) -> str:
    meta_desc = soup.find("meta", {"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc["content"].strip()
    return ""


def extract_article_fields(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    # title
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else ""

    # published time
    t = soup.find("time")
    published_raw = t.get("datetime", "").strip() if t else ""

    # first paragraph
    first_para = ""
    for p in soup.find_all("p"):
        txt = p.get_text(" ", strip=True)
        if txt and len(txt) > 60:
            first_para = txt
            break

    # NEW FIELDS
    author = extract_author(soup)
    description = extract_description(soup)
    section, content_type = detect_content_type(url)

    return {
        "url": url,
        "title": title,
        "published_raw": published_raw,
        "first_paragraph": first_para,
        "author": author,
        "description": description,
        "section": section,
        "content_type": content_type,
    }
