import re
from html import unescape
from typing import Any


_ws_re = re.compile(r"\s+")
_ctrl_re = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _clean_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = unescape(s)
    s = s.replace("\u00a0", " ")
    s = _ctrl_re.sub(" ", s)
    s = _ws_re.sub(" ", s).strip()
    return s


def _infer_section(url: str) -> str:
    if "/news/live/" in url:
        return "live"
    if "/news/videos/" in url:
        return "videos"
    if "/news/articles/" in url:
        return "articles"
    return "other"


def _infer_content_type(url: str) -> str:
    if "/news/live/" in url:
        return "live"
    if "/news/videos/" in url:
        return "video"
    if "/news/articles/" in url:
        return "article"
    return "other"


def clean_record(rec: dict) -> dict:
    url = _clean_text(rec.get("url", ""))

    out = dict(rec)
    out["url"] = url
    out["title"] = _clean_text(rec.get("title", ""))
    out["published_raw"] = _clean_text(rec.get("published_raw", ""))
    out["published_iso"] = _clean_text(rec.get("published_iso", ""))
    out["first_paragraph"] = _clean_text(rec.get("first_paragraph", ""))
    out["run_ts_utc"] = _clean_text(rec.get("run_ts_utc", ""))

    out["author"] = _clean_text(rec.get("author", ""))
    out["description"] = _clean_text(rec.get("description", ""))

    out["section"] = _clean_text(rec.get("section", "")) or _infer_section(url)
    out["content_type"] = _clean_text(rec.get("content_type", "")) or _infer_content_type(url)

    return out
