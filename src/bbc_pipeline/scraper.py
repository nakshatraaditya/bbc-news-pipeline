import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


def is_valid_bbc_news_path(path: str) -> bool:
    return bool(re.match(r"^/news/(articles|videos|live)/", path))


def extract_bbc_links(home_html: str, base_url: str, limit: int = 60) -> list[str]:
    soup = BeautifulSoup(home_html, "lxml")

    seen: set[str] = set()
    out: list[str] = []

    base_netloc = urlparse(base_url).netloc

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue

        full = urljoin(base_url, href)
        parsed = urlparse(full)

        if parsed.scheme not in ("http", "https"):
            continue

        if parsed.netloc and parsed.netloc != base_netloc:
            continue

        if not is_valid_bbc_news_path(parsed.path):
            continue

        full = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

        if full in seen:
            continue

        seen.add(full)
        out.append(full)

        if len(out) >= limit:
            break

    return out
