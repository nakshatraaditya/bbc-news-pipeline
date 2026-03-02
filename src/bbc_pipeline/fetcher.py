import time
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NakshWebDataBot/1.0; +learning-project)"
}

def safe_get_html(url: str, timeout: int = 10, retries: int = 3, backoff: float = 2.0) -> str | None:
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff * (2 ** attempt))
    print(f"[ERROR] All retries failed: {url} -> {last_err}")
    return None
