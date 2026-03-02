from datetime import datetime


VALID_CONTENT_TYPES = {"article", "video", "live"}


def is_valid_iso_datetime(value: str) -> bool:
    if not value:
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except Exception:
        return False


def validate_record(record: dict) -> tuple[bool, list[str]]:
    errors = []

    # 1. Required fields
    if not record.get("url"):
        errors.append("Missing url")

    if not record.get("title"):
        errors.append("Missing title")

    if not record.get("published_iso"):
        errors.append("Missing published_iso")

    # 2. Datetime validation
    if record.get("published_iso") and not is_valid_iso_datetime(record["published_iso"]):
        errors.append("Invalid published_iso datetime")

    # 3. URL validation
    url = record.get("url", "")
    if url and not url.startswith("https://www.bbc.com/news/"):
        errors.append("Invalid BBC news URL")

    # 4. Content type validation
    content_type = record.get("content_type")
    if content_type and content_type not in VALID_CONTENT_TYPES:
        errors.append(f"Invalid content_type: {content_type}")

    return (len(errors) == 0, errors)


def filter_valid_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    valid = []
    invalid = []

    seen_urls = set()

    for r in records:
        ok, errs = validate_record(r)

        if r.get("url") in seen_urls:
            errs.append("Duplicate URL in batch")
            ok = False

        if ok:
            valid.append(r)
            seen_urls.add(r["url"])
        else:
            r["_errors"] = errs
            invalid.append(r)

    return valid, invalid
