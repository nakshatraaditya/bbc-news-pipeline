from bbc_pipeline.pipeline import extract_article_fields, parse_published_to_iso

def test_extract_article_fields_basic():
    html = """
    <html>
      <body>
        <h1>Test Title</h1>
        <time datetime="2026-02-25T04:32:30Z"></time>
        <p>This is a long paragraph that should be picked as the first paragraph because it is definitely longer than sixty characters.</p>
      </body>
    </html>
    """
    out = extract_article_fields(html, "https://example.com/test")
    assert out["title"] == "Test Title"
    assert out["published_raw"] == "2026-02-25T04:32:30Z"
    assert "long paragraph" in out["first_paragraph"].lower()

def test_parse_published_iso_z():
    iso = parse_published_to_iso("2026-02-25T04:32:30Z", "2026-02-25T05:00:00+00:00")
    assert iso.startswith("2026-02-25T04:32:30")

