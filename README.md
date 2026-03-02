# 📰 BBC News Data Pipeline

A production-style Python data pipeline that scrapes, cleans, validates, and stores BBC News articles into a structured SQLite database for downstream analytics and dashboards.

Built as part of my MSc Data Science portfolio to demonstrate **real-world data engineering + scraping + data quality + pipeline design**.

---

## 🚀 Features

✔ Scrapes BBC News homepage  
✔ Extracts articles, live blogs, and videos  
✔ Parses key fields:
- Title
- Published date
- First paragraph
- Section
- Content type (article / video / live)

✔ Cleans and standardises data  
✔ Performs data quality validation  
✔ Stores results in SQLite database  
✔ Logs full pipeline execution  
✔ Idempotent (safe to re-run without duplication)

---

## 🧱 Project Structure

```
bbc-news-pipeline/
│
├── data/
│   └── bbc_articles.db              # SQLite database (generated)
│
├── logs/
│   ├── pipeline.log                 # execution logs
│   └── invalid_records.jsonl        # failed validation records
│
├── scripts/
│   └── run_pipeline.py              # entry script
│
├── src/
│   └── bbc_pipeline/
│       ├── __init__.py
│       ├── __main__.py              # CLI entry point
│       ├── pipeline.py              # main orchestration logic
│       ├── scraper.py               # extract article URLs
│       ├── fetcher.py               # HTTP requests
│       ├── parser.py                # extract article content
│       ├── cleaner.py               # cleaning + standardisation
│       ├── quality.py               # validation checks
│       ├── db.py                    # database operations
│       └── logger.py                # logging configuration
│
├── tests/
│   └── test_parser.py               # unit tests
│
├── requirements.txt
├── pytest.ini
└── README.md
```

---

## ⚙️ Installation

Clone the repository:

```bash
git clone https://github.com/nakshatraaditya/bbc-news-pipeline.git
cd bbc-news-pipeline
