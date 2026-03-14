# Zimbabwe Property Market Intelligence Platform

A production-grade data pipeline that scrapes, stores, and transforms Zimbabwe real estate listings into actionable market intelligence.

## Architecture Overview

```
                ┌─────────────────────────────────────────────┐
                │              SCRAPY SPIDERS                  │
                │  property.co.zw | classifieds.co.zw | agents │
                └──────────────────┬──────────────────────────┘
                                   │
                ┌──────────────────▼──────────────────────────┐
                │           SCRAPY PIPELINE                    │
                │  Validation → Dedup → Snowflake Raw Load     │
                └──────────────────┬──────────────────────────┘
                                   │
                ┌──────────────────▼──────────────────────────┐
                │           SNOWFLAKE LAYERS                   │
                │  RAW (zw_property_listings)                  │
                │  STAGING (cleaned_property_listings)         │
                │  ANALYTICS (price trends, suburb insights)   │
                └──────────────────┬──────────────────────────┘
                                   │
                ┌──────────────────▼──────────────────────────┐
                │             dbt TRANSFORMS                   │
                │  stg → intermediate → marts                  │
                └──────────────────┬──────────────────────────┘
                                   │
                ┌──────────────────▼──────────────────────────┐
                │         PREFECT ORCHESTRATION                │
                │  Daily: Scrape → Load → Transform → QC      │
                └─────────────────────────────────────────────┘
```

## Quick Start

### 1. Clone and configure
```bash
cp configs/.env.example .env
# Fill in Snowflake credentials
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set up Snowflake
```bash
python snowflake/setup_snowflake.py
```

### 4. Run scraper
```bash
cd scraper
scrapy crawl property_co_zw -o data/listings.jsonl
```

### 5. Load to Snowflake
```bash
python pipelines/loader.py
```

### 6. Run dbt
```bash
cd dbt/zim_property
dbt run
dbt test
```

### 7. Run full pipeline via Prefect
```bash
python orchestration/pipeline.py
```

### Docker
```bash
docker-compose up -d
```

## Week 3–4 MVP: Paid PDF Suburb Reports

### 1) Generate a PDF report directly
```bash
python reports/pdf_suburb_report.py --suburb "Borrowdale" --city "Harare" --out reports/output/borrowdale_report.pdf
```

### 2) Run checkout + webhook app
```bash
python reports/order_app.py
```

### 3) Required env vars (in `configs/.env`)
- `STRIPE_SECRET_KEY`
- `STRIPE_WEBHOOK_SECRET`
- `REPORT_APP_BASE_URL` (default `http://localhost:5055`)
- `REPORT_PRICE_CENTS` (default `2999`)
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM`

Flow: order form → Stripe Checkout → webhook confirmation → suburb data query → PDF generated → email delivery.

## Project Structure
```
zim-property-data-platform/
├── scraper/                    # Scrapy project
│   ├── spiders/                # One spider per source
│   ├── middlewares/            # Rotating agents, retry, proxy
│   ├── pipelines/              # Validation, dedup, Snowflake load
│   └── utils/                 # Shared helpers
├── pipelines/                  # Standalone ETL scripts
├── snowflake/                  # DDL SQL + setup scripts
├── dbt/zim_property/           # dbt transformation project
├── orchestration/              # Prefect flows
├── data_quality/               # Great Expectations checks
├── configs/                    # .env, settings
└── docker/                     # Dockerfiles + compose
```
