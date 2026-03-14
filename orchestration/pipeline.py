"""
orchestration/pipeline.py — Dagster pipeline for Zimbabwe Property Platform.

WHY DAGSTER over Prefect/Airflow:
  • Asset-based model — each dataset (scraped data, Snowflake table, dbt model)
    is a first-class object with lineage, freshness policies, and metadata
  • Native dagster-dbt integration — dbt models appear as individual assets
    in the UI with their own lineage, test results, and run history
  • Built-in data catalogue — no separate tool needed
  • Dagster UI (formerly Dagit) shows asset graph, partition status, run history
  • Type-checked I/O with IOManagers — clean separation of compute vs storage
  • Partitioned assets — daily partition = one day of scraped data, easy backfill

Structure:
  orchestration/
    pipeline.py          ← this file: assets, jobs, schedules, definitions
    resources.py         ← Snowflake + dbt resource configs
    sensors.py           ← file sensor for new JSONL data

Usage:
    # Start Dagster UI (Dagit)
    dagster dev -f orchestration/pipeline.py

    # Materialise all assets once
    dagster asset materialize -f orchestration/pipeline.py --select "*"

    # Backfill a specific date partition
    dagster job execute -f orchestration/pipeline.py -j zim_property_daily_job \
        --config '{"ops": {"scrape_all_sources": {"config": {"run_date": "2024-03-15"}}}}'
"""
import os
import sys
import glob
import json
import subprocess
import logging
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from typing import Generator

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

import dagster as dg
from dagster import (
    asset,
    schedule,
    sensor,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    DailyPartitionsDefinition,
    RunRequest,
    SensorEvaluationContext,
    define_asset_job,
    AssetSelection,
    EnvVar,
)
from dagster_dbt import (
    DbtCliResource,
    DbtProject,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    dbt_assets,
)

ROOT = Path(__file__).parent.parent
DBT_PROJECT_DIR = ROOT / "dbt" / "zim_property"

# ── Daily partition — one run per calendar day ──────────────────────────────
daily_partitions = DailyPartitionsDefinition(
    start_date="2024-01-01",
    timezone="UTC",
)


# ─── Resources ────────────────────────────────────────────────────────────────

class SnowflakeConfig(dg.ConfigurableResource):
    """Snowflake connection parameters injected from environment.

    Required vars (must be set): SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
    Optional vars (have defaults): SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_RAW_SCHEMA, SNOWFLAKE_ROLE
    """
    # EnvVar() raises at startup if the variable is missing — good for required fields.
    # os.getenv() evaluated at class load time is used for optional fields with defaults
    # (the .env file is loaded at the top of this module before these classes are defined).
    account:    str = EnvVar("SNOWFLAKE_ACCOUNT")
    user:       str = EnvVar("SNOWFLAKE_USER")
    password:   str = EnvVar("SNOWFLAKE_PASSWORD")
    database:   str = os.getenv("SNOWFLAKE_DATABASE",   "ZIM_PROPERTY_DB")
    warehouse:  str = os.getenv("SNOWFLAKE_WAREHOUSE",  "ZIM_PROPERTY_WH")
    raw_schema: str = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")
    role:       str = os.getenv("SNOWFLAKE_ROLE",       "ZIM_DBT_ROLE")

    def get_connection(self):
        import snowflake.connector
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            warehouse=self.warehouse,
            schema=self.raw_schema,
            role=self.role,
            application="ZimPropertyDagster",
        )


class SlackNotifier(dg.ConfigurableResource):
    """Optional Slack webhook for pipeline alerts."""
    webhook_url: str = os.getenv("SLACK_WEBHOOK_URL", "")

    def send(self, message: str, level: str = "info") -> None:
        if not self.webhook_url:
            return
        import urllib.request
        emoji = {"info": ":white_check_mark:", "warn": ":warning:", "error": ":x:"}.get(level, ":bell:")
        payload = json.dumps({"text": f"{emoji} *ZimProperty* — {message}"})
        req = urllib.request.Request(
            self.webhook_url,
            data=payload.encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)


# ─── Asset: Scraped raw data ──────────────────────────────────────────────────

@asset(
    name="scraped_property_listings",
    group_name="ingestion",
    partitions_def=daily_partitions,
    description="Raw JSONL files produced by Scrapy spiders for each source site",
    metadata={"sources": MetadataValue.text(
        "property.co.zw, classifieds.co.zw, propertybook.co.zw, realtorville.co.zw, "
        "guestandtanner.co.zw, seeff.co.zw, kennanproperties.co.zw, "
        "zimproperties.com, faranani.co.zw, harareproperties.co.zw, "
        "knightfrank.co.zw, pamgolding.co.zw, api.co.zw, "
        "fineandcountry.co.zw, rawsonproperties.com, century21.co.zw, integratedproperties.co.zw, "
        "propsearch.co.zw, stands.co.zw, shonahome.com, privatepropertyzimbabwe.com, "
        "abcauctions.co.zw"
    )},
    kinds={"python", "scrapy"},
)
def scraped_property_listings(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run all Scrapy spiders for the partition date.
    Each spider outputs a JSONL file under data/.
    """
    run_date = context.partition_key   # YYYY-MM-DD
    context.log.info(f"Scraping for partition: {run_date}")

    (ROOT / "data").mkdir(exist_ok=True)
    (ROOT / "logs").mkdir(exist_ok=True)

    spiders = [
        # ── Major portals ────────────────────────────────────────────────────
        {"name": "property_co_zw",    "args": []},
        {"name": "classifieds_co_zw", "args": []},
        {"name": "propertybook_co_zw","args": []},
        # ── PropData platform agencies ───────────────────────────────────────
        {"name": "propdata_zw",       "args": ["-a", "site=realtorville"]},
        {"name": "propdata_zw",       "args": ["-a", "site=guestandtanner"]},
        {"name": "propdata_zw",       "args": ["-a", "site=seeff"]},
        {"name": "propdata_zw",       "args": ["-a", "site=kennan"]},
        {"name": "propdata_zw",       "args": ["-a", "site=zimproperties"]},
        {"name": "propdata_zw",       "args": ["-a", "site=faranani"]},
        {"name": "propdata_zw",       "args": ["-a", "site=harare_properties"]},
        # ── zim_agent — individual agency sites ─────────────────────────────
        {"name": "zim_agent",         "args": ["-a", "agency=knight_frank_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=pam_golding_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=api_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=fine_country_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=rawson_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=century21_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=integrated_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=pam_golding_zimbabwe_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=rawson_zw_local"]},
        {"name": "zim_agent",         "args": ["-a", "agency=robert_root_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=stonebridge_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=john_pocock_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=trevor_dollar_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=newage_properties_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=remax_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=bridges_realestate_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=terezim_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=legacy_realestate_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=exodus_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=leengate_zw"]},
        {"name": "zim_agent",         "args": ["-a", "agency=lucile_realestate_zw"]},
        # ── Secondary portals ────────────────────────────────────────────────
        {"name": "portal_zw",         "args": ["-a", "site=propsearch"]},
        {"name": "portal_zw",         "args": ["-a", "site=stands"]},
        {"name": "portal_zw",         "args": ["-a", "site=shonahome"]},
        {"name": "portal_zw",         "args": ["-a", "site=privateproperty"]},
        {"name": "portal_zw",         "args": ["-a", "site=property24"]},
        {"name": "portal_zw",         "args": ["-a", "site=westprop"]},
        {"name": "portal_zw",         "args": ["-a", "site=zimre"]},
        {"name": "portal_zw",         "args": ["-a", "site=mashonaland"]},
        # ── Auctions ─────────────────────────────────────────────────────────
        {"name": "abcauctions_co_zw", "args": []},
    ]
    # Note: wayback spider is run via historical_property_listings asset (separate job)

    total_records = 0
    outputs = []
    failed = []

    for spider in spiders:
        tag = spider["name"] if not spider["args"] else f"{spider['name']}_{spider['args'][-1]}"
        out_file = ROOT / "data" / f"{tag}_{run_date}.jsonl"
        log_file = ROOT / "logs" / f"{tag}_{run_date}.log"

        cmd = [
            sys.executable, "-m", "scrapy", "crawl", spider["name"],
            "-o", str(out_file),
            "--logfile", str(log_file),
        ] + spider["args"]

        context.log.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=ROOT / "scraper", timeout=3600)

        if result.returncode != 0:
            context.log.warning(f"Spider {tag} failed (exit {result.returncode})")
            failed.append(tag)
            continue

        count = 0
        if out_file.exists():
            with open(out_file) as f:
                count = sum(1 for line in f if line.strip())
        total_records += count
        outputs.append(str(out_file))
        context.log.info(f"  {tag}: {count} records → {out_file.name}")

    if total_records == 0:
        raise dg.Failure(
            description="Zero records scraped across all spiders",
            metadata={"failed_spiders": MetadataValue.text(", ".join(failed))},
        )

    return MaterializeResult(
        metadata={
            "total_records":  MetadataValue.int(total_records),
            "output_files":   MetadataValue.int(len(outputs)),
            "failed_spiders": MetadataValue.text(", ".join(failed) if failed else "none"),
            "run_date":       MetadataValue.text(run_date),
            "file_paths":     MetadataValue.text("\n".join(outputs)),
        }
    )


# ─── Asset: Historical data (Wayback Machine) ────────────────────────────────

@asset(
    name="historical_property_listings",
    group_name="ingestion",
    description=(
        "Historical Zimbabwe property listings scraped from the Internet Archive "
        "(Wayback Machine) covering property.co.zw and classifieds.co.zw from 2009 "
        "(USD adoption) to present. This is a one-time / ad-hoc backfill asset."
    ),
    kinds={"python", "scrapy"},
)
def historical_property_listings(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run the Wayback Machine spider to harvest ALL archived property listings.

    Override defaults via Dagster run config:
        ops:
          historical_property_listings:
            config:
              site: "property"        # "all" | "property" | "classifieds"
              from_year: "2009"       # earliest year to harvest (USD adoption)
    """
    run_config = context.op_config or {}
    site      = run_config.get("site", "all")
    from_year = run_config.get("from_year", "2009")

    (ROOT / "data" / "historical").mkdir(parents=True, exist_ok=True)
    (ROOT / "logs").mkdir(exist_ok=True)

    tag      = f"wayback_{site}_{from_year}"
    out_file = ROOT / "data" / "historical" / f"{tag}.jsonl"
    log_file = ROOT / "logs" / f"{tag}.log"

    cmd = [
        sys.executable, "-m", "scrapy", "crawl", "wayback",
        "-a", f"site={site}",
        "-a", f"from_year={from_year}",
        "-O", str(out_file),      # -O = overwrite (never append to old file)
        "--logfile", str(log_file),
        "--set", "CLOSESPIDER_TIMEOUT=0",   # no timeout — full historical run
    ]

    context.log.info(f"Starting Wayback spider: site={site}, from_year={from_year}")
    context.log.info(f"Output: {out_file}")
    context.log.info(f"This may take many hours — covering ~{2026 - int(from_year)} years of data")

    result = subprocess.run(cmd, cwd=ROOT, timeout=86400)  # 24 hour hard ceiling

    count = 0
    if out_file.exists():
        with open(out_file) as f:
            count = sum(1 for line in f if line.strip())

    if result.returncode != 0 and count == 0:
        raise dg.Failure(
            description=f"Wayback spider failed (exit {result.returncode}) with zero records",
            metadata={"log_file": MetadataValue.text(str(log_file))},
        )

    context.log.info(f"Wayback spider complete: {count:,} historical records")

    return MaterializeResult(
        metadata={
            "total_records": MetadataValue.int(count),
            "site":          MetadataValue.text(site),
            "from_year":     MetadataValue.text(from_year),
            "output_file":   MetadataValue.text(str(out_file)),
            "log_file":      MetadataValue.text(str(log_file)),
        }
    )


# ─── Asset: Snowflake raw load ────────────────────────────────────────────────

@asset(
    name="snowflake_raw_listings",
    group_name="snowflake",
    partitions_def=daily_partitions,
    deps=[scraped_property_listings],
    description="JSONL files loaded into Snowflake RAW.ZW_PROPERTY_LISTINGS via MERGE",
    kinds={"snowflake"},
)
def snowflake_raw_listings(
    context: AssetExecutionContext,
    snowflake: SnowflakeConfig,
    slack: SlackNotifier,
) -> MaterializeResult:
    """
    Loads all JSONL files for the partition date into Snowflake.
    Uses MERGE to deduplicate on listing_id.
    """
    run_date = context.partition_key
    pattern = str(ROOT / "data" / f"*_{run_date}*.jsonl")
    files = glob.glob(pattern)

    if not files:
        raise dg.Failure(
            description=f"No JSONL files found for {run_date}",
            metadata={"pattern": MetadataValue.text(pattern)},
        )

    context.log.info(f"Loading {len(files)} files to Snowflake for {run_date}")

    cmd = [
        sys.executable,
        str(ROOT / "pipelines" / "loader.py"),
        "--input", *files,
        "--batch-size", os.getenv("SNOWFLAKE_BATCH_SIZE", "500"),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)

    if result.returncode != 0:
        context.log.error(result.stderr[-500:])
        slack.send(f"Snowflake load FAILED for {run_date}: {result.stderr[:200]}", level="error")
        raise dg.Failure(description="Snowflake loader failed", metadata={
            "stderr": MetadataValue.text(result.stderr[-500:])
        })

    # Query Snowflake for the actual loaded count as metadata
    conn = snowflake.get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS "
        "WHERE SCRAPED_AT::DATE = %s",
        (run_date,),
    )
    loaded_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    context.log.info(f"Snowflake row count for {run_date}: {loaded_count}")

    return MaterializeResult(
        metadata={
            "rows_loaded":   MetadataValue.int(loaded_count),
            "files_loaded":  MetadataValue.int(len(files)),
            "run_date":      MetadataValue.text(run_date),
            "snowflake_table": MetadataValue.text("ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS"),
        }
    )


# ─── Asset: dbt transformations ───────────────────────────────────────────────
#
# dagster-dbt loads every dbt model as its own Dagster asset automatically.
# This gives per-model lineage, test results, and run history in the UI.

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)

# Build the manifest if it doesn't exist yet (first run / clean checkout).
# In production this is pre-built in the Docker image via `dbt parse`.
_manifest_path = DBT_PROJECT_DIR / "target" / "manifest.json"
if not _manifest_path.exists():
    import subprocess as _sp
    _sp.run(["dbt", "deps", "--profiles-dir", str(DBT_PROJECT_DIR)], cwd=DBT_PROJECT_DIR)
    _sp.run(["dbt", "parse", "--profiles-dir", str(DBT_PROJECT_DIR)], cwd=DBT_PROJECT_DIR)

@dbt_assets(
    manifest=_manifest_path,
    dagster_dbt_translator=DagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
    ),
    partitions_def=daily_partitions,
)
def zim_property_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Runs all dbt models as individual Dagster assets.
    Each model's lineage, tests, and freshness is tracked separately.
    """
    # Pass the partition date as a dbt variable so incremental models filter correctly
    run_date = context.partition_key
    yield from dbt.cli(
        ["run", "--vars", json.dumps({"run_date": run_date})],
        context=context,
    ).stream()

    # Run dbt tests after models
    yield from dbt.cli(
        ["test", "--vars", json.dumps({"run_date": run_date})],
        context=context,
    ).stream()


# ─── Asset: Data quality checks ───────────────────────────────────────────────

@asset(
    name="data_quality_results",
    group_name="quality",
    partitions_def=daily_partitions,
    deps=[snowflake_raw_listings],
    description="Data quality gate results written to DATA_QUALITY.CHECK_RESULTS",
    kinds={"python", "snowflake"},
)
def data_quality_results(
    context: AssetExecutionContext,
    snowflake: SnowflakeConfig,
    slack: SlackNotifier,
) -> MaterializeResult:
    """
    Runs all data quality checks for the partition date.
    Raises Failure on CRITICAL failures; records warnings as metadata.
    """
    run_date = context.partition_key

    cmd = [
        sys.executable,
        str(ROOT / "data_quality" / "checks.py"),
        "--date", run_date,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    if result.returncode != 0:
        slack.send(f"Data quality CRITICAL failure for {run_date}", level="error")
        raise dg.Failure(
            description=f"Data quality checks failed for {run_date}",
            metadata={"output": MetadataValue.text(result.stdout[-1000:])},
        )

    # Parse check results from Snowflake for metadata
    conn = snowflake.get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT CHECK_NAME, METRIC, STATUS
        FROM ZIM_PROPERTY_DB.DATA_QUALITY.CHECK_RESULTS
        WHERE RUN_DATE = %s
        ORDER BY STATUS DESC
        """,
        (run_date,),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    checks_passed  = sum(1 for r in rows if r[2] == "PASS")
    checks_warned  = sum(1 for r in rows if r[2] == "WARNING")
    checks_failed  = sum(1 for r in rows if r[2] == "CRITICAL")

    if checks_warned:
        slack.send(f"{checks_warned} QC warnings on {run_date} — check Dagster UI", level="warn")

    return MaterializeResult(
        metadata={
            "checks_passed":  MetadataValue.int(checks_passed),
            "checks_warned":  MetadataValue.int(checks_warned),
            "checks_failed":  MetadataValue.int(checks_failed),
            "results_table":  MetadataValue.md(
                "| Check | Metric | Status |\n|---|---|---|\n" +
                "\n".join(f"| {r[0]} | {r[1]} | {r[2]} |" for r in rows)
            ),
        }
    )


# ─── Asset: Star schema ETL (STAGING → WAREHOUSE dim/fact tables) ────────────

@asset(
    name="star_schema_etl",
    group_name="snowflake",
    partitions_def=daily_partitions,
    deps=[zim_property_dbt_assets],
    description=(
        "Populates WAREHOUSE schema: inserts new DIM_LOCATION rows, refreshes "
        "DIM_SOURCE stats, and inserts new FACT_LISTINGS from STAGING. "
        "Run after dbt staging models complete."
    ),
    kinds={"snowflake"},
)
def star_schema_etl(
    context: AssetExecutionContext,
    snowflake: SnowflakeConfig,
) -> MaterializeResult:
    """
    Executes the ETL sections from 09_star_schema.sql:
      F1 — DIM_LOCATION upsert from STAGING
      F2 — DIM_SOURCE last_scraped_date refresh
      F3 — FACT_LISTINGS insert from STAGING (new listings only)
    """
    run_date = context.partition_key

    conn = snowflake.get_connection()
    conn.cursor().execute("USE DATABASE ZIM_PROPERTY_DB")

    # F1 — DIM_LOCATION
    conn.cursor().execute("""
        INSERT INTO WAREHOUSE.DIM_LOCATION (suburb_clean, city_clean, province, first_seen_date)
        SELECT DISTINCT
            cl.suburb_clean,
            cl.city_clean,
            CASE cl.city_clean
                WHEN 'Harare'      THEN 'Harare Metropolitan'
                WHEN 'Chitungwiza' THEN 'Harare Metropolitan'
                WHEN 'Ruwa'        THEN 'Harare Metropolitan'
                WHEN 'Norton'      THEN 'Mashonaland West'
                WHEN 'Bulawayo'    THEN 'Bulawayo Metropolitan'
                WHEN 'Mutare'      THEN 'Manicaland'
                WHEN 'Gweru'       THEN 'Midlands'
                WHEN 'Kwekwe'      THEN 'Midlands'
                WHEN 'Masvingo'    THEN 'Masvingo'
                WHEN 'Chinhoyi'    THEN 'Mashonaland West'
                WHEN 'Marondera'   THEN 'Mashonaland East'
                ELSE 'Other'
            END,
            MIN(cl.listing_date)
        FROM STAGING.CLEANED_PROPERTY_LISTINGS cl
        WHERE cl.city_clean IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM WAREHOUSE.DIM_LOCATION dl
              WHERE COALESCE(dl.suburb_clean,'') = COALESCE(cl.suburb_clean,'')
                AND dl.city_clean = cl.city_clean
          )
        GROUP BY 1,2,3
    """)

    # F2 — DIM_SOURCE refresh
    conn.cursor().execute("""
        UPDATE WAREHOUSE.DIM_SOURCE ds
        SET
            last_scraped_date  = latest.last_scraped,
            first_scraped_date = COALESCE(ds.first_scraped_date, latest.first_scraped)
        FROM (
            SELECT source,
                   MIN(scraped_at::DATE) AS first_scraped,
                   MAX(scraped_at::DATE) AS last_scraped
            FROM STAGING.CLEANED_PROPERTY_LISTINGS
            GROUP BY source
        ) latest
        WHERE ds.source = latest.source
    """)

    # F3 — FACT_LISTINGS insert
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO WAREHOUSE.FACT_LISTINGS (
            listing_id, date_key, listing_date_key, location_key,
            property_type_key, source_key, property_key, listing_type,
            currency_original, property_price_usd, property_price_zwl,
            exchange_rate_used, price_per_sqm_usd, property_size_sqm,
            stand_size_sqm, number_of_bedrooms, number_of_bathrooms,
            number_of_garages, feature_count, has_pool, has_borehole,
            has_solar, has_garage, image_count,
            data_quality_score, is_price_valid, is_location_valid, scraped_at
        )
        SELECT
            cl.listing_id,
            TO_NUMBER(TO_CHAR(cl.scraped_at::DATE,'YYYYMMDD')),
            CASE WHEN cl.listing_date IS NOT NULL
                 THEN TO_NUMBER(TO_CHAR(cl.listing_date,'YYYYMMDD')) END,
            dl.location_key, dpt.property_type_key, ds.source_key,
            NULL, cl.listing_type, cl.currency_original,
            cl.property_price_usd, cl.property_price_zwl, cl.exchange_rate_used,
            cl.price_per_sqm_usd, cl.property_size_sqm, cl.stand_size_sqm,
            cl.number_of_bedrooms, cl.number_of_bathrooms, cl.number_of_garages,
            cl.feature_count, cl.has_pool, cl.has_borehole, cl.has_solar, cl.has_garage,
            cl.image_count, cl.data_quality_score, cl.is_price_valid, cl.is_location_valid,
            cl.scraped_at
        FROM STAGING.CLEANED_PROPERTY_LISTINGS cl
        LEFT JOIN WAREHOUSE.DIM_LOCATION dl
            ON COALESCE(dl.suburb_clean,'') = COALESCE(cl.suburb_clean,'')
            AND dl.city_clean = cl.city_clean
        LEFT JOIN WAREHOUSE.DIM_PROPERTY_TYPE dpt ON dpt.property_type = cl.property_type
        LEFT JOIN WAREHOUSE.DIM_SOURCE ds ON ds.source = cl.source
        WHERE NOT EXISTS (
            SELECT 1 FROM WAREHOUSE.FACT_LISTINGS fl WHERE fl.listing_id = cl.listing_id
        )
    """)
    inserted = cursor.rowcount
    conn.commit()
    conn.close()

    context.log.info(f"star_schema_etl({run_date}): {inserted} new FACT rows")
    return MaterializeResult(metadata={
        "fact_rows_inserted": MetadataValue.int(inserted),
        "run_date":           MetadataValue.text(run_date),
    })


# ─── Asset: Property master matching engine ───────────────────────────────────

@asset(
    name="property_master_matching",
    group_name="deduplication",
    partitions_def=daily_partitions,
    deps=[star_schema_etl],
    description=(
        "Runs the Python matching engine (blocker + scorer + linker) to resolve "
        "listing_ids → PROPERTY_MASTER records. Writes links and creates new "
        "master records for unmatched properties. Back-fills FACT_LISTINGS.property_key."
    ),
    kinds={"python", "snowflake"},
)
def property_master_matching(
    context: AssetExecutionContext,
    slack: SlackNotifier,
) -> MaterializeResult:
    """
    Runs matching/linker.py for up to MATCHING_BATCH_SIZE unmatched listings.
    Safe to run daily — idempotent (skips already-linked listings).
    """
    from matching.linker import run_matching_pass

    batch_size = int(os.getenv("MATCHING_BATCH_SIZE", "1000"))
    context.log.info(f"Running matching pass: batch_size={batch_size}")

    stats = run_matching_pass(batch_size=batch_size)

    if stats.errors > 0:
        slack.send(
            f"Matching engine: {stats.errors} errors in {context.partition_key} pass",
            level="warn",
        )

    context.log.info(
        f"Matching complete: processed={stats.processed}, "
        f"new_masters={stats.new_masters}, auto_linked={stats.auto_linked}, "
        f"queued={stats.queued}, errors={stats.errors}"
    )
    return MaterializeResult(metadata={
        "processed":    MetadataValue.int(stats.processed),
        "new_masters":  MetadataValue.int(stats.new_masters),
        "auto_linked":  MetadataValue.int(stats.auto_linked),
        "queued":       MetadataValue.int(stats.queued),
        "errors":       MetadataValue.int(stats.errors),
    })


# ─── Asset: Price history sync ────────────────────────────────────────────────

@asset(
    name="price_history_sync",
    group_name="deduplication",
    partitions_def=daily_partitions,
    deps=[property_master_matching],
    description=(
        "Syncs int_price_changes dbt output into MASTER.PROPERTY_PRICE_HISTORY. "
        "Captures price changes detected by the dbt incremental model and writes "
        "one row per change event per property per day."
    ),
    kinds={"snowflake"},
)
def price_history_sync(
    context: AssetExecutionContext,
    snowflake: SnowflakeConfig,
) -> MaterializeResult:
    """
    Reads the int_price_changes dbt model output (in INTERMEDIATE schema)
    and inserts new price events into MASTER.PROPERTY_PRICE_HISTORY.
    Also triggers PRICE_ALERTS evaluation for any configured alert rules.
    """
    run_date = context.partition_key
    conn = snowflake.get_connection()
    conn.cursor().execute("USE DATABASE ZIM_PROPERTY_DB")

    # Insert new price events from dbt intermediate model
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO MASTER.PROPERTY_PRICE_HISTORY (
            property_id, listing_id, source, observed_date,
            price_usd, price_change_usd, price_change_pct,
            prev_price_usd, prev_observed_date, days_since_prev,
            change_type, listing_type, suburb, city, property_type, bedrooms,
            inserted_by
        )
        SELECT
            ipc.property_id,
            ipc.listing_id,
            ipc.source,
            ipc.observed_date,
            ipc.price_usd,
            ipc.price_change_usd,
            ipc.price_change_pct,
            ipc.prev_price_usd,
            ipc.prev_observed_date,
            ipc.days_since_prev,
            ipc.change_type,
            ipc.listing_type,
            ipc.suburb,
            ipc.city,
            ipc.property_type,
            ipc.bedrooms,
            'dagster:price_history_sync'
        FROM INTERMEDIATE.INT_PRICE_CHANGES ipc
        WHERE ipc.observed_date = %(run_date)s
          AND ipc.change_type != 'unchanged'
          AND NOT EXISTS (
              SELECT 1 FROM MASTER.PROPERTY_PRICE_HISTORY ph
              WHERE ph.property_id  = ipc.property_id
                AND ph.source       = ipc.source
                AND ph.observed_date = ipc.observed_date
          )
    """, {"run_date": run_date})
    events_inserted = cursor.rowcount

    # Check price alert rules — fire any that match today's changes
    cursor.execute("""
        INSERT INTO MASTER.PRICE_ALERTS (
            rule_id, price_event_id, property_id, property_address,
            city, suburb, previous_price_usd, new_price_usd,
            price_change_pct, listing_url
        )
        SELECT
            r.rule_id,
            ph.price_event_id,
            ph.property_id,
            pm.canonical_address,
            ph.city,
            ph.suburb,
            ph.prev_price_usd,
            ph.price_usd,
            ph.price_change_pct,
            lpl.listing_url
        FROM MASTER.PROPERTY_PRICE_HISTORY ph
        JOIN MASTER.PROPERTY_MASTER pm ON pm.property_id = ph.property_id
        LEFT JOIN MASTER.LISTING_PROPERTY_LINK lpl
            ON lpl.property_id = ph.property_id AND lpl.is_canonical = TRUE
        CROSS JOIN MASTER.PRICE_ALERT_RULES r
        WHERE ph.observed_date = %(run_date)s
          AND r.is_active = TRUE
          AND (
              -- Property-scoped rule
              (r.scope_type = 'property' AND r.property_id = ph.property_id)
              -- Suburb-scoped rule
              OR (r.scope_type = 'suburb'
                  AND LOWER(r.suburb) = LOWER(ph.suburb)
                  AND LOWER(r.city)   = LOWER(ph.city))
              -- City-scoped rule
              OR (r.scope_type = 'city' AND LOWER(r.city) = LOWER(ph.city))
          )
          -- Trigger condition match
          AND (
              (r.trigger_type = 'price_drop_pct'  AND ph.price_change_pct <= -r.threshold_value)
              OR (r.trigger_type = 'price_drop_abs'  AND ph.price_change_usd <= -r.threshold_value)
              OR (r.trigger_type = 'price_above'     AND ph.price_usd >= r.threshold_value)
              OR (r.trigger_type = 'price_below'     AND ph.price_usd <= r.threshold_value)
              OR (r.trigger_type = 'new_listing'     AND ph.change_type = 'first_listing')
          )
          -- Don't re-fire the same alert
          AND NOT EXISTS (
              SELECT 1 FROM MASTER.PRICE_ALERTS pa
              WHERE pa.rule_id    = r.rule_id
                AND pa.property_id = ph.property_id
                AND pa.triggered_at::DATE = %(run_date)s
          )
    """, {"run_date": run_date})
    alerts_fired = cursor.rowcount

    conn.commit()
    conn.close()

    context.log.info(
        f"price_history_sync({run_date}): "
        f"{events_inserted} price events, {alerts_fired} alerts fired"
    )
    return MaterializeResult(metadata={
        "price_events_inserted": MetadataValue.int(events_inserted),
        "alerts_fired":          MetadataValue.int(alerts_fired),
        "run_date":              MetadataValue.text(run_date),
    })


# ─── Job: full daily pipeline ─────────────────────────────────────────────────

zim_property_daily_job = define_asset_job(
    name="zim_property_daily_job",
    description="Full daily pipeline: scrape → load → dbt → star schema → matching → price history → quality",
    selection=AssetSelection.groups(
        "ingestion", "snowflake", "dbt_transforms", "deduplication", "quality"
    ),
    partitions_def=daily_partitions,
    tags={"pipeline": "zim_property", "environment": "production"},
)

# Scrape-only job (useful for reruns without hitting dbt)
scrape_and_load_job = define_asset_job(
    name="scrape_and_load_job",
    description="Scrape all sources and load to Snowflake only",
    selection=AssetSelection.groups("ingestion", "snowflake"),
    partitions_def=daily_partitions,
)

# dbt-only job (useful after manual data fixes)
dbt_refresh_job = define_asset_job(
    name="dbt_refresh_job",
    description="Re-run all dbt transformations and quality checks",
    selection=AssetSelection.groups("dbt_transforms", "quality"),
    partitions_def=daily_partitions,
)

# Historical backfill job — run once to harvest all Wayback Machine archives
# Trigger via Dagster UI > Jobs > historical_backfill_job > Launchpad
# Configure: site ("all"|"property"|"classifieds"), from_year ("2009")
historical_backfill_job = define_asset_job(
    name="historical_backfill_job",
    description=(
        "One-time backfill: scrapes ALL archived property listings from the Wayback Machine "
        "(Internet Archive) for property.co.zw and classifieds.co.zw back to 2009 "
        "(Zimbabwe USD adoption). Run this once, then load the output JSONL into Snowflake."
    ),
    selection=AssetSelection.assets(historical_property_listings),
    tags={"pipeline": "zim_property", "type": "historical_backfill"},
)


# ─── Schedule: run at 06:00 UTC daily ────────────────────────────────────────

@schedule(
    job=zim_property_daily_job,
    cron_schedule="0 6 * * *",   # 06:00 UTC = 08:00 CAT (Central Africa Time)
    execution_timezone="UTC",
    name="zim_property_daily_schedule",
    description="Runs the full ZimProperty pipeline once per day at 06:00 UTC",
)
def daily_schedule(context: dg.ScheduleEvaluationContext):
    """
    Emit a RunRequest for yesterday's partition (scrape runs overnight;
    we process the completed day at 06:00 the following morning).
    """
    scheduled_date = context.scheduled_execution_time.date() - timedelta(days=1)
    partition_key = scheduled_date.isoformat()
    return dg.RunRequest(
        run_key=partition_key,
        partition_key=partition_key,
        tags={"scheduled": "true", "partition_date": partition_key},
    )


# ─── Sensor: auto-materialise when new JSONL files land ───────────────────────

@sensor(
    job=scrape_and_load_job,
    name="new_jsonl_sensor",
    description="Watches data/ for new JSONL files and triggers a load run",
    minimum_interval_seconds=300,  # check every 5 minutes
)
def new_jsonl_sensor(context: SensorEvaluationContext) -> Generator:
    """
    Fires a run when a new JSONL file appears in data/ that hasn't been loaded yet.
    Useful for ad-hoc spider runs triggered outside the schedule.
    """
    data_dir = ROOT / "data"
    if not data_dir.exists():
        return

    cursor = json.loads(context.cursor or "{}")
    seen_files: set = set(cursor.get("seen", []))

    new_files = []
    for path in sorted(data_dir.glob("*.jsonl")):
        key = str(path)
        if key not in seen_files and path.stat().st_size > 0:
            new_files.append(path)

    if not new_files:
        return

    # Group new files by date and emit one run per date
    dates_seen: dict[str, list] = {}
    for f in new_files:
        # Filename format: spider_YYYY-MM-DD.jsonl
        parts = f.stem.split("_")
        date_str = next((p for p in parts if len(p) == 10 and p[4] == "-"), None)
        if date_str:
            dates_seen.setdefault(date_str, []).append(f)

    for date_str, files in dates_seen.items():
        context.log.info(f"Sensor: new files for {date_str}: {[f.name for f in files]}")
        yield RunRequest(
            run_key=f"sensor_{date_str}_{len(files)}files",
            partition_key=date_str,
            tags={"trigger": "file_sensor", "partition_date": date_str},
        )
        for f in files:
            seen_files.add(str(f))

    context.update_cursor(json.dumps({"seen": list(seen_files)[-500:]}))  # cap cursor size


# ─── Definitions (single entry point for `dagster dev`) ───────────────────────

defs = dg.Definitions(
    assets=[
        scraped_property_listings,
        historical_property_listings,
        snowflake_raw_listings,
        zim_property_dbt_assets,
        star_schema_etl,
        property_master_matching,
        price_history_sync,
        data_quality_results,
    ],
    jobs=[
        zim_property_daily_job,
        scrape_and_load_job,
        dbt_refresh_job,
        historical_backfill_job,
        define_asset_job(
            name="matching_only_job",
            description="Re-run matching + price history sync without scraping (useful after manual data corrections)",
            selection=AssetSelection.assets(
                star_schema_etl, property_master_matching, price_history_sync
            ),
            partitions_def=daily_partitions,
        ),
    ],
    schedules=[daily_schedule],
    sensors=[new_jsonl_sensor],
    resources={
        "snowflake": SnowflakeConfig(),
        "slack":     SlackNotifier(),
        "dbt":       DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
)
