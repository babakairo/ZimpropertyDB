"""
run_new_sources.py -- Scrape all newly added sources and load to Snowflake.

Run:
    python run_new_sources.py
    python run_new_sources.py --load-only     # skip scraping, just load existing files
    python run_new_sources.py --scrape-only   # skip loading
"""
import os
import sys
import glob
import argparse
import subprocess
import logging
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent

# Use .venv Python (Python 3.12, has all project dependencies)
VENV_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
PYTHON = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

(ROOT / "data").mkdir(exist_ok=True)
(ROOT / "logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            ROOT / "logs" / f"new_sources_{datetime.now().strftime('%Y%m%dT%H%M%S')}.log",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger("new_sources")

RUN_TAG = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Environment for subprocesses: add ROOT to PYTHONPATH so 'scraper' package is importable
SCRAPY_ENV = {
    **os.environ,
    "PYTHONPATH": str(ROOT),
    "PYTHONIOENCODING": "utf-8",
}

# ── NEW spiders only (added 2026-03-13) ───────────────────────────────────────
NEW_SPIDERS = [
    # PropData platform -- new sites
    {"name": "propdata_zw",       "args": ["-a", "site=realtorville"],              "timeout": 600},
    {"name": "propdata_zw",       "args": ["-a", "site=zimproperties"],             "timeout": 600},
    {"name": "propdata_zw",       "args": ["-a", "site=faranani"],                  "timeout": 600},
    {"name": "propdata_zw",       "args": ["-a", "site=harare_properties"],         "timeout": 600},
    # Agency sites -- batch 1 (added 2026-03-13)
    {"name": "zim_agent",         "args": ["-a", "agency=fine_country_zw"],         "timeout": 300},
    {"name": "zim_agent",         "args": ["-a", "agency=rawson_zw"],               "timeout": 300},
    {"name": "zim_agent",         "args": ["-a", "agency=century21_zw"],            "timeout": 300},
    {"name": "zim_agent",         "args": ["-a", "agency=integrated_zw"],           "timeout": 300},
    # Secondary portals -- batch 1
    {"name": "portal_zw",         "args": ["-a", "site=propsearch"],                "timeout": 600},
    {"name": "portal_zw",         "args": ["-a", "site=stands"],                    "timeout": 600},
    {"name": "portal_zw",         "args": ["-a", "site=shonahome"],                 "timeout": 600},
    {"name": "portal_zw",         "args": ["-a", "site=privateproperty"],           "timeout": 600},
    # Auctions
    {"name": "abcauctions_co_zw", "args": [],                                       "timeout": 300},

    # ── Batch 2: additional sources (added 2026-03-13) ────────────────────────
    # Agency sites
    {"name": "zim_agent", "args": ["-a", "agency=pam_golding_zimbabwe_zw"],         "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=rawson_zw_local"],                 "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=robert_root_zw"],                  "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=stonebridge_zw"],                  "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=john_pocock_zw"],                  "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=trevor_dollar_zw"],                "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=newage_properties_zw"],            "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=remax_zw"],                        "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=bridges_realestate_zw"],           "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=terezim_zw"],                      "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=legacy_realestate_zw"],            "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=exodus_zw"],                       "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=leengate_zw"],                     "timeout": 300},
    {"name": "zim_agent", "args": ["-a", "agency=lucile_realestate_zw"],            "timeout": 300},
    # Major portal + developer/listed companies
    {"name": "portal_zw", "args": ["-a", "site=property24"],                        "timeout": 600},
    {"name": "portal_zw", "args": ["-a", "site=westprop"],                          "timeout": 600},
    {"name": "portal_zw", "args": ["-a", "site=zimre"],                             "timeout": 600},
    {"name": "portal_zw", "args": ["-a", "site=mashonaland"],                       "timeout": 600},
]


def run_spiders() -> list[str]:
    """Run each new spider and return list of output JSONL paths that have data."""
    output_files = []
    results = []

    for spider in NEW_SPIDERS:
        tag = (
            spider["name"]
            if not spider["args"]
            else f"{spider['name']}_{spider['args'][-1]}"
        )
        out_file = ROOT / "data" / f"{tag}_{RUN_TAG}.jsonl"
        log_file = ROOT / "logs" / f"{tag}_{RUN_TAG}.log"

        cmd = [
            PYTHON, "-m", "scrapy", "crawl", spider["name"],
            "-o", str(out_file),
            "--logfile", str(log_file),
            "-s", "LOG_LEVEL=WARNING",
        ] + spider["args"]

        log.info(f"Starting: {tag}")
        try:
            result = subprocess.run(
                cmd,
                cwd=ROOT / "scraper",
                env=SCRAPY_ENV,
                timeout=spider["timeout"],
            )
            exit_code = result.returncode
        except subprocess.TimeoutExpired:
            log.warning(f"  TIMEOUT: {tag} after {spider['timeout']}s")
            exit_code = -1

        count = 0
        if out_file.exists():
            with open(out_file, encoding="utf-8") as f:
                count = sum(1 for line in f if line.strip())

        status = "OK" if exit_code == 0 else "FAIL"
        log.info(f"  [{status}] {tag}: {count:,} records (exit={exit_code})")
        results.append({"spider": tag, "records": count, "exit": exit_code})

        if count > 0:
            output_files.append(str(out_file))

    log.info("---- Spider summary ----")
    for r in results:
        log.info(f"  {r['spider']:40s}  {r['records']:>5,} records")
    log.info(f"  {'TOTAL':40s}  {sum(r['records'] for r in results):>5,} records")

    return output_files


def load_to_snowflake(files: list[str]) -> None:
    """Call the existing loader.py on all collected JSONL files."""
    if not files:
        log.warning("No output files with data -- nothing to load to Snowflake.")
        return

    loader = ROOT / "pipelines" / "loader.py"
    cmd = [PYTHON, str(loader), "--input"] + files
    log.info(f"Loading {len(files)} file(s) to Snowflake ...")
    log.info(f"  Files: {', '.join(Path(f).name for f in files)}")

    result = subprocess.run(cmd, cwd=ROOT, env=SCRAPY_ENV)
    if result.returncode == 0:
        log.info("Snowflake load complete.")
    else:
        log.error(f"Loader exited with code {result.returncode}.")
        sys.exit(result.returncode)


def main():
    log.info(f"Using Python: {PYTHON}")
    log.info(f"Run tag: {RUN_TAG}")

    parser = argparse.ArgumentParser()
    parser.add_argument("--load-only",   action="store_true", help="Skip scraping")
    parser.add_argument("--scrape-only", action="store_true", help="Skip Snowflake load")
    args = parser.parse_args()

    if args.load_only:
        date_prefix = RUN_TAG[:8]
        files = sorted(glob.glob(str(ROOT / "data" / f"*_{date_prefix}*.jsonl")))
        files = [f for f in files if Path(f).stat().st_size > 0]
        log.info(f"load-only mode: found {len(files)} files to load")
    else:
        files = run_spiders()

    if not args.scrape_only:
        load_to_snowflake(files)


if __name__ == "__main__":
    main()
