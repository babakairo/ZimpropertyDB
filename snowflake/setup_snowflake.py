"""
setup_snowflake.py — One-time Snowflake setup script.

Reads all SQL files in this directory in order and executes them.

Usage:
    python snowflake/setup_snowflake.py
    python snowflake/setup_snowflake.py --only 02_raw_tables.sql
"""
import os
import re
import sys
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_connection():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )


def split_statements(sql: str) -> list[str]:
    """Split a SQL file into individual statements, skipping blank lines and comments."""
    statements = []
    current: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        current.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
    return statements


def run_file(cursor, sql_path: Path):
    logger.info(f"Running {sql_path.name} ...")
    sql = sql_path.read_text(encoding="utf-8")
    statements = split_statements(sql)
    for i, stmt in enumerate(statements, 1):
        try:
            cursor.execute(stmt)
            logger.info(f"  [{i}/{len(statements)}] OK")
        except Exception as e:
            logger.warning(f"  [{i}/{len(statements)}] SKIPPED: {e}")


def main():
    parser = argparse.ArgumentParser(description="Set up Snowflake schemas for ZimProperty platform")
    parser.add_argument("--only", help="Run only this SQL file (filename only)", default=None)
    args = parser.parse_args()

    sql_dir = Path(__file__).parent
    sql_files = sorted(sql_dir.glob("*.sql"))

    if args.only:
        sql_files = [f for f in sql_files if f.name == args.only]
        if not sql_files:
            logger.error(f"File not found: {args.only}")
            sys.exit(1)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        for sql_file in sql_files:
            run_file(cursor, sql_file)
        conn.commit()
        logger.info("Setup complete.")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        conn.rollback()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
