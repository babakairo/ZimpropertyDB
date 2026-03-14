"""
Persistent seen-URL store backed by SQLite.

Used by all spiders to implement incremental scraping:
  - Skip detail pages already scraped in a previous run
  - Stop paginating when an entire listing page is already known

The DB lives at data/seen_urls.sqlite (auto-created on first run).
One row per unique listing URL; tracks first and last seen timestamps.
"""
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

_DEFAULT_DB = Path(__file__).parent.parent.parent / "data" / "seen_urls.sqlite"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SeenUrlsStore:
    """Thread-safe SQLite store for scraped listing URLs."""

    def __init__(self, db_path: Path = _DEFAULT_DB):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db_path = str(db_path)
        self._local = threading.local()
        self._lock = threading.Lock()
        # Initialise schema
        con = self._conn()
        con.execute("""
            CREATE TABLE IF NOT EXISTS seen_urls (
                url        TEXT PRIMARY KEY,
                first_seen TEXT NOT NULL,
                last_seen  TEXT NOT NULL
            )
        """)
        con.commit()

    def _conn(self) -> sqlite3.Connection:
        """Return a per-thread connection (SQLite is not thread-safe)."""
        if not getattr(self._local, "conn", None):
            self._local.conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

    def is_seen(self, url: str) -> bool:
        row = self._conn().execute(
            "SELECT 1 FROM seen_urls WHERE url = ?", (url,)
        ).fetchone()
        return row is not None

    def mark_seen(self, url: str) -> None:
        now = _utc_now()
        with self._lock:
            self._conn().execute(
                """
                INSERT INTO seen_urls (url, first_seen, last_seen)
                VALUES (?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET last_seen = excluded.last_seen
                """,
                (url, now, now),
            )
            self._conn().commit()

    def filter_new(self, urls: list[str]) -> tuple[list[str], bool]:
        """
        Return (new_urls, all_seen).

        new_urls  — subset of urls not yet in the store
        all_seen  — True when EVERY url in the batch was already seen
                    (caller should stop paginating)
        """
        new = [u for u in urls if not self.is_seen(u)]
        all_seen = len(new) == 0 and len(urls) > 0
        return new, all_seen

    def count(self) -> int:
        row = self._conn().execute("SELECT COUNT(*) FROM seen_urls").fetchone()
        return row[0] if row else 0
