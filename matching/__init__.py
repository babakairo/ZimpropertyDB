"""
matching/
Property deduplication and entity-resolution engine.

Three-pass pipeline:
  1. blocker.py   — fast candidate generation (address hash + geohash)
  2. scorer.py    — weighted multi-signal similarity score (0.0–1.0)
  3. linker.py    — decision + write to MASTER.LISTING_PROPERTY_LINK

Usage (standalone):
    python -m matching.linker --batch-size 500

Usage (Dagster):
    from matching.linker import run_matching_pass
    run_matching_pass(batch_size=500)
"""
