#!/usr/bin/env bash
set -euo pipefail

echo "=== dbt transformations starting at $(date -u) ==="
cd /app/dbt/zim_property

echo "--- dbt deps ---"
dbt deps --profiles-dir .

echo "--- dbt run ---"
dbt run --profiles-dir . --target "${DBT_TARGET:-prod}"

echo "--- dbt test ---"
dbt test --profiles-dir . --target "${DBT_TARGET:-prod}"

echo "--- dbt docs generate ---"
dbt docs generate --profiles-dir . --target "${DBT_TARGET:-prod}"

echo "=== dbt finished at $(date -u) ==="
