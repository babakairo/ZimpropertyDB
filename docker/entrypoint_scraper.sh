#!/usr/bin/env bash
set -euo pipefail

echo "=== ZimProperty Scraper starting at $(date -u) ==="

RUN_DATE=$(date -u +%Y-%m-%d)
SPIDERS=("property_co_zw" "classifieds_co_zw")
FAILED_SPIDERS=()

# Run each spider; continue even if one fails
for SPIDER in "${SPIDERS[@]}"; do
    echo "--- Running spider: $SPIDER ---"
    cd /app/scraper
    if python -m scrapy crawl "$SPIDER" \
        --logfile "/app/logs/${SPIDER}_${RUN_DATE}.log" \
        -o "/app/data/${SPIDER}_${RUN_DATE}.jsonl"; then
        echo "✓ $SPIDER completed"
    else
        echo "✗ $SPIDER FAILED"
        FAILED_SPIDERS+=("$SPIDER")
    fi
    cd /app
done

# Run agent spiders
for AGENCY in "knight_frank_zw" "pam_golding_zw" "api_zw"; do
    echo "--- Running zim_agent for: $AGENCY ---"
    cd /app/scraper
    if python -m scrapy crawl zim_agent -a "agency=$AGENCY" \
        --logfile "/app/logs/zim_agent_${AGENCY}_${RUN_DATE}.log" \
        -o "/app/data/zim_agent_${AGENCY}_${RUN_DATE}.jsonl"; then
        echo "✓ zim_agent/$AGENCY completed"
    else
        echo "✗ zim_agent/$AGENCY FAILED"
    fi
    cd /app
done

# Load all today's files to Snowflake
echo "--- Loading to Snowflake ---"
python /app/pipelines/loader.py \
    --input "/app/data/*_${RUN_DATE}*.jsonl" \
    --batch-size "${SNOWFLAKE_BATCH_SIZE:-500}"

echo "=== Scraper finished at $(date -u) ==="

if [ ${#FAILED_SPIDERS[@]} -gt 0 ]; then
    echo "WARNING: Failed spiders: ${FAILED_SPIDERS[*]}"
    exit 1
fi
