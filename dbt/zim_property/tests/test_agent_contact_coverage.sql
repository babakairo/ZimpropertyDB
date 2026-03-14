-- Alert when recent cleaned listings lose too much agent/contact coverage.
WITH stats AS (
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN has_agent_name = FALSE THEN 1 ELSE 0 END) AS missing_agent_name,
        SUM(CASE WHEN has_agent_contact = FALSE THEN 1 ELSE 0 END) AS missing_agent_contact
    FROM {{ ref('int_cleaned_property_listings') }}
    WHERE scraped_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
)
SELECT
    total,
    missing_agent_name,
    missing_agent_contact,
    ROUND(missing_agent_name / NULLIF(total, 0) * 100, 1) AS pct_missing_agent_name,
    ROUND(missing_agent_contact / NULLIF(total, 0) * 100, 1) AS pct_missing_agent_contact
FROM stats
WHERE missing_agent_name / NULLIF(total, 0) > 0.35
   OR missing_agent_contact / NULLIF(total, 0) > 0.35
