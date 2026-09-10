CREATE OR REPLACE VIEW market_intel_silver.view_dla_contract_history_part_references AS
WITH latest_snapshots AS (
    SELECT
        award_year,
        MAX(source_snapshot_date) AS source_snapshot_date
    FROM market_intel_silver.fact_contract_history_part_references_v2
    GROUP BY 1
),
legacy_references AS (
    SELECT
        LPAD(TRIM(CAST(h.niin AS VARCHAR)), 9, '0') AS niin,
        CASE
            WHEN REGEXP_LIKE(TRIM(CAST(h.fsc AS VARCHAR)), '^[0-9]{4}$')
             AND REGEXP_LIKE(LPAD(TRIM(CAST(h.niin AS VARCHAR)), 9, '0'), '^[0-9]{9}$')
            THEN CONCAT(
                TRIM(CAST(h.fsc AS VARCHAR)),
                LPAD(TRIM(CAST(h.niin AS VARCHAR)), 9, '0')
            )
        END AS nsn,
        UPPER(TRIM(CAST(h.cage AS VARCHAR))) AS cage,
        TRIM(CAST(h.part_number AS VARCHAR)) AS part_number,
        MAX_BY(h.item_name, h.award_date) AS item_name,
        MIN(h.award_date) AS first_observed_date,
        MAX(h.award_date) AS last_observed_date,
        COUNT(*) AS source_reference_rows,
        'DLA_CONTRACT_HISTORY_AWARD_REFERENCE' AS relationship_source
    FROM market_intel_silver.fact_contract_history h
    WHERE NOT EXISTS (
            SELECT 1
            FROM latest_snapshots s
            WHERE TRY_CAST(h.award_year AS INTEGER) = s.award_year
        )
      AND h.niin IS NOT NULL
      AND h.cage IS NOT NULL
      AND NULLIF(TRIM(CAST(h.part_number AS VARCHAR)), '') IS NOT NULL
    GROUP BY 1, 2, 3, 4
),
monthly_references AS (
    SELECT
        h.niin,
        CASE
            WHEN REGEXP_LIKE(TRIM(CAST(h.fsc AS VARCHAR)), '^[0-9]{4}$')
             AND REGEXP_LIKE(LPAD(TRIM(CAST(h.niin AS VARCHAR)), 9, '0'), '^[0-9]{9}$')
            THEN CONCAT(
                TRIM(CAST(h.fsc AS VARCHAR)),
                LPAD(TRIM(CAST(h.niin AS VARCHAR)), 9, '0')
            )
        END AS nsn,
        h.cage,
        h.part_number,
        MAX_BY(h.item_name, h.award_date) AS item_name,
        MIN(h.award_date) AS first_observed_date,
        MAX(h.award_date) AS last_observed_date,
        COUNT(*) AS source_reference_rows,
        'DLA_CONTRACT_HISTORY_AWARD_REFERENCE' AS relationship_source
    FROM market_intel_silver.fact_contract_history_part_references_v2 h
    INNER JOIN latest_snapshots s
        ON h.award_year = s.award_year
       AND h.source_snapshot_date = s.source_snapshot_date
    WHERE NULLIF(TRIM(h.part_number), '') IS NOT NULL
    GROUP BY 1, 2, 3, 4
)
SELECT * FROM legacy_references
UNION ALL
SELECT * FROM monthly_references;
