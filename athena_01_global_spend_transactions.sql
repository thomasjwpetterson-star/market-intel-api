CREATE OR REPLACE VIEW "market_intel_gold"."global_spend_transactions" AS
WITH vendor_sites AS (
    SELECT
        UPPER(REGEXP_REPLACE(cage_code, '[^A-Za-z0-9]', '')) AS cage_norm,
        vendor_name,
        city,
        state,
        CAST(latitude AS VARCHAR) AS latitude,
        CAST(longitude AS VARCHAR) AS longitude,
        location_quality
    FROM "market_intel_gold"."view_vendor_sites_hybrid"
    WHERE cage_code IS NOT NULL
),
wsdc_by_niin AS (
    SELECT
        TRIM(niin) AS niin,
        ARRAY_JOIN(
            ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(UPPER(TRIM(wsdc_code))) FILTER (
                WHERE wsdc_code IS NOT NULL AND TRIM(wsdc_code) <> ''
            ))),
            ' | '
        ) AS wsdc_codes,
        ARRAY_JOIN(
            ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(TRIM(weapon_system_name)) FILTER (
                WHERE weapon_system_name IS NOT NULL
                  AND TRIM(weapon_system_name) <> ''
                  AND UPPER(TRIM(weapon_system_name)) NOT IN ('NONE', 'N/A', 'UNKNOWN')
            ))),
            ' | '
        ) AS wsdc_systems,
        ARBITRARY(TRIM(wsdc_code)) AS representative_wsdc_code
    FROM "market_intel_silver"."ref_wsdc"
    WHERE niin IS NOT NULL
      AND TRIM(niin) <> ''
      AND wsdc_code IS NOT NULL
      AND TRIM(wsdc_code) <> ''
    GROUP BY 1
),
platform_by_wsdc AS (
    SELECT
        TRIM(wsdc_code_ref) AS wsdc_code_ref,
        ARBITRARY(raw_input_name) AS raw_input_name
    FROM "market_intel_silver"."ref_platform_map"
    WHERE wsdc_code_ref IS NOT NULL
      AND TRIM(wsdc_code_ref) <> ''
      AND raw_input_name IS NOT NULL
      AND TRIM(raw_input_name) <> ''
    GROUP BY 1
),
usaspending_ranked AS (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY t.contract_transaction_unique_key
            ORDER BY t.last_modified_date DESC, t.action_date DESC
        ) AS source_row_rank
    FROM "market_intel_silver"."dataset_prime_contracts" t
),
action_unit_pricing AS (
    SELECT
        transaction_key,
        unit_price_item_name,
        unit_price_nsn,
        unit_price_quantity,
        unit_price_quantity_unit,
        unit_price_usd,
        unit_price_basis,
        unit_price_status,
        unit_price_record_count,
        wsdc_systems,
        wsdc_codes
    FROM "market_intel_silver"."v_data_explorer_unit_pricing"
),
usa_award_identity AS (
    SELECT
        UPPER(TRIM(CAST(award_id_piid AS VARCHAR))) AS contract_id,
        MIN(contract_award_unique_key) AS award_key
    FROM usaspending_ranked
    WHERE source_row_rank = 1
      AND award_id_piid IS NOT NULL
      AND contract_award_unique_key IS NOT NULL
    GROUP BY 1
    HAVING COUNT(DISTINCT contract_award_unique_key) = 1
),
dla_single_award_nsn AS (
    SELECT
        UPPER(TRIM(CAST(contract_number AS VARCHAR))) AS contract_id,
        MIN(LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0')) AS niin,
        MIN(
            CASE
                WHEN REGEXP_LIKE(TRIM(CAST(fsc AS VARCHAR)), '^[0-9]{4}$')
                 AND REGEXP_LIKE(LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0'), '^[0-9]{9}$')
                THEN CONCAT(
                    TRIM(CAST(fsc AS VARCHAR)),
                    LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0')
                )
            END
        ) AS nsn
    FROM "market_intel_silver"."view_dla_contract_history_financial"
    WHERE contract_number IS NOT NULL
      AND niin IS NOT NULL
    GROUP BY 1
    HAVING COUNT(DISTINCT LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0')) = 1
       AND COUNT(DISTINCT CASE
            WHEN REGEXP_LIKE(TRIM(CAST(fsc AS VARCHAR)), '^[0-9]{4}$')
             AND REGEXP_LIKE(LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0'), '^[0-9]{9}$')
            THEN CONCAT(
                TRIM(CAST(fsc AS VARCHAR)),
                LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0')
            )
       END) = 1
),
safe_award_nsn AS (
    SELECT
        u.award_key,
        d.contract_id,
        d.niin,
        d.nsn
    FROM dla_single_award_nsn d
    INNER JOIN usa_award_identity u
        ON d.contract_id = u.contract_id
),
usaspending AS (
    SELECT
        'USA_SPENDING' AS source_system,
        t.award_id_piid AS contract_id,
        UPPER(REGEXP_REPLACE(t.cage_code, '[^A-Za-z0-9]', '')) AS vendor_cage,
        COALESCE(NULLIF(t.recipient_name_raw, ''), t.recipient_name) AS vendor_name_raw,
        t.awarding_agency_name AS parent_agency,
        t.awarding_sub_agency_name AS sub_agency,
        t.product_or_service_code AS psc,
        t.naics_code,
        t.action_date,
        TRY_CAST(t.action_date_fiscal_year AS INTEGER) AS year,
        TRY_CAST(t.federal_action_obligation AS DOUBLE) AS spend_amount,
        TRY_CAST(up.unit_price_usd AS DOUBLE) AS unit_price_usd,
        TRY_CAST(up.unit_price_quantity AS DOUBLE) AS unit_price_quantity,
        up.unit_price_quantity_unit,
        up.unit_price_basis,
        CAST(up.unit_price_status AS VARCHAR(64)) AS unit_price_status,
        up.unit_price_record_count,
        up.unit_price_item_name,
        up.wsdc_systems,
        up.wsdc_codes,
        COALESCE(up.unit_price_nsn, b.nsn) AS nsn,
        b.niin,
        CAST(NULL AS VARCHAR) AS part_number,
        COALESCE(NULLIF(t.transaction_description, ''), t.prime_award_base_transaction_description) AS description,
        CAST(NULL AS VARCHAR) AS standardized_vendor_name,
        t.recipient_city_name AS city,
        t.recipient_state_name AS state,
        t.recipient_country_name AS country,
        vs.latitude,
        vs.longitude,
        vs.location_quality,
        COALESCE(
            NULLIF(t.dod_acquisition_program_description, ''),
            NULLIF(t.major_program, ''),
            NULLIF(t.program_acronym, ''),
            'NONE'
        ) AS join_key_mapping,
        NULLIF(t.prime_award_base_transaction_description, '') AS base_award_description,
        NULLIF(t.transaction_description, '') AS action_description,
        NULLIF(t.primary_place_of_performance_city_name, '') AS place_of_performance_city,
        COALESCE(
            NULLIF(t.primary_place_of_performance_state_name, ''),
            NULLIF(t.primary_place_of_performance_state_code, '')
        ) AS place_of_performance_state,
        COALESCE(
            NULLIF(t.primary_place_of_performance_country_name, ''),
            NULLIF(t.primary_place_of_performance_country_code, '')
        ) AS place_of_performance_country,
        NULLIF(t.primary_place_of_performance_zip_4, '') AS place_of_performance_zip,
        t.contract_award_unique_key AS award_key,
        t.contract_transaction_unique_key AS transaction_key,
        t.modification_number,
        t.awarding_agency_code,
        CAST(NULL AS VARCHAR) AS po_number,
        CAST(NULL AS VARCHAR) AS po_item_number,
        CAST(1 AS BIGINT) AS source_reference_rows,
        CAST(NULL AS BIGINT) AS reference_part_number_count,
        CAST(NULL AS VARCHAR) AS part_number_reference_status,
        CASE WHEN b.nsn IS NOT NULL THEN 'DLA_CONTRACT_HISTORY' END AS nsn_source_system,
        CASE WHEN b.nsn IS NOT NULL THEN 'CROSS_SOURCE_PIID_SINGLE_NIIN' END AS nsn_derivation_method,
        CASE WHEN b.nsn IS NOT NULL THEN 'RESOLVED' ELSE 'NOT_ENRICHED' END AS nsn_resolution_status,
        CAST(NULL AS VARCHAR) AS associated_platform_families,
        CAST(0 AS INTEGER) AS associated_platform_count,
        'NOT_APPLICABLE' AS item_platform_attribution_status,
        CAST(NULL AS VARCHAR) AS item_unique_platform_family,
        CAST(NULL AS VARCHAR) AS platform_attribution_source
    FROM usaspending_ranked t
    LEFT JOIN vendor_sites vs
        ON UPPER(REGEXP_REPLACE(t.cage_code, '[^A-Za-z0-9]', '')) = vs.cage_norm
    LEFT JOIN safe_award_nsn b
        ON UPPER(TRIM(CAST(t.award_id_piid AS VARCHAR))) = b.contract_id
       AND t.contract_award_unique_key = b.award_key
    LEFT JOIN action_unit_pricing up
        ON t.contract_transaction_unique_key = up.transaction_key
    WHERE t.source_row_rank = 1
),
usa_dla_contracts AS (
    SELECT DISTINCT award_id_piid AS contract_id
    FROM usaspending_ranked
    WHERE source_row_rank = 1
      AND award_id_piid IS NOT NULL
      AND (
          UPPER(COALESCE(awarding_sub_agency_name, '')) LIKE '%DEFENSE LOGISTICS AGENCY%'
          OR UPPER(COALESCE(awarding_agency_name, '')) LIKE '%DEFENSE LOGISTICS AGENCY%'
      )
),
dla AS (
    SELECT
        'DLA' AS source_system,
        h.contract_number AS contract_id,
        UPPER(REGEXP_REPLACE(h.cage, '[^A-Za-z0-9]', '')) AS vendor_cage,
        vs.vendor_name AS vendor_name_raw,
        'DEPARTMENT OF DEFENSE' AS parent_agency,
        'DEFENSE LOGISTICS AGENCY' AS sub_agency,
        CAST(NULL AS VARCHAR) AS psc,
        CAST(NULL AS VARCHAR) AS naics_code,
        CAST(h.award_date AS VARCHAR) AS action_date,
        CAST(YEAR(h.award_date) + IF(MONTH(h.award_date) >= 10, 1, 0) AS INTEGER) AS year,
        TRY_CAST(h.netprice AS DOUBLE) * TRY_CAST(h.order_qty AS DOUBLE) AS spend_amount,
        CASE
            WHEN TRY_CAST(h.netprice AS DOUBLE) > 0
             AND TRY_CAST(h.netprice AS DOUBLE) < 99999999
             AND TRY_CAST(h.order_qty AS DOUBLE) > 0
            THEN TRY_CAST(h.netprice AS DOUBLE)
        END AS unit_price_usd,
        CASE
            WHEN TRY_CAST(h.netprice AS DOUBLE) > 0
             AND TRY_CAST(h.netprice AS DOUBLE) < 99999999
             AND TRY_CAST(h.order_qty AS DOUBLE) > 0
            THEN TRY_CAST(h.order_qty AS DOUBLE)
        END AS unit_price_quantity,
        CASE
            WHEN TRY_CAST(h.netprice AS DOUBLE) > 0
             AND TRY_CAST(h.netprice AS DOUBLE) < 99999999
             AND TRY_CAST(h.order_qty AS DOUBLE) > 0
            THEN CAST(h.unit AS VARCHAR)
        END AS unit_price_quantity_unit,
        CASE
            WHEN TRY_CAST(h.netprice AS DOUBLE) > 0
             AND TRY_CAST(h.netprice AS DOUBLE) < 99999999
             AND TRY_CAST(h.order_qty AS DOUBLE) > 0
            THEN 'DLA_CONTRACT_HISTORY_NET_PRICE'
        END AS unit_price_basis,
        CAST(
            CASE
                WHEN TRY_CAST(h.netprice AS DOUBLE) > 0
                 AND TRY_CAST(h.netprice AS DOUBLE) < 99999999
                 AND TRY_CAST(h.order_qty AS DOUBLE) > 0
                THEN 'OBSERVED_UNIT_PRICE'
            END AS VARCHAR(64)
        ) AS unit_price_status,
        CASE
            WHEN TRY_CAST(h.netprice AS DOUBLE) > 0
             AND TRY_CAST(h.netprice AS DOUBLE) < 99999999
             AND TRY_CAST(h.order_qty AS DOUBLE) > 0
            THEN 1 ELSE 0
        END AS unit_price_record_count,
        CASE
            WHEN TRY_CAST(h.netprice AS DOUBLE) > 0
             AND TRY_CAST(h.netprice AS DOUBLE) < 99999999
             AND TRY_CAST(h.order_qty AS DOUBLE) > 0
            THEN h.item_name
        END AS unit_price_item_name,
        w.wsdc_systems,
        w.wsdc_codes,
        CASE
            WHEN REGEXP_LIKE(TRIM(h.fsc), '^[0-9]{4}$')
             AND REGEXP_LIKE(TRIM(h.niin), '^[0-9]{9}$')
            THEN CONCAT(TRIM(h.fsc), TRIM(h.niin))
            ELSE NULL
        END AS nsn,
        TRIM(h.niin) AS niin,
        h.part_number,
        h.item_name AS description,
        vs.vendor_name AS standardized_vendor_name,
        vs.city,
        vs.state,
        'US' AS country,
        vs.latitude,
        vs.longitude,
        vs.location_quality,
        COALESCE(NULLIF(pw.raw_input_name, ''), NULLIF(h.item_name, ''), 'NONE') AS join_key_mapping,
        h.item_name AS base_award_description,
        h.item_name AS action_description,
        CAST(NULL AS VARCHAR) AS place_of_performance_city,
        CAST(NULL AS VARCHAR) AS place_of_performance_state,
        CAST(NULL AS VARCHAR) AS place_of_performance_country,
        CAST(NULL AS VARCHAR) AS place_of_performance_zip,
        CONCAT(
            'DLA_AWARD|',
            COALESCE(UPPER(TRIM(CAST(h.contract_number AS VARCHAR))), '<NULL>'),
            '|CAGE|',
            COALESCE(UPPER(TRIM(CAST(h.cage AS VARCHAR))), '<NULL>')
        ) AS award_key,
        CONCAT(
            'DLA_LINE|',
            COALESCE(UPPER(TRIM(CAST(h.contract_number AS VARCHAR))), '<NULL>'), '|',
            COALESCE(TRIM(CAST(h.po_num AS VARCHAR)), '<NULL>'), '|',
            COALESCE(TRIM(CAST(h.po_itmno AS VARCHAR)), '<NULL>'), '|',
            COALESCE(CAST(h.award_date AS VARCHAR), '<NULL>'), '|',
            COALESCE(UPPER(TRIM(CAST(h.cage AS VARCHAR))), '<NULL>'), '|',
            COALESCE(TRIM(CAST(h.niin AS VARCHAR)), '<NULL>'), '|',
            COALESCE(TRIM(CAST(h.fsc AS VARCHAR)), '<NULL>'), '|',
            COALESCE(TRIM(CAST(h.unit AS VARCHAR)), '<NULL>'), '|',
            COALESCE(TRIM(CAST(h.order_qty AS VARCHAR)), '<NULL>'), '|',
            COALESCE(TRIM(CAST(h.netprice AS VARCHAR)), '<NULL>')
        ) AS transaction_key,
        CAST(NULL AS VARCHAR) AS modification_number,
        CAST(NULL AS VARCHAR) AS awarding_agency_code,
        CAST(h.po_num AS VARCHAR) AS po_number,
        CAST(h.po_itmno AS VARCHAR) AS po_item_number,
        h.source_reference_rows,
        h.reference_part_number_count,
        h.part_number_reference_status,
        'DLA_CONTRACT_HISTORY' AS nsn_source_system,
        'REPORTED_DLA_FINANCIAL_LINE' AS nsn_derivation_method,
        'REPORTED' AS nsn_resolution_status,
        p.platform_families AS associated_platform_families,
        COALESCE(p.platform_count, 0) AS associated_platform_count,
        COALESCE(p.platform_attribution_status, 'UNMAPPED') AS item_platform_attribution_status,
        p.unique_platform_family AS item_unique_platform_family,
        p.attribution_source AS platform_attribution_source
    FROM "market_intel_silver"."view_dla_contract_history_financial" h
    LEFT JOIN vendor_sites vs
        ON UPPER(REGEXP_REPLACE(h.cage, '[^A-Za-z0-9]', '')) = vs.cage_norm
    LEFT JOIN "market_intel_gold"."ref_niin_platform_summary" p
        ON LPAD(TRIM(CAST(h.niin AS VARCHAR)), 9, '0') = p.niin
    LEFT JOIN wsdc_by_niin w
        ON TRIM(h.niin) = w.niin
    LEFT JOIN platform_by_wsdc pw
        ON w.representative_wsdc_code = pw.wsdc_code_ref
    WHERE NOT EXISTS (
        SELECT 1
        FROM usa_dla_contracts u
        WHERE u.contract_id = h.contract_number
    )
)
SELECT * FROM usaspending
UNION ALL
SELECT * FROM dla;
