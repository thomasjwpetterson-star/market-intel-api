CREATE OR REPLACE VIEW "market_intel_gold"."dashboard_master_view" AS
WITH base_transactions AS (
    SELECT
        t.*,
        UPPER(TRIM(CAST(t.psc AS VARCHAR))) AS psc_code_clean,
        REGEXP_REPLACE(COALESCE(CAST(t.nsn AS VARCHAR), ''), '[^0-9]', '') AS nsn_digits,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(t.nsn AS VARCHAR), ''), '[^0-9]', '')) >= 4
            THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(t.nsn AS VARCHAR), ''), '[^0-9]', ''), 1, 4)
        END AS fsc_code_clean
    FROM "market_intel_gold"."global_spend_transactions" t
),
unique_map AS (
    SELECT
        UPPER(TRIM(raw_input_name)) AS clean_key,
        ARBITRARY(market_segment) AS market_segment,
        ARBITRARY(tech_type) AS tech_type,
        ARBITRARY(platform_family) AS platform_family,
        ARBITRARY(clean_variant) AS clean_variant,
        ARBITRARY(capability_name) AS capability_name
    FROM "market_intel_silver"."ref_platform_map"
    WHERE raw_input_name IS NOT NULL
    GROUP BY 1
),
manual_contract_map AS (
    SELECT
        UPPER(TRIM(contract_id)) AS contract_id_clean,
        UPPER(TRIM(COALESCE(vendor_cage, ''))) AS vendor_cage_clean,
        ARBITRARY(market_segment) AS market_segment,
        ARBITRARY(tech_type) AS tech_type,
        ARBITRARY(platform_family) AS platform_family,
        ARBITRARY(clean_variant) AS clean_variant,
        ARBITRARY(capability_name) AS capability_name
    FROM "market_intel_silver"."v_ref_manual_platform_map_from_contracts"
    WHERE contract_id IS NOT NULL
      AND TRIM(contract_id) <> ''
      AND (
          market_segment IS NOT NULL
          OR tech_type IS NOT NULL
          OR platform_family IS NOT NULL
          OR clean_variant IS NOT NULL
          OR capability_name IS NOT NULL
      )
    GROUP BY 1, 2
),
psc_domain AS (
    SELECT
        UPPER(TRIM(CAST(psc_code AS VARCHAR))) AS psc_code,
        ARBITRARY(
            CASE UPPER(TRIM(CAST(mimir_derived_domain AS VARCHAR)))
                WHEN 'AIR' THEN 'Air'
                WHEN 'GROUND' THEN 'Ground'
                WHEN 'NAVAL' THEN 'Naval'
                WHEN 'SPACE' THEN 'Space'
                WHEN 'MISSILES & MUNITIONS' THEN 'Missiles & Munitions'
            END
        ) AS mimir_derived_domain
    FROM "market_intel_silver"."ref_psc_mimir_domain_map"
    WHERE psc_code IS NOT NULL
      AND UPPER(TRIM(CAST(mimir_active_row AS VARCHAR))) = 'Y'
      AND UPPER(TRIM(CAST(mimir_recommended_import AS VARCHAR))) = 'Y'
      AND UPPER(TRIM(CAST(mimir_importable AS VARCHAR))) = 'Y'
      AND mimir_derived_domain IS NOT NULL
      AND UPPER(TRIM(CAST(mimir_derived_domain AS VARCHAR))) IN (
          'AIR', 'GROUND', 'NAVAL', 'SPACE', 'MISSILES & MUNITIONS'
      )
    GROUP BY 1
),
fsc_domain AS (
    SELECT
        LPAD(REGEXP_REPLACE(TRIM(CAST(fsc_code AS VARCHAR)), '[^0-9]', ''), 4, '0') AS fsc_code,
        ARBITRARY(
            CASE UPPER(TRIM(CAST(mimir_domain AS VARCHAR)))
                WHEN 'AIR' THEN 'Air'
                WHEN 'GROUND' THEN 'Ground'
                WHEN 'NAVAL' THEN 'Naval'
                WHEN 'SPACE' THEN 'Space'
                WHEN 'MISSILES & MUNITIONS' THEN 'Missiles & Munitions'
            END
        ) AS mimir_domain
    FROM "market_intel_silver"."ref_fsc_mimir_domain_map"
    WHERE fsc_code IS NOT NULL
      AND UPPER(TRIM(CAST(mimir_active_row AS VARCHAR))) = 'Y'
      AND UPPER(TRIM(CAST(fallback_allowed AS VARCHAR))) = 'Y'
      AND UPPER(TRIM(CAST(mimir_importable AS VARCHAR))) = 'Y'
      AND mimir_domain IS NOT NULL
      AND UPPER(TRIM(CAST(mimir_domain AS VARCHAR))) IN (
          'AIR', 'GROUND', 'NAVAL', 'SPACE', 'MISSILES & MUNITIONS'
      )
    GROUP BY 1
),
contract_metadata AS (
    SELECT
        contract_award_unique_key AS award_key,
        MAX(
            COALESCE(
                NULLIF(period_of_performance_current_end_date, ''),
                NULLIF(ordering_period_end_date, ''),
                NULLIF(period_of_performance_potential_end_date, '')
            )
        ) AS current_end_date,
        ARBITRARY(transaction_description) AS silver_desc,
        ARBITRARY(type_of_contract_pricing) AS pricing_type,
        ARBITRARY(extent_competed) AS competition_type,
        ARBITRARY(number_of_offers_received) AS offers_count,
        ARBITRARY(type_of_set_aside) AS set_aside_type,
        ARBITRARY(solicitation_date) AS solicitation_date,
        ARBITRARY(parent_award_id_piid) AS parent_award_id,
        ARBITRARY(solicitation_identifier) AS solicitation_identifier
    FROM "market_intel_silver"."dataset_prime_contracts"
    WHERE contract_award_unique_key IS NOT NULL
    GROUP BY contract_award_unique_key
)
SELECT
    t.source_system,
    t.contract_id,
    t.year,
    t.action_date,
    t.spend_amount,
    t.vendor_cage,
    COALESCE(t.standardized_vendor_name, t.vendor_name_raw) AS vendor_name,
    t.parent_agency,
    t.sub_agency,
    t.psc,
    t.naics_code,
    t.nsn,
    t.niin,
    t.part_number,
    COALESCE(d.silver_desc, t.description) AS description,
    t.city,
    t.state,
    t.country,
    t.latitude,
    t.longitude,
    COALESCE(
        CASE
            WHEN m.market_segment IS NOT NULL
             AND TRIM(CAST(m.market_segment AS VARCHAR)) <> ''
             AND UPPER(TRIM(CAST(m.market_segment AS VARCHAR))) <> 'UNCATEGORIZED'
            THEN m.market_segment
        END,
        CASE
            WHEN cm.market_segment IS NOT NULL
             AND TRIM(CAST(cm.market_segment AS VARCHAR)) <> ''
             AND UPPER(TRIM(CAST(cm.market_segment AS VARCHAR))) <> 'UNCATEGORIZED'
            THEN cm.market_segment
        END,
        psc_map.mimir_derived_domain,
        fsc_map.mimir_domain,
        'Uncategorized'
    ) AS market_segment,
    COALESCE(
        CASE
            WHEN m.tech_type IS NOT NULL
             AND TRIM(CAST(m.tech_type AS VARCHAR)) <> ''
             AND UPPER(TRIM(CAST(m.tech_type AS VARCHAR))) <> 'UNCATEGORIZED'
            THEN m.tech_type
        END,
        CASE
            WHEN cm.tech_type IS NOT NULL
             AND TRIM(CAST(cm.tech_type AS VARCHAR)) <> ''
             AND UPPER(TRIM(CAST(cm.tech_type AS VARCHAR))) <> 'UNCATEGORIZED'
            THEN cm.tech_type
        END,
        'Uncategorized'
    ) AS tech_type,
    COALESCE(
        NULLIF(TRIM(CAST(m.platform_family AS VARCHAR)), ''),
        NULLIF(TRIM(CAST(cm.platform_family AS VARCHAR)), '')
    ) AS platform_family,
    COALESCE(
        NULLIF(TRIM(CAST(m.clean_variant AS VARCHAR)), ''),
        NULLIF(TRIM(CAST(cm.clean_variant AS VARCHAR)), '')
    ) AS clean_variant,
    COALESCE(
        NULLIF(TRIM(CAST(m.capability_name AS VARCHAR)), ''),
        NULLIF(TRIM(CAST(cm.capability_name AS VARCHAR)), '')
    ) AS capability_name,
    t.join_key_mapping AS raw_data_input,
    COALESCE(p.parent_name, COALESCE(t.standardized_vendor_name, t.vendor_name_raw)) AS ultimate_parent_name,
    p.parent_uei AS ultimate_parent_uei,
    d.current_end_date AS completion_date,
    n.title AS naics_description,
    d.pricing_type,
    d.competition_type,
    d.offers_count,
    d.set_aside_type,
    d.solicitation_date,
    d.parent_award_id,
    d.solicitation_identifier,
    t.base_award_description,
    t.action_description,
    t.place_of_performance_city,
    t.place_of_performance_state,
    t.place_of_performance_country,
    t.place_of_performance_zip,
    t.award_key,
    t.transaction_key,
    t.modification_number,
    t.awarding_agency_code,
    t.po_number,
    t.po_item_number,
    t.source_reference_rows,
    t.reference_part_number_count,
    t.part_number_reference_status
FROM base_transactions t
LEFT JOIN unique_map m
    ON UPPER(TRIM(t.join_key_mapping)) = m.clean_key
LEFT JOIN manual_contract_map cm
    ON UPPER(TRIM(t.contract_id)) = cm.contract_id_clean
   AND UPPER(TRIM(COALESCE(t.vendor_cage, ''))) = cm.vendor_cage_clean
LEFT JOIN psc_domain psc_map
    ON t.psc_code_clean = psc_map.psc_code
LEFT JOIN fsc_domain fsc_map
    ON t.fsc_code_clean = fsc_map.fsc_code
LEFT JOIN "market_intel_gold"."ref_parent_child" p
    ON t.vendor_cage = p.child_cage
LEFT JOIN contract_metadata d
    ON t.award_key = d.award_key
LEFT JOIN "market_intel_silver"."ref_naics" n
    ON CAST(t.naics_code AS VARCHAR) = CAST(n.code AS VARCHAR);
