"""Materialise read-optimised payloads for public intelligence entity pages.

The public website needs a small, deterministic subset of the analytical model.
Building that subset once per atomic data release keeps crawler requests away
from the multi-gigabyte source Parquet files.  Each projection stores one JSON
payload per canonical entity, so a request is an indexed point lookup.

These builders deliberately accept an existing DuckDB connection.  The daily
ETL/release job and the API's backwards-compatible fallback can therefore use
the same SQL and produce byte-compatible payloads.
"""

from __future__ import annotations

from public_nsn_policy import (
    PUBLIC_NSN_ACTIVE_SOLICITATION_LIMIT,
    PUBLIC_NSN_CACHE_EPOCH,
    PUBLIC_NSN_CONNECTED_PLATFORM_LIMIT,
    PUBLIC_NSN_PART_NUMBER_LIMIT,
    PUBLIC_NSN_RECENT_CONTRACT_LIMIT,
    PUBLIC_NSN_SCHEMA_VERSION,
    PUBLIC_NSN_SUPPLIER_SITE_LIMIT,
    public_supplier_relationship_sql,
)


def _relation_exists(conn, relation_name: str) -> bool:
    return bool(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE lower(table_name) = lower(?)
            """,
            [relation_name],
        ).fetchone()[0]
    )


def build_public_company_profiles(conn) -> None:
    """Build one ready-to-serve public company payload per released CAGE."""

    profile_columns = {
        str(row[0]).strip().lower()
        for row in conn.execute("DESCRIBE v_profiles").fetchall()
    }
    facility_uei_expression = (
        "NULLIF(TRIM(CAST(p.uei AS VARCHAR)), '')"
        if "uei" in profile_columns
        else "CAST(NULL AS VARCHAR)"
    )

    conn.execute(
        f"""
        CREATE OR REPLACE TABLE public_company_profile_next AS
        WITH released AS (
            SELECT entity_id AS cage
            FROM public_intelligence_manifest_next
            WHERE entity_type = 'cage_company'
        ), identity AS (
            SELECT
                UPPER(TRIM(CAST(p.cage_code AS VARCHAR))) AS cage,
                MAX_BY(
                    NULLIF(TRIM(CAST(p.vendor_name AS VARCHAR)), ''),
                    GREATEST(
                        COALESCE(TRY_CAST(p.total_lifetime_spend AS DOUBLE), 0),
                        COALESCE(TRY_CAST(p.network_flow_total AS DOUBLE), 0)
                    )
                ) AS name,
                MAX({facility_uei_expression}) AS uei,
                MAX(NULLIF(TRIM(CAST(p.ultimate_parent_name AS VARCHAR)), '')) AS ultimate_parent_name,
                MAX(NULLIF(TRIM(CAST(p.ultimate_parent_uei AS VARCHAR)), '')) AS ultimate_parent_uei
            FROM v_profiles p
            INNER JOIN released r
                ON UPPER(TRIM(CAST(p.cage_code AS VARCHAR))) = r.cage
            GROUP BY 1
        ), location AS (
            SELECT
                UPPER(TRIM(CAST(g.cage_code AS VARCHAR))) AS cage,
                MAX(NULLIF(TRIM(CAST(g.city AS VARCHAR)), '')) AS city,
                MAX(NULLIF(TRIM(CAST(g.state AS VARCHAR)), '')) AS state
            FROM v_cage_locations g
            INNER JOIN released r
                ON UPPER(TRIM(CAST(g.cage_code AS VARCHAR))) = r.cage
            GROUP BY 1
        ), activity AS (
            SELECT
                UPPER(TRIM(CAST(s.cage_code AS VARCHAR))) AS cage,
                MAX_BY(
                    NULLIF(TRIM(CAST(s.vendor_name AS VARCHAR)), ''),
                    COALESCE(TRY_CAST(s.total_spend AS DOUBLE), 0)
                ) AS activity_name,
                SUM(COALESCE(TRY_CAST(s.total_spend AS DOUBLE), 0)) AS prime_spend,
                MIN(TRY_CAST(s.year AS INTEGER)) AS first_year,
                MAX(TRY_CAST(s.year AS INTEGER)) AS last_year
            FROM v_summary s
            INNER JOIN released r
                ON UPPER(TRIM(CAST(s.cage_code AS VARCHAR))) = r.cage
            GROUP BY 1
        ), platform_grouped AS (
            SELECT
                UPPER(TRIM(CAST(s.cage_code AS VARCHAR))) AS cage,
                NULLIF(TRIM(CAST(s.platform_family AS VARCHAR)), '') AS name,
                SUM(COALESCE(TRY_CAST(s.total_spend AS DOUBLE), 0)) AS spend
            FROM v_summary s
            INNER JOIN released r
                ON UPPER(TRIM(CAST(s.cage_code AS VARCHAR))) = r.cage
            WHERE NULLIF(TRIM(CAST(s.platform_family AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2
            HAVING SUM(COALESCE(TRY_CAST(s.total_spend AS DOUBLE), 0)) > 0
        ), platform_ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cage ORDER BY spend DESC, name) AS item_rank,
                COUNT(*) OVER (PARTITION BY cage) AS item_count,
                SUM(spend) OVER (PARTITION BY cage) AS mapped_value
            FROM platform_grouped
        ), platforms AS (
            SELECT
                p.cage,
                MAX(p.item_count) AS item_count,
                MAX(p.mapped_value) AS mapped_value,
                LIST(
                    STRUCT_PACK(
                        name := p.name,
                        share := CASE WHEN a.prime_spend > 0 THEN p.spend / a.prime_spend * 100 ELSE 0 END
                    ) ORDER BY p.item_rank
                ) FILTER (WHERE p.item_rank <= 3) AS items
            FROM platform_ranked p
            INNER JOIN activity a USING (cage)
            GROUP BY 1
        ), agency_grouped AS (
            SELECT
                UPPER(TRIM(CAST(s.cage_code AS VARCHAR))) AS cage,
                NULLIF(TRIM(CAST(s.sub_agency AS VARCHAR)), '') AS name,
                SUM(COALESCE(TRY_CAST(s.total_spend AS DOUBLE), 0)) AS spend
            FROM v_summary s
            INNER JOIN released r
                ON UPPER(TRIM(CAST(s.cage_code AS VARCHAR))) = r.cage
            WHERE NULLIF(TRIM(CAST(s.sub_agency AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2
            HAVING SUM(COALESCE(TRY_CAST(s.total_spend AS DOUBLE), 0)) > 0
        ), agency_ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cage ORDER BY spend DESC, name) AS item_rank,
                COUNT(*) OVER (PARTITION BY cage) AS item_count,
                SUM(spend) OVER (PARTITION BY cage) AS mapped_value
            FROM agency_grouped
        ), agencies AS (
            SELECT
                g.cage,
                MAX(g.item_count) AS item_count,
                MAX(g.mapped_value) AS mapped_value,
                LIST(
                    STRUCT_PACK(
                        name := ARRAY_TO_STRING(
                            LIST_TRANSFORM(
                                STR_SPLIT(LOWER(g.name), ' '),
                                value -> UPPER(LEFT(value, 1)) || SUBSTR(value, 2)
                            ),
                            ' '
                        ),
                        cage := CAST(NULL AS VARCHAR),
                        share := CASE WHEN a.prime_spend > 0 THEN g.spend / a.prime_spend * 100 ELSE 0 END
                    ) ORDER BY g.item_rank
                ) FILTER (WHERE g.item_rank <= 3) AS items
            FROM agency_ranked g
            INNER JOIN activity a USING (cage)
            GROUP BY 1
        ), capability_grouped AS (
            SELECT
                UPPER(TRIM(CAST(s.cage_code AS VARCHAR))) AS cage,
                NULLIF(TRIM(CAST(s.psc_description AS VARCHAR)), '') AS name,
                SUM(COALESCE(TRY_CAST(s.total_spend AS DOUBLE), 0)) AS spend
            FROM v_summary s
            INNER JOIN released r
                ON UPPER(TRIM(CAST(s.cage_code AS VARCHAR))) = r.cage
            WHERE NULLIF(TRIM(CAST(s.psc_description AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2
        ), capability_ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cage ORDER BY spend DESC, name) AS item_rank,
                COUNT(*) OVER (PARTITION BY cage) AS item_count
            FROM capability_grouped
        ), capabilities AS (
            SELECT
                cage,
                MAX(item_count) AS item_count,
                LIST(
                    ARRAY_TO_STRING(
                        LIST_TRANSFORM(
                            STR_SPLIT(LOWER(name), ' '),
                            value -> UPPER(LEFT(value, 1)) || SUBSTR(value, 2)
                        ),
                        ' '
                    ) ORDER BY item_rank
                ) FILTER (WHERE item_rank <= 3) AS items
            FROM capability_ranked
            GROUP BY 1
        ), upstream_grouped AS (
            SELECT
                UPPER(TRIM(CAST(n.sub_cage AS VARCHAR))) AS cage,
                NULLIF(TRIM(CAST(n.prime_name AS VARCHAR)), '') AS name,
                NULLIF(UPPER(TRIM(CAST(n.prime_cage AS VARCHAR))), '') AS partner_cage,
                SUM(COALESCE(TRY_CAST(n.subaward_value AS DOUBLE), 0)) AS value
            FROM v_network n
            INNER JOIN released r
                ON UPPER(TRIM(CAST(n.sub_cage AS VARCHAR))) = r.cage
            WHERE NULLIF(TRIM(CAST(n.prime_name AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2, 3
            HAVING SUM(COALESCE(TRY_CAST(n.subaward_value AS DOUBLE), 0)) > 0
        ), upstream_ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cage ORDER BY value DESC, name, partner_cage) AS item_rank,
                COUNT(*) OVER (PARTITION BY cage) AS item_count,
                SUM(value) OVER (PARTITION BY cage) AS network_total
            FROM upstream_grouped
        ), upstream AS (
            SELECT
                cage,
                MAX(item_count) AS item_count,
                MAX(network_total) AS network_total,
                LIST(
                    STRUCT_PACK(
                        name := name,
                        cage := partner_cage,
                        share := CAST(ROUND(value / network_total * 100) AS BIGINT)
                    ) ORDER BY item_rank
                ) FILTER (
                    WHERE item_rank <= 3
                      AND CAST(ROUND(value / network_total * 100) AS BIGINT) > 0
                ) AS items
            FROM upstream_ranked
            GROUP BY 1
        ), downstream_grouped AS (
            SELECT
                UPPER(TRIM(CAST(n.prime_cage AS VARCHAR))) AS cage,
                NULLIF(TRIM(CAST(n.sub_name AS VARCHAR)), '') AS name,
                NULLIF(UPPER(TRIM(CAST(n.sub_cage AS VARCHAR))), '') AS partner_cage,
                SUM(COALESCE(TRY_CAST(n.subaward_value AS DOUBLE), 0)) AS value
            FROM v_network n
            INNER JOIN released r
                ON UPPER(TRIM(CAST(n.prime_cage AS VARCHAR))) = r.cage
            WHERE NULLIF(TRIM(CAST(n.sub_name AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2, 3
            HAVING SUM(COALESCE(TRY_CAST(n.subaward_value AS DOUBLE), 0)) > 0
        ), downstream_ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cage ORDER BY value DESC, name, partner_cage) AS item_rank,
                COUNT(*) OVER (PARTITION BY cage) AS item_count,
                SUM(value) OVER (PARTITION BY cage) AS network_total
            FROM downstream_grouped
        ), downstream AS (
            SELECT
                cage,
                MAX(item_count) AS item_count,
                MAX(network_total) AS network_total,
                LIST(
                    STRUCT_PACK(
                        name := name,
                        cage := partner_cage,
                        share := CAST(ROUND(value / network_total * 100) AS BIGINT)
                    ) ORDER BY item_rank
                ) FILTER (
                    WHERE item_rank <= 3
                      AND CAST(ROUND(value / network_total * 100) AS BIGINT) > 0
                ) AS items
            FROM downstream_ranked
            GROUP BY 1
        ), nsns AS (
            SELECT
                cage,
                MAX(nsn_count) AS item_count,
                LIST(
                    STRUCT_PACK(
                        nsn := CAST(nsn AS VARCHAR),
                        "desc" := COALESCE(
                            NULLIF(
                                ARRAY_TO_STRING(
                                    LIST_TRANSFORM(
                                        STR_SPLIT(LOWER(TRIM(CAST(description AS VARCHAR))), ' '),
                                        value -> UPPER(LEFT(value, 1)) || SUBSTR(value, 2)
                                    ),
                                    ' '
                                ),
                                ''
                            ),
                            'Unspecified Component'
                        )
                    ) ORDER BY nsn_rank
                ) FILTER (WHERE nsn_rank <= 3) AS items
            FROM public_company_top_nsn_next
            GROUP BY 1
        ), assembled AS (
            SELECT
                r.cage,
                COALESCE(i.name, a.activity_name, 'CAGE ' || r.cage) AS name,
                i.uei,
                i.ultimate_parent_name,
                i.ultimate_parent_uei,
                CONCAT_WS(', ', l.city, l.state) AS location,
                a.first_year,
                a.last_year,
                COALESCE(a.prime_spend, 0) AS prime_spend,
                COALESCE(u.network_total, 0) AS upstream_total,
                COALESCE(d.network_total, 0) AS downstream_total,
                COALESCE(p.mapped_value, 0) AS mapped_platform_value,
                COALESCE(p.item_count, 0) AS platform_count,
                p.items AS platform_items,
                COALESCE(g.mapped_value, 0) AS government_mapped_value,
                COALESCE(g.item_count, 0) AS government_count,
                g.items AS government_items,
                COALESCE(c.item_count, 0) AS capability_count,
                c.items AS capability_items,
                COALESCE(u.item_count, 0) AS upstream_count,
                u.items AS upstream_items,
                COALESCE(d.item_count, 0) AS downstream_count,
                d.items AS downstream_items,
                COALESCE(n.item_count, 0) AS nsn_count,
                n.items AS nsn_items,
                NULLIF(TRIM(CAST(ta.base_award_description AS VARCHAR)), '') AS top_contract_desc,
                NULLIF(TRIM(CAST(ta.contract_id AS VARCHAR)), '') AS top_contract_id,
                COALESCE(TRY_CAST(ta.total_spend AS DOUBLE), 0) AS top_contract_value
            FROM released r
            LEFT JOIN activity a USING (cage)
            LEFT JOIN identity i USING (cage)
            LEFT JOIN location l USING (cage)
            LEFT JOIN platforms p USING (cage)
            LEFT JOIN agencies g USING (cage)
            LEFT JOIN capabilities c USING (cage)
            LEFT JOIN upstream u USING (cage)
            LEFT JOIN downstream d USING (cage)
            LEFT JOIN nsns n USING (cage)
            LEFT JOIN public_company_top_award_next ta USING (cage)
        )
        SELECT
            cage,
            TO_JSON(STRUCT_PACK(
                found := TRUE,
                name := name,
                cage := cage,
                uei := uei,
                is_parent := FALSE,
                ultimate_parent_name := ultimate_parent_name,
                ultimate_parent_uei := ultimate_parent_uei,
                location := location,
                time_period := CASE
                    WHEN first_year IS NOT NULL AND last_year IS NOT NULL
                    THEN 'FY' || CAST(first_year AS VARCHAR) || '–FY' || CAST(last_year AS VARCHAR)
                    ELSE 'Observed period unavailable'
                END,
                description := '',
                prime_exposure := prime_spend,
                sub_exposure := upstream_total,
                downstream_subcontract_value := downstream_total,
                total_exposure := prime_spend + upstream_total,
                top_capabilities := COALESCE(capability_items, []::VARCHAR[]),
                capabilities_hidden := GREATEST(capability_count - COALESCE(LEN(capability_items), 0), 0),
                top_platforms := COALESCE(platform_items, []::STRUCT(name VARCHAR, share DOUBLE)[]),
                platforms_hidden := GREATEST(platform_count - COALESCE(LEN(platform_items), 0), 0),
                mapped_platform_value := mapped_platform_value,
                platform_mapping_coverage := CASE WHEN prime_spend > 0 THEN mapped_platform_value / prime_spend * 100 ELSE 0 END,
                network_type := CASE
                    WHEN prime_spend >= upstream_total AND downstream_count > 0 THEN 'Key Supply Chain Partners'
                    WHEN upstream_count > 0 THEN 'Key Customers'
                    WHEN downstream_count > 0 THEN 'Key Supply Chain Partners'
                    ELSE 'Network Partners'
                END,
                network_partners := CASE
                    WHEN prime_spend >= upstream_total AND downstream_count > 0
                    THEN COALESCE(downstream_items, []::STRUCT(name VARCHAR, cage VARCHAR, share BIGINT)[])
                    WHEN upstream_count > 0
                    THEN COALESCE(upstream_items, []::STRUCT(name VARCHAR, cage VARCHAR, share BIGINT)[])
                    WHEN downstream_count > 0
                    THEN COALESCE(downstream_items, []::STRUCT(name VARCHAR, cage VARCHAR, share BIGINT)[])
                    ELSE []::STRUCT(name VARCHAR, cage VARCHAR, share BIGINT)[]
                END,
                network_hidden := CASE
                    WHEN prime_spend >= upstream_total AND downstream_count > 0
                    THEN GREATEST(downstream_count - COALESCE(LEN(downstream_items), 0), 0)
                    WHEN upstream_count > 0
                    THEN GREATEST(upstream_count - COALESCE(LEN(upstream_items), 0), 0)
                    WHEN downstream_count > 0
                    THEN GREATEST(downstream_count - COALESCE(LEN(downstream_items), 0), 0)
                    ELSE 0
                END,
                prime_customers := COALESCE(upstream_items, []::STRUCT(name VARCHAR, cage VARCHAR, share BIGINT)[]),
                prime_customers_hidden := GREATEST(upstream_count - COALESCE(LEN(upstream_items), 0), 0),
                government_customers := COALESCE(government_items, []::STRUCT(name VARCHAR, cage VARCHAR, share DOUBLE)[]),
                government_customers_hidden := GREATEST(government_count - COALESCE(LEN(government_items), 0), 0),
                government_customer_coverage := CASE WHEN prime_spend > 0 THEN government_mapped_value / prime_spend * 100 ELSE 0 END,
                subcontractors := COALESCE(downstream_items, []::STRUCT(name VARCHAR, cage VARCHAR, share BIGINT)[]),
                subcontractors_hidden := GREATEST(downstream_count - COALESCE(LEN(downstream_items), 0), 0),
                platform_share_basis := 'Share of all observed prime obligation value',
                prime_customer_share_basis := 'Share of all tracked subcontract award value received',
                government_customer_share_basis := 'Share of all observed prime obligation value',
                subcontractor_share_basis := 'Share of tracked subcontract awards issued',
                top_nsns := COALESCE(nsn_items, []::STRUCT(nsn VARCHAR, "desc" VARCHAR)[]),
                nsns_hidden := GREATEST(nsn_count - COALESCE(LEN(nsn_items), 0), 0),
                top_contract_desc := top_contract_desc,
                top_contract_id := top_contract_id,
                top_contract_value := top_contract_value
            )) AS payload_json
        FROM assembled
        """
    )


def build_public_platform_profiles(conn) -> None:
    """Build one ready-to-serve public platform payload per released slug."""

    conn.execute(
        """
        CREATE OR REPLACE TABLE public_platform_profile_next AS
        WITH released AS (
            SELECT
                entity_id AS slug,
                display_name AS name,
                UPPER(TRIM(display_name)) AS normalized_name
            FROM public_intelligence_manifest_next
            WHERE entity_type = 'platform'
        ), activity AS (
            SELECT
                r.slug,
                r.name,
                SUM(COALESCE(TRY_CAST(s.total_spend AS DOUBLE), 0)) AS total_obligations,
                SUM(COALESCE(TRY_CAST(s.contract_count AS BIGINT), 0)) AS contract_count,
                COUNT(DISTINCT NULLIF(TRIM(CAST(s.vendor_name AS VARCHAR)), '')) AS contractor_count,
                MIN(TRY_CAST(s.year AS INTEGER)) AS first_year,
                MAX(TRY_CAST(s.year AS INTEGER)) AS last_year
            FROM released r
            INNER JOIN v_summary s
                ON UPPER(TRIM(CAST(s.platform_family AS VARCHAR))) = r.normalized_name
            GROUP BY 1, 2
        ), vendor_labels AS (
            SELECT
                r.slug,
                UPPER(TRIM(CAST(s.cage_code AS VARCHAR))) AS cage,
                NULLIF(TRIM(CAST(s.vendor_name AS VARCHAR)), '') AS name,
                SUM(COALESCE(TRY_CAST(s.total_spend AS DOUBLE), 0)) AS value
            FROM released r
            INNER JOIN v_summary s
                ON UPPER(TRIM(CAST(s.platform_family AS VARCHAR))) = r.normalized_name
            WHERE NULLIF(TRIM(CAST(s.vendor_name AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2, 3
            HAVING SUM(COALESCE(TRY_CAST(s.total_spend AS DOUBLE), 0)) > 0
        ), vendor_grouped AS (
            SELECT
                slug,
                cage,
                MAX_BY(name, value) AS name,
                SUM(value) AS value
            FROM vendor_labels
            GROUP BY 1, 2
        ), vendor_ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY slug ORDER BY value DESC, cage, name) AS item_rank,
                COUNT(*) OVER (PARTITION BY slug) AS item_count
            FROM vendor_grouped
        ), scope_grouped AS (
            SELECT
                slug,
                vendor_cage AS cage,
                MAX(award_description) FILTER (WHERE scope_rank = 1) AS latest_award_description,
                MAX(contract_id) FILTER (WHERE scope_rank = 1) AS latest_award_id,
                MAX(award_date) FILTER (WHERE scope_rank = 1) AS latest_award_date,
                LIST(
                    STRUCT_PACK(
                        description := award_description,
                        contract_id := contract_id,
                        date := CAST(award_date AS VARCHAR)
                    ) ORDER BY scope_rank
                ) FILTER (WHERE scope_rank <= 3) AS recent_award_scopes
            FROM public_platform_award_scope_next
            GROUP BY 1, 2
        ), vendors AS (
            SELECT
                v.slug,
                MAX(v.item_count) AS item_count,
                LIST(
                    STRUCT_PACK(
                        name := v.name,
                        cage := v.cage,
                        total := v.value,
                        latest_award_description := s.latest_award_description,
                        latest_award_id := s.latest_award_id,
                        latest_award_date := CAST(s.latest_award_date AS VARCHAR),
                        recent_award_scopes := COALESCE(
                            s.recent_award_scopes,
                            []::STRUCT(description VARCHAR, contract_id VARCHAR, date VARCHAR)[]
                        )
                    ) ORDER BY v.item_rank
                ) FILTER (WHERE v.item_rank <= 6) AS items
            FROM vendor_ranked v
            LEFT JOIN scope_grouped s USING (slug, cage)
            GROUP BY 1
        ), agency_grouped AS (
            SELECT
                r.slug,
                NULLIF(TRIM(CAST(s.sub_agency AS VARCHAR)), '') AS name,
                SUM(COALESCE(TRY_CAST(s.total_spend AS DOUBLE), 0)) AS value
            FROM released r
            INNER JOIN v_summary s
                ON UPPER(TRIM(CAST(s.platform_family AS VARCHAR))) = r.normalized_name
            WHERE NULLIF(TRIM(CAST(s.sub_agency AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2
        ), agency_ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY slug ORDER BY value DESC, name) AS item_rank
            FROM agency_grouped
        ), agencies AS (
            SELECT
                slug,
                LIST(name ORDER BY item_rank) FILTER (WHERE item_rank <= 5) AS items
            FROM agency_ranked
            GROUP BY 1
        ), bom AS (
            SELECT DISTINCT
                r.slug,
                LPAD(TRIM(CAST(b.niin AS VARCHAR)), 9, '0') AS niin
            FROM released r
            INNER JOIN v_platform_bom b
                ON UPPER(TRIM(CAST(b.platform_family AS VARCHAR))) = r.normalized_name
        ), bom_counts AS (
            SELECT slug, COUNT(*) AS item_count
            FROM bom
            GROUP BY 1
        ), niin_values AS (
            SELECT
                LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') AS niin,
                SUM(COALESCE(TRY_CAST(s.total_revenue AS DOUBLE), 0)) AS value
            FROM v_nsn_supplier_lookup s
            WHERE NULLIF(TRIM(CAST(s.niin AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1
        ), niin_vendor_values AS (
            SELECT
                LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') AS niin,
                UPPER(TRIM(CAST(s.cage AS VARCHAR))) AS cage,
                MAX(NULLIF(TRIM(CAST(s.vendor AS VARCHAR)), '')) AS vendor,
                SUM(COALESCE(TRY_CAST(s.total_revenue AS DOUBLE), 0)) AS vendor_value
            FROM v_nsn_supplier_lookup s
            WHERE NULLIF(TRIM(CAST(s.niin AS VARCHAR)), '') IS NOT NULL
              AND NULLIF(TRIM(CAST(s.cage AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2
        ), niin_vendors AS (
            SELECT
                niin,
                MAX_BY(cage, vendor_value) AS top_vendor,
                MAX_BY(vendor, vendor_value) AS top_vendor_name
            FROM niin_vendor_values
            GROUP BY 1
        ), part_ranked AS (
            SELECT
                b.slug,
                b.niin,
                COALESCE(NULLIF(TRIM(CAST(p.nsn AS VARCHAR)), ''), b.niin) AS nsn,
                COALESCE(NULLIF(TRIM(CAST(p.item_name AS VARCHAR)), ''), 'Unknown item') AS description,
                COALESCE(nv.value, 0) AS value,
                pv.top_vendor,
                pv.top_vendor_name,
                ROW_NUMBER() OVER (
                    PARTITION BY b.slug
                    ORDER BY COALESCE(nv.value, 0) DESC, b.niin
                ) AS item_rank,
                COUNT(*) OVER (PARTITION BY b.slug) AS item_count
            FROM bom b
            LEFT JOIN niin_values nv USING (niin)
            LEFT JOIN niin_vendors pv USING (niin)
            LEFT JOIN v_nsn_profile_lookup p
                ON LPAD(TRIM(CAST(p.niin AS VARCHAR)), 9, '0') = b.niin
            WHERE COALESCE(nv.value, 0) > 0
        ), parts AS (
            SELECT
                slug,
                MAX(item_count) AS item_count,
                LIST(
                    STRUCT_PACK(
                        nsn := nsn,
                        niin := niin,
                        description := description,
                        amount := value,
                        top_vendor := top_vendor,
                        top_vendor_name := top_vendor_name
                    ) ORDER BY item_rank
                ) FILTER (WHERE item_rank <= 4) AS items
            FROM part_ranked
            GROUP BY 1
        ), award_ranked AS (
            SELECT
                r.slug,
                CAST(t.contract_id AS VARCHAR) AS contract_id,
                CAST(t.action_date AS VARCHAR) AS action_date,
                CAST(t.vendor_name AS VARCHAR) AS vendor_name,
                UPPER(TRIM(CAST(t.vendor_cage AS VARCHAR))) AS vendor_cage,
                COALESCE(
                    NULLIF(TRIM(CAST(t.sub_agency AS VARCHAR)), ''),
                    NULLIF(TRIM(CAST(t.parent_agency AS VARCHAR)), '')
                ) AS agency,
                NULLIF(TRIM(CAST(t.description AS VARCHAR)), '') AS description,
                COALESCE(TRY_CAST(t.spend_amount AS DOUBLE), 0) AS spend,
                ROW_NUMBER() OVER (
                    PARTITION BY r.slug
                    ORDER BY TRY_CAST(t.action_date AS DATE) DESC NULLS LAST, CAST(t.contract_id AS VARCHAR)
                ) AS item_rank
            FROM released r
            INNER JOIN v_transactions t
                ON UPPER(TRIM(CAST(t.platform_family AS VARCHAR))) = r.normalized_name
            WHERE COALESCE(TRY_CAST(t.spend_amount AS DOUBLE), 0) >= 500000
        ), awards AS (
            SELECT
                slug,
                LIST(
                    STRUCT_PACK(
                        contract_id := contract_id,
                        action_date := action_date,
                        vendor_name := vendor_name,
                        vendor_cage := vendor_cage,
                        agency := agency,
                        description := description,
                        spend := spend
                    ) ORDER BY item_rank
                ) FILTER (WHERE item_rank <= 4) AS items
            FROM award_ranked
            GROUP BY 1
        )
        SELECT
            a.slug,
            TO_JSON(STRUCT_PACK(
                found := TRUE,
                entity_type := 'platform',
                entity_id := a.slug,
                slug := a.slug,
                name := a.name,
                time_period := CASE
                    WHEN a.first_year IS NOT NULL AND a.last_year IS NOT NULL
                    THEN 'FY' || CAST(a.first_year AS VARCHAR) || '–FY' || CAST(a.last_year AS VARCHAR)
                    ELSE 'Observed period unavailable'
                END,
                total_obligations := a.total_obligations,
                contract_count := CAST(a.contract_count AS BIGINT),
                contractor_count := CAST(a.contractor_count AS BIGINT),
                top_vendors := COALESCE(
                    v.items,
                    []::STRUCT(
                        name VARCHAR,
                        cage VARCHAR,
                        total DOUBLE,
                        latest_award_description VARCHAR,
                        latest_award_id VARCHAR,
                        latest_award_date VARCHAR,
                        recent_award_scopes STRUCT(description VARCHAR, contract_id VARCHAR, date VARCHAR)[]
                    )[]
                ),
                vendors_hidden := GREATEST(CAST(a.contractor_count AS BIGINT) - COALESCE(LEN(v.items), 0), 0),
                top_agencies := COALESCE(g.items, []::VARCHAR[]),
                parts := COALESCE(
                    p.items,
                    []::STRUCT(
                        nsn VARCHAR,
                        niin VARCHAR,
                        description VARCHAR,
                        amount DOUBLE,
                        top_vendor VARCHAR,
                        top_vendor_name VARCHAR
                    )[]
                ),
                parts_hidden := GREATEST(COALESCE(bc.item_count, 0) - COALESCE(LEN(p.items), 0), 0),
                part_count := COALESCE(bc.item_count, 0),
                recent_awards := COALESCE(
                    aw.items,
                    []::STRUCT(
                        contract_id VARCHAR,
                        action_date VARCHAR,
                        vendor_name VARCHAR,
                        vendor_cage VARCHAR,
                        agency VARCHAR,
                        description VARCHAR,
                        spend DOUBLE
                    )[]
                ),
                awards_hidden := GREATEST(CAST(a.contract_count AS BIGINT) - COALESCE(LEN(aw.items), 0), 0)
            )) AS payload_json
        FROM activity a
        LEFT JOIN vendors v USING (slug)
        LEFT JOIN agencies g USING (slug)
        LEFT JOIN parts p USING (slug)
        LEFT JOIN bom_counts bc USING (slug)
        LEFT JOIN awards aw USING (slug)
        """
    )


def _build_public_nsn_profiles_monolithic(conn) -> None:
    """Build one ready-to-serve public NSN payload per released identifier."""

    conn.execute(
        """
        CREATE OR REPLACE TABLE public_nsn_profile_next AS
        WITH released AS (
            SELECT
                entity_id,
                RIGHT(REGEXP_REPLACE(entity_id, '[^0-9]', '', 'g'), 9) AS niin
            FROM public_intelligence_manifest_next
            WHERE entity_type = 'nsn'
        ), profile AS (
            SELECT
                r.entity_id,
                r.niin,
                MAX(NULLIF(TRIM(CAST(p.nsn AS VARCHAR)), '')) AS source_nsn,
                MAX(NULLIF(TRIM(CAST(p.item_name AS VARCHAR)), '')) AS item_name,
                MAX(NULLIF(TRIM(CAST(p.fsc_code AS VARCHAR)), '')) AS fsc_code
            FROM released r
            LEFT JOIN v_nsn_profile_lookup p
                ON LPAD(TRIM(CAST(p.niin AS VARCHAR)), 9, '0') = r.niin
            GROUP BY 1, 2
        ), reference_supplier_grouped AS (
            SELECT
                r.entity_id,
                r.niin,
                UPPER(TRIM(CAST(ref.cage AS VARCHAR))) AS cage,
                MAX(NULLIF(TRIM(CAST(ref.vendor_name AS VARCHAR)), '')) AS vendor,
                MIN(NULLIF(TRIM(CAST(ref.part_number AS VARCHAR)), '')) AS part_number,
                BOOL_OR(COALESCE(TRY_CAST(ref.is_active_authorized_source AS BOOLEAN), FALSE)) AS is_active_authorized_source,
                BOOL_OR(COALESCE(TRY_CAST(ref.is_procurement_authorized AS BOOLEAN), FALSE)) AS is_procurement_authorized,
                MAX(NULLIF(TRIM(CAST(ref.supplier_status AS VARCHAR)), '')) AS supplier_status
            FROM released r
            INNER JOIN v_nsn_cage_reference ref
                ON LPAD(TRIM(CAST(ref.niin AS VARCHAR)), 9, '0') = r.niin
            WHERE NULLIF(TRIM(CAST(ref.cage AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2, 3
        ), reference_supplier_ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY entity_id
                    ORDER BY
                        is_active_authorized_source DESC,
                        is_procurement_authorized DESC,
                        CASE WHEN vendor IS NULL THEN 1 ELSE 0 END,
                        vendor,
                        cage
                ) AS supplier_rank,
                ROW_NUMBER() OVER (
                    PARTITION BY entity_id
                    ORDER BY
                        CASE WHEN is_active_authorized_source THEN 0 ELSE 1 END,
                        CASE WHEN vendor IS NULL THEN 1 ELSE 0 END,
                        vendor,
                        cage
                ) AS approved_rank,
                COUNT(*) OVER (PARTITION BY entity_id) AS supplier_count,
                COUNT(*) FILTER (WHERE is_active_authorized_source) OVER (PARTITION BY entity_id) AS approved_count
            FROM reference_supplier_grouped
        ), reference_suppliers AS (
            SELECT
                entity_id,
                MAX(supplier_count) AS supplier_count,
                MAX(approved_count) AS approved_count,
                LIST(
                    STRUCT_PACK(
                        cage := cage,
                        vendor := COALESCE(vendor, 'CAGE ' || cage),
                        part_number := LIST_EXTRACT(part_numbers, 1),
                        status := CASE
                            WHEN is_active_authorized_source THEN 'Active DLA-authorised source'
                            WHEN is_procurement_authorized THEN 'DLA procurement-authorised; CAGE not active'
                            ELSE COALESCE(supplier_status, 'Observed or reference-linked supplier')
                        END,
                        is_active_authorized_source := is_active_authorized_source,
                        is_procurement_authorized := is_procurement_authorized
                    ) ORDER BY supplier_rank
                ) FILTER (WHERE supplier_rank <= 2) AS items,
                LIST_EXTRACT(
                    LIST(
                        STRUCT_PACK(
                            cage := cage,
                            vendor := COALESCE(vendor, 'CAGE ' || cage),
                            part_number := LIST_EXTRACT(part_numbers, 1)
                        ) ORDER BY approved_rank
                    ) FILTER (WHERE is_active_authorized_source),
                    1
                ) AS approved_source
            FROM reference_supplier_ranked
            GROUP BY 1
        ), reference_parts AS (
            SELECT
                r.entity_id,
                COUNT(DISTINCT NULLIF(TRIM(CAST(ref.part_number AS VARCHAR)), '')) AS part_count,
                LIST_SLICE(
                    LIST_SORT(
                        LIST(DISTINCT NULLIF(TRIM(CAST(ref.part_number AS VARCHAR)), ''))
                            FILTER (WHERE NULLIF(TRIM(CAST(ref.part_number AS VARCHAR)), '') IS NOT NULL)
                    ),
                    1,
                    8
                ) AS items
            FROM released r
            LEFT JOIN v_nsn_cage_reference ref
                ON LPAD(TRIM(CAST(ref.niin AS VARCHAR)), 9, '0') = r.niin
            GROUP BY 1
        ), financial_supplier_counts AS (
            SELECT
                r.entity_id,
                COUNT(DISTINCT NULLIF(UPPER(TRIM(CAST(s.cage AS VARCHAR))), '')) AS supplier_count
            FROM released r
            LEFT JOIN v_nsn_supplier_lookup s
                ON LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') = r.niin
            GROUP BY 1
        ), platform_rows AS (
            SELECT
                r.entity_id,
                MAX(NULLIF(TRIM(CAST(s.platform_families AS VARCHAR)), '')) AS platform_families,
                MAX(COALESCE(TRY_CAST(s.platform_count AS BIGINT), 0)) AS platform_count
            FROM released r
            LEFT JOIN v_nsn_summary s
                ON LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') = r.niin
            GROUP BY 1
        ), contract_grouped AS (
            SELECT
                r.entity_id,
                NULLIF(TRIM(CAST(s.contract_id AS VARCHAR)), '') AS contract_id,
                MAX(CAST(s.last_sold AS VARCHAR)) AS action_date,
                MAX(COALESCE(
                    NULLIF(TRIM(CAST(s.sub_agency AS VARCHAR)), ''),
                    NULLIF(TRIM(CAST(s.parent_agency AS VARCHAR)), '')
                )) AS agency,
                MAX(NULLIF(TRIM(CAST(s.vendor AS VARCHAR)), '')) AS vendor_name,
                MAX(NULLIF(UPPER(TRIM(CAST(s.cage AS VARCHAR))), '')) AS vendor_cage,
                SUM(COALESCE(TRY_CAST(s.total_revenue AS DOUBLE), 0)) AS observed_value
            FROM released r
            INNER JOIN v_nsn_supplier_lookup s
                ON LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') = r.niin
            WHERE NULLIF(TRIM(CAST(s.contract_id AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2
        ), contract_ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY entity_id
                    ORDER BY TRY_CAST(action_date AS DATE) DESC NULLS LAST, contract_id
                ) AS item_rank,
                COUNT(*) OVER (PARTITION BY entity_id) AS item_count
            FROM contract_grouped
        ), contracts AS (
            SELECT
                entity_id,
                MAX(item_count) AS item_count,
                LIST(
                    STRUCT_PACK(
                        contract_id := contract_id,
                        action_date := action_date,
                        agency := agency,
                        vendor_name := vendor_name,
                        vendor_cage := vendor_cage,
                        observed_value := observed_value
                    ) ORDER BY item_rank
                ) FILTER (WHERE item_rank <= 3) AS items
            FROM contract_ranked
            GROUP BY 1
        )
        SELECT
            p.entity_id,
            TO_JSON(STRUCT_PACK(
                found := TRUE,
                entity_type := 'nsn',
                entity_id := p.entity_id,
                item_name := COALESCE(p.item_name, 'Unknown item'),
                nsn := CASE
                    WHEN LENGTH(REGEXP_REPLACE(p.entity_id, '[^0-9]', '', 'g')) = 13
                    THEN SUBSTR(p.entity_id, 1, 4) || '-' || SUBSTR(p.entity_id, 5, 2) || '-' || SUBSTR(p.entity_id, 7, 3) || '-' || SUBSTR(p.entity_id, 10, 4)
                    ELSE CAST(NULL AS VARCHAR)
                END,
                niin := p.niin,
                fsc_code := COALESCE(
                    p.fsc_code,
                    CASE WHEN LENGTH(p.entity_id) = 13 THEN LEFT(p.entity_id, 4) ELSE CAST(NULL AS VARCHAR) END
                ),
                associated_part_number_count := COALESCE(rp.part_count, 0),
                part_numbers := COALESCE(rp.items, []::VARCHAR[]),
                part_numbers_hidden := GREATEST(COALESCE(rp.part_count, 0) - COALESCE(LEN(rp.items), 0), 0),
                associated_supplier_site_count := GREATEST(
                    COALESCE(rs.supplier_count, 0),
                    COALESCE(fs.supplier_count, 0)
                ),
                approved_source := rs.approved_source,
                approved_sources_hidden := GREATEST(COALESCE(rs.approved_count, 0) - CASE WHEN rs.approved_source IS NULL THEN 0 ELSE 1 END, 0),
                supplier_sites := COALESCE(
                    rs.items,
                    []::STRUCT(
                        cage VARCHAR,
                        vendor VARCHAR,
                        part_number VARCHAR,
                        status VARCHAR,
                        is_active_authorized_source BOOLEAN,
                        is_procurement_authorized BOOLEAN
                    )[]
                ),
                supplier_sites_hidden := GREATEST(COALESCE(rs.supplier_count, 0) - COALESCE(LEN(rs.items), 0), 0),
                platforms := CASE
                    WHEN pr.platform_families IS NULL THEN []::VARCHAR[]
                    ELSE LIST_SLICE(
                        LIST_TRANSFORM(
                            LIST_FILTER(STR_SPLIT(pr.platform_families, '|'), value -> TRIM(value) <> ''),
                            value -> TRIM(value)
                        ),
                        1,
                        2
                    )
                END,
                platforms_hidden := GREATEST(
                    COALESCE(pr.platform_count, 0) - CASE
                        WHEN pr.platform_families IS NULL THEN 0
                        ELSE LEN(LIST_SLICE(
                            LIST_TRANSFORM(
                                LIST_FILTER(STR_SPLIT(pr.platform_families, '|'), value -> TRIM(value) <> ''),
                                value -> TRIM(value)
                            ),
                            1,
                            2
                        ))
                    END,
                    0
                ),
                is_multi_platform := COALESCE(pr.platform_count, 0) > 1,
                recent_contracts := COALESCE(
                    c.items,
                    []::STRUCT(
                        contract_id VARCHAR,
                        action_date VARCHAR,
                        agency VARCHAR,
                        vendor_name VARCHAR,
                        vendor_cage VARCHAR,
                        observed_value DOUBLE
                    )[]
                ),
                observed_contract_count := COALESCE(c.item_count, 0),
                contracts_hidden := GREATEST(COALESCE(c.item_count, 0) - COALESCE(LEN(c.items), 0), 0)
            )) AS payload_json
        FROM profile p
        LEFT JOIN reference_suppliers rs USING (entity_id)
        LEFT JOIN reference_parts rp USING (entity_id)
        LEFT JOIN financial_supplier_counts fs USING (entity_id)
        LEFT JOIN platform_rows pr USING (entity_id)
        LEFT JOIN contracts c USING (entity_id)
        """
    )


def build_public_nsn_profiles(conn) -> None:
    """Build NSN payloads in bounded-memory stages.

    A single query across the reference, supplier, platform and contract
    universes retains several large hash tables at once.  These release-owned
    work tables let DuckDB spill each aggregation independently and keep the
    peak below the web service's memory ceiling.
    """

    supplier_label_sql, supplier_rank_sql = public_supplier_relationship_sql(
        active="is_active",
        procurement="is_procurement",
        rncc="rncc_codes",
        rnvc="rnvc_codes",
        rnsc="rnsc_codes",
        source="relationship_source",
    )

    conn.execute("DROP TABLE IF EXISTS public_nsn_profile_next")
    conn.execute("DROP TABLE IF EXISTS public_nsn_released_work")
    conn.execute("""
        CREATE TABLE public_nsn_released_work AS
        SELECT
            entity_id,
            RIGHT(REGEXP_REPLACE(entity_id, '[^0-9]', '', 'g'), 9) AS niin
        FROM public_intelligence_manifest_next
        WHERE entity_type = 'nsn'
    """)

    conn.execute("""
        CREATE TABLE public_nsn_profile_next AS
        WITH profile_lookup AS (
            SELECT
                r.entity_id,
                r.niin,
                MAX(NULLIF(TRIM(CAST(p.item_name AS VARCHAR)), '')) AS item_name,
                MAX(NULLIF(TRIM(CAST(p.fsc_code AS VARCHAR)), '')) AS fsc_code
            FROM public_nsn_released_work r
            LEFT JOIN v_nsn_profile_lookup p
                ON LPAD(TRIM(CAST(p.niin AS VARCHAR)), 9, '0') = r.niin
            GROUP BY 1, 2
        ), reference_profile AS (
            SELECT
                r.entity_id,
                MAX(NULLIF(TRIM(CAST(reference.description AS VARCHAR)), '')) AS item_name,
                MAX(NULLIF(TRIM(CAST(reference.fsc_code AS VARCHAR)), '')) AS fsc_code
            FROM public_nsn_released_work r
            INNER JOIN v_nsn_cage_reference reference
                ON LPAD(TRIM(CAST(reference.niin AS VARCHAR)), 9, '0') = r.niin
            GROUP BY 1
        ), profile AS (
            SELECT
                profile_lookup.entity_id,
                profile_lookup.niin,
                COALESCE(profile_lookup.item_name, reference_profile.item_name) AS item_name,
                COALESCE(profile_lookup.fsc_code, reference_profile.fsc_code) AS fsc_code
            FROM profile_lookup
            LEFT JOIN reference_profile USING (entity_id)
        )
        SELECT
            entity_id,
            niin,
            TO_JSON(STRUCT_PACK(
                found := TRUE,
                entity_type := 'nsn',
                entity_id := entity_id,
                item_name := COALESCE(item_name, 'Unknown item'),
                nsn := CASE
                    WHEN LENGTH(REGEXP_REPLACE(entity_id, '[^0-9]', '', 'g')) = 13
                    THEN SUBSTR(entity_id, 1, 4) || '-' || SUBSTR(entity_id, 5, 2) || '-' || SUBSTR(entity_id, 7, 3) || '-' || SUBSTR(entity_id, 10, 4)
                    ELSE CAST(NULL AS VARCHAR)
                END,
                niin := niin,
                fsc_code := COALESCE(
                    fsc_code,
                    CASE WHEN LENGTH(entity_id) = 13 THEN LEFT(entity_id, 4) ELSE CAST(NULL AS VARCHAR) END
                ),
                associated_part_number_count := 0,
                part_numbers := []::VARCHAR[],
                part_numbers_hidden := 0,
                associated_supplier_site_count := 0,
                approved_source := CAST(NULL AS STRUCT(cage VARCHAR, vendor VARCHAR, part_number VARCHAR)),
                approved_sources_hidden := 0,
                supplier_sites := []::STRUCT(
                    cage VARCHAR,
                    vendor VARCHAR,
                    part_number VARCHAR,
                    status VARCHAR,
                    is_active_authorized_source BOOLEAN,
                    is_procurement_authorized BOOLEAN
                )[],
                supplier_sites_hidden := 0,
                platforms := []::VARCHAR[],
                platforms_hidden := 0,
                is_multi_platform := FALSE,
                recent_contracts := []::STRUCT(
                    contract_id VARCHAR,
                    action_date VARCHAR,
                    agency VARCHAR,
                    vendor_name VARCHAR,
                    vendor_cage VARCHAR,
                    observed_value DOUBLE
                )[],
                observed_contract_count := 0,
                contracts_hidden := 0,
                logistics_summary := CAST(NULL AS JSON),
                observed_price_summary := CAST(NULL AS JSON),
                opportunity_summary := STRUCT_PACK(
                    active_solicitation_count := 0,
                    next_response_deadline := CAST(NULL AS VARCHAR),
                    next_solicitation_number := CAST(NULL AS VARCHAR),
                    next_quantity := CAST(NULL AS DOUBLE),
                    next_unit_of_issue := CAST(NULL AS VARCHAR),
                    next_solicitation_type_indicator := CAST(NULL AS VARCHAR),
                    next_small_business_set_aside_indicator := CAST(NULL AS VARCHAR),
                    active_solicitations := []::STRUCT(
                        solicitation_number VARCHAR,
                        response_deadline VARCHAR,
                        quantity DOUBLE,
                        unit_of_issue VARCHAR,
                        small_business_set_aside_indicator VARCHAR
                    )[],
                    solicitations_hidden := 0
                ),
                demand_supply_teaser := CAST(NULL AS JSON),
                public_schema_version := {PUBLIC_NSN_SCHEMA_VERSION},
                cache_epoch := '{PUBLIC_NSN_CACHE_EPOCH}'
            )) AS payload_json
        FROM profile
    """.format(
        PUBLIC_NSN_SCHEMA_VERSION=PUBLIC_NSN_SCHEMA_VERSION,
        PUBLIC_NSN_CACHE_EPOCH=PUBLIC_NSN_CACHE_EPOCH,
    ))

    conn.execute("DROP TABLE IF EXISTS public_nsn_reference_supplier_work")
    conn.execute(f"""
        CREATE TABLE public_nsn_reference_supplier_work AS
        WITH grouped AS (
            SELECT
                r.entity_id,
                UPPER(TRIM(CAST(ref.cage AS VARCHAR))) AS cage,
                MAX(NULLIF(TRIM(CAST(ref.vendor_name AS VARCHAR)), '')) AS vendor,
                MIN(NULLIF(TRIM(CAST(ref.part_number AS VARCHAR)), '')) AS part_number,
                BOOL_OR(COALESCE(TRY_CAST(ref.is_active_authorized_source AS BOOLEAN), FALSE)) AS is_active,
                BOOL_OR(COALESCE(TRY_CAST(ref.is_procurement_authorized AS BOOLEAN), FALSE)) AS is_procurement,
                STRING_AGG(DISTINCT NULLIF(TRIM(CAST(ref.rncc_codes AS VARCHAR)), ''), ',') AS rncc_codes,
                STRING_AGG(DISTINCT NULLIF(TRIM(CAST(ref.rnvc_codes AS VARCHAR)), ''), ',') AS rnvc_codes,
                STRING_AGG(DISTINCT NULLIF(TRIM(CAST(ref.rnsc_codes AS VARCHAR)), ''), ',') AS rnsc_codes,
                STRING_AGG(DISTINCT NULLIF(TRIM(CAST(ref.reference_source AS VARCHAR)), ''), ',') AS relationship_source
            FROM public_nsn_released_work r
            INNER JOIN v_nsn_cage_reference ref
                ON LPAD(TRIM(CAST(ref.niin AS VARCHAR)), 9, '0') = r.niin
            WHERE NULLIF(TRIM(CAST(ref.cage AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2
        ), ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY entity_id
                    ORDER BY {supplier_rank_sql},
                             CASE WHEN vendor IS NULL THEN 1 ELSE 0 END, vendor, cage
                ) AS supplier_rank,
                ROW_NUMBER() OVER (
                    PARTITION BY entity_id
                    ORDER BY CASE WHEN is_active THEN 0 ELSE 1 END,
                             CASE WHEN vendor IS NULL THEN 1 ELSE 0 END, vendor, cage
                ) AS approved_rank,
                COUNT(*) OVER (PARTITION BY entity_id) AS supplier_count,
                COUNT(*) FILTER (WHERE is_active) OVER (PARTITION BY entity_id) AS approved_count
            FROM grouped
        )
        SELECT
            entity_id,
            TO_JSON(STRUCT_PACK(
                associated_supplier_site_count := MAX(supplier_count),
                approved_source := LIST_EXTRACT(
                    LIST(STRUCT_PACK(
                        cage := cage,
                        vendor := COALESCE(vendor, 'CAGE ' || cage),
                        part_number := part_number
                    ) ORDER BY approved_rank) FILTER (WHERE is_active),
                    1
                ),
                approved_sources_hidden := GREATEST(MAX(approved_count) - CASE WHEN MAX(approved_count) > 0 THEN 1 ELSE 0 END, 0),
                supplier_sites := LIST(STRUCT_PACK(
                    cage := cage,
                    vendor := COALESCE(vendor, 'CAGE ' || cage),
                    part_number := part_number,
                    status := {supplier_label_sql},
                    is_active_authorized_source := is_active,
                    is_procurement_authorized := is_procurement
                ) ORDER BY supplier_rank) FILTER (WHERE supplier_rank <= {PUBLIC_NSN_SUPPLIER_SITE_LIMIT}),
                supplier_sites_hidden := GREATEST(MAX(supplier_count) - LEAST(MAX(supplier_count), {PUBLIC_NSN_SUPPLIER_SITE_LIMIT}), 0)
            )) AS component_json
        FROM ranked
        GROUP BY 1
    """)
    conn.execute(f"""
        UPDATE public_nsn_profile_next AS target
        SET payload_json = CAST(JSON_MERGE_PATCH(target.payload_json, component.component_json) AS VARCHAR)
        FROM public_nsn_reference_supplier_work AS component
        WHERE target.entity_id = component.entity_id
    """)
    conn.execute("DROP TABLE public_nsn_reference_supplier_work")

    conn.execute("DROP TABLE IF EXISTS public_nsn_reference_part_work")
    conn.execute(f"""
        CREATE TABLE public_nsn_reference_part_work AS
        WITH grouped AS (
            SELECT
                r.entity_id,
                LIST_SORT(
                    LIST(DISTINCT NULLIF(TRIM(CAST(ref.part_number AS VARCHAR)), ''))
                        FILTER (WHERE NULLIF(TRIM(CAST(ref.part_number AS VARCHAR)), '') IS NOT NULL)
                ) AS part_numbers,
                COUNT(DISTINCT NULLIF(TRIM(CAST(ref.part_number AS VARCHAR)), '')) AS part_count
            FROM public_nsn_released_work r
            LEFT JOIN v_nsn_cage_reference ref
                ON LPAD(TRIM(CAST(ref.niin AS VARCHAR)), 9, '0') = r.niin
            GROUP BY 1
        )
        SELECT
            entity_id,
            TO_JSON(STRUCT_PACK(
                associated_part_number_count := part_count,
                part_numbers := LIST_SLICE(part_numbers, 1, {PUBLIC_NSN_PART_NUMBER_LIMIT}),
                part_numbers_hidden := GREATEST(part_count - LEAST(part_count, {PUBLIC_NSN_PART_NUMBER_LIMIT}), 0)
            )) AS component_json
        FROM grouped
        WHERE part_count > 0
    """)
    conn.execute("""
        UPDATE public_nsn_profile_next AS target
        SET payload_json = CAST(JSON_MERGE_PATCH(target.payload_json, component.component_json) AS VARCHAR)
        FROM public_nsn_reference_part_work AS component
        WHERE target.entity_id = component.entity_id
    """)
    conn.execute("DROP TABLE public_nsn_reference_part_work")

    conn.execute("DROP TABLE IF EXISTS public_nsn_financial_supplier_work")
    conn.execute("""
        CREATE TABLE public_nsn_financial_supplier_work AS
        SELECT
            r.entity_id,
            COUNT(DISTINCT NULLIF(UPPER(TRIM(CAST(s.cage AS VARCHAR))), '')) AS supplier_count
        FROM public_nsn_released_work r
        INNER JOIN v_nsn_supplier_lookup s
            ON LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') = r.niin
        GROUP BY 1
    """)
    conn.execute("""
        UPDATE public_nsn_profile_next AS target
        SET payload_json = CAST(JSON_MERGE_PATCH(
            target.payload_json,
            JSON_OBJECT(
                'associated_supplier_site_count',
                GREATEST(
                    COALESCE(TRY_CAST(JSON_EXTRACT(target.payload_json, '$.associated_supplier_site_count') AS BIGINT), 0),
                    component.supplier_count
                ),
                'supplier_sites_hidden',
                GREATEST(
                    GREATEST(
                        COALESCE(TRY_CAST(JSON_EXTRACT(target.payload_json, '$.associated_supplier_site_count') AS BIGINT), 0),
                        component.supplier_count
                    ) - COALESCE(JSON_ARRAY_LENGTH(target.payload_json, '$.supplier_sites'), 0),
                    0
                )
            )
        ) AS VARCHAR)
        FROM public_nsn_financial_supplier_work AS component
        WHERE target.entity_id = component.entity_id
    """)
    conn.execute("DROP TABLE public_nsn_financial_supplier_work")

    conn.execute("DROP TABLE IF EXISTS public_nsn_platform_work")
    conn.execute(f"""
        CREATE TABLE public_nsn_platform_work AS
        WITH grouped AS (
            SELECT
                r.entity_id,
                MAX(NULLIF(TRIM(CAST(s.platform_families AS VARCHAR)), '')) AS platform_families,
                MAX(COALESCE(TRY_CAST(s.platform_count AS BIGINT), 0)) AS platform_count
            FROM public_nsn_released_work r
            LEFT JOIN v_nsn_summary s
                ON LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') = r.niin
            GROUP BY 1
        ), normalized AS (
            SELECT
                entity_id,
                platform_count,
                CASE
                    WHEN platform_families IS NULL THEN []::VARCHAR[]
                    ELSE LIST_TRANSFORM(
                        LIST_FILTER(STR_SPLIT(platform_families, '|'), value -> TRIM(value) <> ''),
                        value -> TRIM(value)
                    )
                END AS platforms
            FROM grouped
        )
        SELECT
            entity_id,
            TO_JSON(STRUCT_PACK(
                platforms := LIST_SLICE(platforms, 1, {PUBLIC_NSN_CONNECTED_PLATFORM_LIMIT}),
                platforms_hidden := GREATEST(platform_count - LEAST(platform_count, {PUBLIC_NSN_CONNECTED_PLATFORM_LIMIT}), 0),
                is_multi_platform := platform_count > 1
            )) AS component_json
        FROM normalized
        WHERE platform_count > 0
    """)
    conn.execute(f"""
        UPDATE public_nsn_profile_next AS target
        SET payload_json = CAST(JSON_MERGE_PATCH(target.payload_json, component.component_json) AS VARCHAR)
        FROM public_nsn_platform_work AS component
        WHERE target.entity_id = component.entity_id
    """)
    conn.execute("DROP TABLE public_nsn_platform_work")

    conn.execute("DROP TABLE IF EXISTS public_nsn_contract_work")
    conn.execute(f"""
        CREATE TABLE public_nsn_contract_work AS
        WITH grouped AS (
            SELECT
                r.entity_id,
                NULLIF(TRIM(CAST(s.contract_id AS VARCHAR)), '') AS contract_id,
                MAX(CAST(s.last_sold AS VARCHAR)) AS action_date,
                MAX(COALESCE(
                    NULLIF(TRIM(CAST(s.sub_agency AS VARCHAR)), ''),
                    NULLIF(TRIM(CAST(s.parent_agency AS VARCHAR)), '')
                )) AS agency,
                MAX(NULLIF(TRIM(CAST(s.vendor AS VARCHAR)), '')) AS vendor_name,
                MAX(NULLIF(UPPER(TRIM(CAST(s.cage AS VARCHAR))), '')) AS vendor_cage,
                SUM(COALESCE(TRY_CAST(s.total_revenue AS DOUBLE), 0)) AS observed_value
            FROM public_nsn_released_work r
            INNER JOIN v_nsn_supplier_lookup s
                ON LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') = r.niin
            WHERE NULLIF(TRIM(CAST(s.contract_id AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2
        ), ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY entity_id
                    ORDER BY TRY_CAST(action_date AS DATE) DESC NULLS LAST, contract_id
                ) AS item_rank,
                COUNT(*) OVER (PARTITION BY entity_id) AS item_count
            FROM grouped
        )
        SELECT
            entity_id,
            TO_JSON(STRUCT_PACK(
                recent_contracts := LIST(STRUCT_PACK(
                    contract_id := contract_id,
                    action_date := action_date,
                    agency := agency,
                    vendor_name := vendor_name,
                    vendor_cage := vendor_cage,
                    observed_value := observed_value
                ) ORDER BY item_rank) FILTER (WHERE item_rank <= {PUBLIC_NSN_RECENT_CONTRACT_LIMIT}),
                observed_contract_count := MAX(item_count),
                contracts_hidden := GREATEST(MAX(item_count) - LEAST(MAX(item_count), {PUBLIC_NSN_RECENT_CONTRACT_LIMIT}), 0)
            )) AS component_json
        FROM ranked
        GROUP BY 1
    """)
    conn.execute("""
        UPDATE public_nsn_profile_next AS target
        SET payload_json = CAST(JSON_MERGE_PATCH(target.payload_json, component.component_json) AS VARCHAR)
        FROM public_nsn_contract_work AS component
        WHERE target.entity_id = component.entity_id
    """)
    conn.execute("DROP TABLE public_nsn_contract_work")

    if _relation_exists(conn, "v_nsn_profile_lookup"):
        conn.execute("DROP TABLE IF EXISTS public_nsn_logistics_work")
        conn.execute("""
            CREATE TABLE public_nsn_logistics_work AS
            SELECT
                r.entity_id,
                TO_JSON(STRUCT_PACK(
                    logistics_summary := STRUCT_PACK(
                        unit_of_issue := MAX(NULLIF(TRIM(CAST(p.unit_of_issue AS VARCHAR)), '')),
                        managing_supply_activity := MAX(NULLIF(TRIM(CAST(p.source_of_supply AS VARCHAR)), '')),
                        source_of_supply := MAX(NULLIF(TRIM(CAST(p.source_of_supply AS VARCHAR)), '')),
                        acquisition_advice_code := MAX(NULLIF(TRIM(CAST(p.acquisition_advice_code AS VARCHAR)), '')),
                        shelf_life_code := MAX(NULLIF(TRIM(CAST(p.shelf_life_code AS VARCHAR)), ''))
                    )
                )) AS component_json
            FROM public_nsn_released_work r
            INNER JOIN v_nsn_profile_lookup p
                ON LPAD(TRIM(CAST(p.niin AS VARCHAR)), 9, '0') = r.niin
            GROUP BY 1
        """)
        conn.execute("""
            UPDATE public_nsn_profile_next AS target
            SET payload_json = CAST(JSON_MERGE_PATCH(target.payload_json, component.component_json) AS VARCHAR)
            FROM public_nsn_logistics_work AS component
            WHERE target.entity_id = component.entity_id
        """)
        conn.execute("DROP TABLE public_nsn_logistics_work")

    if _relation_exists(conn, "v_nsn_supply_state"):
        conn.execute("DROP TABLE IF EXISTS public_nsn_supply_work")
        conn.execute("""
            CREATE TABLE public_nsn_supply_work AS
            SELECT
                r.entity_id,
                TO_JSON(STRUCT_PACK(
                    demand_supply_teaser := STRUCT_PACK(
                        supply_signal := CAST(s.supply_signal AS VARCHAR),
                        total_stock := TRY_CAST(s.total_stock AS DOUBLE),
                        backorder_qty := TRY_CAST(s.backorder_qty AS DOUBLE),
                        annual_demand_quantity := TRY_CAST(s.annual_demand_quantity AS DOUBLE),
                        reorder_point := TRY_CAST(s.reorder_point AS DOUBLE),
                        reorder_point_gap := TRY_CAST(s.reorder_point_gap AS DOUBLE),
                        below_reorder_point := COALESCE(TRY_CAST(s.below_reorder_point AS BOOLEAN), FALSE),
                        forecast_3m_qty := TRY_CAST(s.forecast_3m_qty AS BIGINT),
                        forecast_12m_qty := TRY_CAST(s.forecast_12m_qty AS BIGINT),
                        forecast_stock_cover_months := TRY_CAST(s.forecast_stock_cover_months AS DOUBLE)
                    )
                )) AS component_json
            FROM public_nsn_released_work r
            INNER JOIN v_nsn_supply_state s
                ON LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') = r.niin
        """)
        conn.execute("""
            UPDATE public_nsn_profile_next AS target
            SET payload_json = CAST(JSON_MERGE_PATCH(target.payload_json, component.component_json) AS VARCHAR)
            FROM public_nsn_supply_work AS component
            WHERE target.entity_id = component.entity_id
        """)
        conn.execute("DROP TABLE public_nsn_supply_work")

    if _relation_exists(conn, "v_nsn_price_summary"):
        conn.execute("DROP TABLE IF EXISTS public_nsn_price_work")
        conn.execute("""
            CREATE TABLE public_nsn_price_work AS
            SELECT
                r.entity_id,
                TO_JSON(STRUCT_PACK(
                    observed_price_summary := STRUCT_PACK(
                        latest_net_price := TRY_CAST(p.latest_net_price AS DOUBLE),
                        latest_price_date := CAST(p.latest_price_date AS VARCHAR),
                        latest_unit_of_issue := CAST(p.latest_unit_of_issue AS VARCHAR),
                        trailing_12m_min_price := TRY_CAST(p.trailing_12m_min_price AS DOUBLE),
                        trailing_12m_median_price := TRY_CAST(p.trailing_12m_median_price AS DOUBLE),
                        trailing_12m_max_price := TRY_CAST(p.trailing_12m_max_price AS DOUBLE),
                        trailing_12m_observation_count := TRY_CAST(p.trailing_12m_observation_count AS BIGINT)
                    )
                )) AS component_json
            FROM public_nsn_released_work r
            INNER JOIN v_nsn_price_summary p
                ON LPAD(TRIM(CAST(p.niin AS VARCHAR)), 9, '0') = r.niin
        """)
        conn.execute("""
            UPDATE public_nsn_profile_next AS target
            SET payload_json = CAST(JSON_MERGE_PATCH(target.payload_json, component.component_json) AS VARCHAR)
            FROM public_nsn_price_work AS component
            WHERE target.entity_id = component.entity_id
        """)
        conn.execute("DROP TABLE public_nsn_price_work")

    if _relation_exists(conn, "v_nsn_opportunity_detail"):
        conn.execute("DROP TABLE IF EXISTS public_nsn_opportunity_work")
        conn.execute(f"""
            CREATE TABLE public_nsn_opportunity_work AS
            WITH distinct_rows AS (
                SELECT
                    r.entity_id,
                    NULLIF(TRIM(CAST(o.solicitation_number AS VARCHAR)), '') AS solicitation_number,
                    CAST(o.response_deadline AS VARCHAR) AS response_deadline,
                    TRY_CAST(o.quantity AS DOUBLE) AS quantity,
                    NULLIF(TRIM(CAST(o.unit_of_issue AS VARCHAR)), '') AS unit_of_issue,
                    NULLIF(TRIM(CAST(o.small_business_set_aside_indicator AS VARCHAR)), '') AS small_business_set_aside_indicator,
                    ROW_NUMBER() OVER (
                        PARTITION BY r.entity_id, NULLIF(TRIM(CAST(o.solicitation_number AS VARCHAR)), '')
                        ORDER BY TRY_CAST(o.response_deadline AS DATE), CAST(o.solicitation_line_number AS VARCHAR)
                    ) AS solicitation_row
                FROM public_nsn_released_work r
                INNER JOIN v_nsn_opportunity_detail o
                    ON LPAD(TRIM(CAST(o.niin AS VARCHAR)), 9, '0') = r.niin
                WHERE NULLIF(TRIM(CAST(o.solicitation_number AS VARCHAR)), '') IS NOT NULL
            ), ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY entity_id
                        ORDER BY TRY_CAST(response_deadline AS DATE), solicitation_number
                    ) AS item_rank,
                    COUNT(*) OVER (PARTITION BY entity_id) AS item_count
                FROM distinct_rows
                WHERE solicitation_row = 1
            )
            SELECT
                entity_id,
                TO_JSON(STRUCT_PACK(
                    opportunity_summary := STRUCT_PACK(
                        active_solicitation_count := MAX(item_count),
                        next_response_deadline := MAX(response_deadline) FILTER (WHERE item_rank = 1),
                        next_solicitation_number := MAX(solicitation_number) FILTER (WHERE item_rank = 1),
                        next_quantity := MAX(quantity) FILTER (WHERE item_rank = 1),
                        next_unit_of_issue := MAX(unit_of_issue) FILTER (WHERE item_rank = 1),
                        next_solicitation_type_indicator := CAST(NULL AS VARCHAR),
                        next_small_business_set_aside_indicator := MAX(small_business_set_aside_indicator) FILTER (WHERE item_rank = 1),
                        active_solicitations := LIST(STRUCT_PACK(
                            solicitation_number := solicitation_number,
                            response_deadline := response_deadline,
                            quantity := quantity,
                            unit_of_issue := unit_of_issue,
                            small_business_set_aside_indicator := small_business_set_aside_indicator
                        ) ORDER BY item_rank) FILTER (WHERE item_rank <= {PUBLIC_NSN_ACTIVE_SOLICITATION_LIMIT}),
                        solicitations_hidden := GREATEST(MAX(item_count) - LEAST(MAX(item_count), {PUBLIC_NSN_ACTIVE_SOLICITATION_LIMIT}), 0)
                    )
                )) AS component_json
            FROM ranked
            GROUP BY 1
        """)
        conn.execute("""
            UPDATE public_nsn_profile_next AS target
            SET payload_json = CAST(JSON_MERGE_PATCH(target.payload_json, component.component_json) AS VARCHAR)
            FROM public_nsn_opportunity_work AS component
            WHERE target.entity_id = component.entity_id
        """)
        conn.execute("DROP TABLE public_nsn_opportunity_work")

    conn.execute("DROP TABLE public_nsn_released_work")


def build_public_page_projections(conn) -> None:
    """Build every complete public-page projection supported by this version."""

    build_public_company_profiles(conn)
    build_public_platform_profiles(conn)
    build_public_nsn_profiles(conn)
