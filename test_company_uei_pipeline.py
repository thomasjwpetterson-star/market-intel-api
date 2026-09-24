from pathlib import Path


def test_profiles_release_retains_authoritative_sam_uei():
    etl_source = Path("run_etl.py").read_text()
    api_source = Path("main.py").read_text()

    assert '"market_intel_silver"."ref_sam_entities"' in etl_source
    assert "REGEXP_LIKE(UPPER(TRIM(unique_entity_id)), '^[A-Z0-9]{12}$')" in etl_source
    assert 'df_profiles["uei"] = df_profiles["uei"].fillna("")' in etl_source
    assert '"uei": _clean_optional_value(row.get(\'uei\'))' in api_source
    assert 'uei = _clean_optional_value(full_profile.get("uei"))' in api_source
    assert '"uei": uei' in api_source
