from unittest.mock import patch

from ispypsa.iasr_table_caching.local_cache import _build_required_tables


def test_build_required_tables_new_format():
    with patch(
        "ispypsa.iasr_table_caching.local_cache.FEATURE_FLAGS",
        {"use_new_table_format": True},
    ):
        result = _build_required_tables()
    assert result == ["sub_regional_reference_nodes", "renewable_energy_zones"]


def test_build_required_tables_old_format():
    with patch(
        "ispypsa.iasr_table_caching.local_cache.FEATURE_FLAGS",
        {"use_new_table_format": False},
    ):
        result = _build_required_tables()
    assert "sub_regional_reference_nodes" in result
    assert "initial_build_limits" in result
    assert "existing_generators_summary" in result
    assert "battery_properties" in result
    assert "vic_renewable_target_trajectory" in result
    assert "build_costs_current_policies" in result
    assert "expected_closure_years" in result
    assert "maximum_capacity_existing_generators" in result
