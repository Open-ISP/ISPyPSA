from unittest.mock import patch

from ispypsa.iasr_table_caching.local_cache import _build_required_tables


def test_build_required_tables_new_format():
    with patch(
        "ispypsa.iasr_table_caching.local_cache.FEATURE_FLAGS",
        {"use_new_table_format": True},
    ):
        result = _build_required_tables("7.5")
    # Base topology tables come first
    assert result[:4] == [
        "sub_regional_reference_nodes",
        "renewable_energy_zones",
        "flow_path_transfer_capability",
        "initial_transmission_limits",
    ]
    # Augmentation tables discovered from the manifest by prefix
    assert "flow_path_augmentation_options_CQ-NQ" in result
    assert "flow_path_augmentation_costs_step_change_CQ-NQ" in result
    assert "rez_augmentation_options_NSW" in result
    assert "rez_augmentation_costs_step_change_NSW" in result
    # Typo'd table is included so the templater can pick it up while
    # Open-ISP/isp-workbook-parser#80 is open.
    assert "flow_path_augmentation_cost_slower_growth_CNSW-NNSW" in result


def test_build_required_tables_old_format():
    with patch(
        "ispypsa.iasr_table_caching.local_cache.FEATURE_FLAGS",
        {"use_new_table_format": False},
    ):
        result = _build_required_tables("6.0")
    assert "sub_regional_reference_nodes" in result
    assert "initial_build_limits" in result
    assert "existing_generators_summary" in result
    assert "battery_properties" in result
    assert "vic_renewable_target_trajectory" in result
    assert "build_costs_current_policies" in result
    assert "expected_closure_years" in result
    assert "maximum_capacity_existing_generators" in result
