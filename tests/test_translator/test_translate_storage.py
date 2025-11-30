import logging

import pandas as pd
import pytest

from ispypsa.translator.mappings import (
    _BATTERY_ATTRIBUTE_ORDER,
    _ECAA_BATTERY_ATTRIBUTES,
    _NEW_ENTRANT_BATTERY_ATTRIBUTES,
)
from ispypsa.translator.storage import (
    _add_new_entrant_battery_build_costs,
    _calculate_annuitised_new_entrant_battery_capital_costs,
    _translate_ecaa_batteries,
    _translate_new_entrant_batteries,
)


def test_translate_ecaa_batteries_empty_input(caplog):
    """Test that empty input returns empty output."""
    ispypsa_tables = {"ecaa_batteries": pd.DataFrame()}
    investment_periods = [2025]

    with caplog.at_level(logging.WARNING):
        result = _translate_ecaa_batteries(ispypsa_tables, investment_periods)

    assert (
        "Templated table 'ecaa_batteries' is empty - no ECAA batteries will be included in this model."
        in caplog.text
    )
    assert result.empty


def test_translate_ecaa_batteries_basic(csv_str_to_df):
    """Test basic functionality of ECAA batteries translation."""
    # Create test input data
    ecaa_batteries_csv = """
    storage_name, sub_region_id, region_id, rez_id, commissioning_date, closure_year, maximum_capacity_mw,  charging_efficiency_%,  discharging_efficiency_%,   storage_duration_hours, fuel_type,  isp_resource_type
    Battery1,     CNSW,          NSW,       ,       2020-01-01,         2040,         100,                  0.95,                   0.95,                       4,                      Battery,    Battery__Storage__4h
    Battery2,     NNSW,          NSW,       ,       2022-07-1,          2042,         200,                  0.90,                   0.90,                       2,                      Battery,    Battery__Storage__2h
    """

    ispypsa_tables = {"ecaa_batteries": csv_str_to_df(ecaa_batteries_csv)}
    investment_periods = [2025]

    result = _translate_ecaa_batteries(ispypsa_tables, investment_periods)

    # Assert expected columns and values
    for templater_name, translated_name in _ECAA_BATTERY_ATTRIBUTES.items():
        assert translated_name in result.columns

    assert "bus" in result.columns  # check this separately (not in mapping)
    assert not result["p_nom_extendable"].any()  # All should be False
    assert (result["capital_cost"] == 0.0).all()  # All should be 0.0

    # Check that no extra columns were added:
    extra_cols = set(result.columns) - set(_BATTERY_ATTRIBUTE_ORDER)
    assert not extra_cols


def test_translate_ecaa_batteries_regional_granularity(csv_str_to_df):
    """Test different regional granularity settings."""
    # Create test input data
    ecaa_batteries_csv = """
    storage_name, sub_region_id, region_id, rez_id, commissioning_date,      closure_year, maximum_capacity_mw,  charging_efficiency_%,  discharging_efficiency_%,   storage_duration_hours, fuel_type,  isp_resource_type
    Battery2,     NNSW,          NSW,       ,       2022-07-1,               2042,         200,                  0.90,                   0.90,                       2,                      Battery,    Battery__Storage__2h
    """

    ispypsa_tables = {"ecaa_batteries": csv_str_to_df(ecaa_batteries_csv)}
    investment_periods = [2025]

    # Test sub_regions setting
    result_sub = _translate_ecaa_batteries(
        ispypsa_tables, investment_periods, regional_granularity="sub_regions"
    )
    assert (result_sub["bus"] == "NNSW").all()

    # Test nem_regions setting
    result_nem = _translate_ecaa_batteries(
        ispypsa_tables, investment_periods, regional_granularity="nem_regions"
    )
    assert (result_nem["bus"] == "NSW").all()

    # Test single_region setting
    result_single = _translate_ecaa_batteries(
        ispypsa_tables, investment_periods, regional_granularity="single_region"
    )
    assert (result_single["bus"] == "NEM").all()


def test_translate_ecaa_batteries_rez_handling(csv_str_to_df):
    """Test REZ handling options."""
    # Create test input with REZ
    ecaa_batteries_csv = """
    storage_name, sub_region_id, region_id, rez_id, commissioning_date, closure_year, maximum_capacity_mw,  charging_efficiency_%,  discharging_efficiency_%,   storage_duration_hours, fuel_type,  isp_resource_type
    Battery1,     CNSW,          NSW,       ,       2020-01-01,         2040,         100,                  0.95,                   0.95,                       4,                      Battery,    Battery__Storage__4h
    Battery2,     CNSW,          NSW,       N3,     2022-07-01,         2042,         200,                  0.90,                   0.90,                       2,                      Battery,    Battery__Storage__2h
    """

    ispypsa_tables = {"ecaa_batteries": csv_str_to_df(ecaa_batteries_csv)}
    investment_periods = [2025]

    # Test discrete_nodes
    result_discrete = _translate_ecaa_batteries(
        ispypsa_tables, investment_periods, rez_handling="discrete_nodes"
    )
    assert set(result_discrete["bus"].values) == set(["N3", "CNSW"])

    # Test attached_to_parent_node
    result_attached = _translate_ecaa_batteries(
        ispypsa_tables, investment_periods, rez_handling="attached_to_parent_node"
    )
    assert (result_attached["bus"] == "CNSW").all()
    assert "N3" not in result_attached["bus"].values


def test_translate_ecaa_batteries_lifetime_calculation(csv_str_to_df):
    """Test lifetime calculation based on closure year."""
    # Create test input
    ecaa_batteries_csv = """
    storage_name, sub_region_id, region_id, rez_id, commissioning_date, closure_year, maximum_capacity_mw,  charging_efficiency_%,  discharging_efficiency_%,   storage_duration_hours, fuel_type,  isp_resource_type
    Battery1,     CNSW,          NSW,       ,       2020-01-01,         2040,         100,                  0.95,                   0.95,                       4,                      Battery,    Battery__Storage__4h
    Battery2,     CNSW,          NSW,       ,       2022-07-01,         2045,         200,                  0.90,                   0.90,                       2,                      Battery,    Battery__Storage__2h
    Battery3,     CNSW,          NSW,       ,       2010-04-01,         2020,         300,                  0.92,                   0.92,                       8,                      Battery,    Battery__Storage__8h
    """

    ispypsa_tables = {"ecaa_batteries": csv_str_to_df(ecaa_batteries_csv)}
    investment_periods = [2025]

    result = _translate_ecaa_batteries(ispypsa_tables, investment_periods)

    # Battery3 should be filtered out (closure_year < investment_period)
    expected_result_csv = """
    name,       bus,    p_nom,      p_nom_extendable,   carrier,    max_hours,  capital_cost,   build_year, lifetime,   efficiency_store,   efficiency_dispatch,    isp_resource_type,      isp_rez_id
    Battery1,   CNSW,   100.0,      False,              Battery,    4,          0.0,            2020,       15,         0.95,               0.95,                   Battery__Storage__4h,
    Battery2,   CNSW,   200.0,      False,              Battery,    2,          0.0,            2023,       20,         0.90,               0.90,                   Battery__Storage__2h,
    """
    expected_result = csv_str_to_df(expected_result_csv)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True).sort_values("name"),
        expected_result.reset_index(drop=True).sort_values("name"),
        check_dtype=False,
    )


def test_translate_new_entrant_batteries_empty_input(caplog):
    """Test that empty input returns empty output."""
    ispypsa_tables = {"new_entrant_batteries": pd.DataFrame()}
    investment_periods = [2025]
    wacc = 0.05

    with caplog.at_level(logging.WARNING):
        result = _translate_new_entrant_batteries(
            ispypsa_tables, investment_periods, wacc
        )

    assert (
        "Templated table 'new_entrant_batteries' is empty - no new entrant batteries will be included in this model."
        in caplog.text
    )
    assert result.empty


def test_translate_new_entrant_batteries_basic(csv_str_to_df, sample_ispypsa_tables):
    """Test basic functionality of new entrant batteries translation."""
    # Create test input data
    new_entrant_batteries_csv = """
    storage_name,          sub_region_id, region_id,       rez_id,     technology_type,                     lifetime,    charging_efficiency_%,  discharging_efficiency_%,   storage_duration_hours, fuel_type,  technology_specific_lcf_%,     connection_cost_$/mw,   fom_$/kw/annum,     isp_resource_type
    NewBattery1,           CNSW,          NSW,             ,           Battery__Storage__(2hrs__storage),   20,          0.90,                   0.90,                       2,                      Battery,    100.0,                         55000.0,                10.0,               Battery__Storage__2h
    """

    ispypsa_tables = {
        "new_entrant_batteries": csv_str_to_df(new_entrant_batteries_csv),
        "new_entrant_build_costs": sample_ispypsa_tables["new_entrant_build_costs"],
    }
    investment_periods = [2025, 2027]
    wacc = 0.05

    result = _translate_new_entrant_batteries(ispypsa_tables, investment_periods, wacc)

    # Assert expected columns and values
    for templater_name, translated_name in _NEW_ENTRANT_BATTERY_ATTRIBUTES.items():
        assert translated_name in result.columns

    assert "bus" in result.columns  # check this separately (it's not in mapping)
    assert result["p_nom_extendable"].all()  # All should be True
    assert len(result) == 2


def test_translate_new_entrant_batteries_regional_granularity(
    csv_str_to_df, sample_ispypsa_tables
):
    """Test different regional granularity settings for new entrant batteries."""
    new_entrant_batteries_csv = """
    storage_name,          sub_region_id, region_id,       rez_id,     technology_type,                     lifetime,    charging_efficiency_%,  discharging_efficiency_%,   storage_duration_hours, fuel_type,  technology_specific_lcf_%,     connection_cost_$/mw,   fom_$/kw/annum,     isp_resource_type
    NewBattery1,           CNSW,          NSW,             ,           Battery__Storage__(2hrs__storage),   20,          0.90,                   0.90,                       2,                      Battery,    100.0,                         55000.0,                10.0,               Battery__Storage__2h
    NewBattery2,           CNSW,          NSW,             ,           Battery__Storage__(2hrs__storage),   20,          0.95,                   0.95,                       2,                      Battery,    100.0,                         55000.0,                7.0,                Battery__Storage__2h
    """

    ispypsa_tables = {
        "new_entrant_batteries": csv_str_to_df(new_entrant_batteries_csv),
        "new_entrant_build_costs": sample_ispypsa_tables["new_entrant_build_costs"],
    }
    investment_periods = [2025]
    wacc = 0.05

    # Test sub_regions setting
    result_sub = _translate_new_entrant_batteries(
        ispypsa_tables, investment_periods, wacc, regional_granularity="sub_regions"
    )
    assert (result_sub["bus"] == "CNSW").all()

    # Test nem_regions setting
    result_nem = _translate_new_entrant_batteries(
        ispypsa_tables, investment_periods, wacc, regional_granularity="nem_regions"
    )
    assert (result_nem["bus"] == "NSW").all()

    # Test single_region setting
    result_single = _translate_new_entrant_batteries(
        ispypsa_tables, investment_periods, wacc, regional_granularity="single_region"
    )
    assert (result_single["bus"] == "NEM").all()


def test_translate_new_entrant_batteries_rez_handling(
    csv_str_to_df, sample_ispypsa_tables
):
    """Test REZ handling options for new entrant batteries."""
    new_entrant_batteries_csv = """
    storage_name,          sub_region_id, region_id,       rez_id,     technology_type,                     lifetime,    charging_efficiency_%,  discharging_efficiency_%,   storage_duration_hours, fuel_type,  technology_specific_lcf_%,     connection_cost_$/mw,   fom_$/kw/annum,     isp_resource_type
    NewBattery1,           CNSW,          NSW,             ,           Battery__Storage__(2hrs__storage),   20,          0.90,                   0.90,                       2,                      Battery,    100.0,                         55000.0,                10.0,               Battery__Storage__2h
    NewBattery2,           CNSW,          NSW,             N3,         Battery__Storage__(2hrs__storage),   20,          0.95,                   0.95,                       2,                      Battery,    100.0,                         55000.0,                7.0,                Battery__Storage__2h
    """

    ispypsa_tables = {
        "new_entrant_batteries": csv_str_to_df(new_entrant_batteries_csv),
        "new_entrant_build_costs": sample_ispypsa_tables["new_entrant_build_costs"],
    }
    investment_periods = [2025]
    wacc = 0.05

    # Test discrete_nodes
    result_discrete = _translate_new_entrant_batteries(
        ispypsa_tables, investment_periods, wacc, rez_handling="discrete_nodes"
    )
    assert set(result_discrete["bus"].values) == set(["N3", "CNSW"])

    # Test attached_to_parent_node
    result_attached = _translate_new_entrant_batteries(
        ispypsa_tables,
        investment_periods,
        wacc,
        regional_granularity="sub_regions",
        rez_handling="attached_to_parent_node",
    )
    assert (result_attached["bus"] == "CNSW").all()
    assert "N3" not in result_attached["bus"].values


def test_add_new_entrant_build_costs(csv_str_to_df, sample_ispypsa_tables):
    """Test that build costs are correctly merged into the batteries table."""
    batteries_csv = """
    storage_name,   technology_type,                     build_year
    Battery_4h,     Battery__Storage__(4hrs__storage),   2025
    Battery_2h,     Battery__Storage__(2hrs__storage),   2027
    """
    batteries_df = csv_str_to_df(batteries_csv)

    build_costs_df = sample_ispypsa_tables["new_entrant_build_costs"]

    # Call the function
    result = _add_new_entrant_battery_build_costs(batteries_df, build_costs_df)

    expected_result_csv = """
    storage_name,   technology_type,                     build_year,    build_cost_$/mw
    Battery_4h,     Battery__Storage__(4hrs__storage),   2025,          3900000
    Battery_2h,     Battery__Storage__(2hrs__storage),   2027,          2700000
    """
    expected_result = csv_str_to_df(expected_result_csv)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True).sort_values("storage_name"),
        expected_result.reset_index(drop=True).sort_values("storage_name"),
        check_dtype=False,
    )


def test_add_new_entrant_build_costs_missing_build_year(
    csv_str_to_df, sample_ispypsa_tables
):
    """Test that the function raises an error when build_year column is missing."""
    batteries_csv = """
    storage_name,     technology_type
    Battery_4hr,      Battery_4hr
    """
    batteries_df = csv_str_to_df(batteries_csv)

    build_costs_df = sample_ispypsa_tables["new_entrant_build_costs"]

    with pytest.raises(
        ValueError,
        match="new_entrant_batteries table must have column 'build_year' to merge in build costs.",
    ):
        _add_new_entrant_battery_build_costs(batteries_df, build_costs_df)


def test_add_new_entrant_build_costs_undefined_build_costs(
    csv_str_to_df, sample_ispypsa_tables
):
    """Test that the function raises an error when build costs are undefined for some batteries."""
    batteries_csv = """
    storage_name,     technology_type,                          build_year
    Battery_4hr,      Battery__Storage__(4hrs__storage),        2028
    Battery_2hr,      Battery__Storage__(2hrs__storage),        2024
    NonExistentType,  NonExistentType,                          2025
    NonExistentYear,  Battery__Storage__(2hrs__storage),        2010
    """
    batteries_df = csv_str_to_df(batteries_csv)

    build_costs_df = sample_ispypsa_tables["new_entrant_build_costs"]
    with pytest.raises(
        ValueError,
        match=r"Undefined build costs for new entrant batteries: \[\('NonExistentType', 2025\), \('NonExistentYear', 2010\)\]",
    ):
        _add_new_entrant_battery_build_costs(batteries_df, build_costs_df)


def test_calculate_annuitised_new_entrant_battery_capital_costs(csv_str_to_df):
    """Test that capital costs are correctly annuitised."""
    batteries_csv = """
    storage_name,       lifetime, fom_$/kw/annum,     connection_cost_$/mw,   build_cost_$/mw, technology_specific_lcf_%
    Battery_2hr_2024,   30,       15.0,               85000,                  1950000,         100.0
    Battery_2hr_2026,   25,       20.0,               90000,                  1400000,         95.0
    """
    batteries_df = csv_str_to_df(batteries_csv)
    wacc = 0.05  # 5% weighted average cost of capital

    result = _calculate_annuitised_new_entrant_battery_capital_costs(batteries_df, wacc)

    expected_result_csv = """
    storage_name,       lifetime, fom_$/kw/annum,     connection_cost_$/mw,   build_cost_$/mw, technology_specific_lcf_%,   capital_cost
    Battery_2hr_2024,   30,       15.0,               85000,                  1950000,         100.0,                       147379.67
    Battery_2hr_2026,   25,       20.0,               90000,                  1400000,         95.0,                        120752.49
    """
    expected_result = csv_str_to_df(expected_result_csv)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True).sort_values("storage_name"),
        expected_result.reset_index(drop=True).sort_values("storage_name"),
        check_dtype=False,
        check_exact=False,
        atol=1e-2,
    )
