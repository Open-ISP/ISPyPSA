import re

import pandas as pd
import pytest

from ispypsa.templater.connection_and_build_costs import (
    _add_system_strength_costs,
    _build_vre_cost_rows,
    _calculate_connection_cost_per_mw,
    _drop_excluded_technologies,
    _enforce_numeric_cols,
    _extract_new_entrant_pairs,
    _get_unique_vre_geo_id_rows,
    _map_to_canonical_technologies,
    _merge_region_costs_onto_new_entrant_pairs,
    _normalise_connection_cost_forecast_frame,
    _normalise_system_strength_cost_frame,
    _set_non_ibr_system_strength_cost_to_zero,
    _set_solar_thermal_system_strength_costs_to_zero,
    _template_connection_costs,
    _template_costs_new_entrant_build,
    _template_non_vre_connection_costs,
    _template_vre_connection_costs,
    _warn_nan_connection_costs,
)

_GEO_TECH_COLS = ["geo_id", "technology"]
_CONNECTION_ONLY_COST_COLS = ["geo_id", "technology", "year", "connection_cost"]
_CONNECTION_SYSTEM_STRENGTH_COST_COLS = [
    "geo_id",
    "technology",
    "year",
    "connection_cost",
    "system_strength_cost",
]


# --- _enforce_numeric_cols ---


def test_enforce_numeric_cols(csv_str_to_df):
    # String numbers are converted; non-numeric values are coerced to NaN.
    # Only specified columns are treated.
    df = csv_str_to_df("""
        name,   cost,       capacity
        1,      100,        400
        one,    200.5,      not_a_number
        10^6,   ,           0.03
    """)

    result = _enforce_numeric_cols(df, ["cost", "capacity"])

    expected = csv_str_to_df("""
        name,   cost,   capacity
        1,      100.0,  400.0
        one,    200.5,
        10^6,   ,       0.03
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


# --- _normalise_connection_cost_forecast_frame ---


def test_normalise_connection_cost_forecast_frame(csv_str_to_df):
    # Wide-to-long reshape, FY string -> int year, extra columns dropped (REZ names).
    connection_cost_forecast = csv_str_to_df("""
        geo_id,  REZ Names,       connection_capacity, 2024-25,     2025-26
        N1,      North West NSW,  400,                         730000000,   740000000
        Q9,      Banana,          1800,                        8540000000,  8670000000
    """)

    result = _normalise_connection_cost_forecast_frame(
        connection_cost_forecast,
        id_cols=["geo_id", "connection_capacity"],
    )

    expected = csv_str_to_df("""
        geo_id,  connection_capacity,  year,  connection_cost
        N1,      400.0,                2025,  730000000.0
        N1,      400.0,                2026,  740000000.0
        Q9,      1800.0,               2025,  8540000000.0
        Q9,      1800.0,               2026,  8670000000.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["geo_id", "year"]).reset_index(drop=True),
        expected.sort_values(["geo_id", "year"]).reset_index(drop=True),
        check_exact=False,
        rtol=1e-5,
        check_dtype=False,
    )


def test_normalise_connection_cost_forecast_frame_non_vre_renames(csv_str_to_df):
    # Non-VRE path melts with a 3-key id_cols
    connection_cost_forecast = csv_str_to_df("""
        region_id,  technology,  connection_capacity,  2024-25,   2025-26
        NSW,        CCGT,        400,                  40000000,  42000000
        QLD,        CCGT,        300,                  36000000,  37000000
    """)

    result = _normalise_connection_cost_forecast_frame(
        connection_cost_forecast,
        id_cols=["region_id", "technology", "connection_capacity"],
    )

    expected = csv_str_to_df("""
        region_id,  technology,  connection_capacity,  year,  connection_cost
        NSW,        CCGT,        400.0,                2025,  40000000.0
        NSW,        CCGT,        400.0,                2026,  42000000.0
        QLD,        CCGT,        300.0,                2025,  36000000.0
        QLD,        CCGT,        300.0,                2026,  37000000.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["region_id", "year"]).reset_index(drop=True),
        expected.sort_values(["region_id", "year"]).reset_index(drop=True),
        check_exact=False,
        rtol=1e-5,
        check_dtype=False,
    )


def test_normalise_connection_cost_forecast_frame_preserves_nan_rows(csv_str_to_df):
    # Rows with blank/empty cost values are retained with NaN rather than dropped.
    connection_cost_forecast = csv_str_to_df("""
        geo_id,  connection_capacity,   2024-25
        DN1,     150,
        Q1,      ,                      160000000.0
    """)

    result = _normalise_connection_cost_forecast_frame(
        connection_cost_forecast, id_cols=["geo_id", "connection_capacity"]
    )

    expected = csv_str_to_df("""
        geo_id,     connection_capacity,    year,   connection_cost
        DN1,        150.0,                  2025,
        Q1,         ,                       2025,   160000000.0
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_normalise_connection_cost_forecast_frame_empty_input(csv_str_to_df):
    connection_cost_forecast = csv_str_to_df("""
        geo_id,  REZ Names, connection_capacity, 2024-25,     2025-26
    """)

    result = _normalise_connection_cost_forecast_frame(
        connection_cost_forecast, ["geo_id", "connection_capacity"]
    )

    expected = csv_str_to_df("""
        geo_id,     connection_capacity,    year,   connection_cost
    """)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


# --- _calculate_connection_cost_per_mw ---


def test_calculate_connection_cost_per_mw(csv_str_to_df):
    # Normal division result, NaN cost preserved, connection_capacity col dropped.
    # All three cases in one input DataFrame.
    cost_and_capacity_df = csv_str_to_df("""
        geo_id,  connection_capacity,  year,  connection_cost
        N1,      400,                  2025,  730000000.0
        Q9,      1800,                 2025,  8540000000.0
        R12,     150,                  2025,
    """)

    result = _calculate_connection_cost_per_mw(cost_and_capacity_df)

    expected = csv_str_to_df("""
        geo_id,  year,  connection_cost
        N1,      2025,  1825000.0
        Q9,      2025,  4744444.444
        R12,     2025,
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("geo_id").reset_index(drop=True),
        expected.sort_values("geo_id").reset_index(drop=True),
        check_dtype=False,
        check_exact=False,
        rtol=1e-5,
    )


def test_calculate_connection_cost_per_mw_zero_capacity_gives_na(csv_str_to_df):
    # Division by zero produces inf, which is replaced with pd.NA -> NaN.
    # Division by NaN results NaN.
    cost_and_capacity_df = csv_str_to_df("""
        geo_id,     connection_capacity,  year,     connection_cost
        R13,        0,                    2025,     120000000.0
        V8,         ,                     2025,     10000000.0
    """)

    result = _calculate_connection_cost_per_mw(cost_and_capacity_df)

    expected = csv_str_to_df("""
        geo_id,     year,     connection_cost
        R13,        2025,
        V8,         2025,
    """)

    pd.testing.assert_frame_equal(
        result.sort_values("geo_id").reset_index(drop=True),
        expected.sort_values("geo_id").reset_index(drop=True),
        check_dtype=False,
    )


# --- _warn_nan_connection_costs ---


def test_warn_nan_connection_costs_logs_warning_with_identifiers(csv_str_to_df, caplog):
    # tests that nan connection cost values get logged as expected, including
    # not logging duplicate results.
    costs = csv_str_to_df("""
        geo_id,     year,   connection_cost
        N1,         2025,   182500.0
        DN1,        2025,
        DN1,        2025,
    """)

    with caplog.at_level("WARNING"):
        _warn_nan_connection_costs(costs, id_cols=["geo_id"])

    msg = (
        "NaN connection cost after per-MW calculation for: ['geo_id=DN1, year=2025'] "
        "— no additional connection cost will be applied here"
    )
    assert msg in caplog.text
    assert caplog.messages.count(msg) == 1


def test_warn_nan_connection_costs_no_warning_when_all_costs_valid(
    csv_str_to_df, caplog
):
    costs = csv_str_to_df("""
        geo_id,  year,  connection_cost
        N1,      2025,  182500.0
        Q9,      2025,  474444.4
    """)

    with caplog.at_level("WARNING"):
        _warn_nan_connection_costs(costs, id_cols=["geo_id"])

    assert "NaN connection cost" not in caplog.text


# --- _normalise_system_strength_cost_frame ---


def test_normalise_system_strength_cost_frame(csv_str_to_df):
    # Wide single-row table -> long (year, system_strength_cost).
    # $/kW * 1000 = $/MW conversion applied.
    # FY strings converted to year end int.
    # Non-year label column dropped.
    system_strength_cost_table = csv_str_to_df("""
        label,            2024-25,  2025-26,    2026-27,        Notes
        IBR remediation,  10,       12,         15,             Some extra note
    """)

    result = _normalise_system_strength_cost_frame(system_strength_cost_table)

    expected = csv_str_to_df("""
        year,  system_strength_cost
        2025,  10000.0
        2026,  12000.0
        2027,  15000.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("year").reset_index(drop=True),
        expected.sort_values("year").reset_index(drop=True),
        check_exact=False,
        rtol=1e-5,
        check_dtype=False,
    )


def test_normalise_system_strength_cost_frame_empty_input_df(csv_str_to_df):
    # Should return an empty df with columns ('year', 'system_strength_cost')
    system_strength_cost_table = csv_str_to_df("""
        label,  2024-25,  2025-26,    2026-27
    """)

    result = _normalise_system_strength_cost_frame(system_strength_cost_table)

    expected = csv_str_to_df("""
        year,  system_strength_cost
    """)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


# --- _set_non_ibr_system_strength_cost_to_zero ---


def test_set_non_ibr_system_strength_cost_to_zero(csv_str_to_df):
    # IBR technologies (solar, wind, battery) keep their system strength cost.
    # This function does NOT set Solar Thermal cost to 0.0.
    # Non-IBR technologies get 0.0. Does not change any other cols.
    connection_costs = csv_str_to_df("""
        geo_id,     year,   technology,             connection_cost, system_strength_cost
        S1,         2035,   Large scale Solar PV,   120000.0,        10000.0
        S1,         2035,   Wind,                   130000.0,        10000.0
        S1,         2035,   Battery Storage (1hr),  140000.0,        10000.0
        S1,         2028,   Solar Thermal (16hrs),  150000.0,        10000.0
        SESA,       2035,   Small OCGT,             160000.0,        10000.0
        SESA,       2035,   CCGT,                   170000.0,        10000.0
    """)

    result = _set_non_ibr_system_strength_cost_to_zero(connection_costs)

    expected = csv_str_to_df("""
        geo_id,     year,   technology,             connection_cost, system_strength_cost
        S1,         2035,   Large scale Solar PV,   120000.0,        10000.0
        S1,         2035,   Wind,                   130000.0,        10000.0
        S1,         2035,   Battery Storage (1hr),  140000.0,        10000.0
        S1,         2028,   Solar Thermal (16hrs),  150000.0,        10000.0
        SESA,       2035,   Small OCGT,             160000.0,        0.0
        SESA,       2035,   CCGT,                   170000.0,        0.0
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_exact=False,
    )


# --- _set_solar_thermal_system_strength_costs_to_zero ---


def test_set_solar_thermal_system_strength_costs_to_zero(csv_str_to_df):
    # "Solar Thermal" is zeroed despite containing "solar".
    # Other solar technologies are unchanged.
    connection_costs = csv_str_to_df("""
        geo_id, year,   technology,                 system_strength_cost
        V1,     2028,   Large scale Solar PV,       10000.0
        V5,     2028,   Solar Thermal (16hrs),      10000.0
    """)

    result = _set_solar_thermal_system_strength_costs_to_zero(connection_costs)

    expected = csv_str_to_df("""
        geo_id, year,   technology,                 system_strength_cost
        V1,     2028,   Large scale Solar PV,       10000.0
        V5,     2028,   Solar Thermal (16hrs),      0.0
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_exact=False,
    )


# --- _add_system_strength_costs ---
# Not tested: interaction with NaN connection_cost values — system strength is
# applied independently of connection_cost, so NaN connection_cost rows simply
# pass through with their system_strength_cost value unchanged.


def test_add_system_strength_costs(csv_str_to_df):
    # IBR gets system strength cost, non-IBR gets 0.0, solar thermal gets 0.0.
    # Extra years present in system_strength_costs dropped in the merge.
    connection_costs = csv_str_to_df("""
        geo_id,  technology,            year,  connection_cost
        N1,      Large scale Solar PV,  2025,  182500.0
        N1,      Wind,                  2025,  182500.0
        QLD,     Small OCGT,            2025,  500000.0
        TAS,     Solar Thermal,         2025,  100000.0
    """)
    system_strength_costs = csv_str_to_df("""
        year,   system_strength_cost
        2025,   10000.0
        2026,   10000.0
    """)

    result = _add_system_strength_costs(connection_costs, system_strength_costs)

    expected = csv_str_to_df("""
        geo_id,  technology,            year,  connection_cost,  system_strength_cost
        N1,      Large scale Solar PV,  2025,  182500.0,         10000.0
        N1,      Wind,                  2025,  182500.0,         10000.0
        QLD,     Small OCGT,            2025,  500000.0,         0.0
        TAS,     Solar Thermal,         2025,  100000.0,         0.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["geo_id", "technology"]).reset_index(drop=True),
        expected.sort_values(["geo_id", "technology"]).reset_index(drop=True),
        check_exact=False,
        rtol=1e-5,
    )


def test_add_system_strength_costs_year_not_in_system_strength_gives_nan(
    csv_str_to_df,
):
    # Year in connection_costs but absent from system_strength_costs -> NaN
    connection_costs = csv_str_to_df("""
        geo_id,  technology,  year,  connection_cost
        N1,      Wind,        2025,  182500.0
        N1,      Wind,        2027,  200000.0
    """)
    system_strength_costs = csv_str_to_df("""
        year,  system_strength_cost
        2026,  10000.0
        2027,  10000.0
    """)

    result = _add_system_strength_costs(connection_costs, system_strength_costs)

    expected = csv_str_to_df("""
        geo_id,  technology,  year,  connection_cost,   system_strength_cost
        N1,      Wind,        2025,  182500.0,
        N1,      Wind,        2027,  200000.0,          10000.0
    """)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_add_system_strength_costs_empty_inputs(csv_str_to_df):
    # Empty connection_costs -> empty output with correct columns.
    system_strength_costs = csv_str_to_df("""
        year,  system_strength_cost
        2025,  10000.0
    """)

    result_empty_costs = _add_system_strength_costs(
        pd.DataFrame(columns=_CONNECTION_ONLY_COST_COLS),
        system_strength_costs,
    )
    expected_empty_cc = pd.DataFrame(columns=_CONNECTION_SYSTEM_STRENGTH_COST_COLS)
    pd.testing.assert_frame_equal(
        result_empty_costs.reset_index(drop=True),
        expected_empty_cc.reset_index(drop=True),
        check_dtype=False,
    )

    # Empty system_strength_costs -> all system_strength_cost values are NaN.
    empty_system_strength_costs = csv_str_to_df("""
        year,  system_strength_cost
    """)
    connection_costs = csv_str_to_df("""
        geo_id,  technology,  year,  connection_cost
        N1,      Wind,        2025,  182500.0
    """)

    result_empty_ss = _add_system_strength_costs(
        connection_costs, empty_system_strength_costs
    )
    expected_empty_ss = csv_str_to_df("""
        geo_id,  technology,  year,  connection_cost,  system_strength_cost
        N1,      Wind,        2025,  182500.0
    """)
    pd.testing.assert_frame_equal(
        result_empty_ss.reset_index(drop=True),
        expected_empty_ss.reset_index(drop=True),
        check_dtype=False,
    )


# --- _get_unique_vre_geo_id_rows ---


def test_get_unique_vre_geo_id_rows(csv_str_to_df):
    # Non-VRE rows (Pumped Hydro) are excluded. 'Distributed' rows excluded
    # (including 'Distributed Resources Solar' despite containing 'solar').
    # Input is assumed already deduplicated upstream — this function does not dedup.
    canonical_tech_geo_ids = csv_str_to_df("""
        geo_id,  technology
        N1,      Large scale Solar PV
        N1,      Wind
        NNSW,    Pumped Hydro (24hrs storage)
        Q9,      Large scale Solar PV
        V8,      Wind - offshore (fixed)
        GG,      Distributed Resources Batteries
        SESA,    Distributed Resources Solar
    """)

    result = _get_unique_vre_geo_id_rows(canonical_tech_geo_ids)

    expected = csv_str_to_df("""
        geo_id,  technology
        N1,      Large scale Solar PV
        N1,      Wind
        Q9,      Large scale Solar PV
        V8,      Wind - offshore (fixed)
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(_GEO_TECH_COLS).reset_index(drop=True),
        expected.sort_values(_GEO_TECH_COLS).reset_index(drop=True),
    )


def test_get_unique_vre_geo_id_rows_empty_input():
    result = _get_unique_vre_geo_id_rows(pd.DataFrame(columns=_GEO_TECH_COLS))
    expected = pd.DataFrame(columns=_GEO_TECH_COLS)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


# --- _build_vre_cost_rows ---


def test_build_vre_cost_rows(csv_str_to_df):
    # N1 and Q9 (in both tables) get cost values for all years.
    # V8 (in vre_technologies but NOT costs_per_mw) gets NaN for all years.
    # S1 (in costs_per_mw but NOT vre_technologies) is dropped.
    costs_per_mw = csv_str_to_df("""
        geo_id,  year,  connection_cost
        N1,      2025,  182500.0
        N1,      2026,  185000.0
        Q9,      2025,  474444.4
        Q9,      2026,  481666.7
        S1,      2025,  300000.0
    """)
    vre_technologies = csv_str_to_df("""
        geo_id,  technology
        N1,      Large scale Solar PV
        N1,      Wind
        Q9,      Large scale Solar PV
        V8,      Wind - offshore (fixed)
    """)

    result = _build_vre_cost_rows(costs_per_mw, vre_technologies)

    expected = csv_str_to_df("""
        geo_id,  technology,                    year,  connection_cost
        N1,      Wind,                          2025,  182500.0
        N1,      Wind,                          2026,  185000.0
        N1,      Large scale Solar PV,          2025,  182500.0
        N1,      Large scale Solar PV,          2026,  185000.0
        Q9,      Large scale Solar PV,          2025,  474444.4
        Q9,      Large scale Solar PV,          2026,  481666.7
        V8,      Wind - offshore (fixed),       2025,
        V8,      Wind - offshore (fixed),       2026,
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["geo_id", "technology", "year"]).reset_index(drop=True),
        expected.sort_values(["geo_id", "technology", "year"]).reset_index(drop=True),
        check_exact=False,
        rtol=1e-5,
        check_dtype=False,
    )


def test_build_vre_cost_rows_empty_inputs(csv_str_to_df):
    # Empty vre_technologies -> no (tech, year) combinations -> empty output.
    # Empty costs_per_mw -> no years to cross-join -> empty output.
    costs_per_mw = csv_str_to_df("""
        geo_id,  year,  connection_cost
        N1,      2025,  182500.0
    """)
    vre_technologies = csv_str_to_df("""
        geo_id,  technology
        N1,      Wind
    """)
    expected_empty_result = pd.DataFrame(columns=_CONNECTION_ONLY_COST_COLS)

    result_empty_techs = _build_vre_cost_rows(
        costs_per_mw, pd.DataFrame(columns=_GEO_TECH_COLS)
    )

    pd.testing.assert_frame_equal(
        result_empty_techs.reset_index(drop=True),
        expected_empty_result.reset_index(drop=True),
        check_dtype=False,
    )

    result_empty_costs = _build_vre_cost_rows(
        pd.DataFrame(columns=["geo_id", "year", "connection_cost"]), vre_technologies
    )
    pd.testing.assert_frame_equal(
        result_empty_costs.reset_index(drop=True),
        expected_empty_result.reset_index(drop=True),
        check_dtype=False,
    )

    result_all_empty = _build_vre_cost_rows(
        pd.DataFrame(columns=["geo_id", "year", "connection_cost"]),
        pd.DataFrame(columns=_GEO_TECH_COLS),
    )
    pd.testing.assert_frame_equal(
        result_all_empty.reset_index(drop=True),
        expected_empty_result.reset_index(drop=True),
        check_dtype=False,
    )


# --- _template_vre_connection_costs (integration) ---
# Wiring test only — detailed content covered by unit tests above.
# Not tested: scenario fuzzy matching (covered by _filter_table_by_isp_scenario
# tests); NaN warning log (covered by _warn_nan_connection_costs tests).


def test_template_vre_connection_costs(csv_str_to_df, caplog):
    iasr_tables = {
        "connection_cost_forecast_wind_and_solar": csv_str_to_df("""
            REZ ID,  REZ names,         Scenario,     2024-25,    2025-26
            N1,      North West NSW,    Step Change, 73000000,   74000000
            Q9,      Banana,            Step Change, 854000000,  867000000
        """),
        "connection_costs_for_wind_and_solar": csv_str_to_df("""
            REZ ID,     Connection capacity (MVA)
            N1,         400
            Q9,         1800
            V8,
        """),
    }
    new_entrant_pairs = csv_str_to_df("""
        geo_id,     technology
        N1,         Wind
        N1,         Large scale Solar PV
        Q9,         Large scale Solar PV
        V8,         Wind - offshore (fixed)
        NNSW,       CCGT
        SQ,         Pumped Hydro
    """)

    with caplog.at_level("WARNING"):
        result = _template_vre_connection_costs(
            iasr_tables, "Step Change", new_entrant_pairs
        )

    # No warning raised for offshore REZ NaN cost (row added after NaNs checked)
    assert "NaN connection cost" not in caplog.text

    # Columns correct; 3 (geo_id, tech) pairs × 2 years + 2 offshore NaN rows = 8
    assert list(result.columns) == _CONNECTION_ONLY_COST_COLS
    assert len(result) == 8  # detailed content covered by unit tests above


def test_template_vre_connection_costs_all_empty_inputs(csv_str_to_df):
    # test that even with all empty inputs we get an empty df with expected columns.
    connection_cost_forecast_vre = csv_str_to_df("""
        REZ ID,  REZ names,       Scenario,     2024-25,    2025-26
    """)
    connection_costs_for_vre = csv_str_to_df("""
        REZ ID,  Connection capacity (MVA)
    """)

    result = _template_vre_connection_costs(
        connection_cost_forecast_vre,
        connection_costs_for_vre,
        "Step Change",
        pd.DataFrame(columns=_GEO_TECH_COLS),
    )

    assert list(result.columns) == _CONNECTION_ONLY_COST_COLS
    assert result.empty


def test_template_vre_connection_costs_some_empty_inputs(csv_str_to_df):
    # test that even with combinations of empty/full inputs we get an empty df
    # with expected columns.
    connection_cost_forecast_vre = csv_str_to_df("""
        REZ ID, REZ names,          Scenario,     2024-25,    2025-26
        N1,     North West NSW,     Step Change, 73000000,   74000000
        Q9,     Banana,             Step Change, 854000000,  867000000
    """)
    empty_capacity = csv_str_to_df("""
        REZ ID,  Connection capacity (MVA)
    """)

    result = _template_vre_connection_costs(
        connection_cost_forecast_vre,
        empty_capacity,
        "Step Change",
        pd.DataFrame(columns=_GEO_TECH_COLS),
    )

    assert list(result.columns) == _CONNECTION_ONLY_COST_COLS
    assert result.empty

    # non-empty vre_technologies_by_geo_id input -> empty output
    empty_forecast = csv_str_to_df("""
        REZ ID,  REZ names,       Scenario,     2024-25,    2025-26
    """)
    vre_technologies_by_geo_id = csv_str_to_df("""
        geo_id,  technology
        N1,      Wind
    """)

    result = _template_vre_connection_costs(
        empty_forecast,
        empty_capacity,
        "Step Change",
        vre_technologies_by_geo_id,
    )

    assert list(result.columns) == _CONNECTION_ONLY_COST_COLS
    assert result.empty


def test_template_vre_connection_costs_empty_capacity_gives_nan_costs(csv_str_to_df):
    # Non-empty forecast and technologies but empty capacity -> NaN connection_cost
    connection_cost_forecast_vre = csv_str_to_df("""
        REZ ID, REZ names,          Scenario,     2024-25,    2025-26
        N1,     North West NSW,     Step Change, 73000000,   74000000
        Q9,     Banana,             Step Change, 854000000,  867000000
    """)
    empty_capacity = csv_str_to_df("""
        REZ ID,  Connection capacity (MVA)
    """)
    vre_technologies_by_geo_id = csv_str_to_df("""
        geo_id,  technology
        N1,      Wind
    """)

    result = _template_vre_connection_costs(
        connection_cost_forecast_vre,
        empty_capacity,
        "Step Change",
        vre_technologies_by_geo_id,
    )
    expected = csv_str_to_df("""
        geo_id,     technology,     year,   connection_cost
        N1,         Wind,           2025,
        N1,         Wind,           2026,
    """)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


# --- _extract_new_entrant_pairs ---


def test_extract_new_entrant_pairs(csv_str_to_df):
    # The two CNSW OCGT options share one pair, and BOTN takes its own technology
    # rather than the generic PHES 24h it's listed as (so TAS keeps both).
    new_entrants_summary = csv_str_to_df("""
        IASR ID / DLT names,   Technology Type,               Sub-region,  REZ ID
        N1 Wind,               Wind,                          NNSW,        N1
        CNSW OCGT Small,       OCGT (small GT),               CNSW,        Not Applicable
        WOO OCGT Small,        OCGT (small GT),               CNSW,        Not Applicable
        TAS Pumped Hydro,      Pumped Hydro (24hrs storage),  TAS,         Not Applicable
        BOTN - Cethana - 20h,  Pumped Hydro (24hrs storage),  TAS,         Not Applicable
    """)

    result = _extract_new_entrant_pairs(new_entrants_summary)

    expected = csv_str_to_df("""
        geo_id,  technology
        N1,      Wind
        CNSW,    OCGT (small GT)
        TAS,     Pumped Hydro (24hrs storage)
        TAS,     BOTN - Cethana
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(_GEO_TECH_COLS).reset_index(drop=True),
        expected.sort_values(_GEO_TECH_COLS).reset_index(drop=True),
    )


# --- _merge_region_costs_onto_new_entrant_pairs ---


def test_merge_region_costs_onto_new_entrant_pairs(csv_str_to_df):
    # QLD CCGT has no new entrant and N1 Wind has no regional cost, so both drop out.
    # S1 is a REZ, so it takes SA's cost.
    region_costs = csv_str_to_df("""
        region_id,  technology,  year,  connection_cost
        NSW,        CCGT,        2025,  100000.0
        QLD,        CCGT,        2025,  130000.0
        SA,         Battery,     2025,  90000.0
    """)
    new_entrant_pairs = csv_str_to_df("""
        geo_id,  technology
        CNSW,    CCGT
        SNW,     CCGT
        S1,      Battery
        N1,      Wind
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
        SNW,     subregion,  NSW
        S1,      rez,        SA
        N1,      rez,        NSW
    """)

    result = _merge_region_costs_onto_new_entrant_pairs(
        region_costs, new_entrant_pairs, sub_regional_geography
    )

    expected = csv_str_to_df("""
        geo_id,  technology,  year,  connection_cost
        CNSW,    CCGT,        2025,  100000.0
        SNW,     CCGT,        2025,  100000.0
        S1,      Battery,     2025,  90000.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(_GEO_TECH_COLS).reset_index(drop=True),
        expected.sort_values(_GEO_TECH_COLS).reset_index(drop=True),
    )


# --- _template_non_vre_connection_costs ---


@pytest.mark.parametrize(
    "regional_granularity, expected_csv",
    [
        (
            "sub_regions",
            """
            geo_id,  technology,            year,  connection_cost
            CNSW,    CCGT,                  2025,  100000.0
            SNW,     CCGT,                  2025,  100000.0
            NQ,      CCGT,                  2025,  130000.0
            S1,      Battery Storage (4h),  2025,  90000.0
            """,
        ),
        (
            "nem_regions",
            """
            geo_id,  technology,            year,  connection_cost
            NSW,     CCGT,                  2025,  100000.0
            QLD,     CCGT,                  2025,  130000.0
            S1,      Battery Storage (4h),  2025,  90000.0
            """,
        ),
        (
            "single_region",
            # CNSW, SNW and NQ each count once: mean(100000, 100000, 130000)
            """
            geo_id,  technology,            year,  connection_cost
            NEM,     CCGT,                  2025,  110000.0
            S1,      Battery Storage (4h),  2025,  90000.0
            """,
        ),
    ],
)
def test_template_non_vre_connection_costs(
    csv_str_to_df, regional_granularity, expected_csv
):
    # NSW CCGT = 40000000 / 400 = 100000, QLD CCGT = 39000000 / 300 = 130000 and
    # SA battery = 27000000 / 300 = 90000. VIC CCGT has no new entrant and N1 Wind
    # is VRE, so neither appears at any granularity. S1 is a REZ, so its geo_id isn't
    # remapped at any granularity.
    iasr_tables = {
        "connection_cost_forecast_other": csv_str_to_df("""
            Generator Type,        Region,  Scenario,     2024-25
            CCGT,                  NSW,     Step Change,  40000000
            CCGT,                  QLD,     Step Change,  39000000
            CCGT,                  VIC,     Step Change,  20000000
            Battery Storage (4h),  SA,      Step Change,  27000000
        """),
        "connection_capacity_non_vre": csv_str_to_df("""
            Region,  Generator Type,        Connection capacity (MVA)
            NSW,     CCGT,                  400
            QLD,     CCGT,                  300
            VIC,     CCGT,                  250
            SA,      Battery Storage (4h),  300
        """),
    }
    new_entrant_pairs = csv_str_to_df("""
        geo_id,  technology
        CNSW,    CCGT
        SNW,     CCGT
        NQ,      CCGT
        S1,      Battery Storage (4h)
        N1,      Wind
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
        SNW,     subregion,  NSW
        NQ,      subregion,  QLD
        S1,      rez,        SA
        N1,      rez,        NSW
    """)

    result = _template_non_vre_connection_costs(
        iasr_tables,
        "Step Change",
        new_entrant_pairs,
        regional_granularity,
        sub_regional_geography,
    )

    expected = csv_str_to_df(expected_csv)
    pd.testing.assert_frame_equal(
        result.sort_values(_GEO_TECH_COLS).reset_index(drop=True),
        expected.sort_values(_GEO_TECH_COLS).reset_index(drop=True),
    )


def test_template_non_vre_connection_costs_empty_capacity_gives_nan_costs(
    csv_str_to_df,
):
    # Non-empty forecast, technologies and geography but empty capacity
    # -> present but NaN connection_cost
    forecast = csv_str_to_df("""
        Generator Type,        Region,     Scenario,       2024-25,    2025-26
        CCGT,                  NSW,        Step Change,   40000000,   41000000
    """)
    empty_capacity = pd.DataFrame(
        columns=["Region", "Generator Type", "Connection capacity (MVA)"]
    )
    pairs = csv_str_to_df("""
        geo_id,     technology
        CNSW,       CCGT
    """)
    geography = csv_str_to_df("""
        geo_id,     geo_type,   region_id
        CNSW,       subregion,  NSW
    """)
    result_nans = _template_non_vre_connection_costs(
        forecast,
        empty_capacity,
        "Step Change",
        pairs,
        "sub_regions",
        geography,
    )
    assert list(result_nans.columns) == _CONNECTION_ONLY_COST_COLS
    assert len(result_nans) == 2  # 2 years, 1 tech, 1 subregion
    assert all(result_nans["connection_cost"].isna())


# --- _template_connection_costs (integration) ---
# Wiring test only — detailed content covered by unit tests above.


def test_template_connection_costs(csv_str_to_df):
    # Wiring test: exercises the VRE path and the non-VRE merge-then-collapse path
    # (including a REZ battery) together. Detailed content is covered by the
    # per-helper unit tests above, so we assert column set + row count only.
    iasr_tables = {
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,  Technology Type,       Sub-region,  REZ ID
            N1 Wind,              Wind,                  NNSW,        N1
            CNSW CCGT,            CCGT,                  CNSW,        Not Applicable
            CNSW Battery,         Battery Storage (4h),  CNSW,        Not Applicable
            N1 Battery,           Battery Storage (4h),  NNSW,        N1
        """),
        "connection_cost_forecast_wind_and_solar": csv_str_to_df("""
            REZ ID,  Scenario,      2024-25
            N1,      Step Change,  73000000
        """),
        "connection_costs_for_wind_and_solar": csv_str_to_df("""
            REZ ID,  Connection capacity (MVA)
            N1,      400
        """),
        "connection_cost_forecast_other": csv_str_to_df("""
            Generator Type,         Region,  Scenario,      2024-25
            CCGT,                   NSW,     Step Change,  40000000
            Battery Storage (4h),   NSW,     Step Change,  20000000
        """),
        "connection_capacity_non_vre": csv_str_to_df("""
            Region,  Generator Type,         Connection capacity (MVA)
            NSW,     CCGT,                   400
            NSW,     Battery Storage (4h),   400
        """),
        "efficient_level_of_system_strength_cost": csv_str_to_df("""
            label,  2024-25
            IBR,    10
        """),
    }
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
        N1,      rez,        NSW
    """)

    result = _template_connection_costs(
        iasr_tables, "Step Change", "nem_regions", sub_regional_geography
    )

    # 1 VRE (N1 Wind) + 2 non-VRE collapsed to NSW (CCGT, Battery)
    # + 1 REZ battery (N1, inheriting the NSW cost) = 4 rows.
    assert list(result.columns) == _CONNECTION_SYSTEM_STRENGTH_COST_COLS
    assert len(result) == 4


def test_template_connection_costs_empty_inputs_give_empty_output(csv_str_to_df):
    # All empty inputs -> empty output with all expected columns present.
    iasr_tables = {
        "connection_cost_forecast_wind_and_solar": csv_str_to_df("""
            REZ ID,  Scenario,  2024-25
        """),
        "connection_costs_for_wind_and_solar": csv_str_to_df("""
            REZ ID,  Connection capacity (MVA)
        """),
        "connection_cost_forecast_other": csv_str_to_df("""
            Generator Type,  Region,  Scenario,  2024-25
        """),
        "connection_capacity_non_vre": csv_str_to_df("""
            Region,  Generator Type,  Connection capacity (MVA)
        """),
        "efficient_level_of_system_strength_cost": csv_str_to_df("""
            label,  2024-25
        """),
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,  Technology Type,  Sub-region,  REZ ID
        """),
    }

    result = _template_connection_costs(
        iasr_tables,
        "Step Change",
        "sub_regions",
        pd.DataFrame(columns=["geo_id", "geo_type", "region_id"]),
    )

    assert list(result.columns) == _CONNECTION_SYSTEM_STRENGTH_COST_COLS
    assert result.empty


# --- _drop_excluded_technologies ---


def test_drop_excluded_technologies_logs_dropped_technologies(csv_str_to_df, caplog):
    # Alkaline Electrolyser repeats (as with one row per region) but is logged once.
    df = csv_str_to_df("""
        technology,                 region
        CCGT,                       NSW
        Alkaline Electrolyser,      NSW
        Alkaline Electrolyser,      QLD
    """)

    with caplog.at_level("INFO"):
        result = _drop_excluded_technologies(
            df, {"Alkaline Electrolyser"}, "templating test costs"
        )

    expected = csv_str_to_df("""
        technology, region
        CCGT,       NSW
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)
    msg = (
        "KNOWN EXCLUSION: technology(s) ['Alkaline Electrolyser'] excluded while "
        "templating test costs."
    )
    assert caplog.messages.count(msg) == 1


def test_drop_excluded_technologies_no_log_when_nothing_excluded(csv_str_to_df, caplog):
    df = csv_str_to_df("""
        technology
        CCGT
        Wind
    """)

    with caplog.at_level("INFO"):
        result = _drop_excluded_technologies(
            df, {"Alkaline Electrolyser"}, "templating test costs"
        )

    pd.testing.assert_frame_equal(result, df)
    assert "KNOWN EXCLUSION" not in caplog.text


# --- _map_to_canonical_technologies ---


def test_map_to_canonical_technologies(caplog):
    # Solar Thermal repeats (as with one row per region) but is logged once.
    technologies = pd.Series(
        [
            "Wind",
            "Solar Thermal (16hrs Storage)",  # upper case 'Storage'
            "Solar Thermal (16hrs Storage)",  # same
        ],
        name="technology",
    )

    with caplog.at_level("WARNING"):
        result = _map_to_canonical_technologies(
            technologies,
            {"Wind", "Solar Thermal (16hrs storage)"},  # lower case 'storage'
            "templating test costs",
        )

    expected = pd.Series(
        [
            "Wind",
            "Solar Thermal (16hrs storage)",  # mapped to lower case
            "Solar Thermal (16hrs storage)",  # same
        ],
        name="technology",
    )
    pd.testing.assert_series_equal(result, expected)
    msg = (
        "FUZZY MATCH: no exact match for 'Solar Thermal (16hrs Storage)' found whilst "
        "templating test costs. Replaced by best fuzzy match "
        "'Solar Thermal (16hrs storage)'."
    )
    assert caplog.messages.count(msg) == 1


def test_map_to_canonical_technologies_no_warning_when_all_match(caplog):
    technologies = pd.Series(["Wind", "CCGT"], name="technology")

    with caplog.at_level("WARNING"):
        result = _map_to_canonical_technologies(
            technologies, {"Wind", "CCGT"}, "templating test costs"
        )

    pd.testing.assert_series_equal(result, technologies)
    assert "FUZZY MATCH" not in caplog.text


def test_map_to_canonical_technologies_raises_when_no_fuzzy_match():
    # Solar Thermal has a fuzzy match, so only Alkaline Electrolyser is listed.
    # This function only raises in one direction: technologies <= canonical, so
    # canonical "Wind" having no entry in technologies doesn't register here.
    technologies = pd.Series(
        ["Solar Thermal (16hrs Storage)", "Alkaline Electrolyser"],
        name="technology",
    )

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Could not fuzzy match to an allowed value whilst templating test costs: "
            "['Alkaline Electrolyser']"
        ),
    ):
        _map_to_canonical_technologies(
            technologies,
            {"Wind", "Solar Thermal (16hrs storage)"},
            "templating test costs",
        )


# --- _template_costs_new_entrant_build ---


def test_template_costs_new_entrant_build(csv_str_to_df):
    iasr_tables = {
        "build_costs": csv_str_to_df("""
            Technology,                     IASR Scenario,  2024-25,  2025-26
            Wind,                           Step Change,    3412,     3266
            Wind,                           Slower Growth,  3500,     3400
            Solar Thermal (16hrs Storage),  Step Change,    6892,     6769
            BOTN - Cethana,                 Step Change,    3665,     3595
            Alkaline Electrolyser,          Step Change,    2755,     2558
        """),
        "new_entrants_summary": csv_str_to_df("""
            IASR ID / DLT names,   Technology Type,               Sub-region,  REZ ID
            N1 Wind,               Wind,                          NNSW,        N1
            Q4 Solar Thermal,      Solar Thermal (16hrs storage), CQ,          Q4
            BOTN - Cethana - 20h,  Pumped Hydro (24hrs storage),  TAS,         Not Applicable
        """),
    }

    result = _template_costs_new_entrant_build(iasr_tables, "Step Change")

    # The Slower Growth Wind row is filtered out and Alkaline Electrolyser is excluded.
    # Solar Thermal takes the summary's spelling, BOTN - Cethana matches through the
    # BOTN override on the summary, and costs go from $/kW to $/MW (x 1000).
    expected = csv_str_to_df("""
        technology,                     year,  cost
        Wind,                           2025,  3412000
        Wind,                           2026,  3266000
        Solar Thermal (16hrs storage),  2025,  6892000
        Solar Thermal (16hrs storage),  2026,  6769000
        BOTN - Cethana,                 2025,  3665000
        BOTN - Cethana,                 2026,  3595000
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["technology", "year"]).reset_index(drop=True),
        expected.sort_values(["technology", "year"]).reset_index(drop=True),
    )
