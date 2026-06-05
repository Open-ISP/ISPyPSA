import pandas as pd
import pytest

from ispypsa.templater.connection_and_build_costs import (
    _NON_VRE_COLUMN_RENAMES,
    _VRE_COLUMN_RENAMES,
    _average_connection_costs_across_regions,
    _build_vre_cost_rows,
    _calculate_connection_cost_per_mw,
    _canonicalise_non_vre_technologies,
    _create_non_vre_rez_cost_rows,
    _enforce_numeric_cols,
    _expand_non_vre_connection_costs_to_subregions,
    _filter_connection_costs_by_regional_granularity,
    _filter_table_by_isp_scenario,
    _get_canon_technology_and_geo_id_pairs,
    _get_unique_vre_geo_id_rows,
    _merge_and_filter_system_strength_costs,
    _merge_connection_cost_and_capacity_frames,
    _normalise_connection_cost_forecast_frame,
    _normalise_system_strength_cost_frame,
    _set_non_ibr_system_strength_cost_to_zero,
    _set_solar_thermal_system_strength_costs_to_zero,
    _template_connection_costs,
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


# --- _filter_table_by_isp_scenario ---


def test_filter_table_by_isp_scenario(csv_str_to_df):
    # Matching rows kept, non-matching dropped, Scenario col dropped.
    # NaN values in non-Scenario columns are preserved. Fuzzy matching
    # applied correctly so small typos are fixed before filtering.
    table = csv_str_to_df("""
        Scenario,       REZ__ID,    value
        Step__Change,   N1,         100
        step__change,   N2,         150
        Step__Change,   N3,
        Stop__Change,   N4,         175
        Slower__Growth, N1,         200
    """)

    result = _filter_table_by_isp_scenario(table, "Step Change", "Scenario", "test")

    expected = csv_str_to_df("""
        REZ__ID,    value
        N1,         100
        N2,         150
        N3,
        N4,         175
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_filter_table_by_isp_scenario_no_matching_rows(csv_str_to_df, caplog):
    # No rows have matching "Scenario" values - return empty df with non-'scenario'
    # columns.
    table = csv_str_to_df("""
        Scenario,       REZ__ID,    value
        Step__Change,   N1,         100
        Slower__Growth, N1,         200
    """)

    with caplog.at_level("WARNING"):
        result = _filter_table_by_isp_scenario(
            table, "Accelerated Transition", "Scenario", "test"
        )

    expected = csv_str_to_df("""
        REZ__ID,    value
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )

    assert (
        "No rows matched scenario 'Accelerated Transition' in test table "
        "— filtered table will be empty"
    ) in caplog.text


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
    # Wide-to-long reshape, FY string → int year, extra columns dropped (REZ names).
    connection_cost_forecast = csv_str_to_df("""
        REZ__ID, REZ__names,        Connection__capacity__(MVA), 2024-25,     2025-26
        N1,      North__West__NSW,  400,                         730000000,   740000000
        Q9,      Banana,            1800,                        8540000000,  8670000000
    """)

    result = _normalise_connection_cost_forecast_frame(
        connection_cost_forecast, id_cols_rename=_VRE_COLUMN_RENAMES
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
    # Non-VRE path melts with a 3-key rename dict (Region, Generator Type,
    # capacity) — a different, multi-id-column shape from the VRE case above.
    connection_cost_forecast = csv_str_to_df("""
        Region,  Generator__Type,  Connection__capacity__(MVA),  2024-25,   2025-26
        NSW,     CCGT,             400,                          40000000,  42000000
        QLD,     CCGT,             300,                          36000000,  37000000
    """)

    result = _normalise_connection_cost_forecast_frame(
        connection_cost_forecast, id_cols_rename=_NON_VRE_COLUMN_RENAMES
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
        REZ__ID,  Connection__capacity__(MVA),  2024-25
        DN1,      150,
        Q1,       ,                             160000000.0
    """)

    result = _normalise_connection_cost_forecast_frame(
        connection_cost_forecast, id_cols_rename=_VRE_COLUMN_RENAMES
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
        REZ__ID, REZ__names,        Connection__capacity__(MVA), 2024-25,     2025-26
    """)

    result = _normalise_connection_cost_forecast_frame(
        connection_cost_forecast, _VRE_COLUMN_RENAMES
    )

    expected = csv_str_to_df("""
        geo_id,     connection_capacity,    year,   connection_cost
    """)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


# --- _merge_connection_cost_and_capacity_frames ---


def test_merge_connection_cost_and_capacity_frames(csv_str_to_df):
    # Left join on cost_df: cost rows with matching capacity are merged,
    # cost row with no capacity (R99) gets NaN connection_capacity,
    # capacity-only row (N0) is dropped. (T4) with NaN values but present in
    # both is retained.
    cost_df = csv_str_to_df("""
        REZ__ID,  2024-25
        N1,       73000000
        Q9,       854000000
        R99,      120000000
        T4,
    """)
    capacity_df = csv_str_to_df("""
        REZ__ID,  Connection__capacity__(MVA)
        N1,       400
        Q9,       1800
        N0,       200
        T4,
    """)

    result = _merge_connection_cost_and_capacity_frames(
        cost_df, capacity_df, ["REZ ID"]
    )

    expected = csv_str_to_df("""
        REZ__ID,  2024-25,     Connection__capacity__(MVA)
        N1,       73000000,    400
        Q9,       854000000,   1800
        R99,      120000000,
        T4,       ,
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("REZ ID").reset_index(drop=True),
        expected.sort_values("REZ ID").reset_index(drop=True),
        check_dtype=False,
    )


def test_merge_connection_cost_and_capacity_frames_empty_inputs(csv_str_to_df):
    # Empty cost_df → empty output (left join produces no rows).
    # Empty capacity_df → cost rows survive with NaN capacity (left join).
    empty_cost_df = csv_str_to_df("""
        REZ__ID,  2024-25
    """)
    capacity_df = csv_str_to_df("""
        REZ__ID,  Connection__capacity__(MVA)
        N1,       400
    """)

    result_empty_cost = _merge_connection_cost_and_capacity_frames(
        empty_cost_df,
        capacity_df,
        ["REZ ID"],
    )
    expected_empty_cost = csv_str_to_df("""
    REZ__ID,  2024-25,  Connection__capacity__(MVA)
    """)
    pd.testing.assert_frame_equal(
        result_empty_cost.sort_values("REZ ID").reset_index(drop=True),
        expected_empty_cost.sort_values("REZ ID").reset_index(drop=True),
        check_dtype=False,
    )

    cost_df = csv_str_to_df("""
        REZ__ID,    2024-25
        N1,         73000000
    """)
    empty_capacity_df = csv_str_to_df("""
        REZ__ID,  Connection__capacity__(MVA)
    """)

    result_empty_capacity = _merge_connection_cost_and_capacity_frames(
        cost_df,
        empty_capacity_df,
        ["REZ ID"],
    )
    expected_nan_capacity = csv_str_to_df("""
        REZ__ID,  2024-25,   Connection__capacity__(MVA)
        N1,       73000000,
    """)
    pd.testing.assert_frame_equal(
        result_empty_capacity.reset_index(drop=True),
        expected_nan_capacity.reset_index(drop=True),
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
    # Division by zero produces inf, which is replaced with pd.NA → NaN.
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
    # Wide single-row table → long (year, system_strength_cost).
    # $/kW * 1000 = $/MW conversion applied.
    # FY strings converted to year int, long-form year string fix applied correctly.
    # Non-year label column dropped.
    system_strength_cost_table = csv_str_to_df("""
        label,            2024-25,  2025-26,    2026-2027,      Notes
        IBR__remediation, 10,       12,         15,             Some__extra__note
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
        geo_id,     year,   technology,                    connection_cost, system_strength_cost
        S1,         2035,   Large__scale__Solar__PV,       120000.0,        10000.0
        S1,         2035,   Wind,                          130000.0,        10000.0
        S1,         2035,   Battery__Storage__(1hr),       140000.0,        10000.0
        S1,         2028,   Solar__Thermal__(16hrs),       150000.0,        10000.0
        SESA,       2035,   Small__OCGT,                   160000.0,        10000.0
        SESA,       2035,   CCGT,                          170000.0,        10000.0
    """)

    result = _set_non_ibr_system_strength_cost_to_zero(connection_costs)

    expected = csv_str_to_df("""
        geo_id,     year,   technology,                    connection_cost, system_strength_cost
        S1,         2035,   Large__scale__Solar__PV,       120000.0,        10000.0
        S1,         2035,   Wind,                          130000.0,        10000.0
        S1,         2035,   Battery__Storage__(1hr),       140000.0,        10000.0
        S1,         2028,   Solar__Thermal__(16hrs),       150000.0,        10000.0
        SESA,       2035,   Small__OCGT,                   160000.0,        0.0
        SESA,       2035,   CCGT,                          170000.0,        0.0
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
        geo_id, year,   technology,                    system_strength_cost
        V1,     2028,   Large__scale__Solar__PV,       10000.0
        V5,     2028,   Solar__Thermal__(16hrs),       10000.0
    """)

    result = _set_solar_thermal_system_strength_costs_to_zero(connection_costs)

    expected = csv_str_to_df("""
        geo_id, year,   technology,                    system_strength_cost
        V1,     2028,   Large__scale__Solar__PV,       10000.0
        V5,     2028,   Solar__Thermal__(16hrs),       0.0
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_exact=False,
    )


# --- _merge_and_filter_system_strength_costs ---
# Not tested: interaction with NaN connection_cost values — system strength is
# applied independently of connection_cost, so NaN connection_cost rows simply
# pass through with their system_strength_cost value unchanged.


def test_merge_and_filter_system_strength_costs(csv_str_to_df):
    # IBR gets system strength cost, non-IBR gets 0.0, solar thermal gets 0.0.
    # Extra years present in system_strength_costs dropped in the merge.
    connection_costs = csv_str_to_df("""
        geo_id,  technology,                year,  connection_cost
        N1,      Large__scale__Solar__PV,   2025,  182500.0
        N1,      Wind,                      2025,  182500.0
        QLD,     Small__OCGT,               2025,  500000.0
        TAS,     Solar__Thermal,            2025,  100000.0
    """)
    system_strength_costs = csv_str_to_df("""
        year,   system_strength_cost
        2025,   10000.0
        2026,   10000.0
    """)

    result = _merge_and_filter_system_strength_costs(
        connection_costs, system_strength_costs
    )

    expected = csv_str_to_df("""
        geo_id,  technology,                year,  connection_cost,  system_strength_cost
        N1,      Large__scale__Solar__PV,   2025,  182500.0,         10000.0
        N1,      Wind,                      2025,  182500.0,         10000.0
        QLD,     Small__OCGT,               2025,  500000.0,         0.0
        TAS,     Solar__Thermal,            2025,  100000.0,         0.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["geo_id", "technology"]).reset_index(drop=True),
        expected.sort_values(["geo_id", "technology"]).reset_index(drop=True),
        check_exact=False,
        rtol=1e-5,
    )


def test_merge_and_filter_system_strength_costs_year_not_in_system_strength_gives_nan(
    csv_str_to_df,
):
    # Year in connection_costs but absent from system_strength_costs → NaN
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

    result = _merge_and_filter_system_strength_costs(
        connection_costs, system_strength_costs
    )

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


def test_merge_and_filter_system_strength_costs_empty_inputs(csv_str_to_df):
    # Empty connection_costs → empty output with correct columns.
    system_strength_costs = csv_str_to_df("""
        year,  system_strength_cost
        2025,  10000.0
    """)

    result_empty_costs = _merge_and_filter_system_strength_costs(
        pd.DataFrame(columns=_CONNECTION_ONLY_COST_COLS),
        system_strength_costs,
    )
    expected_empty_cc = pd.DataFrame(columns=_CONNECTION_SYSTEM_STRENGTH_COST_COLS)
    pd.testing.assert_frame_equal(
        result_empty_costs.reset_index(drop=True),
        expected_empty_cc.reset_index(drop=True),
        check_dtype=False,
    )

    # Empty system_strength_costs → all system_strength_cost values are NaN.
    empty_system_strength_costs = csv_str_to_df("""
        year,  system_strength_cost
    """)
    connection_costs = csv_str_to_df("""
        geo_id,  technology,  year,  connection_cost
        N1,      Wind,        2025,  182500.0
    """)

    result_empty_ss = _merge_and_filter_system_strength_costs(
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
        N1,      Large__scale__Solar__PV
        N1,      Wind
        NNSW,    Pumped__Hydro__(24hrs__storage)
        Q9,      Large__scale__Solar__PV
        V8,      Wind__-__offshore__(fixed)
        GG,      Distributed__Resources__Batteries
        SESA,    Distributed__Resources__Solar
    """)

    result = _get_unique_vre_geo_id_rows(canonical_tech_geo_ids)

    expected = csv_str_to_df("""
        geo_id,  technology
        N1,      Large__scale__Solar__PV
        N1,      Wind
        Q9,      Large__scale__Solar__PV
        V8,      Wind__-__offshore__(fixed)
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
        N1,      Large__scale__Solar__PV
        N1,      Wind
        Q9,      Large__scale__Solar__PV
        V8,      Wind__-__offshore__(fixed)
    """)

    result = _build_vre_cost_rows(costs_per_mw, vre_technologies)

    expected = csv_str_to_df("""
        geo_id,  technology,                    year,  connection_cost
        N1,      Wind,                          2025,  182500.0
        N1,      Wind,                          2026,  185000.0
        N1,      Large__scale__Solar__PV,       2025,  182500.0
        N1,      Large__scale__Solar__PV,       2026,  185000.0
        Q9,      Large__scale__Solar__PV,       2025,  474444.4
        Q9,      Large__scale__Solar__PV,       2026,  481666.7
        V8,      Wind__-__offshore__(fixed),    2025,
        V8,      Wind__-__offshore__(fixed),    2026,
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["geo_id", "technology", "year"]).reset_index(drop=True),
        expected.sort_values(["geo_id", "technology", "year"]).reset_index(drop=True),
        check_exact=False,
        rtol=1e-5,
        check_dtype=False,
    )


def test_build_vre_cost_rows_empty_inputs(csv_str_to_df):
    # Empty vre_technologies → no (tech, year) combinations → empty output.
    # Empty costs_per_mw → no years to cross-join → empty output.
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


def test_template_vre_connection_costs(csv_str_to_df):
    connection_cost_forecast_vre = csv_str_to_df("""
        REZ__ID,  REZ__names,       Scenario,     2024-25,    2025-26
        N1,       North__West__NSW, Step__Change, 73000000,   74000000
        Q9,       Banana,           Step__Change, 854000000,  867000000
    """)
    connection_costs_for_vre = csv_str_to_df("""
        REZ__ID,  Connection__capacity__(MVA)
        N1,       400
        Q9,       1800
    """)
    vre_technologies_by_geo_id = csv_str_to_df("""
        geo_id,  technology
        N1,      Wind
        N1,      Large__scale__Solar__PV
        Q9,      Large__scale__Solar__PV
        V8,      Wind__-__offshore__(fixed)
    """)

    result = _template_vre_connection_costs(
        connection_cost_forecast_vre,
        connection_costs_for_vre,
        "Step Change",
        vre_technologies_by_geo_id,
    )

    # Columns correct; 3 (geo_id, tech) pairs × 2 years + 2 offshore NaN rows = 8
    assert list(result.columns) == _CONNECTION_ONLY_COST_COLS
    assert len(result) == 8  # detailed content covered by unit tests above


def test_template_vre_connection_costs_all_empty_inputs(csv_str_to_df):
    # test that even with all empty inputs we get an empty df with expected columns.
    connection_cost_forecast_vre = csv_str_to_df("""
        REZ__ID,  REZ__names,       Scenario,     2024-25,    2025-26
    """)
    connection_costs_for_vre = csv_str_to_df("""
        REZ__ID,  Connection__capacity__(MVA)
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
        REZ__ID,  REZ__names,       Scenario,     2024-25,    2025-26
        N1,       North__West__NSW, Step__Change, 73000000,   74000000
        Q9,       Banana,           Step__Change, 854000000,  867000000
    """)
    empty_capacity = csv_str_to_df("""
        REZ__ID,  Connection__capacity__(MVA)
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
        REZ__ID,  REZ__names,       Scenario,     2024-25,    2025-26
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
    # Non-empty forecast and technologies but empty capacity → NaN connection_cost
    connection_cost_forecast_vre = csv_str_to_df("""
        REZ__ID,  REZ__names,       Scenario,     2024-25,    2025-26
        N1,       North__West__NSW, Step__Change, 73000000,   74000000
        Q9,       Banana,           Step__Change, 854000000,  867000000
    """)
    empty_capacity = csv_str_to_df("""
        REZ__ID,  Connection__capacity__(MVA)
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


# --- _get_canon_technology_and_geo_id_pairs ---


def test_get_canon_technology_and_geo_id_pairs(csv_str_to_df):
    # Union of generator and storage (geo_id, technology) pairs, deduplicated.
    # (CNSW, CCGT) appears in both tables and as a within-table duplicate → one row.
    generators_new_entrant = csv_str_to_df("""
        geo_id,  technology
        N1,      Wind
        CNSW,    CCGT
        CNSW,    CCGT
    """)
    storage_new_entrant = csv_str_to_df("""
        geo_id,  technology
        CNSW,    Battery__Storage__(4h)
        CNSW,    CCGT
    """)

    result = _get_canon_technology_and_geo_id_pairs(
        generators_new_entrant, storage_new_entrant
    )

    expected = csv_str_to_df("""
        geo_id,  technology
        N1,      Wind
        CNSW,    CCGT
        CNSW,    Battery__Storage__(4h)
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(_GEO_TECH_COLS).reset_index(drop=True),
        expected.sort_values(_GEO_TECH_COLS).reset_index(drop=True),
    )


# --- _canonicalise_non_vre_technologies ---


def test_canonicalise_non_vre_technologies(csv_str_to_df):
    # Excluded techs dropped; remaining values fuzzy-mapped to canonical names.
    df = csv_str_to_df("""
        region_id,  technology,         year,  connection_capacity,  connection_cost
        NSW,        CCGT,               2025,  400,                  40000000.0
        NSW,        OCGT__small__GT,    2025,  400,                  32000000.0
        TAS,        BOTN__-__Cethana,   2025,  250,                  0.0
    """)
    canonical_technologies = {"CCGT", "OCGT (small GT)"}

    result = _canonicalise_non_vre_technologies(df, canonical_technologies)

    expected = csv_str_to_df("""
        region_id,  technology,         year,  connection_capacity,  connection_cost
        NSW,        CCGT,               2025,  400,                  40000000.0
        NSW,        OCGT__(small__GT),  2025,  400,                  32000000.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["region_id", "technology"]).reset_index(drop=True),
        expected.sort_values(["region_id", "technology"]).reset_index(drop=True),
        check_dtype=False,
    )


def test_canonicalise_non_vre_technologies_unmatched_raises(csv_str_to_df):
    # A technology that can't be fuzzy-matched to any canonical name raises.
    # Pins the (deferred) behaviour for only-VRE / inconsistent inputs — see the
    # NOTE in _canonicalise_non_vre_technologies.
    df = csv_str_to_df("""
        region_id,  technology,  year,  connection_capacity,  connection_cost
        NSW,        CCGT,        2025,  400,                  40000000.0
    """)
    canonical_technologies = {"Wind", "Large scale Solar PV"}

    with pytest.raises(ValueError, match="Could not fuzzy match"):
        _canonicalise_non_vre_technologies(df, canonical_technologies)


def test_canonicalise_non_vre_technologies_empty_canonical_set_returns_empty(
    csv_str_to_df,
):
    # Brownfield: no new-entrant non-VRE technologies → empty output, no raise.
    df = csv_str_to_df("""
        region_id,  technology,  year,  connection_capacity,  connection_cost
        NSW,        CCGT,        2025,  400,                  40000000.0
    """)

    result = _canonicalise_non_vre_technologies(df, set())

    expected = csv_str_to_df("""
        region_id,  technology,  year,  connection_capacity,  connection_cost
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


# --- _create_non_vre_rez_cost_rows ---


def test_create_non_vre_rez_cost_rows(csv_str_to_df):
    # Only non-VRE technologies sited in REZs are returned, with parent region.
    # S1 Battery (REZ, non-VRE) kept, inherits parent region (SA) cost;
    # S1 CCGT (REZ, non-VRE) not in connection_costs -> excluded;
    # S1 Wind (REZ but VRE) excluded; CSA Battery and NQ CCGT (non-VRE but
    # subregions, not REZs) excluded; NQ Wind (VRE, not REZ) excluded.
    canon_tech_geo_id_pairs = csv_str_to_df("""
        geo_id,     technology
        S1,         Battery__Storage__(4h)
        S1,         CCGT
        S1,         Wind
        CSA,        Battery__Storage__(4h)
        NQ,         CCGT
        NQ,         Wind
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,     geo_type,       region_id
        S1,         rez,            SA
        CSA,        subregion,      SA
        NQ,         subregion,      QLD
    """)
    connection_costs = csv_str_to_df("""
        region_id,  technology,             year,   connection_cost
        SA,         Battery__Storage__(4h), 2025,   90000.0
        SA,         Battery__Storage__(4h), 2026,   95000.0
        QLD,        CCGT,                   2025,   100000.0
        QLD,        CCGT,                   2026,   110000.0
    """)

    result = _create_non_vre_rez_cost_rows(
        canon_tech_geo_id_pairs, sub_regional_geography, connection_costs
    )

    expected = csv_str_to_df("""
        geo_id,     technology,             year,   connection_cost
        S1,         Battery__Storage__(4h), 2025,   90000.0
        S1,         Battery__Storage__(4h), 2026,   95000.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["geo_id", "year"]).reset_index(drop=True),
        expected.sort_values(["geo_id", "year"]).reset_index(drop=True),
        check_dtype=False,
    )


# --- _expand_non_vre_connection_costs_to_subregions ---


def test_expand_non_vre_connection_costs_to_subregions(csv_str_to_df):
    # Region-level costs fan out to one row per subregion; REZ rows in the
    # geography are ignored (inner merge on subregions only).
    non_vre_connection_costs = csv_str_to_df("""
        region_id,  technology,         year,  connection_cost
        QLD,        OCGT__(small__GT),  2025,  120000.0
        QLD,        OCGT__(small__GT),  2026,  130000.0
        NSW,        OCGT__(small__GT),  2025,  100000.0
        NSW,        OCGT__(small__GT),  2026,  110000.0
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        Q9,      rez,        QLD
        NQ,      subregion,  QLD
        CQ,      subregion,  QLD
        NNSW,    subregion,  NSW
    """)

    result = _expand_non_vre_connection_costs_to_subregions(
        non_vre_connection_costs, sub_regional_geography
    )

    expected = csv_str_to_df("""
        geo_id,  technology,         year,  connection_cost
        NQ,      OCGT__(small__GT),  2025,  120000.0
        NQ,      OCGT__(small__GT),  2026,  130000.0
        CQ,      OCGT__(small__GT),  2025,  120000.0
        CQ,      OCGT__(small__GT),  2026,  130000.0
        NNSW,    OCGT__(small__GT),  2025,  100000.0
        NNSW,    OCGT__(small__GT),  2026,  110000.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("geo_id").reset_index(drop=True),
        expected.sort_values("geo_id").reset_index(drop=True),
        check_dtype=False,
    )


# --- _average_connection_costs_across_regions ---


def test_average_connection_costs_across_regions(csv_str_to_df):
    # Costs averaged across regions per (technology, year); geo_id set to "NEM".
    non_vre_connection_costs = csv_str_to_df("""
        region_id,  technology,         year,  connection_cost
        NSW,        OCGT__(small__GT),  2025,  100000.0
        NSW,        OCGT__(small__GT),  2026,  110000.0
        QLD,        OCGT__(small__GT),  2025,  120000.0
        QLD,        OCGT__(small__GT),  2026,  130000.0
        VIC,        OCGT__(small__GT),  2025,  80000.0
        VIC,        OCGT__(small__GT),  2026,  90000.0
    """)

    result = _average_connection_costs_across_regions(non_vre_connection_costs)

    expected = csv_str_to_df("""
        geo_id,  technology,         year,  connection_cost
        NEM,     OCGT__(small__GT),  2025,  100000.0
        NEM,     OCGT__(small__GT),  2026,  110000.0
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


# --- _filter_connection_costs_by_regional_granularity ---


def test_filter_connection_costs_by_regional_granularity(csv_str_to_df):
    # All three branches share inputs; each appends the REZ-battery row (N1,
    # inheriting the NSW cost). Asserted per branch below.
    non_vre_connection_costs = csv_str_to_df("""
        region_id,  technology,              year,  connection_cost
        NSW,        CCGT,                    2025,  100000.0
        NSW,        Battery__Storage__(4h),  2025,  50000.0
        QLD,        CCGT,                    2025,  120000.0
        QLD,        Battery__Storage__(4h),  2025,  60000.0
    """)
    canon_tech_geo_id_pairs = csv_str_to_df("""
        geo_id,  technology
        N1,      Battery__Storage__(4h)
        SNW,     Battery__Storage__(4h)
        SNW,     CCGT
        CQ,      Battery__Storage__(4h)
        CQ,      CCGT
        NQ,      Battery__Storage__(4h)
        NQ,      CCGT
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        N1,      rez,        NSW
        SNW,     subregion,  NSW
        CQ,      subregion,  QLD
        NQ,      subregion,  QLD
    """)

    sort_cols = ["geo_id", "technology"]

    # nem_regions: region_id renamed to geo_id; REZ row appended.
    result_nem = _filter_connection_costs_by_regional_granularity(
        non_vre_connection_costs,
        "nem_regions",
        canon_tech_geo_id_pairs,
        sub_regional_geography,
    )
    expected_nem = csv_str_to_df("""
        geo_id,  technology,              year,  connection_cost
        NSW,     CCGT,                    2025,  100000.0
        NSW,     Battery__Storage__(4h),  2025,  50000.0
        QLD,     CCGT,                    2025,  120000.0
        QLD,     Battery__Storage__(4h),  2025,  60000.0
        N1,      Battery__Storage__(4h),  2025,  50000.0
    """)
    pd.testing.assert_frame_equal(
        result_nem.sort_values(sort_cols).reset_index(drop=True),
        expected_nem.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )

    # sub_regions: costs expanded to each subregion; REZ row appended.
    result_sub = _filter_connection_costs_by_regional_granularity(
        non_vre_connection_costs,
        "sub_regions",
        canon_tech_geo_id_pairs,
        sub_regional_geography,
    )
    expected_sub = csv_str_to_df("""
        geo_id,  technology,              year,  connection_cost
        SNW,     CCGT,                    2025,  100000.0
        SNW,     Battery__Storage__(4h),  2025,  50000.0
        CQ,      CCGT,                    2025,  120000.0
        CQ,      Battery__Storage__(4h),  2025,  60000.0
        NQ,      CCGT,                    2025,  120000.0
        NQ,      Battery__Storage__(4h),  2025,  60000.0
        N1,      Battery__Storage__(4h),  2025,  50000.0
    """)
    pd.testing.assert_frame_equal(
        result_sub.sort_values(sort_cols).reset_index(drop=True),
        expected_sub.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )

    # single_region: costs averaged to one "NEM" geo_id; REZ row keeps NSW cost.
    result_single = _filter_connection_costs_by_regional_granularity(
        non_vre_connection_costs,
        "single_region",
        canon_tech_geo_id_pairs,
        sub_regional_geography,
    )
    expected_single = csv_str_to_df("""
        geo_id,  technology,              year,  connection_cost
        NEM,     CCGT,                    2025,  110000.0
        NEM,     Battery__Storage__(4h),  2025,  55000.0
        N1,      Battery__Storage__(4h),  2025,  50000.0
    """)
    pd.testing.assert_frame_equal(
        result_single.sort_values(sort_cols).reset_index(drop=True),
        expected_single.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_filter_connection_costs_by_regional_granularity_invalid_raises(csv_str_to_df):
    non_vre_connection_costs = csv_str_to_df("""
        region_id,  technology,  year,  connection_cost
        NSW,        CCGT,        2025,  100000.0
    """)
    canon_tech_geo_id_pairs = csv_str_to_df("""
        geo_id,  technology
        SNW,     CCGT
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        SNW,     subregion,  NSW
    """)

    with pytest.raises(ValueError, match="Unknown regional_granularity"):
        _filter_connection_costs_by_regional_granularity(
            non_vre_connection_costs,
            "various_swamps",
            canon_tech_geo_id_pairs,
            sub_regional_geography,
        )


# --- _template_non_vre_connection_costs (integration) ---
# Wiring test only — detailed content covered by the unit tests above.


def test_template_non_vre_connection_costs(csv_str_to_df):
    connection_cost_forecast_other = csv_str_to_df("""
        Generator__Type,        Region,     Scenario,       2024-25
        CCGT,                   NSW,        Step__Change,   40000000
        Battery__Storage__(4h), NSW,        Step__Change,   20000000
        CCGT,                   QLD,        Step__Change,   40000000
        Battery__Storage__(4h), QLD,        Step__Change,   20000000
    """)
    connection_capacity_df = csv_str_to_df("""
        Region,     Generator__Type,        Connection__capacity__(MVA)
        NSW,        CCGT,                   400
        NSW,        Battery__Storage__(4h), 400
        QLD,        CCGT,                   300
        QLD,        Battery__Storage__(4h), 300
    """)
    canon_tech_geo_id_pairs = csv_str_to_df("""
        geo_id,     technology
        SQ,         CCGT
        SQ,         Battery__Storage__(4h)
        CNSW,       CCGT
        CNSW,       Battery__Storage__(4h)
        SNW,        CCGT
        SNW,        Battery__Storage__(4h)
        N1,         Battery__Storage__(4h)
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,     geo_type,   region_id
        SQ,         subregion,  QLD
        CNSW,       subregion,  NSW
        SNW,        subregion,  NSW
        N1,         rez,        NSW
    """)

    result_subregions = _template_non_vre_connection_costs(
        connection_cost_forecast_other,
        connection_capacity_df,
        "Step Change",
        canon_tech_geo_id_pairs,
        "sub_regions",
        sub_regional_geography,
    )

    # 6 subregion rows: (CCGT, Battery) x (CNSW, SNW, SQ) + 1 REZ-battery append (N1) = 7
    assert list(result_subregions.columns) == _CONNECTION_ONLY_COST_COLS
    assert len(result_subregions) == 7

    result_nem_regions = _template_non_vre_connection_costs(
        connection_cost_forecast_other,
        connection_capacity_df,
        "Step Change",
        canon_tech_geo_id_pairs,
        "nem_regions",
        sub_regional_geography,
    )

    # 4 NEM region rows (Battery, CCGT) x (NSW, QLD) + 1 REZ-battery append (N1) = 5
    assert list(result_nem_regions.columns) == _CONNECTION_ONLY_COST_COLS
    assert len(result_nem_regions) == 5

    result_single_region = _template_non_vre_connection_costs(
        connection_cost_forecast_other,
        connection_capacity_df,
        "Step Change",
        canon_tech_geo_id_pairs,
        "single_region",
        sub_regional_geography,
    )

    # 2 NEM rows (CCGT, Battery) + 1 REZ-battery append (N1) = 3.
    assert list(result_single_region.columns) == _CONNECTION_ONLY_COST_COLS
    assert len(result_single_region) == 3


# --- _template_connection_costs (integration) ---
# Wiring test only — detailed content covered by unit tests above.


def test_template_connection_costs(csv_str_to_df):
    # Wiring test: exercises the VRE path, the non-VRE region-expansion path, and
    # the REZ-battery append path together. Detailed content is covered by the
    # per-helper unit tests above, so we assert column set + row count only.
    iasr_tables = {
        "connection_cost_forecast_wind_and_solar": csv_str_to_df("""
            REZ__ID,  Scenario,      2024-25
            N1,       Step__Change,  73000000
        """),
        "connection_costs_for_wind_and_solar": csv_str_to_df("""
            REZ__ID,  Connection__capacity__(MVA)
            N1,       400
        """),
        "connection_cost_forecast_other": csv_str_to_df("""
            Generator__Type,         Region,  Scenario,      2024-25
            CCGT,                    NSW,     Step__Change,  40000000
            Battery__Storage__(4h),  NSW,     Step__Change,  20000000
        """),
        "connection_capacity_non_vre": csv_str_to_df("""
            Region,  Generator__Type,         Connection__capacity__(MVA)
            NSW,     CCGT,                    400
            NSW,     Battery__Storage__(4h),  400
        """),
        "efficient_level_of_system_strength_cost": csv_str_to_df("""
            label,  2024-25
            IBR,    10
        """),
    }
    generators_new_entrant = csv_str_to_df("""
        geo_id,  technology
        N1,      Wind
        CNSW,    CCGT
    """)
    storage_new_entrant = csv_str_to_df("""
        geo_id,  technology
        CNSW,    Battery__Storage__(4h)
        N1,      Battery__Storage__(4h)
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
        N1,      rez,        NSW
    """)

    result = _template_connection_costs(
        iasr_tables,
        "Step Change",
        "sub_regions",
        generators_new_entrant,
        storage_new_entrant,
        sub_regional_geography,
    )

    # 1 VRE (N1 Wind) + 2 non-VRE subregion (CNSW CCGT, CNSW Battery)
    # + 1 REZ-battery append (N1 Battery, inheriting the NSW cost) = 4 rows.
    assert list(result.columns) == _CONNECTION_SYSTEM_STRENGTH_COST_COLS
    assert len(result) == 4


def test_template_connection_costs_empty_inputs_give_empty_output(csv_str_to_df):
    # All empty inputs → empty output with all expected columns present.
    iasr_tables = {
        "connection_cost_forecast_wind_and_solar": csv_str_to_df("""
            REZ__ID,  Scenario,  2024-25
        """),
        "connection_costs_for_wind_and_solar": csv_str_to_df("""
            REZ__ID,  Connection__capacity__(MVA)
        """),
        "connection_cost_forecast_other": csv_str_to_df("""
            Generator__Type,  Region,  Scenario,  2024-25
        """),
        "connection_capacity_non_vre": csv_str_to_df("""
            Region,  Generator__Type,  Connection__capacity__(MVA)
        """),
        "efficient_level_of_system_strength_cost": csv_str_to_df("""
            label,  2024-25
        """),
    }

    result = _template_connection_costs(
        iasr_tables,
        "Step Change",
        "sub_regions",
        pd.DataFrame(columns=_GEO_TECH_COLS),
        pd.DataFrame(columns=_GEO_TECH_COLS),
        pd.DataFrame(columns=["geo_id", "geo_type", "region_id"]),
    )

    assert list(result.columns) == _CONNECTION_SYSTEM_STRENGTH_COST_COLS
    assert result.empty
