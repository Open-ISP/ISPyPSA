import pandas as pd

from ispypsa.templater.connection_and_build_costs import (
    _VRE_COLUMN_RENAMES,
    _build_vre_cost_rows,
    _calculate_connection_cost_per_mw,
    _enforce_numeric_cols,
    _filter_table_by_isp_scenario,
    _get_unique_vre_geo_id_rows,
    _merge_and_filter_system_strength_costs,
    _merge_connection_cost_and_capacity_frames,
    _normalise_connection_cost_forecast_frame,
    _normalise_system_strength_cost_frame,
    _set_non_ibr_system_strength_cost_to_zero,
    _set_solar_thermal_system_strength_costs_to_zero,
    _template_connection_costs,
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
        geo_id,  year,  connection_capacity,  connection_cost
        N1,      2025,  400.0,                730000000.0
        N1,      2026,  400.0,                740000000.0
        Q9,      2025,  1800.0,               8540000000.0
        Q9,      2026,  1800.0,               8670000000.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["geo_id", "year"]).reset_index(drop=True),
        expected.sort_values(["geo_id", "year"]).reset_index(drop=True),
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
        geo_id,     year,   connection_capacity,    connection_cost
        DN1,        2025,   150.0,
        Q1,         2025,   ,                       160000000.0
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
        geo_id,     year,   connection_capacity,    connection_cost
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
        "NaN connection cost after per-MW calculation for: (geo_id=DN1, year=2025) "
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
    # FY strings converted to year int. Non-year label column dropped.
    system_strength_cost_table = csv_str_to_df("""
        label,            2024-25,  2025-26,    Notes
        IBR__remediation, 10,       12,         Some__extra__note
    """)

    result = _normalise_system_strength_cost_frame(system_strength_cost_table)

    expected = csv_str_to_df("""
        year,  system_strength_cost
        2025,  10000.0
        2026,  12000.0
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
    # Non-VRE rows (Pumped Hydro) are excluded. 'Distributed' rows excluded.
    # Duplicate (N1, Solar PV) rows are deduplicated to one.
    generators_new_entrant = csv_str_to_df("""
        geo_id,  technology
        N1,      Large__scale__Solar__PV
        N1,      Large__scale__Solar__PV
        N1,      Wind
        NNSW,    Pumped__Hydro__(24hrs__storage)
        Q9,      Large__scale__Solar__PV
        V8,      Wind__-__offshore__(fixed)
        GG,      Distributed__Resources__Batteries
        SESA,    Distributed__Resources__Solar
    """)

    result = _get_unique_vre_geo_id_rows(generators_new_entrant)

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


# --- _template_connection_costs (integration) ---
# Wiring test only — detailed content covered by unit tests above.


def test_template_connection_costs(csv_str_to_df):
    connection_cost_forecast_vre = csv_str_to_df("""
        REZ__ID,  Scenario,     2024-25
        N1,       Step__Change, 73000000
    """)
    connection_costs_for_vre = csv_str_to_df("""
        REZ__ID,  Connection__capacity__(MVA)
        N1,       400
    """)
    system_strength_cost_table = csv_str_to_df("""
        label,  2024-25
        IBR,    10
    """)
    generators_new_entrant = csv_str_to_df("""
        geo_id,  technology
        N1,      Wind
    """)

    result = _template_connection_costs(
        connection_cost_forecast_vre,
        connection_costs_for_vre,
        system_strength_cost_table,
        "Step Change",
        generators_new_entrant,
    )

    assert list(result.columns) == _CONNECTION_SYSTEM_STRENGTH_COST_COLS
    assert len(result) == 1  # one (geo_id, technology, year) combination


def test_template_connection_costs_empty_inputs_give_empty_output(csv_str_to_df):
    # All empty inputs → empty output with all expected columns present.
    connection_cost_forecast_vre = csv_str_to_df("""
        REZ__ID,  Scenario,     2024-25
    """)
    connection_costs_for_vre = csv_str_to_df("""
        REZ__ID,  Connection__capacity__(MVA)
    """)
    system_strength_cost_table = csv_str_to_df("""
        label,  2024-25
    """)
    generators_new_entrant = csv_str_to_df("""
        geo_id,  technology
    """)

    result = _template_connection_costs(
        connection_cost_forecast_vre,
        connection_costs_for_vre,
        system_strength_cost_table,
        "Step Change",
        generators_new_entrant,
    )

    assert list(result.columns) == _CONNECTION_SYSTEM_STRENGTH_COST_COLS
    assert result.empty
