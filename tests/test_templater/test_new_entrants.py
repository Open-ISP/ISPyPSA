import pandas as pd
import pytest

from ispypsa.templater.new_entrants import (
    _GENERATOR_IDENTITY_COLUMNS,
    _GENERATOR_PROPERTY_COLUMNS,
    _STORAGE_IDENTITY_COLUMNS,
    _STORAGE_PROPERTY_COLUMNS,
    _add_resource_type,
    _assert_botn_technology_expected,
    _assert_property_table_attrs,
    _derive_phes_symmetric_efficiency,
    _group_by_source_key,
    _merge_phes_properties,
    _merge_properties,
    _normalise_phes_botn_key,
    _override_botn_technology,
    _set_geo_id,
    _template_generators_new_entrant,
    _template_storage_new_entrant,
)

# --- orchestrators ---


def test_template_generators_new_entrant(csv_str_to_df):
    # Wiring check only (per-helper behaviour is covered below): storage is dropped,
    # and the identity + property columns are produced, one row per generating unit.
    # Detailed content is covered by the per-helper tests.
    new_entrants_summary = csv_str_to_df("""
        IASR ID / DLT names,  Technology Type,                Fuel type,  Fuel cost mapping,  REZ ID,         Sub-region
        Q1_WH_Far North QLD,  Wind,                           Wind,       Wind,               Q1,             NQ
        NQ OCGT Small,        OCGT (small GT),                Gas,        QLD new OCGT,       Not Applicable, NQ
        NQ Battery 2hrs,      Battery Storage (2hrs storage), Battery,    Battery,            Not Applicable, NQ
    """)
    iasr_tables = {
        "new_entrants_summary": new_entrants_summary,
        "fixed_opex_new_entrants": csv_str_to_df("""
            Technology Type,  Base value ($/kW/year)),  Unit
            Wind,             20.0,                     $
            OCGT (small GT),  17.0,                     $
        """),
        "variable_opex_new_entrants": csv_str_to_df("""
            Generator,        Base value
            Wind,             0.0
            OCGT (small GT),  16.4
        """),
        "lead_time_and_project_life": csv_str_to_df("""
            Technology,       Economic life (years),  Technical life (years)
            Wind,             25,                     30
            OCGT (small GT),  25,                     40
        """),
        "heat_rates_new_entrants": csv_str_to_df("""
            Technology,       Heat rate (GJ/MWh)
            Wind,             0.0
            OCGT (small GT),  10.6
        """),
        "gpg_min_stable_level_new_entrants": csv_str_to_df("""
            Technology,       Min Stable Level (% of nameplate)
            Wind,             0.0
            OCGT (small GT),  50.0
        """),
    }

    result = _template_generators_new_entrant(iasr_tables)

    # storage row dropped -> 2 gen rows; identity + property columns produced in order
    assert (
        list(result.columns)
        == _GENERATOR_IDENTITY_COLUMNS + _GENERATOR_PROPERTY_COLUMNS
    )
    assert len(result) == 2


def _storage_property_tables(csv_str_to_df):
    """The IASR property tables the storage orchestrator merges from (shared by tests)."""
    return {
        "battery_properties": csv_str_to_df("""
            Technology,                      Energy capacity_Hours, Charge efficiency_%, Discharge efficiency_%, Allowable max state of charge_%, Allowable min state of charge_%, Annual degradation_%
            Battery storage (2hrs storage),  2.0,                   92.0,                92.0,                   100,                             0,                               1.8
            Distributed Resources Batteries, 2.0,                   92.0,                92.0,                   100,                             0,                               1.8
        """),
        "pumped_hydro_new_entrant_properties": csv_str_to_df("""
            Power Station / Technology,    Storage capacity (hours), Pumping efficiency (%)
            Pumped Hydro (24hrs storage),  24,                       76
            BOTN - Cethana - 20h,          20,                       80
        """),
        "fixed_opex_new_entrants": csv_str_to_df("""
            Technology Type,                 Base value ($/kW/year)),  Unit
            Battery storage (2hrs storage),  13.5,                     $
            Distributed Resources Batteries, 13.5,                     $
            Pumped Hydro (24hrs storage),    50.0,                     $
            BOTN - Cethana,                  50.0,                     $
        """),
        "lead_time_and_project_life": csv_str_to_df("""
            Technology,                      Economic life (years),  Technical life (years)
            Battery storage (2hrs storage),  20,                     20
            Distributed Resources Batteries, 20,                     20
            Pumped Hydro (24hrs storage),    40,                     90
            BOTN - Cethana,                  40,                     90
        """),
        "gpg_min_stable_level_new_entrants": csv_str_to_df("""
            Technology,                      Min Stable Level (% of nameplate)
            Battery storage (2hrs storage),  0.0
            Distributed Resources Batteries, 0.0
            Pumped Hydro (24hrs storage),    40.0
            BOTN - Cethana,                  40.0
        """),
    }


def test_template_storage_new_entrant(csv_str_to_df):
    # Wiring check only (per-helper behaviour is covered below): generators are
    # dropped, identity + property columns are produced, and one row per surviving
    # storage unit (battery + PHES) is returned. Detailed content is covered by the
    # per-helper tests.
    new_entrants_summary = csv_str_to_df("""
        IASR ID / DLT names,            Technology Type,                 Fuel type,  Fuel cost mapping,  REZ ID,         Sub-region
        Q1_WH_Far North QLD,            Wind,                            Wind,       Wind,               Q1,             NQ
        NQ OCGT Small,                  OCGT (small GT),                 Gas,        QLD new OCGT,       Not Applicable, NQ
        NQ Battery 2hrs,                Battery Storage (2hrs storage),  Battery,    Battery,            N3,             NQ
        NQ Battery - Distributed,       Distributed Resources Batteries, Battery,    Battery,            Not Applicable, NQ
        BOTN - Cethana - 20h,           Pumped Hydro (24hrs storage),    Water,      Hydro,              Not Applicable, NQ
    """)
    iasr_tables = {
        "new_entrants_summary": new_entrants_summary,
        **_storage_property_tables(csv_str_to_df),
    }

    result = _template_storage_new_entrant(iasr_tables)

    # generator rows dropped -> 3 of 5 rows survive; identity + property columns in order
    assert list(result.columns) == _STORAGE_IDENTITY_COLUMNS + _STORAGE_PROPERTY_COLUMNS
    assert len(result) == 3


# --- _assert_property_table_attrs ---


def test_assert_property_table_attrs_valid_table(csv_str_to_df):
    # Table has both required columns and at least one row - no error raised.
    table = csv_str_to_df("""
        Technology,  Base value
        Wind,        20.0
    """)
    attrs = {
        "fom": {
            "table": "fixed_opex_new_entrants",
            "technology_col": "Technology",
            "value_col": "Base value",
            "scale": 1000.0,
        }
    }
    # should not raise
    _assert_property_table_attrs(table, "fixed_opex_new_entrants", attrs)


def test_assert_property_table_attrs_raises_missing_columns(csv_str_to_df):
    # Table is missing technology_col - raised message names the source table,
    # and the missing columns - including the 'Storage Hours' column with different
    # capitalisation to expected 'Storage hours'.
    # Two properties share the source table - both missing columns are reported
    # together in one raise.
    table = csv_str_to_df("""
        Technology,    Storage Hours
        Battery (2h),  2
    """)
    attrs = {
        "storage_hours": {
            "table": "battery_properties",
            "technology_col": "Technology",
            "value_col": "Storage hours",
        },
        "degradation_annual": {
            "table": "battery_properties",
            "technology_col": "Technology",
            "value_col": "Variable value",
        },
    }

    with pytest.raises(
        ValueError,
        match=r"'battery_properties' table missing required columns: "
        r"\['Storage hours', 'Variable value'\]",
    ):
        _assert_property_table_attrs(table, "battery_properties", attrs)


def test_assert_property_table_attrs_raises_empty_table():
    # Table has both required columns but no rows - raise, naming every property
    # sourced from the table.
    table = pd.DataFrame(columns=["Technology", "Base value"])
    attrs = {
        "fom": {
            "table": "fixed_opex_new_entrants",
            "technology_col": "Technology",
            "value_col": "Base value",
            "scale": 1000.0,
        }
    }

    with pytest.raises(
        ValueError,
        match=r"'fixed_opex_new_entrants' table is empty - cannot merge properties '\['fom'\]'",
    ):
        _assert_property_table_attrs(table, "fixed_opex_new_entrants", attrs)


# --- _group_by_source_key ---


def test_group_by_source_key():
    # Two properties sharing a (table, technology_col) source are grouped together,
    # each keeping its original attrs dict unchanged; two properties from the same
    # table but with different technology_cols are independent.
    property_map = {
        "storage_hours": {
            "table": "battery_properties",
            "technology_col": "Technology",
            "value_col": "Energy capacity_Hours",
        },
        "efficiency_charge": {
            "table": "battery_properties",
            "technology_col": "Technology",
            "value_col": "Charge efficiency_%",
        },
        "lifetime_technical": {
            "table": "lead_time_and_project_life",
            "technology_col": "Technology",
            "value_col": "Technical life (years)",
        },
        "different_tech_col": {
            "table": "lead_time_and_project_life",
            "technology_col": "Alternate Technology",
            "value_col": "Test",
        },
    }

    result = _group_by_source_key(property_map)

    expected = {
        ("battery_properties", "Technology"): {
            "storage_hours": property_map["storage_hours"],
            "efficiency_charge": property_map["efficiency_charge"],
        },
        ("lead_time_and_project_life", "Technology"): {
            "lifetime_technical": property_map["lifetime_technical"],
        },
        ("lead_time_and_project_life", "Alternate Technology"): {
            "different_tech_col": property_map["different_tech_col"]
        },
    }
    assert result == expected


# --- _merge_properties ---


def test_merge_properties(csv_str_to_df, caplog):
    # storage_hours and efficiency_charge both come from battery_properties/Technology
    # (as in _STORAGE_BATTERY_PROPERTY_MAP): both are merged correctly in one pass,
    # NaN property values are retained untouched, and - because they share a source
    # table - the fuzzy match against it runs once, so a corrected technology name is
    # logged once, not once per property sourced from that table.
    new_entrants = csv_str_to_df("""
        name,             technology
        NQ Battery - 2h,  battery storage (2hrs storage)
        NQ CCGT,          CCGT
    """)
    property_map = {
        "storage_hours": {
            "table": "battery_properties",
            "technology_col": "Technology",
            "value_col": "Energy capacity_Hours",
        },
        "efficiency_charge": {
            "table": "battery_properties",
            "technology_col": "Technology",
            "value_col": "Charge efficiency_%",
        },
    }
    iasr_tables = {
        "battery_properties": csv_str_to_df("""
            Technology,                      Energy capacity_Hours, Charge efficiency_%
            Battery Storage (2hrs storage),  2.0,                   92.0
            CCGT,                            ,
        """),
    }

    with caplog.at_level("INFO"):
        result = _merge_properties(new_entrants, iasr_tables, property_map)

    expected = csv_str_to_df("""
        name,             technology,                      storage_hours, efficiency_charge
        NQ Battery - 2h,  battery storage (2hrs storage),  2.0,           92.0
        NQ CCGT,          CCGT,                            ,
    """)
    pd.testing.assert_frame_equal(result, expected)

    msg = (
        "'battery storage (2hrs storage)' matched to "
        "'Battery Storage (2hrs storage)' whilst merging new entrant properties "
        "from 'battery_properties'"
    )
    assert caplog.messages.count(msg) == 1


# --- _merge_phes_properties / _override_botn_technology / _derive_phes_symmetric_efficiency ---


def test_merge_phes_properties(csv_str_to_df):
    # storage_hours is merged by technology after BOTN's technology is overridden to its own
    # name and the pumped-hydro table's BOTN key normalised to match; charge/discharge
    # efficiency are derived from the single round-trip pumping efficiency.
    phes = csv_str_to_df("""
        name,                  technology
        NQ Pumped Hydro - 24h, Pumped Hydro (24hrs storage)
        BOTN - Cethana - 20h,  Pumped Hydro (24hrs storage)
    """)
    iasr_tables = {
        "pumped_hydro_new_entrant_properties": csv_str_to_df("""
            Power Station / Technology,    Storage capacity (hours), Pumping efficiency (%)
            Pumped Hydro (24hrs storage),  24,                       64
            BOTN - Cethana - 20h,          20,                       81
        """)
    }

    result = _merge_phes_properties(phes, iasr_tables)

    expected = csv_str_to_df("""
        name,                  technology,                    storage_hours, round_trip_efficiency, efficiency_charge, efficiency_discharge
        NQ Pumped Hydro - 24h, Pumped Hydro (24hrs storage),  24.0,          64.0,                  80.0,              80.0
        BOTN - Cethana - 20h,  BOTN - Cethana,                20.0,          81.0,                  90.0,              90.0
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=False, rtol=1e-6)


def test_override_botn_technology(csv_str_to_df):
    # BOTN's row takes its own name as 'technology'; other PHES rows are untouched.
    phes = csv_str_to_df("""
        name,                  technology
        NQ Pumped Hydro - 24h, Pumped Hydro (24hrs storage)
        BOTN - Cethana - 20h,  Pumped Hydro (24hrs storage)
    """)

    result = _override_botn_technology(phes)

    expected = pd.Series(
        ["Pumped Hydro (24hrs storage)", "BOTN - Cethana"], name="technology"
    )
    pd.testing.assert_series_equal(result, expected)


def test_override_botn_technology_no_botn_row(csv_str_to_df):
    # BOTN absent (e.g. a scenario without it) -> technology returned unchanged, no raise.
    phes = csv_str_to_df("""
        name,                  technology
        NQ Pumped Hydro - 24h, Pumped Hydro (24hrs storage)
    """)

    result = _override_botn_technology(phes)

    expected = pd.Series(["Pumped Hydro (24hrs storage)"], name="technology")
    pd.testing.assert_series_equal(result, expected)


def test_assert_botn_technology_expected_raises_on_unexpected_value(csv_str_to_df):
    # BOTN's summary 'technology' isn't the expected value -> raise before overriding,
    # flagging a new_entrants_summary change the override would otherwise mishandle.
    phes = csv_str_to_df("""
        name,                  technology
        BOTN - Cethana - 20h,  Pumped Hydro (48hrs storage)
    """)

    with pytest.raises(
        ValueError,
        match=(
            r"'BOTN - Cethana' technology should be 'Pumped Hydro \(24hrs storage\)': "
            r"got \['Pumped Hydro \(48hrs storage\)'\]"
        ),
    ):
        _assert_botn_technology_expected(phes)


def test_derive_phes_symmetric_efficiency(csv_str_to_df):
    # A single round-trip efficiency becomes equal charge and discharge legs, each its
    # square root: sqrt(0.91) ≈ 0.9 -> 90.0%.
    phes = csv_str_to_df("""
        name,                  round_trip_efficiency
        NQ Pumped Hydro - 24h, 81.0
    """)

    result = _derive_phes_symmetric_efficiency(phes)

    expected = csv_str_to_df("""
        name,                  round_trip_efficiency, efficiency_charge, efficiency_discharge
        NQ Pumped Hydro - 24h, 81.0,                  90.0,              90.0
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=False, rtol=1e-6)


def test_merge_phes_properties_empty(csv_str_to_df):
    # No PHES rows -> returns empty with the PHES-derived columns added.
    phes = pd.DataFrame(columns=["name", "technology"])
    iasr_tables = {
        "pumped_hydro_new_entrant_properties": csv_str_to_df("""
            Power Station / Technology,    Storage capacity (hours), Pumping efficiency (%)
            Pumped Hydro (24hrs storage),  24,                       76
        """)
    }

    result = _merge_phes_properties(phes, iasr_tables)

    expected = csv_str_to_df("""
        name, technology, storage_hours, round_trip_efficiency, efficiency_charge, efficiency_discharge
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _normalise_phes_botn_key ---


def test_normalise_phes_botn_key(csv_str_to_df):
    # The pumped-hydro table's full BOTN spelling is renamed to the bare name so it matches
    # the overridden 'technology'; the shared iasr_tables dict is not mutated.
    pumped_hydro = csv_str_to_df("""
        Power Station / Technology,    Storage capacity (hours), Pumping efficiency (%)
        Pumped Hydro (24hrs storage),  24,                       76
        BOTN - Cethana - 20h,          20,                       81
    """)
    iasr_tables = {"pumped_hydro_new_entrant_properties": pumped_hydro}
    before = pumped_hydro.copy()

    result = _normalise_phes_botn_key(iasr_tables)

    expected = csv_str_to_df("""
        Power Station / Technology,    Storage capacity (hours), Pumping efficiency (%)
        Pumped Hydro (24hrs storage),  24,                       76
        BOTN - Cethana,                20,                       81
    """)
    pd.testing.assert_frame_equal(
        result["pumped_hydro_new_entrant_properties"], expected
    )
    # the shared dict's table is left untouched
    pd.testing.assert_frame_equal(
        iasr_tables["pumped_hydro_new_entrant_properties"], before
    )


# --- _set_geo_id ---


def test_set_geo_id(csv_str_to_df):
    # Check that the wrapper adds 'geo_id' column, correctly applying ``_pick_location``
    # and not impacting existing columns.
    new_entrants = csv_str_to_df("""
        technology,                     REZ ID,         Sub-region
        Wind,                           N3,             CNSW
        OCGT (small GT),                Not Applicable, NQ
    """)

    result = _set_geo_id(new_entrants)

    expected = csv_str_to_df("""
        technology,                     REZ ID,         Sub-region, geo_id
        Wind,                           N3,             CNSW,       N3
        OCGT (small GT),                Not Applicable, NQ,         NQ
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_set_geo_id_empty_input(csv_str_to_df):
    # Empty input still returns the added geo_id column
    new_entrants = pd.DataFrame(columns=["technology", "REZ ID", "Sub-region"])

    result = _set_geo_id(new_entrants)

    expected = csv_str_to_df("""
        technology, REZ ID, Sub-region, geo_id
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _add_resource_type (generator-specific) ---


def test_add_resource_type(csv_str_to_df):
    # resource_type is read from the underscore-delimited code in `name`. WH/WM are
    # already separate rows (no explosion). CST (solar thermal) maps to "solar"; the
    # underscore-free thermal / distributed IDs map to NaN (blank field).
    gens = csv_str_to_df("""
        name,                               technology
        Q1_WH_Far North QLD,                Wind
        Q1_WM_Far North QLD,                Wind
        N10_WFX_Hunter Coast,               Wind - offshore (fixed)
        DREZ_SAT_Dubbo,                     Large scale Solar PV
        N0_CST_NSW,                         Solar Thermal (16hrs storage)
        CNSW SAT - Distributed Resources,   Distributed Resources Solar
        CNSW OCGT Small,                    OCGT (small GT)
    """)

    result = _add_resource_type(gens)

    expected = csv_str_to_df("""
        name,                               technology,                     resource_type
        Q1_WH_Far North QLD,                Wind,                           wind_high
        Q1_WM_Far North QLD,                Wind,                           wind_medium
        N10_WFX_Hunter Coast,               Wind - offshore (fixed),        wind_offshore_fixed
        DREZ_SAT_Dubbo,                     Large scale Solar PV,           solar
        N0_CST_NSW,                         Solar Thermal (16hrs storage),  solar
        CNSW SAT - Distributed Resources,   Distributed Resources Solar,
        CNSW OCGT Small,                    OCGT (small GT),
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_add_resource_type_empty_input():
    # test empty input still returns the input df columns + resource_type column
    empty_input = pd.DataFrame(columns=["name", "technology"])

    result = _add_resource_type(empty_input)

    expected = pd.DataFrame(columns=["name", "technology", "resource_type"])
    pd.testing.assert_frame_equal(result, expected)
