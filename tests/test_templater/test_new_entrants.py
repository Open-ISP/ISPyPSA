import pandas as pd
import pytest

from ispypsa.templater.new_entrants import (
    _GENERATOR_IDENTITY_COLUMNS,
    _GENERATOR_PROPERTY_COLUMNS,
    _STORAGE_IDENTITY_COLUMNS,
    _STORAGE_PROPERTY_COLUMNS,
    _add_resource_type,
    _assert_botn_cethana_values_match_technology,
    _assert_property_table_attrs,
    _derive_phes_symmetric_efficiency,
    _merge_battery_properties,
    _merge_phes_properties,
    _merge_technology_keyed_property,
    _phes_lookup_key,
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
        "table": "fixed_opex_new_entrants",
        "technology_col": "Technology",
        "value_col": "Base value",
        "scale": 1000.0,
    }
    # should not raise
    _assert_property_table_attrs(table, attrs, "fom")


def test_assert_property_table_attrs_raises_missing_columns(csv_str_to_df):
    # Table is missing technology_col - raised message names the source table,
    # and the missing columns - including the 'Base Value' column with different
    # capitalisation to expected 'Base value'.
    table = csv_str_to_df("""
        Base Value
        20.0
    """)
    attrs = {
        "table": "heat_rate",
        "technology_col": "Technology",
        "value_col": "Base value",
        "scale": 1.0,
    }

    with pytest.raises(
        ValueError,
        match=r"'heat_rate' table missing required columns: \['Base value', 'Technology'\]",
    ):
        _assert_property_table_attrs(table, attrs, "fom")


def test_assert_property_table_attrs_raises_empty_table():
    # Table has both required columns but no rows - raise
    table = pd.DataFrame(columns=["Technology", "Base value"])
    attrs = {
        "table": "fixed_opex_new_entrants",
        "technology_col": "Technology",
        "value_col": "Base value",
        "scale": 1000.0,
    }

    with pytest.raises(
        ValueError,
        match="'fixed_opex_new_entrants' table is empty - cannot merge property 'fom'",
    ):
        _assert_property_table_attrs(table, attrs, "fom")


# --- _merge_technology_property ---


def test_merge_technology_property(csv_str_to_df):
    # Looks up one value per technology, fuzzy-matching spelling differences
    # and applying the scale. Duplicate technologies all receive the value;
    # canon spelling is kept. NaN property values are retained untouched.
    new_entrants = csv_str_to_df("""
        name,   technology
        A,      Wind
        B,      Battery Storage (2hrs storage)
        C,      Wind
        D,      CCGT
    """)
    property_table = csv_str_to_df("""
        Technology,                     Base value
        Wind,                           20.0
        Battery storage (2hrs storage), 17.0
        CCGT,                           NaN
    """)

    result = _merge_technology_keyed_property(
        new_entrants,
        property_table,
        "Technology",
        "Base value",
        "fom",
        scale=1000.0,
        key_col="technology",
    )

    expected = csv_str_to_df("""
        name,   technology,                     fom
        A,      Wind,                           20000.0
        B,      Battery Storage (2hrs storage), 17000.0
        C,      Wind,                           20000.0
        D,      CCGT,                           NaN
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_merge_technology_property_empty_new_entrants(csv_str_to_df):
    # Test that empty new_entrants df returns with new column added (empty)
    new_entrants = csv_str_to_df("""
        name,   technology
    """)
    property_table = csv_str_to_df("""
        Technology,                     Base value
        Wind,                           20.0
        Battery storage (2hrs storage), 17.0
        CCGT,                           NaN
    """)

    expected_result = pd.DataFrame(columns=["name", "technology", "fom"])
    result = _merge_technology_keyed_property(
        new_entrants, property_table, "Technology", "Base value", "fom", 1000.0
    )
    pd.testing.assert_frame_equal(
        result,
        expected_result,
        check_dtype=False,
    )


# --- _merge_battery_properties ---


def test_merge_battery_properties(csv_str_to_df):
    # Every battery property is looked up by technology from battery_properties, with the
    # summary's capitalisation ("Battery Storage") fuzzy-matched to the table's spelling.
    batteries = csv_str_to_df("""
        name,             technology
        NQ Battery - 2h,  Battery Storage (2hrs storage)
    """)
    iasr_tables = {
        "battery_properties": csv_str_to_df("""
            Technology,                     Energy capacity_Hours, Charge efficiency_%, Discharge efficiency_%, Allowable max state of charge_%, Allowable min state of charge_%, Annual degradation_%
            Battery storage (2hrs storage), 2.0,                   92.0,                92.0,                   100,                             0,                               1.8
        """)
    }

    result = _merge_battery_properties(batteries, iasr_tables)

    expected = csv_str_to_df("""
        name,             technology,                     storage_hours, efficiency_charge, efficiency_discharge, soc_max, soc_min, degradation_annual
        NQ Battery - 2h,  Battery Storage (2hrs storage), 2.0,           92.0,              92.0,                 100.0,   0.0,     1.8
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_merge_battery_properties_empty(csv_str_to_df):
    # No battery rows -> returns empty with the battery property columns added.
    batteries = pd.DataFrame(columns=["name", "technology"])
    iasr_tables = {
        "battery_properties": csv_str_to_df("""
            Technology,                     Energy capacity_Hours, Charge efficiency_%, Discharge efficiency_%, Allowable max state of charge_%, Allowable min state of charge_%, Annual degradation_%
            Battery storage (2hrs storage), 2.0,                   92.0,                92.0,                   100,                             0,                               1.8
        """)
    }

    result = _merge_battery_properties(batteries, iasr_tables)

    expected = csv_str_to_df("""
        name, technology, storage_hours, efficiency_charge, efficiency_discharge, soc_max, soc_min, degradation_annual
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _merge_phes_properties / _phes_lookup_key / _derive_phes_symmetric_efficiency ---


def test_merge_phes_properties(csv_str_to_df):
    # storage_hours is merged by name-or-technology key; charge/discharge efficiency are
    # derived from the single round-trip pumping efficiency. The named station (BOTN) picks
    # up its own 20h/80% row, the generic PHES its technology's 24h/76% row.
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
        name,                  technology,                    phes_key,                      storage_hours, round_trip_efficiency, efficiency_charge, efficiency_discharge
        NQ Pumped Hydro - 24h, Pumped Hydro (24hrs storage),  Pumped Hydro (24hrs storage),  24.0,          64.0,                  80.0,              80.0
        BOTN - Cethana - 20h,  Pumped Hydro (24hrs storage),  BOTN - Cethana - 20h,          20.0,          81.0,                  90.0,              90.0
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=False, rtol=1e-6)


def test_phes_lookup_key(csv_str_to_df):
    # Named stations (present in ``_BOTN_CETHANA_DETAILS``) keys on name - only
    # 'BOTN - Cethana - 20h' at v7.5; generic rows key on technology.
    phes = csv_str_to_df("""
        name,                  technology
        NQ Pumped Hydro - 24h, Pumped Hydro (24hrs storage)
        BOTN - Cethana - 20h,  Pumped Hydro (24hrs storage)
    """)

    result = _phes_lookup_key(phes)

    expected = pd.Series(
        ["Pumped Hydro (24hrs storage)", "BOTN - Cethana - 20h"], name="name"
    )
    pd.testing.assert_series_equal(result, expected)


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
        name, technology, phes_key, storage_hours, round_trip_efficiency, efficiency_charge, efficiency_discharge
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _assert_botn_cethana_values_match_technology ---


def _botn_common_tables(csv_str_to_df, botn_fom="75.0"):
    """The common tables the BOTN guard checks, with BOTN keyed by its bare name
    alongside its 'Pumped Hydro (24hrs storage)' archetype. ``botn_fom`` lets a test make
    BOTN's fom diverge from the matching technology's (75.0)."""
    return {
        "fixed_opex_new_entrants": csv_str_to_df(f"""
            Technology Type,               Base value ($/kW/year)),  Unit
            Pumped Hydro (24hrs storage),  75.0,                     $
            BOTN - Cethana,                {botn_fom},               $
        """),
        "lead_time_and_project_life": csv_str_to_df("""
            Technology,                    Economic life (years),  Technical life (years)
            Pumped Hydro (24hrs storage),  40,                     90
            BOTN - Cethana,                40,                     90
        """),
        "gpg_min_stable_level_new_entrants": csv_str_to_df("""
            Technology,                    Min Stable Level (% of nameplate)
            Pumped Hydro (24hrs storage),  40.0
            BOTN - Cethana,                40.0
        """),
    }


def test_assert_botn_cethana_values_match_technology_passes_when_matching(
    csv_str_to_df,
):
    # BOTN's values equal its technology's across every common table -> no raise.
    iasr_tables = _botn_common_tables(csv_str_to_df)
    _assert_botn_cethana_values_match_technology(iasr_tables)


def test_assert_botn_cethana_values_match_technology_raises_on_divergence(
    csv_str_to_df,
):
    # BOTN's fom no longer matches the technology's -> raise, naming the property and table.
    iasr_tables = _botn_common_tables(csv_str_to_df, botn_fom="99.0")

    with pytest.raises(
        ValueError,
        match=(
            r"'BOTN - Cethana' diverges from its technology "
            r"'Pumped Hydro \(24hrs storage\)' for 'Base value \(\$\/kW\/year\)\)' "
            r"in 'fixed_opex_new_entrants'"
        ),
    ):
        _assert_botn_cethana_values_match_technology(iasr_tables)


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
