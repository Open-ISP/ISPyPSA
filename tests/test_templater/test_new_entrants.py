import pandas as pd
import pytest

from ispypsa.templater.new_entrants import (
    _GENERATOR_IDENTITY_COLUMNS,
    _GENERATOR_PROPERTY_COLUMNS,
    _STORAGE_IDENTITY_COLUMNS,
    _add_resource_type,
    _assert_property_table_attrs,
    _merge_technology_keyed_property,
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


def test_template_storage_new_entrant(csv_str_to_df):
    # Wiring check only (per-helper behaviour is covered below): generators are
    # dropped, the identity columns are produced, and one row per surviving storage
    # unit is returned. Detailed content is covered by the per-helper tests.
    new_entrants_summary = csv_str_to_df("""
        IASR ID / DLT names,            Technology Type,                 Fuel type,  Fuel cost mapping,  REZ ID,         Sub-region
        Q1_WH_Far North QLD,            Wind,                            Wind,       Wind,               Q1,             NQ
        NQ OCGT Small,                  OCGT (small GT),                 Gas,        QLD new OCGT,       Not Applicable, NQ
        NQ Battery 2hrs,                Battery Storage (2hrs storage),  Battery,    Battery,            N3,             NQ
        NQ Battery - Distributed,       Distributed Resources Batteries, Battery,    Battery,            Not Applicable, NQ
        Snowy PH 24hr,                  Pumped Hydro (24hrs storage),    Water,      Water,              Not Applicable, NQ
    """)

    result = _template_storage_new_entrant(new_entrants_summary)

    # generator rows dropped -> 3 of 5 rows survive; identity columns produced in order
    assert list(result.columns) == _STORAGE_IDENTITY_COLUMNS
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
        new_entrants, property_table, "Technology", "Base value", "fom", scale=1000.0
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
