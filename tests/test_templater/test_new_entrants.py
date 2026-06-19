import pandas as pd
import pytest

from ispypsa.templater.new_entrants import (
    _GENERATOR_IDENTITY_COLUMNS,
    _STORAGE_IDENTITY_COLUMNS,
    _add_resource_type,
    _filter_to_technology_group,
    _set_geo_id,
    _template_generators_new_entrant,
    _template_storage_new_entrant,
)

# --- orchestrators ---


def test_template_generators_new_entrant(csv_str_to_df):
    # Wiring check only (per-helper behaviour is covered below): storage is dropped,
    # the identity columns are produced, and one row per surviving generating unit
    # is returned. Detailed content is covered by the per-helper tests.
    new_entrants_summary = csv_str_to_df("""
        IASR ID / DLT names,            Technology Type,                Fuel type,  Fuel cost mapping,  REZ ID,         Sub-region
        Q1_WH_Far North QLD,            Wind,                           Wind,       Wind,               Q1,             NQ
        Q1_WM_Far North QLD,            Wind,                           Wind,       Wind,               Q1,             NQ
        Q1_SAT_Far North QLD,           Large scale Solar PV,           Solar,      Solar,              Q1,             NQ
        NQ OCGT Small,                  OCGT (small GT),                Gas,        QLD new OCGT,       Not Applicable, NQ
        NQ SAT - Distributed Resources, Distributed Resources Solar,    Solar,      Solar,              Not Applicable, NQ
        NQ Battery 2hrs,                Battery Storage (2hrs storage), Battery,    Battery,            Not Applicable, NQ
    """)

    result = _template_generators_new_entrant(new_entrants_summary)

    # storage row dropped -> 5 of 6 rows survive; identity columns produced in order
    assert list(result.columns) == _GENERATOR_IDENTITY_COLUMNS
    assert len(result) == 5


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


# --- _filter_to_technology_group ---


def test_filter_to_technology_group(csv_str_to_df):
    # test core split/filter function returns both groups correctly
    new_entrants_summary = csv_str_to_df("""
        Technology Type,                    REZ ID
        Wind,                               N3
        Large scale Solar PV,               N3
        Battery Storage (2hrs storage),     N3
        Distributed Resources Batteries,    Not Applicable
        Pumped Hydro (24hrs storage),       Not Applicable
        OCGT (small GT),                    Not Applicable
    """)

    # All storage variants (batteries, distributed batteries, pumped hydro) are
    # dropped; generation rows pass through unchanged with other columns intact.
    generators = _filter_to_technology_group(new_entrants_summary, "generators")

    expected_gens = csv_str_to_df("""
        Technology Type,        REZ ID
        Wind,                   N3
        Large scale Solar PV,   N3
        OCGT (small GT),        Not Applicable
    """)
    pd.testing.assert_frame_equal(generators, expected_gens)

    # Only storage variants (batteries, distributed batteries, pumped hydro) are
    # kept - unchanged, with other columns intact.
    storage = _filter_to_technology_group(new_entrants_summary, "storage")

    expected_storage = csv_str_to_df("""
        Technology Type,                    REZ ID
        Battery Storage (2hrs storage),     N3
        Distributed Resources Batteries,    Not Applicable
        Pumped Hydro (24hrs storage),       Not Applicable
    """)
    pd.testing.assert_frame_equal(storage, expected_storage)


def test_filter_to_technology_group_raises_unknown_group(csv_str_to_df, caplog):
    # Raises on non-permitted 'group' arg (not "generators" or "storage")
    new_entrants_summary = csv_str_to_df("""
        Technology Type,                    REZ ID
        Wind,                               N3
        Pumped Hydro (24hrs storage),       Not Applicable
    """)

    with pytest.raises(ValueError, match="group must be 'generators' or 'storage'"):
        _filter_to_technology_group(new_entrants_summary, "computers")


def test_filter_to_technology_group_empty_input(csv_str_to_df):
    # Empty input (all columns, no rows) returns an empty frame, no errors.
    new_entrants_summary = pd.DataFrame(columns=["Technology Type", "REZ ID"])

    result = _filter_to_technology_group(new_entrants_summary, "storage")

    expected = csv_str_to_df("""
        Technology Type, REZ ID
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _set_geo_id ---


def test_set_geo_id(csv_str_to_df):
    # REZ-located rows take their REZ ID (incl. Non-REZ N0/V0); thermal, pumped hydro
    # and distributed rows ("Not Applicable") fall back to their Sub-region.
    new_entrants = csv_str_to_df("""
        technology,                     REZ ID,         Sub-region
        Wind,                           N3,             CNSW
        Large scale Solar PV,           N0,             CNSW
        OCGT (small GT),                Not Applicable, NQ
        Pumped Hydro (24hrs storage),   Not Applicable, SNW
    """)

    result = _set_geo_id(new_entrants)

    expected = csv_str_to_df("""
        technology,                     REZ ID,         Sub-region, geo_id
        Wind,                           N3,             CNSW,       N3
        Large scale Solar PV,           N0,             CNSW,       N0
        OCGT (small GT),                Not Applicable, NQ,         NQ
        Pumped Hydro (24hrs storage),   Not Applicable, SNW,        SNW
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_set_geo_id_empty_input(csv_str_to_df):
    # Empty input still returns the geo_id column (all columns, no rows).
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
