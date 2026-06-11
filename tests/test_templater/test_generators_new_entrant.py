import pandas as pd

from ispypsa.templater.generators_new_entrant import (
    _IDENTITY_COLUMNS,
    _add_resource_type,
    _drop_storage_technologies,
    _rename_summary_columns,
    _set_geo_id,
    _template_generators_new_entrant,
)

# --- _template_generators_new_entrant (orchestrator) ---


def test_template_generators_new_entrant(csv_str_to_df):
    # Wiring check only (per-helper behaviour is covered above): storage is dropped,
    # the identity columns are produced, and one row per surviving generating unit
    # is returned. Detailed content is covered by the per-helper tests.
    new_entrants_summary = csv_str_to_df("""
        IASR__ID__/__DLT__names,           Technology__Type,                  Fuel__type, Fuel__cost__mapping, REZ__ID,         Sub-region
        Q1_WH_Far__North__QLD,             Wind,                              Wind,       Wind,                Q1,              NQ
        Q1_WM_Far__North__QLD,             Wind,                              Wind,       Wind,                Q1,              NQ
        Q1_SAT_Far__North__QLD,            Large__scale__Solar__PV,           Solar,      Solar,               Q1,              NQ
        NQ__OCGT__Small,                   OCGT__(small__GT),                 Gas,        QLD__new__OCGT,      Not__Applicable, NQ
        NQ__SAT__-__Distributed__Resources,Distributed__Resources__Solar,     Solar,      Solar,               Not__Applicable, NQ
        NQ__Battery__2hrs,                 Battery__Storage__(2hrs__storage), Battery,    Battery,             Not__Applicable, NQ
    """)

    result = _template_generators_new_entrant(new_entrants_summary)

    # storage row dropped -> 5 of 6 rows survive; identity columns produced in order
    assert list(result.columns) == _IDENTITY_COLUMNS
    assert len(result) == 5


# --- _drop_storage_technologies ---


def test_drop_storage_technologies(csv_str_to_df):
    # All storage variants (batteries, distributed batteries, pumped hydro) are
    # dropped; generation rows pass through unchanged with other columns intact.
    new_entrants_summary = csv_str_to_df("""
        Technology__Type,                  REZ__ID
        Wind,                              N3
        Large__scale__Solar__PV,           N3
        Battery__Storage__(2hrs__storage), N3
        Distributed__Resources__Batteries, Not__Applicable
        Pumped__Hydro__(24hrs__storage),   Not__Applicable
        OCGT__(small__GT),                 Not__Applicable
    """)

    result = _drop_storage_technologies(new_entrants_summary)

    expected = csv_str_to_df("""
        Technology__Type,                  REZ__ID
        Wind,                              N3
        Large__scale__Solar__PV,           N3
        OCGT__(small__GT),                 Not__Applicable
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_drop_storage_technologies_empty_input(csv_str_to_df):
    # Empty input (all columns, no rows) returns an empty frame, no errors.
    new_entrants_summary = pd.DataFrame(columns=["Technology Type", "REZ ID"])

    result = _drop_storage_technologies(new_entrants_summary)

    expected = csv_str_to_df("""
        Technology__Type, REZ__ID
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _rename_summary_columns ---


def test_rename_summary_columns(csv_str_to_df):
    # The IASR ID, technology and fuel columns are renamed to their schema names;
    # other columns (REZ ID) pass through untouched.
    gens = csv_str_to_df("""
        IASR__ID__/__DLT__names, Technology__Type, Fuel__type, Fuel__cost__mapping, REZ__ID
        Q1_WH_Far__North__QLD,   Wind,             Wind,       Wind,                Q1
        CNSW__OCGT__Small,       OCGT__(small__GT),Gas,        NSW__new__OCGT,      Not__Applicable
    """)

    result = _rename_summary_columns(gens)

    expected = csv_str_to_df("""
        name,                    technology,       fuel_type,  fuel_price_mapping,  REZ__ID
        Q1_WH_Far__North__QLD,   Wind,             Wind,       Wind,                Q1
        CNSW__OCGT__Small,       OCGT__(small__GT),Gas,        NSW__new__OCGT,      Not__Applicable
    """)
    pd.testing.assert_frame_equal(result, expected)


# --- _set_geo_id ---


def test_set_geo_id(csv_str_to_df):
    # REZ-located rows take their REZ ID (incl. Non-REZ N0/V0); thermal and
    # distributed rows ("Not Applicable") fall back to their Sub-region.
    gens = csv_str_to_df("""
        technology,                   REZ__ID,         Sub-region
        Wind,                         N3,              CNSW
        Large__scale__Solar__PV,      N0,              CNSW
        OCGT__(small__GT),            Not__Applicable, NQ
        Distributed__Resources__Solar,Not__Applicable, SQ
    """)

    result = _set_geo_id(gens)

    expected = csv_str_to_df("""
        technology,                   REZ__ID,         Sub-region, geo_id
        Wind,                         N3,              CNSW,       N3
        Large__scale__Solar__PV,      N0,              CNSW,       N0
        OCGT__(small__GT),            Not__Applicable, NQ,         NQ
        Distributed__Resources__Solar,Not__Applicable, SQ,        SQ
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_set_geo_id_empty_input(csv_str_to_df):
    # Empty input still returns the geo_id column (all columns, no rows).
    gens = pd.DataFrame(columns=["technology", "REZ ID", "Sub-region"])

    result = _set_geo_id(gens)

    expected = csv_str_to_df("""
        technology, REZ__ID, Sub-region, geo_id
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _add_resource_type ---


def test_add_resource_type(csv_str_to_df):
    # resource_type is read from the underscore-delimited code in `name`. WH/WM are
    # already separate rows (no explosion). CST (solar thermal) maps to "solar"; the
    # underscore-free thermal / distributed IDs map to NaN (blank field).
    gens = csv_str_to_df("""
        name,                              technology
        Q1_WH_Far__North__QLD,             Wind
        Q1_WM_Far__North__QLD,             Wind
        N10_WFX_Hunter__Coast,             Wind__-__offshore__(fixed)
        DREZ_SAT_Dubbo,                    Large__scale__Solar__PV
        N0_CST_NSW,                        Solar__Thermal__(16hrs__storage)
        CNSW__SAT__-__Distributed__Resources, Distributed__Resources__Solar
        CNSW__OCGT__Small,                 OCGT__(small__GT)
    """)

    result = _add_resource_type(gens)

    expected = csv_str_to_df("""
        name,                              technology,                       resource_type
        Q1_WH_Far__North__QLD,             Wind,                             wind_high
        Q1_WM_Far__North__QLD,             Wind,                             wind_medium
        N10_WFX_Hunter__Coast,             Wind__-__offshore__(fixed),       wind_offshore_fixed
        DREZ_SAT_Dubbo,                    Large__scale__Solar__PV,          solar
        N0_CST_NSW,                        Solar__Thermal__(16hrs__storage), solar
        CNSW__SAT__-__Distributed__Resources, Distributed__Resources__Solar,
        CNSW__OCGT__Small,                 OCGT__(small__GT),
    """)
    pd.testing.assert_frame_equal(result, expected)
