import pandas as pd
import pytest

from ispypsa.templater.new_entrants import (
    _GENERATOR_IDENTITY_COLUMNS,
    _GENERATOR_PROPERTY_COLUMNS,
    _STORAGE_IDENTITY_COLUMNS,
    _STORAGE_PROPERTY_COLUMNS,
    _add_resource_type,
    _assert_botn_technology_expected,
    _assert_build_cost_zone_matches_geo_id,
    _collapse_geo_id_to_granularity,
    _merge_lcf_build,
    _merge_lcf_om,
    _merge_phes_properties,
    _override_botn_technology,
    _rekey_names_to_collapsed_geo_id,
    _reshape_technology_specific_lcfs,
    _template_generators_new_entrant,
    _template_storage_new_entrant,
)

# --- orchestrators ---

# A stand-in for tests where sub_regional_geography's content doesn't matter - e.g.
# regional_granularity="sub_regions" is a no-op in the collapse step, so it's never
# read at all; or the frame being collapsed is itself empty, so nothing ever gets
# looked up in it. See _collapse_geo_id_to_granularity.
_UNUSED_SUB_REGIONAL_GEOGRAPHY = pd.DataFrame(
    columns=["geo_id", "geo_type", "region_id"]
)


def test_template_generators_new_entrant(csv_str_to_df):
    # Wiring check only (per-helper behaviour is covered below): storage is dropped,
    # and the identity + property columns are produced, one row per generating unit.
    # Detailed content is covered by the per-helper tests.
    new_entrants_summary = csv_str_to_df("""
        IASR ID / DLT names,  Technology Type,                Fuel type,  REZ ID,         Sub-region, Regional build cost zone
        Q1_WH_Far North QLD,  Wind,                           Wind,       Q1,             NQ,         Q1
        NQ OCGT Small,        OCGT (small GT),                Gas,        Not Applicable, NQ,         NQ
        NQ Battery 2hrs,      Battery Storage (2hrs storage), Battery,    Not Applicable, NQ,         NQ
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
        "technology_specific_lcfs": csv_str_to_df("""
            Cost zone / REZ ID, REZ name / Description, Wind,           OCGT (small GT)
            Q1,                 Far North QLD,          1.05,           Not Applicable
            NQ,                 Subregional Ref Node,   Not Applicable, 1.08
        """),
        "locational_cost_factors": csv_str_to_df("""
            Cost zone / REZ ID, O&M costs 3
            Q1,                 122.0
            NQ,                 115.0
        """),
    }

    result = _template_generators_new_entrant(
        iasr_tables, "sub_regions", _UNUSED_SUB_REGIONAL_GEOGRAPHY
    )

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
        "technology_specific_lcfs": csv_str_to_df("""
            Cost zone / REZ ID, REZ name / Description, Battery storage (2hrs storage), Distributed Resources Batteries, BOTN - Cethana
            N3,                 Central-West Orana,     1.04,                           Not Applicable,                  Not Applicable
            NQ,                 Subregional Ref Node,   Not Applicable,                 1.06,                            100
        """),
        "locational_cost_factors": csv_str_to_df("""
            Cost zone / REZ ID, O&M costs 3
            N3,                 119.0
            NQ,                 115.0
        """),
    }


def test_template_storage_new_entrant(csv_str_to_df):
    # Wiring check only (per-helper behaviour is covered below): generators are
    # dropped, identity + property columns are produced, and one row per surviving
    # storage unit (battery + PHES) is returned. Detailed content is covered by the
    # per-helper tests.
    new_entrants_summary = csv_str_to_df("""
        IASR ID / DLT names,            Technology Type,                 Fuel type,  REZ ID,         Sub-region, Regional build cost zone
        Q1_WH_Far North QLD,            Wind,                            Wind,       Q1,             NQ,         Q1
        NQ OCGT Small,                  OCGT (small GT),                 Gas,        Not Applicable, NQ,         NQ
        NQ Battery 2hrs,                Battery Storage (2hrs storage),  Battery,    N3,             NQ,         N3
        NQ Battery - Distributed,       Distributed Resources Batteries, Battery,    Not Applicable, NQ,         NQ
        BOTN - Cethana - 20h,           Pumped Hydro (24hrs storage),    Water,      Not Applicable, NQ,         NQ
    """)
    iasr_tables = {
        "new_entrants_summary": new_entrants_summary,
        **_storage_property_tables(csv_str_to_df),
    }

    result = _template_storage_new_entrant(
        iasr_tables, "sub_regions", _UNUSED_SUB_REGIONAL_GEOGRAPHY
    )

    # generator rows dropped -> 3 of 5 rows survive; identity + property columns in order
    assert list(result.columns) == _STORAGE_IDENTITY_COLUMNS + _STORAGE_PROPERTY_COLUMNS
    assert len(result) == 3


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
        name,                  technology,                    storage_hours, efficiency_charge, efficiency_discharge
        NQ Pumped Hydro - 24h, Pumped Hydro (24hrs storage),  24.0,          80.0,              80.0
        BOTN - Cethana - 20h,  BOTN - Cethana,                20.0,          90.0,              90.0
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
        name, technology, storage_hours, efficiency_charge, efficiency_discharge
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# (BOTN's pumped-hydro key correction is now a plain _apply_iasr_table_replacements
# call -- see helpers.py / test_helpers.py for that mechanism's own tests. Its wiring
# here is covered incidentally by test_merge_phes_properties above, which already
# exercises a full-spelling BOTN row resolving correctly.)


# --- _collapse_geo_id_to_granularity ---


def test_collapse_geo_id_to_granularity_sub_regions_is_noop(csv_str_to_df):
    # sub_regions is already the finest granularity - returned as-is.
    new_entrants = csv_str_to_df("""
        name,            technology, geo_id, value
        CNSW OCGT Small, OCGT,       CNSW,   104.0
        Q1_WH,           Wind,       Q1,     999.0
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,     geo_type,       region_id
        CNSW,       subregion,      NSW
        Q1,         rez,            QLD
    """)

    result = _collapse_geo_id_to_granularity(
        new_entrants,
        "sub_regions",
        sub_regional_geography,
        ["technology"],
        ["value"],
    )

    expected = csv_str_to_df("""
        name,            technology, geo_id, value
        CNSW OCGT Small, OCGT,       CNSW,   104.0
        Q1_WH,           Wind,       Q1,     999.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_collapse_geo_id_to_granularity_averages_across_sub_regions(csv_str_to_df):
    # NSW's average must be the plain mean of its two sub-regions' values (102.0),
    # each counted once. The REZ row (Q1) is untouched by the collapse regardless
    # of granularity. BOTN - Cethana retains original name.
    new_entrants = csv_str_to_df("""
        name,                   technology,     geo_id, value
        CNSW OCGT Small,        OCGT,           CNSW,   104.0
        SNW OCGT Small,         OCGT,           SNW,    100.0
        Q1_WH,                  Wind,           Q1,     999.0
        BOTN - Cethana - 20h,   BOTN - Cethana, TAS,    100.0
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,     geo_type,       region_id
        CNSW,       subregion,      NSW
        SNW,        subregion,      NSW
        Q1,         rez,            QLD
        TAS,        subregion,      TAS
    """)

    result = _collapse_geo_id_to_granularity(
        new_entrants,
        "nem_regions",
        sub_regional_geography,
        ["technology"],
        ["value"],
    )

    expected = csv_str_to_df("""
        name,                   technology,     geo_id, value
        Q1_WH,                  Wind,           Q1,     999.0
        NSW OCGT Small,         OCGT,           NSW,    102.0
        BOTN - Cethana - 20h,   BOTN - Cethana, TAS,    100.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("technology").reset_index(drop=True),
        expected.sort_values("technology").reset_index(drop=True),
    )


def test_collapse_geo_id_to_granularity_single_region_maps_to_nem(csv_str_to_df):
    # Sub-regions in different NEM regions (NSW, TAS) all collapse to geo_id="NEM".
    # BOTN keeps name but geo_id still set to "NEM".
    new_entrants = csv_str_to_df("""
        name,                   technology,     geo_id, value
        CNSW OCGT Small,        OCGT,           CNSW,   104.0
        TAS OCGT Small,         OCGT,           TAS,    108.0
        BOTN - Cethana - 20h,   BOTN - Cethana, TAS,    100.0
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,     geo_type,       region_id
        CNSW,       subregion,      NSW
        TAS,        subregion,      TAS
    """)

    result = _collapse_geo_id_to_granularity(
        new_entrants,
        "single_region",
        sub_regional_geography,
        ["technology"],
        ["value"],
    )

    expected = csv_str_to_df("""
        name,                   technology,     geo_id, value
        NEM OCGT Small,         OCGT,           NEM,    106.0
        BOTN - Cethana - 20h,   BOTN - Cethana, NEM,    100.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("name").reset_index(drop=True),
        expected.sort_values("name").reset_index(drop=True),
    )


def test_collapse_geo_id_to_granularity_empty_input(csv_str_to_df):
    new_entrants = csv_str_to_df("""
        name,   technology,     geo_id,     value
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,     geo_type,       region_id
        CNSW,       subregion,      NSW
        Q1,         rez,            QLD
    """)

    result = _collapse_geo_id_to_granularity(
        new_entrants,
        "nem_regions",
        sub_regional_geography,
        ["technology"],
        ["value"],
    )

    expected = csv_str_to_df("""
        name,   technology,     geo_id,     value
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_rekey_names_to_collapsed_geo_id(csv_str_to_df):
    # Specifically checks that the 'same prefix, different geo_id' case correctly
    # re-keys based on the corresponding row-specific geo_id value, and that
    # 'unknown' (not 'old' geo_id values) prefixed names pass through unchanged.

    # only necessary columns - abbreviated input
    new_entrants = csv_str_to_df("""
        name,                       geo_id
        NSA Pumped Hydro - 10h,     SA
        WOO CCGT,                   NSW
        NQ OCGT Small,              QLD
        NQ Biomass,                 VIC
        Named Generator,            TAS
    """)

    known_prefixes = {"NSA", "WOO", "NQ"}
    result = _rekey_names_to_collapsed_geo_id(new_entrants, known_prefixes)

    expected = pd.Series(
        [
            "SA Pumped Hydro - 10h",
            "NSW CCGT",
            "QLD OCGT Small",
            "VIC Biomass",
            "Named Generator",
        ],
        name="name",
    )
    pd.testing.assert_series_equal(result, expected)


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


# --- _assert_build_cost_zone_matches_geo_id (LCF) ---


def test_assert_build_cost_zone_matches_geo_id(csv_str_to_df):
    # geo_id equals 'Regional build cost zone' for every row -> no raise.
    # Includes existing 'known typo' (NSA -> CSA) which should be explicitly handled.
    new_entrants = csv_str_to_df("""
        geo_id, Regional build cost zone
        Q1,     Q1
        NQ,     NQ
        NSA,    CSA
    """)
    _assert_build_cost_zone_matches_geo_id(new_entrants)


def test_assert_build_cost_zone_matches_geo_id_raises(csv_str_to_df):
    # An unexpected (geo_id, cost zone) split that isn't the known typo -> raise.
    new_entrants = csv_str_to_df("""
        geo_id, Regional build cost zone
        NSA,    CSA
        Q1,     Q2
        Test,
    """)

    with pytest.raises(
        ValueError,
        match=(
            r"Unexpected divergence between geo_id and 'Regional build cost zone' in "
            r"new_entrants_summary: \[\('Q1', 'Q2'\), \('Test', nan\)\]"
        ),
    ):
        _assert_build_cost_zone_matches_geo_id(new_entrants)


# --- _reshape_technology_specific_lcfs (LCF) ---


def test_reshape_technology_specific_lcfs(csv_str_to_df):
    # Wide -> long: factors become percentages (x100), the bespoke BOTN column (already a
    # percentage) is left unscaled, and "Not Applicable" cells are dropped.
    technology_specific_lcfs = csv_str_to_df("""
        Cost zone / REZ ID, REZ name / Description, Wind,           BOTN - Cethana
        Q1,                 Far North QLD,          1.05,           Not Applicable
        TAS,                Subregional Ref Node,   Not Applicable, 100
    """)

    result = _reshape_technology_specific_lcfs(technology_specific_lcfs)

    expected = csv_str_to_df("""
        geo_id, lcf_technology, lcf_build
        Q1,     Wind,           105.0
        TAS,    BOTN - Cethana, 100.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("geo_id").reset_index(drop=True),
        expected.sort_values("geo_id").reset_index(drop=True),
    )


def test_reshape_technology_specific_lcfs_raises_on_invalid_table():
    # Regression: confirms the table is actually validated before reshaping.
    # Exact raise behaviour is covered by _assert_table_valid's own tests.
    technology_specific_lcfs = pd.DataFrame(
        columns=["Cost zone / REZ ID", "REZ name / Description", "Wind"]
    )

    with pytest.raises(ValueError):
        _reshape_technology_specific_lcfs(technology_specific_lcfs)


# --- _merge_lcf_build (LCF) ---


def test_merge_lcf_build(csv_str_to_df):
    # lcf_build is looked up by (geo_id, technology), with the technology fuzzy-matched to
    # the lcf table's column spelling ("OCGT (Small GT)") and factors converted to percentages.
    # BOTN's 'technology' arrives already overridden to its own name.
    new_entrants = csv_str_to_df("""
        name,                  technology,        geo_id
        Q1_WH_Far North QLD,   Wind,              Q1
        NQ OCGT Small,         OCGT (small GT),   NQ
        BOTN - Cethana - 20h,  BOTN - Cethana,    TAS
    """)
    technology_specific_lcfs = csv_str_to_df("""
        Cost zone / REZ ID, REZ name / Description, Wind,           OCGT (Small GT), Pumped Hydro (24hrs storage), BOTN - Cethana
        Q1,                 Far North QLD,          1.05,           Not Applicable,  Not Applicable,               Not Applicable
        NQ,                 Subregional Ref Node,   Not Applicable, 1.08,            Not Applicable,               Not Applicable
        TAS,                Subregional Ref Node,   Not Applicable, Not Applicable,  1.0469,                       100
    """)

    result = _merge_lcf_build(new_entrants, technology_specific_lcfs)

    expected = csv_str_to_df("""
        name,                  technology,        geo_id, lcf_build
        Q1_WH_Far North QLD,   Wind,              Q1,     105.0
        NQ OCGT Small,         OCGT (small GT),   NQ,     108.0
        BOTN - Cethana - 20h,  BOTN - Cethana,    TAS,    100.0
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("name").reset_index(drop=True),
        expected.sort_values("name").reset_index(drop=True),
    )


def test_merge_lcf_build_empty(csv_str_to_df):
    # No new_entrant rows -> returns empty with the lcf_build column added.
    new_entrants = pd.DataFrame(columns=["name", "technology", "geo_id"])
    technology_specific_lcfs = csv_str_to_df("""
        Cost zone / REZ ID, REZ name / Description, Wind
        Q1,                 Far North QLD,          1.05
    """)

    result = _merge_lcf_build(new_entrants, technology_specific_lcfs)

    expected = csv_str_to_df("""
        name, technology, geo_id, lcf_build
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _merge_lcf_om (LCF) ---


def test_merge_lcf_om(csv_str_to_df):
    # lcf_om is a single per-zone value (technology-independent) looked up by geo_id,
    # already a percentage so taken as-is.
    new_entrants = csv_str_to_df("""
        name,                 geo_id
        Q1_WH_Far North QLD,  Q1
        NQ OCGT Small,        NQ
    """)
    locational_cost_factors = csv_str_to_df("""
        Cost zone / REZ ID, Equipment and installation costs, O&M costs 3
        Q1,                 110.0,                            122.27
        NQ,                 105.0,                            114.997
    """)

    result = _merge_lcf_om(new_entrants, locational_cost_factors)

    expected = csv_str_to_df("""
        name,                 geo_id, lcf_om
        Q1_WH_Far North QLD,  Q1,     122.27
        NQ OCGT Small,        NQ,     114.997
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_merge_lcf_om_empty(csv_str_to_df):
    # No rows -> returns empty with the lcf_om column added.
    new_entrants = pd.DataFrame(columns=["name", "geo_id"])
    locational_cost_factors = csv_str_to_df("""
        Cost zone / REZ ID, O&M costs 3
        Q1,                 122.27
    """)

    result = _merge_lcf_om(new_entrants, locational_cost_factors)

    expected = csv_str_to_df("""
        name, geo_id, lcf_om
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_merge_lcf_om_raises_on_invalid_source_table(csv_str_to_df):
    # Regression: confirms the source table is actually validated before merging.
    # Exact raise behaviour is covered by _assert_table_valid's own tests.
    new_entrants = csv_str_to_df("""
        name,                 geo_id
        Q1_WH_Far North QLD,  Q1
    """)
    locational_cost_factors = pd.DataFrame(
        columns=["Cost zone / REZ ID", "O&M costs 3"]
    )

    with pytest.raises(ValueError):
        _merge_lcf_om(new_entrants, locational_cost_factors)
