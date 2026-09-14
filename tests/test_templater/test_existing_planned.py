import pandas as pd
import pytest

from ispypsa.templater.existing_planned import (
    _format_commissioning_date,
    _is_existing_planned_storage_row,
    _merge_minimum_load,
    _merge_unit_keyed_properties,
    _resolve_unit_keys,
    _template_generators_existing_planned,
    _template_storage_existing_planned,
    _validate_phes_routing,
)

# --- _is_existing_planned_storage_row ---


def test_is_existing_planned_storage_row(csv_str_to_df):
    summary = csv_str_to_df("""
        Power Station,  Technology Type
        Wivenhoe,       Hydro
        Tarong,         Steam Sub Critical
        Q8 Battery,     Battery Storage (2hrs storage)
        QEJP - Borumba, Pumped Hydro (24hrs storage)
    """)
    phes_properties = csv_str_to_df("""
        Power Station
        Wivenhoe
        Borumba
    """)

    result = _is_existing_planned_storage_row(summary, phes_properties)

    # Wivenhoe routes to storage by PHES-table presence despite being labelled plain
    # "Hydro"; Q8 Battery routes by technology; Tarong is neither; QEJP - Borumba
    # routes by technology AND is matched to Borumba in phes_properties.
    expected = pd.Series([True, False, True, True])
    pd.testing.assert_series_equal(result, expected)


def test_is_existing_planned_storage_row_empty_summary():
    summary = pd.DataFrame(columns=["Power Station", "Technology Type"])
    phes_properties = pd.DataFrame(columns=["Power Station"])

    result = _is_existing_planned_storage_row(summary, phes_properties)

    expected = pd.Series([], dtype=bool)
    pd.testing.assert_series_equal(result, expected)


def test_is_existing_planned_storage_row_empty_phes_properties(csv_str_to_df):
    # Battery technologies still get correctly identified
    summary = csv_str_to_df("""
        Power Station,  Technology Type
        Tarong,         Steam Sub Critical
        Q8 Battery,     Battery Storage (2hrs storage)
    """)
    phes_properties = pd.DataFrame(columns=["Power Station"])

    result = _is_existing_planned_storage_row(summary, phes_properties)

    expected = pd.Series([False, True])
    pd.testing.assert_series_equal(result, expected)


def test_is_existing_planned_storage_row_empty_summary_with_phes_properties(
    csv_str_to_df,
):
    # An empty summary can't account for any PHES station, so routing validation
    # raises rather than silently classifying nothing as storage.
    summary = pd.DataFrame(columns=["Power Station", "Technology Type"])
    phes_properties = csv_str_to_df("""
        Power Station
        Wivenhoe
    """)

    with pytest.raises(ValueError, match=r"\['Wivenhoe'\]"):
        _validate_phes_routing(summary, phes_properties)


# --- _validate_phes_routing ---


def test_validate_phes_routing_tolerates_lower_tumut(csv_str_to_df):
    summary = csv_str_to_df("""
        Power Station,  Technology Type
        Tumut 3,        Hydro
        QEJP - Borumba, Pumped Hydro (24hrs storage)
        Tarong,         Steam Sub Critical
    """)
    phes_properties = csv_str_to_df("""
        Power Station
        Lower Tumut
        Borumba
    """)

    _validate_phes_routing(summary, phes_properties)  # no error


def test_validate_phes_routing_raises_on_unrecognised_station(csv_str_to_df):
    summary = csv_str_to_df("""
        Power Station,  Technology Type
        Tumut 3,        Hydro
        QEJP - Borumba, Pumped Hydro (24hrs storage)
        Tarong,         Steam Sub Critical
    """)
    phes_properties = csv_str_to_df("""
        Power Station
        Some New Station
    """)

    with pytest.raises(ValueError, match=r"\['Some New Station'\]"):
        _validate_phes_routing(summary, phes_properties)


# --- _resolve_unit_keys ---


def test_resolve_unit_keys():
    # A single-character typo in the table's key is close enough (threshold=90) to
    # resolve -- the *table's* spelling is returned, but in `names` order.
    # A non-empty exclude_unit_keys correctly removes known out-of-scope table_keys
    # before fuzzy-matching ("BAYSWATER02").
    names = pd.Series(["BW01", "BW02", "BAYSWATER01"])
    table_keys = pd.Series(["BW01", "bAYSWATER01", "BW02", "BAYSWATER02"])

    # if "BAYSWATER02" were not excluded it would win the fuzzy-match (incorrectly)
    exclude_unit_keys = set(["BAYSWATER02"])

    result = _resolve_unit_keys(names, table_keys, "heat_rates_...", exclude_unit_keys)

    expected_result = pd.Series(["BW01", "BW02", "bAYSWATER01"])
    pd.testing.assert_series_equal(result, expected_result)


def test_resolve_unit_keys_raises_on_unmatched():
    names = pd.Series(["BW01", "UNKNOWN1"])
    table_keys = pd.Series(["BW01"])

    with pytest.raises(ValueError, match=r"'heat_rates_\.\.\.'.*\['UNKNOWN1'\]"):
        _resolve_unit_keys(names, table_keys, "heat_rates_...")


# --- _merge_unit_keyed_properties ---


def test_merge_unit_keyed_properties_single_table_multiple_columns(csv_str_to_df):
    # capacity and commissioning_date both come from the same table - key-resolved once.
    # Includes small typo fuzzy-match check (EXAMPLE_GEN <-> EXAMPLE_GEn) and non-numeric
    # values (not coerced).
    generators = csv_str_to_df("""
        name
        BW01
        EXAMPLE_GEN
    """)
    maximum_capacity = csv_str_to_df("""
        IASR ID,        Installed capacity (MW),  Commissioning date
        BW01,           660.0,                    2028-12-01
        EXAMPLE_GEn,    100.0,
    """)
    property_map = {
        "capacity": {
            "table": "maximum_capacity",
            "key_col": "IASR ID",
            "value_col": "Installed capacity (MW)",
        },
        "commissioning_date": {
            "table": "maximum_capacity",
            "key_col": "IASR ID",
            "value_col": "Commissioning date",
            "numeric": False,
        },
    }
    iasr_tables = {"maximum_capacity": maximum_capacity}

    result = _merge_unit_keyed_properties(generators, iasr_tables, property_map)

    expected_result = csv_str_to_df("""
        name,           capacity,   commissioning_date
        BW01,           660.0,      2028-12-01
        EXAMPLE_GEN,    100.0,
    """)

    pd.testing.assert_frame_equal(result, expected_result)


# --- _format_commissioning_date ---


def test_format_commissioning_date(csv_str_to_df):
    generators = csv_str_to_df("""
        name,  commissioning_date
        BW01,  2028-12-01
        BW02,
    """)

    result = _format_commissioning_date(generators)

    expected = csv_str_to_df("""
        name,  commissioning_date
        BW01,  01/12/2028
        BW02,
    """)
    pd.testing.assert_frame_equal(result, expected)


# --- _merge_minimum_load ---


def test_merge_minimum_load(csv_str_to_df):
    generators = csv_str_to_df("""
        name,    technology
        BW01,    Steam Sub Critical
        ANGAS1,  CCGT
        Q1G1,    Large scale Solar PV
    """)
    coal = csv_str_to_df("""
        IASR ID,  Technology Type,      Minimum Stable Level (MW)_Typical Lowest Band
        BW01,     Steam Sub Critical,   260.0
    """)
    gas = csv_str_to_df("""
        IASR ID,  Technology Type,  Min Stable Level (MW)
        ANGAS1,   CCGT,             3.0
    """)
    iasr_tables = {
        "coal_minimum_stable_level": coal,
        "gpg_min_stable_level_existing_generators": gas,
    }

    result = _merge_minimum_load(generators, iasr_tables)

    # Q1G1 is neither coal nor gas - left NaN, nan_fill applied downstream.
    expected = csv_str_to_df("""
        name,    technology,            minimum_load
        BW01,    Steam Sub Critical,    260.0
        ANGAS1,  CCGT,                  3.0
        Q1G1,    Large scale Solar PV,
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_merge_minimum_load_bounds_matching_by_technology(csv_str_to_df):
    # The technology filter bounds the fuzzy-match candidate pool: BAYSWATER01 is
    # coal-typed so its typo'd key in the coal table still resolves, while BW0l is
    # not, so it never gets the chance to match BW01.
    generators = csv_str_to_df("""
        name,         technology
        BAYSWATER01,  Steam Sub Critical
        BW0l,         Large scale Solar PV
    """)
    coal = csv_str_to_df("""
        IASR ID,      Technology Type,      Minimum Stable Level (MW)_Typical Lowest Band
        BAYSWATER0l,  Steam Sub Critical,   260.0
        BW01,         Steam Sub Critical,   182.0
    """)
    # gas isn't relevant to this test's assertion, but needs a real row -- an empty
    # table always raises regardless of whether it would even match anything.
    gas = csv_str_to_df("""
        IASR ID,              Technology Type,  Min Stable Level (MW)
        SOME_OTHER_GAS_UNIT,  CCGT,             5.0
    """)
    iasr_tables = {
        "coal_minimum_stable_level": coal,
        "gpg_min_stable_level_existing_generators": gas,
    }

    result = _merge_minimum_load(generators, iasr_tables)

    expected = csv_str_to_df("""
        name,         technology,            minimum_load
        BAYSWATER01,  Steam Sub Critical,    260.0
        BW0l,         Large scale Solar PV,
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_merge_minimum_load_empty_generators(csv_str_to_df):
    # Nothing to merge onto -- the coal/gas tables aren't even looked at, so an
    # empty/invalid one doesn't matter here (mirrors _merge_unit_keyed_properties).
    generators = pd.DataFrame(columns=["name", "technology"])
    iasr_tables = {
        "coal_minimum_stable_level": pd.DataFrame(
            columns=[
                "IASR ID",
                "Technology Type",
                "Minimum Stable Level (MW)_Typical Lowest Band",
            ]
        ),
        "gpg_min_stable_level_existing_generators": pd.DataFrame(
            columns=["IASR ID", "Technology Type", "Min Stable Level (MW)"]
        ),
    }

    result = _merge_minimum_load(generators, iasr_tables)

    expected = csv_str_to_df("""
        name,  technology,  minimum_load
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


@pytest.mark.parametrize(
    "empty_table",
    ["coal_minimum_stable_level", "gpg_min_stable_level_existing_generators"],
)
def test_merge_minimum_load_raises_on_empty_property_table(empty_table, csv_str_to_df):
    # Both property tables are validated before merging, so an empty one raises
    # whichever it is. Exact raise behaviour is covered by _assert_table_valid.
    # Test to check that assertion is happening here
    generators = csv_str_to_df("""
        name,  technology
        BW01,  Steam Sub Critical
    """)
    iasr_tables = {
        "coal_minimum_stable_level": csv_str_to_df("""
            IASR ID,  Technology Type,      Minimum Stable Level (MW)_Typical Lowest Band
            BW01,     Steam Sub Critical,   260.0
        """),
        "gpg_min_stable_level_existing_generators": csv_str_to_df("""
            IASR ID,  Technology Type,  Min Stable Level (MW)
            ANGAS1,   CCGT,             3.0
        """),
    }
    iasr_tables[empty_table] = pd.DataFrame(columns=iasr_tables[empty_table].columns)

    with pytest.raises(ValueError, match=f"'{empty_table}' table is empty"):
        _merge_minimum_load(generators, iasr_tables)


# --- _template_generators_existing_planned ---


def test_template_generators_existing_planned(csv_str_to_df):
    # Wiring only - the behaviour behind each column is covered by the per-helper
    # tests above. Columns are compared in order: this is the only test that pins
    # _GENERATOR_COLUMNS' schema ordering (elsewhere it's compared as a set).
    summary = csv_str_to_df("""
        IASR ID / DLT names,    Power Station,  Technology Type,        REZ ID,         Sub-region, Fuel type,  Fuel cost mapping
        BW01,                   Bayswater,      Steam Sub Critical,     Not Applicable, CNSW,       Black Coal, Bayswater
        Q1G1,                   Solar Farm,     Large scale Solar PV,   Q1,             SQ,         Solar,      Solar
    """)
    phes_properties = pd.DataFrame(columns=["Power Station"])
    maximum_capacity = csv_str_to_df("""
        IASR ID,    Installed capacity (MW),    Commissioning date
        BW01,       660.0,
        Q1G1,       100.0,                      2028-12-01
    """)
    variable_opex = pd.DataFrame(  # literal comma
        {"IASR ID": ["BW01", "Q1G1"], "Variable OPEX ($/MWh sent out)1,": [8.0, 0.0]}
    )
    heat_rates = csv_str_to_df("""
        IASR ID,    Heat rate (GJ/MWh)
        BW01,       10.05
        Q1G1,       0.0
    """)
    closure_years = csv_str_to_df("""
        IASR ID,    Expected Closure Year (Calendar year)
        BW01,       2033
        Q1G1,       2100
    """)
    coal = csv_str_to_df("""
        IASR ID,    Technology Type,    Minimum Stable Level (MW)_Typical Lowest Band
        BW01,       Steam Sub Critical, 260.0
    """)
    gas = csv_str_to_df("""
        IASR ID,  Technology Type,  Min Stable Level (MW)
        ANGAS1,   CCGT,             3.0
    """)  # not used, but needs to be non-empty else table validation raises
    iasr_tables = {
        "existing_committed_anticipated_additional_generator_summary": summary,
        "pumped_hydro_existing_committed_anticipated_additional_properties": phes_properties,
        "maximum_capacity_existing_committed_anticipated_additional_generators": maximum_capacity,
        "variable_opex_existing_committed_anticipated_additional_generators": variable_opex,
        "heat_rates_existing_committed_anticipated_additional_generators": heat_rates,
        "expected_closure_years": closure_years,
        "coal_minimum_stable_level": coal,
        "gpg_min_stable_level_existing_generators": gas,
    }
    sub_regional_geography = csv_str_to_df("""
        geo_id,     geo_type,   region_id
        CNSW,       subregion,  NSW
        Q1,         rez,        QLD
    """)

    result = _template_generators_existing_planned(
        iasr_tables, "sub_regions", sub_regional_geography
    )
    assert list(result.columns) == [
        "name",
        "power_station",
        "technology",
        "geo_id",
        "fuel_type",
        "fuel_price_mapping",
        "capacity",
        "vom",
        "heat_rate",
        "commissioning_date",
        "closure_year",
        "minimum_load",
    ]
    assert len(result) == 2


def test_template_generators_existing_planned_empty(csv_str_to_df):
    columns = [
        "IASR ID / DLT names",
        "Power Station",
        "Technology Type",
        "REZ ID",
        "Sub-region",
        "Fuel type",
        "Fuel cost mapping",
    ]
    summary = pd.DataFrame(columns=columns)
    phes_properties = pd.DataFrame(columns=["Power Station"])
    maximum_capacity = pd.DataFrame(
        columns=["IASR ID", "Installed capacity (MW)", "Commissioning date"]
    )
    variable_opex = pd.DataFrame(
        columns=["IASR ID", "Variable OPEX ($/MWh sent out)1,"]
    )
    heat_rates = pd.DataFrame(columns=["IASR ID", "Heat rate (GJ/MWh)"])
    closure_years = pd.DataFrame(
        columns=["IASR ID", "Expected Closure Year (Calendar year)"]
    )
    coal = pd.DataFrame(
        columns=[
            "IASR ID",
            "Technology Type",
            "Minimum Stable Level (MW)_Typical Lowest Band",
        ]
    )
    gas = pd.DataFrame(columns=["IASR ID", "Technology Type", "Min Stable Level (MW)"])
    iasr_tables = {
        "existing_committed_anticipated_additional_generator_summary": summary,
        "pumped_hydro_existing_committed_anticipated_additional_properties": phes_properties,
        "maximum_capacity_existing_committed_anticipated_additional_generators": maximum_capacity,
        "variable_opex_existing_committed_anticipated_additional_generators": variable_opex,
        "heat_rates_existing_committed_anticipated_additional_generators": heat_rates,
        "expected_closure_years": closure_years,
        "coal_minimum_stable_level": coal,
        "gpg_min_stable_level_existing_generators": gas,
    }
    sub_regional_geography = pd.DataFrame(columns=["geo_id", "geo_type", "region_id"])

    result = _template_generators_existing_planned(
        iasr_tables, "sub_regions", sub_regional_geography
    )

    expected = csv_str_to_df("""
        name, power_station, technology, geo_id, fuel_type, fuel_price_mapping, capacity, vom, heat_rate, commissioning_date, closure_year, minimum_load
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _template_storage_existing_planned ---


def test_template_storage_existing_planned(csv_str_to_df):
    summary = csv_str_to_df("""
        IASR ID / DLT names,  Power Station,  Technology Type
        BW01,                 Bayswater,      Steam Sub Critical
        WHOE1,                Wivenhoe,       Hydro
        Q8_BATT_2H,           Q8 Battery,     Battery Storage (2hrs storage)
    """)
    phes_properties = csv_str_to_df("""
        Power Station
        Wivenhoe
    """)
    iasr_tables = {
        "existing_committed_anticipated_additional_generator_summary": summary,
        "pumped_hydro_existing_committed_anticipated_additional_properties": phes_properties,
    }

    storage = _template_storage_existing_planned(iasr_tables)

    expected_storage = csv_str_to_df("""
        name,        power_station,  technology
        WHOE1,       Wivenhoe,       Hydro
        Q8_BATT_2H,  Q8 Battery,     Battery Storage (2hrs storage)
    """)
    pd.testing.assert_frame_equal(storage.reset_index(drop=True), expected_storage)


def test_template_storage_existing_planned_empty(csv_str_to_df):
    summary = pd.DataFrame(
        columns=["IASR ID / DLT names", "Power Station", "Technology Type"]
    )
    phes_properties = pd.DataFrame(columns=["Power Station"])
    iasr_tables = {
        "existing_committed_anticipated_additional_generator_summary": summary,
        "pumped_hydro_existing_committed_anticipated_additional_properties": phes_properties,
    }

    storage = _template_storage_existing_planned(iasr_tables)

    expected_storage = csv_str_to_df("""
        name,  power_station,  technology
    """)
    pd.testing.assert_frame_equal(storage, expected_storage, check_dtype=False)
