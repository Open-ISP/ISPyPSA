import re

import pandas as pd
import pytest

from ispypsa.templater.fuel_prices import (
    _combine_gas_price_tables,
    _melt_fuel_prices_to_long,
    _merge_prices_onto_generators,
    _prepare_price_tables,
    _select_generator_keys,
    _template_biomethane_prices,
    _template_fuel_prices,
    _template_new_entrant_gas_prices,
    _template_secondary_liquid_fuel_prices,
    _template_station_keyed_prices,
)

# --- _template_fuel_prices ---


def test_template_fuel_prices(csv_str_to_df):
    iasr_tables = {
        "existing_committed_anticipated_additional_generator_summary": csv_str_to_df("""
            IASR ID / DLT names,  Power Station,        Technology Type,     Sub-region,  REZ ID,          Fuel type,    Fuel cost mapping
            BW01,                 Bayswater,            Steam Sub Critical,  CNSW,        Not Applicable,  Black Coal,   Bayswater
            BW02,                 Bayswater,            Steam Sub Critical,  CNSW,        Not Applicable,  Black Coal,   Bayswater
            TALWA1,               Tallawarra,           CCGT,                SNW,         Not Applicable,  Gas,          Tallawarra
            Kogan Gas,            Brigalow GT,          OCGT (large GT),     SQ,          Not Applicable,  Gas,          QLD new OCGT
            MSTUART1,             Mt Stuart,            OCGT (small GT),     NQ,          Not Applicable,  Liquid Fuel,  Mt Stuart
            BODWF1,               Bodangora Wind Farm,  Wind,                CNSW,        N3,              Wind,         Wind
        """),
        "new_entrants_summary": csv_str_to_df("""
            Power Station,    Technology Type,  Sub-region,  REZ ID,          Fuel type,  Fuel cost mapping
            CNSW OCGT Small,  OCGT (small GT),  CNSW,        Not Applicable,  Gas,        NSW new OCGT
            SNW OCGT Small,   OCGT (small GT),  SNW,         Not Applicable,  Gas,        NSW new OCGT
        """),
        "coal_fuel_price": csv_str_to_df("""
            Generator,  Scenario,     2025-26
            Bayswater,  Step Change,  4.12
        """),
        "gas_prices_existing_generators": csv_str_to_df("""
            Generator,   Gas price scenario,  2025-26
            Tallawarra,  Step Change,         15.00
        """),
        "gas_prices_new_entrants": csv_str_to_df("""
            New generating stations,  Gas price scenario,  2025-26
            NSW new OCGT,             Step Change,         16.50
            QLD new OCGT,             Step Change,         14.50
        """),
        "liquid_fuel_prices": csv_str_to_df("""
            Generator,  Gas price scenario,  2025-26
            Mt Stuart,  Step Change,         29.50
        """),
        "gpg_secondary_fuel_prices": csv_str_to_df("""
            Generator,            Gas price scenario,  2025-26
            Tallawarra_2nd_Fuel,  Step Change,         31.00
        """),
        "biomass_fuel_price": csv_str_to_df("""
            Biomass price,  Scenario,     2025-26
            Biomass,        Step Change,  0.66
        """),
        "hydrogen_prices": csv_str_to_df("""
            Hydrogen price,  Hydrogen price scenario,  2025-26
            Hydrogen,        Step Change,              41.86
        """),
        "biomethane_prices": csv_str_to_df("""
            Biomethane price,           Biomethane price scenario,  2025-26
            Biomethane (Waste),         Step Change,                25.57
            Biomethane (Landfill gas),  Step Change,                13.31
        """),
    }
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
        SNW,     subregion,  NSW
        NQ,      subregion,  QLD
        SQ,      subregion,  QLD
        N3,      rez,        NSW
    """)

    result = _template_fuel_prices(
        iasr_tables, "Step Change", "nem_regions", sub_regional_geography
    )

    # In pipeline order: existing stations (BW01 and BW02 collapse into one Bayswater
    # row; Brigalow GT is priced off a new entrant key), secondary liquid fuel
    # (Brigalow GT excluded), new entrant gas (CNSW and SNW collapse into NSW),
    # fuel-wide prices, then zero-cost rows. Blank key cells are wildcards, so
    # Bodangora Wind Farm is priced by the Wind row rather than by name.
    expected = csv_str_to_df("""
        fuel_type,    power_station,  technology,          geo_id,  year,  price
        Black Coal,   Bayswater,      Steam Sub Critical,  NSW,     2026,  4.12
        Gas,          Tallawarra,     CCGT,                NSW,     2026,  15.00
        Gas,          Brigalow GT,    OCGT (large GT),     QLD,     2026,  14.50
        Liquid Fuel,  Mt Stuart,      OCGT (small GT),     QLD,     2026,  29.50
        Liquid Fuel,  Tallawarra,     CCGT,                NSW,     2026,  31.00
        Gas,          ,               OCGT (small GT),     NSW,     2026,  16.50
        Biomass,      ,               ,                    ,        2026,  0.66
        Hydrogen,     ,               ,                    ,        2026,  41.86
        Biomethane,   ,               ,                    ,        2026,  25.57
        Solar,        ,               ,                    ,        ,      0.0
        Wind,         ,               ,                    ,        ,      0.0
        Water,        ,               ,                    ,        ,      0.0
    """)
    sort_cols = ["fuel_type", "power_station", "technology", "geo_id"]
    pd.testing.assert_frame_equal(
        result.sort_values(sort_cols).reset_index(drop=True),
        expected.sort_values(sort_cols).reset_index(drop=True),
    )


# --- _prepare_price_tables ---


def test_prepare_price_tables(csv_str_to_df):
    iasr_tables = {
        "coal_fuel_price": csv_str_to_df("""
            Generator,  Scenario,       2025-26
            Bayswater,  Step Change,    4.12
            Bayswater,  Slower Growth,  4.30
        """),
        "gas_prices_existing_generators": csv_str_to_df("""
            Generator,   Gas price scenario,  2025-26
            Tallawarra,  Step Change,         15.00
            Tallawarra,  Slower Growth,       15.80
        """),
        "gas_prices_new_entrants": csv_str_to_df("""
            New generating stations,  Gas price scenario,  2025-26
            QLD new OCGT,             Step Change,         14.50
            QLD new OCGT,             Slower Growth,       15.20
        """),
        "liquid_fuel_prices": csv_str_to_df("""
            Generator,  Gas price scenario,  2025-26
            Mt Stuart,  Step Change,         29.50
            Mt Stuart,  Slower Growth,       30.10
        """),
        "gpg_secondary_fuel_prices": csv_str_to_df("""
            Generator,            Gas price scenario,  2025-26
            Tallawarra_2nd_Fuel,  Step Change,         31.00
            Tallawarra_2nd_Fuel,  Slower Growth,       31.60
        """),
        "biomass_fuel_price": csv_str_to_df("""
            Biomass price,  Scenario,       2025-26
            Biomass,        Step Change,    0.66
            Biomass,        Slower Growth,  0.70
        """),
        "hydrogen_prices": csv_str_to_df("""
            Hydrogen price,  Hydrogen price scenario,  2025-26
            Hydrogen,        Step Change,              41.86
            Hydrogen,        Slower Growth,            43.10
        """),
        "biomethane_prices": csv_str_to_df("""
            Biomethane price,           Biomethane price scenario,  2025-26
            Biomethane (Waste),         Step Change,                25.57
            Biomethane (Waste),         Slower Growth,              29.58
            Biomethane (Landfill gas),  Step Change,                13.00
            Biomethane (Landfill gas),  Slower Growth,              16.20
        """),
    }

    result = _prepare_price_tables(iasr_tables, "Step Change")

    coal_expected = csv_str_to_df("""
        fuel_cost_mapping,  2025-26
        Bayswater,          4.12
    """)
    pd.testing.assert_frame_equal(result["coal_fuel_price"], coal_expected)

    gas_existing_expected = csv_str_to_df("""
        fuel_cost_mapping,  2025-26
        Tallawarra,         15.00
    """)
    pd.testing.assert_frame_equal(
        result["gas_prices_existing_generators"], gas_existing_expected
    )

    gas_new_expected = csv_str_to_df("""
        fuel_cost_mapping,  2025-26
        QLD new OCGT,       14.50
    """)
    pd.testing.assert_frame_equal(result["gas_prices_new_entrants"], gas_new_expected)

    liquid_expected = csv_str_to_df("""
        fuel_cost_mapping,  2025-26
        Mt Stuart,          29.50
    """)
    pd.testing.assert_frame_equal(result["liquid_fuel_prices"], liquid_expected)

    secondary_expected = csv_str_to_df("""
        fuel_cost_mapping,    2025-26
        Tallawarra_2nd_Fuel,  31.00
    """)
    pd.testing.assert_frame_equal(
        result["gpg_secondary_fuel_prices"], secondary_expected
    )

    biomass_expected = csv_str_to_df("""
        fuel_type,  2025-26
        Biomass,    0.66
    """)
    pd.testing.assert_frame_equal(result["biomass_fuel_price"], biomass_expected)

    hydrogen_expected = csv_str_to_df("""
        fuel_type,  2025-26
        Hydrogen,   41.86
    """)
    pd.testing.assert_frame_equal(result["hydrogen_prices"], hydrogen_expected)

    # biomethane - only filtered to ISP scenario at this stage, still has
    # biomethane-type options present
    biomethane_expected = csv_str_to_df("""
        fuel_type,                  2025-26
        Biomethane (Waste),         25.57
        Biomethane (Landfill gas),  13.00
    """)
    pd.testing.assert_frame_equal(result["biomethane_prices"], biomethane_expected)


# --- _template_station_keyed_prices / _combine_gas_price_tables ---


def test_template_station_keyed_prices_prices_existing_gas_off_new_entrant_key(
    csv_str_to_df,
):
    # Brigalow GT is an existing station mapped to a new entrant key.
    price_tables = {
        "gas_prices_existing_generators": csv_str_to_df("""
            fuel_cost_mapping,  2025-26
            Tallawarra,         15.00
        """),
        "gas_prices_new_entrants": csv_str_to_df("""
            fuel_cost_mapping,  2025-26
            QLD new OCGT,       14.50
        """),
    }
    summary_existing = csv_str_to_df("""
        Power Station,  Technology Type,  geo_id,  Fuel type,  Fuel cost mapping
        Tallawarra,     CCGT,             SNW,     Gas,        Tallawarra
        Brigalow GT,    OCGT (large GT),  SQ,      Gas,        QLD new OCGT
    """)

    result = _template_station_keyed_prices(
        _combine_gas_price_tables(price_tables),
        summary_existing,
        ["Gas"],
        "gas_prices_existing_generators / gas_prices_new_entrants",
    )

    expected = csv_str_to_df("""
        fuel_type,  technology,       geo_id,  power_station,  year,  price
        Gas,        CCGT,             SNW,     Tallawarra,     2026,  15.00
        Gas,        OCGT (large GT),  SQ,      Brigalow GT,    2026,  14.50
    """)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize("prices_rows", ["populated", "empty"])
def test_template_station_keyed_prices_with_no_stations(prices_rows, csv_str_to_df):
    prices = csv_str_to_df("""
        fuel_cost_mapping,  2025-26
        Bayswater,          4.12
    """)
    prices_options = {
        "populated": prices,
        "empty": pd.DataFrame(columns=prices.columns),
    }
    # empty summary - no stations
    summary_existing = pd.DataFrame(
        columns=[
            "Power Station",
            "Technology Type",
            "geo_id",
            "Fuel type",
            "Fuel cost mapping",
        ]
    )

    result = _template_station_keyed_prices(
        prices_options[prices_rows],
        summary_existing,
        ["Black Coal"],
        "coal_fuel_price",
    )

    expected = csv_str_to_df("""
        fuel_type,  technology,  geo_id,  power_station,  year,  price
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_template_station_keyed_prices_raises_on_empty_prices(csv_str_to_df):
    prices = pd.DataFrame(columns=["fuel_cost_mapping", "2025-26"])
    summary_existing = csv_str_to_df("""
        Power Station,  Technology Type,     geo_id,  Fuel type,   Fuel cost mapping
        Bayswater,      Steam Sub Critical,  CNSW,    Black Coal,  Bayswater
    """)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Generator fuel cost mappings with no price in 'coal_fuel_price': "
            "['Bayswater']"
        ),
    ):
        _template_station_keyed_prices(
            prices, summary_existing, ["Black Coal"], "coal_fuel_price"
        )


# --- _template_secondary_liquid_fuel_prices ---


def test_template_secondary_liquid_fuel_prices(csv_str_to_df, caplog):
    # Brigalow GT has no '_2nd_Fuel' row, so it would raise if it weren't excluded.
    prices = csv_str_to_df("""
        fuel_cost_mapping,    2025-26
        Tallawarra_2nd_Fuel,  31.00
    """)
    summary_existing = csv_str_to_df("""
        Power Station,  Technology Type,  geo_id,  Fuel type,  Fuel cost mapping
        Tallawarra,     CCGT,             SNW,     Gas,        Tallawarra
        Brigalow GT,    OCGT (large GT),  SQ,      Gas,        QLD new OCGT
    """)

    with caplog.at_level("INFO"):
        result = _template_secondary_liquid_fuel_prices(prices, summary_existing)

    expected = csv_str_to_df("""
        fuel_type,    technology,  geo_id,  power_station,  year,  price
        Liquid Fuel,  CCGT,        SNW,     Tallawarra,     2026,  31.00
    """)
    pd.testing.assert_frame_equal(result, expected)
    assert (
        "KNOWN EXCLUSION: power station(s) ['Brigalow GT'] excluded while "
        "templating secondary fuel prices for named GPG."
    ) in caplog.text


# --- _template_new_entrant_gas_prices ---


def test_template_new_entrant_gas_prices_drops_duplicate_options(csv_str_to_df):
    # WOO OCGT Small shares CNSW's technology, geo_id and price row, so it's dropped;
    # CNSW OCGT Large shares the price row but not the technology, so it's kept.
    prices = csv_str_to_df("""
        fuel_cost_mapping,  2025-26
        NSW new OCGT,       16.50
    """)
    summary_new_entrant = csv_str_to_df("""
        Power Station,    Technology Type,  REZ ID,          Sub-region,  Fuel type,  Fuel cost mapping
        CNSW OCGT Small,  OCGT (small GT),  Not Applicable,  CNSW,        Gas,        NSW new OCGT
        WOO OCGT Small,   OCGT (small GT),  Not Applicable,  CNSW,        Gas,        NSW new OCGT
        CNSW OCGT Large,  OCGT (large GT),  Not Applicable,  CNSW,        Gas,        NSW new OCGT
        SNW OCGT Small,   OCGT (small GT),  Not Applicable,  SNW,         Gas,        NSW new OCGT
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
        SNW,     subregion,  NSW
    """)

    result = _template_new_entrant_gas_prices(
        prices, summary_new_entrant, "sub_regions", sub_regional_geography
    )

    expected = csv_str_to_df("""
        fuel_type,  technology,       geo_id,  year,  price
        Gas,        OCGT (large GT),  CNSW,    2026,  16.50
        Gas,        OCGT (small GT),  CNSW,    2026,  16.50
        Gas,        OCGT (small GT),  SNW,     2026,  16.50
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_new_entrant_gas_prices_weights_mean_by_sub_region(csv_str_to_df):
    # NNSW and CNSW share a price row but each count once: mean(16, 16, 13), not
    # mean(16, 13).
    prices = csv_str_to_df("""
        fuel_cost_mapping,  2025-26
        NSW new OCGT,       16.0
        QLD new OCGT,       13.0
    """)
    summary_new_entrant = csv_str_to_df("""
        Power Station,    Technology Type,  REZ ID,          Sub-region,  Fuel type,  Fuel cost mapping
        NNSW OCGT Small,  OCGT (small GT),  Not Applicable,  NNSW,        Gas,        NSW new OCGT
        CNSW OCGT Small,  OCGT (small GT),  Not Applicable,  CNSW,        Gas,        NSW new OCGT
        SQ OCGT Small,    OCGT (small GT),  Not Applicable,  SQ,          Gas,        QLD new OCGT
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        NNSW,    subregion,  NSW
        CNSW,    subregion,  NSW
        SQ,      subregion,  QLD
    """)

    result = _template_new_entrant_gas_prices(
        prices, summary_new_entrant, "single_region", sub_regional_geography
    )

    expected = csv_str_to_df("""
        fuel_type,  technology,       geo_id,  year,  price
        Gas,        OCGT (small GT),  NEM,     2026,  15.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_new_entrant_gas_prices_with_no_options(csv_str_to_df):
    prices = csv_str_to_df("""
        fuel_cost_mapping,  2025-26
        NSW new OCGT,       16.50
    """)
    summary_new_entrant = pd.DataFrame(
        columns=[
            "Power Station",
            "Technology Type",
            "REZ ID",
            "Sub-region",
            "Fuel type",
            "Fuel cost mapping",
        ]
    )
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
    """)

    result = _template_new_entrant_gas_prices(
        prices, summary_new_entrant, "nem_regions", sub_regional_geography
    )

    expected = csv_str_to_df("""
        fuel_type,  technology,  geo_id,  year,  price
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _template_biomethane_prices ---


def test_template_biomethane_prices(csv_str_to_df):
    prices = csv_str_to_df("""
        fuel_type,                  2025-26
        Biomethane (Landfill gas),  13.31
        Biomethane (Waste),         25.57
        Biomethane (Crop residue),  36.54
    """)

    result = _template_biomethane_prices(prices)

    expected = csv_str_to_df("""
        fuel_type,   year,  price
        Biomethane,  2026,  25.57
    """)
    pd.testing.assert_frame_equal(result, expected)


# --- _select_generator_keys ---


def test_select_generator_keys_collapses_units_into_one_row_per_station(
    csv_str_to_df,
):
    summary = csv_str_to_df("""
        IASR ID / DLT names,  Power Station,  Technology Type,     geo_id,  Fuel type,   Fuel cost mapping
        BW01,                 Bayswater,      Steam Sub Critical,  CNSW,    Black Coal,  Bayswater
        BW02,                 Bayswater,      Steam Sub Critical,  CNSW,    Black Coal,  Bayswater
    """)

    result = _select_generator_keys(summary, ["Black Coal"])

    expected = csv_str_to_df("""
        fuel_type,   technology,          geo_id,  power_station,  fuel_cost_mapping
        Black Coal,  Steam Sub Critical,  CNSW,    Bayswater,      Bayswater
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_select_generator_keys_keeps_one_row_per_technology(csv_str_to_df):
    # Bayswater isn't Gas, so it's filtered out.
    summary = csv_str_to_df("""
        IASR ID / DLT names,  Power Station,  Technology Type,       geo_id,  Fuel type,   Fuel cost mapping
        CPSA_GT1,             Condamine A,    CCGT - Gas Turbine,    SQ,      Gas,         Condamine A
        CPSA_GT2,             Condamine A,    CCGT - Gas Turbine,    SQ,      Gas,         Condamine A
        CPSA_ST,              Condamine A,    CCGT - Steam Turbine,  SQ,      Gas,         Condamine A
        BW01,                 Bayswater,      Steam Sub Critical,    CNSW,    Black Coal,  Bayswater
    """)

    result = _select_generator_keys(summary, ["Gas"])

    expected = csv_str_to_df("""
        fuel_type,  technology,            geo_id,  power_station,  fuel_cost_mapping
        Gas,        CCGT - Gas Turbine,    SQ,      Condamine A,    Condamine A
        Gas,        CCGT - Steam Turbine,  SQ,      Condamine A,    Condamine A
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


# --- _merge_prices_onto_generators ---


def test_merge_prices_onto_generators_ignores_unused_price_rows(csv_str_to_df):
    generators = csv_str_to_df("""
        power_station,  fuel_cost_mapping
        Bayswater,      Bayswater
    """)
    prices = csv_str_to_df("""
        fuel_cost_mapping,  2025-26
        Bayswater,          4.12
        Eraring,            3.80
    """)

    result = _merge_prices_onto_generators(prices, generators, "coal_fuel_price")

    expected = csv_str_to_df("""
        power_station,  fuel_cost_mapping,  2025-26
        Bayswater,      Bayswater,          4.12
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_merge_prices_onto_generators_raises_on_unpriced_generators(csv_str_to_df):
    # Mappings are listed once each (Condamine A has two technologies), sorted.
    generators = csv_str_to_df("""
        power_station,  technology,            fuel_cost_mapping
        Tallawarra,     CCGT,                  Tallawarra
        Osborne,        CCGT - Gas Turbine,    Osborne
        Condamine A,    CCGT - Gas Turbine,    Condamine A
        Condamine A,    CCGT - Steam Turbine,  Condamine A
    """)
    prices = csv_str_to_df("""
        fuel_cost_mapping,  2025-26
        Tallawarra,         15.00
    """)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Generator fuel cost mappings with no price in "
            "'gas_prices_existing_generators': ['Condamine A', 'Osborne']"
        ),
    ):
        _merge_prices_onto_generators(
            prices, generators, "gas_prices_existing_generators"
        )


# --- _melt_fuel_prices_to_long ---


def test_melt_fuel_prices_to_long(csv_str_to_df):
    table = csv_str_to_df("""
        power_station,  fuel_cost_mapping,  2024-25,  2025-26
        Bayswater,      Bayswater,          4.12,     3.95
    """)

    result = _melt_fuel_prices_to_long(table, ["power_station"])

    expected = csv_str_to_df("""
        power_station,  year,  price
        Bayswater,      2025,  4.12
        Bayswater,      2026,  3.95
    """)
    pd.testing.assert_frame_equal(result, expected)
