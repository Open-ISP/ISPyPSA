from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ispypsa.translator.create_pypsa_friendly import _filter_and_save_timeseries
from ispypsa.translator.generators import (
    _add_new_entrant_generator_build_costs,
    _add_new_entrant_generator_connection_costs,
    _calculate_annuitised_new_entrant_gen_capital_costs,
    _calculate_blended_fuel_prices,
    _calculate_dynamic_marginal_costs_single_generator,
    _get_dynamic_fuel_prices,
    _get_single_carrier_fuel_prices,
    _get_vre_connection_costs_dict,
    _set_offshore_wind_connection_costs_to_zero,
    _translate_ecaa_generators,
    _translate_new_entrant_generators,
    create_pypsa_friendly_dynamic_marginal_costs,
    create_pypsa_friendly_ecaa_generator_timeseries,
    create_pypsa_friendly_new_entrant_generator_timeseries,
)
from ispypsa.translator.snapshots import (
    _add_investment_periods,
    _create_complete_snapshots_index,
)


def test_translate_ecaa_generators(csv_str_to_df, translated_generator_column_order):
    """Test translation of existing generators (ECAA) to PyPSA format."""
    # Set up input data using csv_str_to_df
    ecaa_generators_csv = """
    generator,      technology_type,      region_id,  sub_region_id,  fuel_type,    fuel_cost_mapping,  minimum_load_mw,  vom_$/mwh_sent_out,  heat_rate_gj/mwh,  commissioning_date,  closure_year,  maximum_capacity_mw, rez_id
    Bayswater,      Steam__Sub__Critical, NSW,        CNSW,           Black__Coal,  Bayswater,          150.0,            2.5,                 9.8,               NaN,                 2031,          660.0,               NaN
    Borumba,        Pumped__Hydro,        QLD,        SQ,             Hydro,        Hydro,              0.0,              0.0,                 0.0,               2030-01-01,          -1,           200.0,               NaN
    Tallawarra,     CCGT,                 NSW,        SNSW,           Gas,          Tallawarra,         170.0,            3.5,                 7.2,               NaN,                 2039,          420.0,               NaN
    """
    ecaa_generators = csv_str_to_df(ecaa_generators_csv)

    # Define input tables and investment periods
    ispypsa_tables = {
        "ecaa_generators": ecaa_generators,
    }
    investment_periods = [2025, 2026]

    # Call the function under test
    result = _translate_ecaa_generators(ispypsa_tables, investment_periods)

    # Define expected output
    expected_output_csv = """
    name,        p_nom,  p_min_pu,  build_year,  carrier,      lifetime,  isp_fuel_cost_mapping,  isp_vom_$/mwh_sent_out,  isp_heat_rate_gj/mwh,  bus,   marginal_cost,  p_nom_extendable,  capital_cost,  isp_technology_type,     isp_rez_id
    Bayswater,   660.0,  0.227,     2024,        Black__Coal,  7.0,       Bayswater,              2.5,                     9.8,                   CNSW,  bayswater,      False,             0.0,           Steam__Sub__Critical,    NaN
    Borumba,     200.0,  0.0,       2030,        Hydro,        Infinity,  Hydro,                  0.0,                     0.0,                   SQ,    borumba,        False,             0.0,           Pumped__Hydro,           NaN
    Tallawarra,  420.0,  0.405,     2024,        Gas,          15.0,      Tallawarra,             3.5,                     7.2,                   SNSW,  tallawarra,     False,             0.0,           CCGT,                    NaN
    """
    expected_output = csv_str_to_df(expected_output_csv)
    expected_output = expected_output.replace("Infinity", np.inf)

    expected_column_order = translated_generator_column_order["ecaa_column_order"]
    expected_output = expected_output[expected_column_order]

    # Compare results with expected output
    pd.testing.assert_frame_equal(
        result.sort_values("name").reset_index(drop=True),
        expected_output.sort_values("name").reset_index(drop=True),
        check_like=True,
        check_exact=False,
        atol=1e-3,
    )


def test_translate_ecaa_generators_region_handling(csv_str_to_df):
    """Test that the function correctly handles different region settings."""
    # Set up input data
    generator_csv = """
    generator,  technology_type,  region_id,  sub_region_id,  fuel_type,  fuel_cost_mapping,  minimum_load_mw,  vom_$/mwh_sent_out,  heat_rate_gj/mwh,  commissioning_date,  closure_year,  maximum_capacity_mw,   rez_id
    TestGen,    CCGT,             NSW,        CNSW,           Gas,        TestGen,            50.0,             2.0,                 7.0,               ,                    2035,          100.0,                 NaN
    TestWind,   Wind,             NSW,        CNSW,           Wind,       TestWind,           0.0,              5.0,                 0.0,               ,                    -1,            100.0,                 N3
    """
    generator = csv_str_to_df(generator_csv)

    ispypsa_tables = {"ecaa_generators": generator}
    investment_periods = [2025]

    # Test with different region settings
    result_single_region = _translate_ecaa_generators(
        ispypsa_tables, investment_periods, "single_region"
    )
    result_nem_regions = _translate_ecaa_generators(
        ispypsa_tables, investment_periods, "nem_regions"
    )
    result_sub_regions = _translate_ecaa_generators(
        ispypsa_tables, investment_periods, "sub_regions"
    )

    # Check bus assignment - for default rez config "discrete_nodes"
    # single_region: "NEM"
    single_region_non_rez_bus = result_single_region.loc[
        result_single_region["name"] == "TestGen", "bus"
    ].values[0]
    assert single_region_non_rez_bus == "NEM"
    single_region_rez_bus = result_single_region.loc[
        result_single_region["name"] == "TestWind", "bus"
    ].values[0]
    assert single_region_rez_bus == "N3"

    # nem_regions: region_id
    nem_regions_non_rez_bus = result_nem_regions.loc[
        result_nem_regions["name"] == "TestGen", "bus"
    ].values[0]
    assert nem_regions_non_rez_bus == "NSW"
    nem_regions_rez_bus = result_nem_regions.loc[
        result_nem_regions["name"] == "TestWind", "bus"
    ].values[0]
    assert nem_regions_rez_bus == "N3"

    # sub_regions: sub_region_id
    sub_regions_non_rez_bus = result_sub_regions.loc[
        result_sub_regions["name"] == "TestGen", "bus"
    ].values[0]
    assert sub_regions_non_rez_bus == "CNSW"
    sub_regions_rez_bus = result_sub_regions.loc[
        result_sub_regions["name"] == "TestWind", "bus"
    ].values[0]
    assert sub_regions_rez_bus == "N3"


def test_translate_new_entrant_generators(
    sample_generator_translator_tables, translated_generator_column_order
):
    """Test translation of new entrant generators to PyPSA format."""

    new_entrant_generators = pd.DataFrame(
        {
            "generator_name": ["CCGT", "Large scale Solar PV", "Wind"],
            "technology_type": ["CCGT", "Large scale Solar PV", "Wind"],
            "region_id": ["SA", "NSW", "QLD"],
            "sub_region_id": ["CSA", "SNSW", "NQ"],
            "rez_location": [None, "Tumut", "Far North QLD"],
            "fuel_type": ["Gas", "Solar", "Wind"],
            "fuel_cost_mapping": ["SA new CCGT", "Solar", "Wind"],
            "minimum_stable_level_%": [45.0, 0.0, 0.0],
            "fom_$/kw/annum": [10.0, 15.0, 25.0],
            "vom_$/mwh_sent_out": [4.0, 0.0, 0.0],
            "heat_rate_gj/mwh": [7.2, 0.0, 0.0],
            # "maximum_capacity_mw": [400, None, None],
            # "unit_capacity_mw": [50, None, None],
            "lifetime": [40, 30, 30],
            "connection_cost_technology": ["CCGT", "Large scale Solar PV", "Wind"],
            "connection_cost_rez/_region_id": ["SA", "Tumut", "Far North Queensland"],
            "technology_specific_lcf_%": [100.0, 95.0, 103.0],
            "isp_resource_type": ["CCGT", "SAT", "WM"],
            "rez_id": [None, "N7", "Q1"],
            "generator": ["ccgt_csa", "large_scale_solar_pv_n7_sat", "wind_q1_wm"],
        }
    )

    # Define input tables and investment periods
    ispypsa_tables = {
        "new_entrant_generators": new_entrant_generators,
        **{
            k: sample_generator_translator_tables[k]
            for k in [
                "gas_prices",
                "biomethane_prices",
                "new_entrant_build_costs",
                "new_entrant_wind_and_solar_connection_costs",
                "new_entrant_non_vre_connection_costs",
            ]
        },
    }
    investment_periods = [2025]
    wacc = 0.05

    # Call the function under test
    result = _translate_new_entrant_generators(ispypsa_tables, investment_periods, wacc)

    # Define expected output
    expected_output = pd.DataFrame(
        {
            "name": [
                "ccgt_csa_2025",
                "large_scale_solar_pv_n7_sat_2025",
                "wind_q1_wm_2025",
            ],
            "p_nom": [0.0, 0.0, 0.0],
            # "p_nom_mod": [50, 0.0, 0.0],
            # "p_nom_max": [400, np.inf, np.inf],
            "p_min_pu": [0.45, 0.0, 0.0],
            "build_year": [2025, 2025, 2025],
            "carrier": ["Gas", "Solar", "Wind"],
            "lifetime": [40, 30, 30],
            "isp_fuel_cost_mapping": [
                "SA new CCGT",
                "Solar",
                "Wind",
            ],
            "isp_vom_$/mwh_sent_out": [4.0, 0.0, 0.0],
            "isp_heat_rate_gj/mwh": [7.2, 0.0, 0.0],
            "bus": ["CSA", "N7", "Q1"],
            "marginal_cost": [
                "ccgt_csa",
                "large_scale_solar_pv_n7_sat",
                "wind_q1_wm",
            ],
            "p_nom_extendable": [True, True, True],
            "capital_cost": [
                127139.1039,
                124741.771,
                142873.2004,
            ],
            "isp_technology_type": [
                "CCGT",
                "Large scale Solar PV",
                "Wind",
            ],
            "isp_resource_type": ["CCGT", "SAT", "WM"],
            "isp_rez_id": [None, "N7", "Q1"],
        }
    )

    expected_col_order = translated_generator_column_order["new_entrant_column_order"]
    expected_output = expected_output[expected_col_order]

    # Compare results with expected output
    pd.testing.assert_frame_equal(
        result.sort_values("name").reset_index(drop=True),
        expected_output.sort_values("name").reset_index(drop=True),
        check_like=True,
        check_exact=False,
        atol=1e-3,
    )


def test_translate_new_entrant_generators_region_handling(
    sample_generator_translator_tables,
):
    """Test that the function correctly handles different region settings."""
    # Set up input data
    new_entrant_generators = pd.DataFrame(
        {
            "generator_name": ["CCGT", "Wind"],
            "technology_type": ["CCGT", "Wind"],
            "region_id": ["QLD", "QLD"],
            "sub_region_id": ["CQ", "SQ"],
            "rez_location": [None, "Wide Bay"],
            "fuel_type": ["Gas", "Wind"],
            "fuel_cost_mapping": ["QLD new CCGT", "Wind"],
            "minimum_stable_level_%": [45.0, 0.0],
            "fom_$/kw/annum": [10.0, 25.0],
            "vom_$/mwh_sent_out": [4.0, 0.0],
            "heat_rate_gj/mwh": [7.2, 0.0],
            "maximum_capacity_mw": [400, None],
            "unit_capacity_mw": [50, None],
            "lifetime": [40, 30],
            "connection_cost_technology": ["CCGT", "Wind"],
            "connection_cost_rez/_region_id": ["QLD", "Wide Bay"],
            "technology_specific_lcf_%": [100.0, 103.0],
            "rez_id": [None, "Q7"],
            "generator": ["CCGT_CQ", "Wind_Q7_WM"],
            "isp_resource_type": ["CCGT", "WM"],
        }
    )

    # Define input tables and investment periods
    ispypsa_tables = {
        "new_entrant_generators": new_entrant_generators,
        **{
            k: sample_generator_translator_tables[k]
            for k in [
                "new_entrant_build_costs",
                "new_entrant_wind_and_solar_connection_costs",
                "new_entrant_non_vre_connection_costs",
            ]
        },
    }
    investment_periods = [2025]
    wacc = 0.05

    # Test with different region settings
    result_single_region = _translate_new_entrant_generators(
        ispypsa_tables, investment_periods, wacc, "single_region"
    )
    result_nem_regions = _translate_new_entrant_generators(
        ispypsa_tables, investment_periods, wacc, "nem_regions"
    )
    result_sub_regions = _translate_new_entrant_generators(
        ispypsa_tables, investment_periods, wacc, "sub_regions"
    )

    # Check bus assignment - for default rez config "discrete_nodes"
    # single_region: "NEM"
    single_region_non_rez_bus = result_single_region.loc[
        result_single_region["name"] == "CCGT_CQ_2025", "bus"
    ].values[0]
    assert single_region_non_rez_bus == "NEM"
    single_region_wind_bus = result_single_region.loc[
        result_single_region["name"] == "Wind_Q7_WM_2025", "bus"
    ].values[0]
    assert single_region_wind_bus == "Q7"

    # nem_regions: region_id
    nem_regions_non_rez_bus = result_nem_regions.loc[
        result_nem_regions["name"] == "CCGT_CQ_2025", "bus"
    ].values[0]
    assert nem_regions_non_rez_bus == "QLD"
    nem_regions_wind_bus = result_nem_regions.loc[
        result_nem_regions["name"] == "Wind_Q7_WM_2025", "bus"
    ].values[0]
    assert nem_regions_wind_bus == "Q7"

    # sub_regions: sub_region_id
    sub_regions_non_rez_bus = result_sub_regions.loc[
        result_sub_regions["name"] == "CCGT_CQ_2025", "bus"
    ].values[0]
    assert sub_regions_non_rez_bus == "CQ"
    sub_regions_wind_bus = result_sub_regions.loc[
        result_sub_regions["name"] == "Wind_Q7_WM_2025", "bus"
    ].values[0]
    assert sub_regions_wind_bus == "Q7"


def test_add_new_entrant_generator_build_costs(
    csv_str_to_df, sample_generator_translator_tables
):
    """Test that build costs are correctly merged into the generators table."""
    # Set up test data
    generators_csv = """
    generator_name,           build_year
    CCGT,                     2025
    Large__scale__Solar__PV,  2023
    Wind,                     2024
    """
    generators_df = csv_str_to_df(generators_csv)

    build_costs_df = sample_generator_translator_tables["new_entrant_build_costs"]

    # Call the function
    result = _add_new_entrant_generator_build_costs(generators_df, build_costs_df)

    expected_result = """
    generator_name,           build_year,    build_cost_$/mw
    CCGT,                     2025,          1900000
    Large__scale__Solar__PV,  2023,          1600000
    Wind,                     2024,          1700000
    """
    expected_result_df = csv_str_to_df(expected_result)

    pd.testing.assert_frame_equal(result, expected_result_df, check_dtype=False)


def test_add_new_entrant_generator_build_costs_missing_build_year(
    csv_str_to_df, sample_generator_translator_tables
):
    """Test that the function raises an error when build_year column is missing."""
    generators_csv = """
    generator_name,           technology_specific_lcf_%
    CCGT,                     100.0
    """
    generators_df = csv_str_to_df(generators_csv)

    build_costs_df = sample_generator_translator_tables["new_entrant_build_costs"]

    # Check that the function raises a ValueError
    with pytest.raises(
        ValueError,
        match="new_entrant_generators_table must have column 'build_year' to merge in build costs.",
    ):
        _add_new_entrant_generator_build_costs(generators_df, build_costs_df)


def test_add_new_entrant_generator_build_costs_undefined_build_costs(
    csv_str_to_df, sample_generator_translator_tables
):
    """Test that the function raises an error when build costs are undefined for some generators."""
    generators_csv = """
    generator_name,           build_year
    CCGT,                     2025
    Large__scale__Solar__PV,  2023
    Wind,                     2024
    NonExistentGenerator,     2025
    """
    generators_df = csv_str_to_df(generators_csv)

    build_costs_df = sample_generator_translator_tables["new_entrant_build_costs"]

    # Check that the function raises a ValueError for undefined build costs
    with pytest.raises(
        ValueError,
        match=r"Undefined build costs for new entrant generators: \['NonExistentGenerator'\]",
    ):
        _add_new_entrant_generator_build_costs(generators_df, build_costs_df)


def test_get_vre_connection_costs_dict(csv_str_to_df):
    """Test that VRE connection costs are correctly converted to a dictionary."""
    vre_costs_csv = """
    REZ__names,                2024_25_$/mw,  2039_40_$/mw, system_strength_connection_cost_$/mw
    Northern__Qld,             50000,         45000,        10000
    Wide__Bay,                 55000,         50000,        12000
    Leigh__Creek,              120000,        120000,       0
    Tumut,                     NaN,           NaN,          220000
    """
    vre_costs_df = csv_str_to_df(vre_costs_csv)

    # Call the function
    result = _get_vre_connection_costs_dict(vre_costs_df)

    expected_result = {
        "Northern Qld_2025": 60000.0,
        "Northern Qld_2040": 55000.0,
        "Wide Bay_2025": 67000.0,
        "Wide Bay_2040": 62000.0,
        "Leigh Creek_2025": 120000.0,
        "Leigh Creek_2040": 120000.0,
        "Tumut_2025": 220000.0,
        "Tumut_2040": 220000.0,
    }

    assert result == expected_result


def test_set_offshore_wind_connection_costs_to_zero(csv_str_to_df):
    """Test that offshore wind connection costs are set to zero."""
    generators_csv = """
    generator_name,                  connection_cost_$/mw
    Wind__-__offshore__(fixed),      50000
    Wind__-__offshore__(floating),   60000
    Wind,                            40000
    Large__scale__Solar__PV,         30000
    CCGT,                            20000
    """
    generators_df = csv_str_to_df(generators_csv)

    # Call the function
    result = _set_offshore_wind_connection_costs_to_zero(generators_df)

    expected_result = """
    generator_name,                  connection_cost_$/mw
    Wind__-__offshore__(fixed),      0
    Wind__-__offshore__(floating),   0
    Wind,                            40000
    Large__scale__Solar__PV,         30000
    CCGT,                            20000
    """
    expected_result_df = csv_str_to_df(expected_result)

    pd.testing.assert_frame_equal(result, expected_result_df, check_dtype=False)


def test_add_new_entrant_generator_connection_costs(
    csv_str_to_df, sample_generator_translator_tables
):
    generators_csv = """
    generator_name,                fuel_type,     build_year,   connection_cost_technology,     connection_cost_rez/_region_id,     generator
    CCGT,                          Gas,           2024,         CCGT,                           NSW,                                ccgt_nsw
    Large__scale__Solar__PV,       Solar,         2025,         Large__scale__Solar__PV,        Far__North__Queensland,             large_scale_solar_pv_sat_q1
    Wind,                          Wind,          2023,         Wind,                           Tumut,                              wind_wm_n7
    Wind__-__offshore__(floating), Wind,          2024,         Wind__-__offshore__(floating),  Leigh__Creek,                       wind_wfl_s6
    Biomass,                       Biomass,       2025,         Biomass,                        QLD,                                biomass_qld
    """
    generators_df = csv_str_to_df(generators_csv)

    vre_connection_costs = sample_generator_translator_tables[
        "new_entrant_wind_and_solar_connection_costs"
    ]

    non_vre_connection_costs = sample_generator_translator_tables[
        "new_entrant_non_vre_connection_costs"
    ]

    # Call the function
    result = _add_new_entrant_generator_connection_costs(
        generators_df, vre_connection_costs, non_vre_connection_costs
    )
    result_col_order = result.columns

    expected_result = """
    generator_name,                fuel_type,     build_year,   connection_cost_technology,     connection_cost_rez/_region_id,        connection_cost_$/mw,    generator
    CCGT,                          Gas,           2024,         CCGT,                           NSW,                                   85000,                   ccgt_nsw
    Large__scale__Solar__PV,       Solar,         2025,         Large__scale__Solar__PV,        Far__North__Queensland,                267000,                  large_scale_solar_pv_sat_q1
    Wind,                          Wind,          2023,         Wind,                           Tumut,                                 337000,                  wind_wm_n7
    Wind__-__offshore__(floating), Wind,          2024,         Wind__-__offshore__(floating),  Leigh__Creek,                          0,                       wind_wfl_s6
    Biomass,                       Biomass,       2025,         Biomass,                        QLD,                                   120000,                  biomass_qld
    """

    expected_result_df = csv_str_to_df(expected_result)
    expected_result_df = expected_result_df[result_col_order]

    pd.testing.assert_frame_equal(result, expected_result_df, check_dtype=False)

    # check that if there's an extra generator that doesn't have connection costs the
    # correct error gets returned:
    extra_generator_csv = """
    generator_name,                fuel_type,     build_year,   connection_cost_technology,     connection_cost_rez/_region_id,     generator
    Dummy__Gen,                    Gas,           2024,         Dummy__Gen,                     NSW,                                dummy_gen_nsw
    """
    extra_generator = csv_str_to_df(extra_generator_csv)
    with_extra_gen = pd.concat([generators_df, extra_generator], ignore_index=True)

    with pytest.raises(ValueError) as excinfo:
        _add_new_entrant_generator_connection_costs(
            with_extra_gen, vre_connection_costs, non_vre_connection_costs
        )
    assert (
        str(excinfo.value)
        == "Missing connection costs for the following generators: ['dummy_gen_nsw']"
    )


def test_calculate_annuitised_new_entrant_gen_capital_costs(csv_str_to_df):
    """Test that capital costs are correctly annuitised."""
    generators_csv = """
    generator_name,                lifetime,    fom_$/kw/annum,  connection_cost_$/mw,  build_cost_$/mw,  technology_specific_lcf_%
    CCGT,                          40,          15.0,            85000,                 1950000,          100.0
    Large__scale__Solar__PV,       30,          20.0,            267000,                1400000,          95.0
    Wind,                          30,          25.0,            337000,                1800000,          98.0
    Wind__-__offshore__(floating), 30,          600.0,           0,                     4400000,          107.0
    Biomass,                       25,          150.0,           120000,                2500000,          100.0
    Dummy__Gen,                    100,         10.0,            0,                     9000000,          80.0
    """
    generators_df = csv_str_to_df(generators_csv)
    wacc = 0.05  # 5% weighted average cost of capital

    # Call the function
    result = _calculate_annuitised_new_entrant_gen_capital_costs(generators_df, wacc)

    expected_result = """
    generator_name,                lifetime,    fom_$/kw/annum,  connection_cost_$/mw,  build_cost_$/mw,  technology_specific_lcf_%,    capital_cost
    CCGT,                          40,          15.0,            85000,                 1950000,          100.0,                        133596.058
    Large__scale__Solar__PV,       30,          20.0,            267000,                1400000,          95.0,                         123887.1418
    Wind,                          30,          25.0,            337000,                1800000,          98.0,                         161673.0651
    Wind__-__offshore__(floating), 30,          600.0,           0,                     4400000,          107.0,                        906262.1564
    Biomass,                       25,          150.0,           120000,                2500000,          100.0,                        335895.4381
    Dummy__Gen,                    100,         10.0,            0,                     9000000,          80.0,                         372758.5941
    """

    expected_result_df = csv_str_to_df(expected_result)

    pd.testing.assert_frame_equal(
        result, expected_result_df, check_dtype=False, check_exact=False
    )


def test_get_single_carrier_fuel_prices_simple(
    sample_generator_translator_tables, csv_str_to_df
):
    # Just test with simple Black Coal carrier (no calcs performed)
    ispypsa_tables = {
        key: sample_generator_translator_tables[key]
        for key in [
            "gas_prices",
            "coal_prices",
            "biomass_prices",
            "hydrogen_prices",
            "biomethane_prices",
            "liquid_fuel_prices",
            "gpg_emissions_reduction_h2",
            "gpg_emissions_reduction_biomethane",
        ]
    }
    generators_df = sample_generator_translator_tables["translated_generators_df"]

    # first test with "Black Coal" carrier
    black_coal_prices = _get_single_carrier_fuel_prices(
        "Black Coal", generators_df, ispypsa_tables
    )

    expected_black_coal_prices = """
    isp_fuel_cost_mapping,   2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj,  carrier
    Eraring,                 1.0,           2.0,           3.0,           Black__Coal
    """
    expected_black_coal_prices = csv_str_to_df(expected_black_coal_prices)

    pd.testing.assert_frame_equal(
        black_coal_prices.sort_values("isp_fuel_cost_mapping").reset_index(drop=True),
        expected_black_coal_prices.sort_values("isp_fuel_cost_mapping").reset_index(
            drop=True
        ),
    )


def test_get_single_carrier_fuel_prices_hyblend(
    sample_generator_translator_tables, csv_str_to_df
):
    # Test with hyblend carrier, which should perform some calculations and
    # create bespoke inputs for the _calculate_hyblend_prices function:
    ispypsa_tables = {
        key: sample_generator_translator_tables[key]
        for key in [
            "gas_prices",
            "coal_prices",
            "biomass_prices",
            "hydrogen_prices",
            "biomethane_prices",
            "liquid_fuel_prices",
            "gpg_emissions_reduction_h2",
            "gpg_emissions_reduction_biomethane",
        ]
    }
    generators_df = sample_generator_translator_tables["translated_generators_df"]

    hyblend_prices = _get_single_carrier_fuel_prices(
        "Hyblend", generators_df, ispypsa_tables
    )

    expected_hyblend_prices = """
    isp_fuel_cost_mapping,   2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj,   carrier
    QLD__new__OCGT,          20.0,          21.44,         22.95,          Hyblend
    """
    expected_hyblend_prices = csv_str_to_df(expected_hyblend_prices)

    pd.testing.assert_frame_equal(
        hyblend_prices.sort_values("isp_fuel_cost_mapping").reset_index(drop=True),
        expected_hyblend_prices.sort_values("isp_fuel_cost_mapping").reset_index(
            drop=True
        ),
    )


def test_calculate_blended_fuel_prices_simple(csv_str_to_df):
    base_prices = """
    generator,     2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj,  2025_26_$/gj
    generator_X,            1.0,           2.0,           3.0,           4.0
    """
    base_prices = csv_str_to_df(base_prices).set_index("generator")

    blend_prices = pd.DataFrame(
        data={
            "2022_23_$/gj": [10.0],
            "2023_24_$/gj": [20.0],
            "2024_25_$/gj": [30.0],
            "2025_26_$/gj": [40.0],
        },
    )

    base_percentages = """
    2022_23_%,     2023_24_%,     2024_25_%,     2025_26_%
    50.0,          50.0,          50.0,          50.0
    """
    base_percentages = csv_str_to_df(base_percentages)

    calculated_blended_prices = _calculate_blended_fuel_prices(
        base_prices, blend_prices, "isp_fuel_cost_mapping", base_percentages
    )

    expected_blended_prices = """
    isp_fuel_cost_mapping,   2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj,  2025_26_$/gj
    generator_X,             5.5,           11.0,          16.5,          22.0
    """
    expected_blended_prices = csv_str_to_df(expected_blended_prices).set_index(
        "isp_fuel_cost_mapping"
    )

    pd.testing.assert_frame_equal(
        calculated_blended_prices.reset_index(),
        expected_blended_prices.reset_index(),
    )


def test_calculate_blended_fuel_prices_multiple_gens(csv_str_to_df):
    base_prices = """
    generator,      2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj,  2025_26_$/gj
    mapping_1,               1.0,           2.0,           3.0,           4.0
    mapping_2,               5.0,           6.0,           7.0,           8.0
    mapping_3,               1.0,           3.0,           6.0,          10.0
    """
    base_prices = csv_str_to_df(base_prices).set_index("generator")

    blend_prices = pd.DataFrame(
        data={
            "2022_23_$/gj": [10.0],
            "2023_24_$/gj": [20.0],
            "2024_25_$/gj": [30.0],
            "2025_26_$/gj": [40.0],
        },
    )

    base_percentages = """
    2022_23_%, 2023_24_%,  2024_25_%,  2025_26_%
    NaN,           100.0,       50.0,       25.0
    """
    base_percentages = csv_str_to_df(base_percentages)

    calculated_blended_prices = _calculate_blended_fuel_prices(
        base_prices, blend_prices, "generator", base_percentages
    )

    expected_blended_prices = """
    isp_fuel_cost_mapping,   2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj,  2025_26_$/gj
    mapping_1,                        NaN,           2.0,          16.5,          31.0
    mapping_2,                        NaN,           6.0,          18.5,          32.0
    mapping_3,                        NaN,           3.0,          18.0,          32.5
    """
    expected_blended_prices = csv_str_to_df(expected_blended_prices).set_index(
        "isp_fuel_cost_mapping"
    )

    pd.testing.assert_frame_equal(
        calculated_blended_prices.reset_index(),
        expected_blended_prices.reset_index(),
    )


def test_calculate_blended_fuel_prices_multiple_gens_and_percents(csv_str_to_df):
    base_prices = """
    generator,      2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj,  2025_26_$/gj
    mapping_1,               1.0,           2.0,           3.0,           4.0
    mapping_2,               5.0,           6.0,           7.0,           8.0
    mapping_3,               1.0,           2.0,           3.0,           4.0
    """
    base_prices = csv_str_to_df(base_prices).set_index("generator")

    blend_prices = pd.DataFrame(
        data={
            "extra_col": ["extra_content"],
            "2022_23_$/gj": [10.0],
            "2023_24_$/gj": [20.0],
            "2024_25_$/gj": [30.0],
            "2025_26_$/gj": [40.0],
        },
    )

    base_percentages = """
    generator,      2022_23_%,  2023_24_%,  2024_25_%,  2025_26_%
    gen_1,               50.0,       50.0,       50.0,       50.0
    gen_2,               80.0,       60.0,       50.0,        0.0
    gen_3,                NaN,      100.0,       50.0,       25.0
    """
    base_percentages = csv_str_to_df(base_percentages)

    generator_to_fuel_cost_mapping = {
        "gen_1": "mapping_1",
        "gen_2": "mapping_2",
        "gen_3": "mapping_3",
    }

    calculated_blended_prices = _calculate_blended_fuel_prices(
        base_prices,
        blend_prices,
        "generator",
        base_percentages,
        generator_to_fuel_cost_mapping,
    )

    expected_blended_prices = """
    isp_fuel_cost_mapping,   2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj,  2025_26_$/gj
    mapping_1,               5.5,           11.0,          16.5,          22.0
    mapping_2,               6.0,           11.6,          18.5,          40.0
    mapping_3,               NaN,           2.0,           16.5,          31.0
    """
    expected_blended_prices = csv_str_to_df(expected_blended_prices).set_index(
        "isp_fuel_cost_mapping"
    )

    pd.testing.assert_frame_equal(
        calculated_blended_prices.reset_index(),
        expected_blended_prices.reset_index(),
    )


def test_calculate_blended_fuel_prices_missing_mapping_column(csv_str_to_df):
    """Test that an error is raised when base_percentages has multiple rows but no mapping column."""
    base_prices = """
    generator,      2022_23_$/gj,  2023_24_$/gj
    mapping_1,               1.0,           2.0
    """
    base_prices = csv_str_to_df(base_prices).set_index("generator")

    blend_prices = pd.DataFrame(
        data={
            "2022_23_$/gj": [10.0],
            "2023_24_$/gj": [20.0],
        },
    )

    # Multiple rows but missing the fuel_cost_mapping_col
    base_percentages = """
    wrong_col,      2022_23_%,     2023_24_%
    mapping_1,           50.0,          50.0
    mapping_2,           80.0,          60.0
    """
    base_percentages = csv_str_to_df(base_percentages)

    expected_error_msg = (
        "base_percentages must have column 'generator' if more than one row present."
    )

    with pytest.raises(ValueError, match=expected_error_msg):
        _calculate_blended_fuel_prices(
            base_prices, blend_prices, "generator", base_percentages
        )


def test_calculate_blended_fuel_prices_missing_mappings(csv_str_to_df):
    """Test that an error is raised when base_percentages is missing mappings."""
    base_prices = """
    isp_fuel_cost_mapping,   2022_23_$/gj,  2023_24_$/gj
    mapping_1,               1.0,           2.0
    mapping_2,               3.0,           4.0
    """
    base_prices = csv_str_to_df(base_prices).set_index("isp_fuel_cost_mapping")

    blend_prices = pd.DataFrame(
        data={
            "2022_23_$/gj": [10.0],
            "2023_24_$/gj": [20.0],
        },
    )

    # Missing mapping_2
    base_percentages = """
    isp_fuel_cost_mapping,   2022_23_%,     2023_24_%
    mapping_1,                    50.0,          50.0
    """
    base_percentages = csv_str_to_df(base_percentages)

    expected_error_msg = r"base_percentages missing values for: \(\{'mapping_2'\}\)"

    with pytest.raises(ValueError, match=expected_error_msg):
        _calculate_blended_fuel_prices(
            base_prices, blend_prices, "isp_fuel_cost_mapping", base_percentages
        )


def test_calculate_blended_fuel_prices_multiple_scenarios(csv_str_to_df):
    """Test that an error is raised when blend_prices has multiple rows (scenarios)."""
    base_prices = """
    isp_fuel_cost_mapping,   2022_23_$/gj,  2023_24_$/gj
    mapping_1,               1.0,           2.0
    """
    base_prices = csv_str_to_df(base_prices).set_index("isp_fuel_cost_mapping")

    # Multiple rows in blend_prices
    blend_prices = pd.DataFrame(
        {
            "2022_23_$/gj": [10.0, 15.0],
            "2023_24_$/gj": [20.0, 25.0],
        }
    )

    base_percentages = """
    2022_23_%,     2023_24_%
    50.0,          50.0
    """
    base_percentages = csv_str_to_df(base_percentages)

    expected_error_msg = (
        r"Expected blend_prices for a single scenario \(row\), received multiple."
    )

    with pytest.raises(ValueError, match=expected_error_msg):
        _calculate_blended_fuel_prices(
            base_prices, blend_prices, "isp_fuel_cost_mapping", base_percentages
        )


def test_get_dynamic_fuel_prices(sample_generator_translator_tables, csv_str_to_df):
    # Extract tables from the fixture
    ispypsa_tables = {
        key: sample_generator_translator_tables[key]
        for key in [
            "gas_prices",
            "coal_prices",
            "biomass_prices",
            "hydrogen_prices",
            "biomethane_prices",
            "liquid_fuel_prices",
            "gpg_emissions_reduction_h2",
            "gpg_emissions_reduction_biomethane",
        ]
    }
    generators_df = sample_generator_translator_tables["translated_generators_df"]

    # Set up snapshots
    snapshots_csv = """
    investment_periods,     snapshots
    2024,                   2023-07-01__12:00:00
    2024,                   2023-10-01__12:00:00
    2024,                   2024-01-01__12:00:00
    2024,                   2024-04-01__12:00:00
    2025,                   2024-07-01__12:00:00
    2025,                   2024-10-01__12:00:00
    2025,                   2025-01-01__12:00:00
    2025,                   2025-04-01__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Call the function with the test data
    dynamic_fuel_prices = _get_dynamic_fuel_prices(
        ispypsa_tables, generators_df, snapshots
    )

    # Check that the fuel costs are calculated correctly
    expected_fuel_prices_csv = """
    isp_fuel_cost_mapping,     2022_23_$/gj,  2023_24_$/gj,  2024_25_$/gj, carrier
    Bairnsdale,                22.0,          23.8,          24.4,         Gas
    Eraring,                   1.0,           2.0,           3.0,          Black__Coal
    SA__new__CCGT,             22.0,          23.8,          24.4,         Gas
    QLD__new__OCGT,            20.0,          21.44,         22.95,        Hyblend
    Large__Scale__Solar__PV,   0.0,           0.0,           0.0,          Solar
    """
    expected_fuel_prices = csv_str_to_df(expected_fuel_prices_csv)

    pd.testing.assert_frame_equal(
        dynamic_fuel_prices.sort_values("isp_fuel_cost_mapping").reset_index(drop=True),
        expected_fuel_prices.sort_values("isp_fuel_cost_mapping").reset_index(
            drop=True
        ),
        check_dtype=False,
    )


def test_calculate_dynamic_marginal_cost_single_generator(
    csv_str_to_df,
):
    """Test that the function correctly calculates marginal costs for a single generator."""
    # Set up snapshots
    snapshots_csv = """
    investment_periods,     snapshots
    2024,                   2023-07-01__12:00:00
    2024,                   2023-10-01__12:00:00
    2024,                   2024-01-01__12:00:00
    2024,                   2024-04-01__12:00:00
    2025,                   2024-07-01__12:00:00
    2025,                   2024-10-01__12:00:00
    2025,                   2025-01-01__12:00:00
    2025,                   2025-04-01__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Set up generator data
    generator_csv = """
    name,                carrier,     isp_fuel_cost_mapping,   isp_heat_rate_gj/mwh,  isp_vom_$/mwh_sent_out,  marginal_cost
    Eraring,             Black__Coal, Eraring,                 10,                    5,                       eraring
    """
    generator_df = csv_str_to_df(generator_csv)
    generator_row = generator_df.iloc[0]

    coal_prices = pd.Series(
        {"2022_23_$/gj": 1.0, "2023_24_$/gj": 2.0, "2024_25_$/gj": 3.0}
    )

    # Call the function
    result = _calculate_dynamic_marginal_costs_single_generator(
        generator_row, coal_prices, snapshots
    )

    # Expected result: dynamic_marginal_cost = fuel_price * heat_rate + VOM
    # For Eraring: 10 * fuel_price + 5
    expected_result_csv = """
    investment_periods,     snapshots,                  marginal_cost
    2024,                   2023-07-01__12:00:00,       25.0
    2024,                   2023-10-01__12:00:00,       25.0
    2024,                   2024-01-01__12:00:00,       25.0
    2024,                   2024-04-01__12:00:00,       25.0
    2025,                   2024-07-01__12:00:00,       35.0
    2025,                   2024-10-01__12:00:00,       35.0
    2025,                   2025-01-01__12:00:00,       35.0
    2025,                   2025-04-01__12:00:00,       35.0
    """
    expected_result = csv_str_to_df(expected_result_csv)
    expected_result["snapshots"] = pd.to_datetime(expected_result["snapshots"])

    pd.testing.assert_frame_equal(result.sort_index(), expected_result.sort_index())


def test_calculate_dynamic_marginal_cost_single_generator_input_validation(
    csv_str_to_df,
):
    """Test that the function correctly validates input types."""
    # Set up snapshots
    snapshots_csv = """
    investment_periods,     snapshots
    2024,                   2023-07-01__12:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Set up generator data
    generator_csv = """
    name,                carrier,     isp_fuel_cost_mapping,   isp_heat_rate_gj/mwh,  isp_vom_$/mwh_sent_out,  marginal_cost
    Eraring,             Black__Coal, Eraring,                 10,                    5,                       eraring
    """
    generator_df = csv_str_to_df(generator_csv)
    generator_row = generator_df.iloc[0]

    # Invalid input: gen_fuel_prices as DataFrame instead of Series
    invalid_gen_fuel_prices = pd.DataFrame(
        {"year": ["2022_23", "2023_24"], "price": [1.0, 2.0]}
    )

    # Check that the function raises a TypeError
    with pytest.raises(TypeError, match="Expected gen_fuel_prices to be a series"):
        _calculate_dynamic_marginal_costs_single_generator(
            generator_row, invalid_gen_fuel_prices, snapshots
        )


def test_create_pypsa_friendly_dynamic_marginal_costs_single_gen(
    csv_str_to_df,
    sample_generator_translator_tables,
    tmp_path: Path,
):
    # test snapshots at each quarter across two financial years (2023 and 2024):
    snapshots = """
    investment_periods,     snapshots
    2024,                   2023-07-01__12:00:00
    2024,                   2023-10-01__12:00:00
    2024,                   2024-01-01__12:00:00
    2024,                   2024-04-01__12:00:00
    2025,                   2024-07-01__12:00:00
    2025,                   2024-10-01__12:00:00
    2025,                   2025-01-01__12:00:00
    2025,                   2025-04-01__12:00:00
    """
    snapshots = csv_str_to_df(snapshots)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    test_generator_csv = """
    name,                carrier,     isp_fuel_cost_mapping,   isp_heat_rate_gj/mwh,  isp_vom_$/mwh_sent_out,  marginal_cost
    Eraring,             Black__Coal, Eraring,                 10,                    5,                       eraring
    """
    test_generator = csv_str_to_df(test_generator_csv)
    # Extract tables from the fixture
    ispypsa_tables = {"coal_prices": sample_generator_translator_tables["coal_prices"]}

    # Call the function with the test data
    create_pypsa_friendly_dynamic_marginal_costs(
        ispypsa_tables, test_generator, snapshots, tmp_path
    )

    expected_marginal_costs_csv = """
    investment_periods,     snapshots,                  marginal_cost
    2024,                   2023-07-01__12:00:00,       25.0
    2024,                   2023-10-01__12:00:00,       25.0
    2024,                   2024-01-01__12:00:00,       25.0
    2024,                   2024-04-01__12:00:00,       25.0
    2025,                   2024-07-01__12:00:00,       35.0
    2025,                   2024-10-01__12:00:00,       35.0
    2025,                   2025-01-01__12:00:00,       35.0
    2025,                   2025-04-01__12:00:00,       35.0
    """
    expected_marginal_costs = csv_str_to_df(expected_marginal_costs_csv)
    expected_marginal_costs["snapshots"] = pd.to_datetime(
        expected_marginal_costs["snapshots"]
    )

    output_file = tmp_path / "marginal_cost_timeseries/eraring.parquet"
    marginal_costs = pd.read_parquet(output_file)
    marginal_costs["snapshots"] = pd.to_datetime(marginal_costs["snapshots"])

    pd.testing.assert_frame_equal(
        marginal_costs.sort_values(by="snapshots"),
        expected_marginal_costs.sort_values(by="snapshots"),
    )


def test_create_pypsa_friendly_dynamic_marginal_costs_multiple_gens(
    csv_str_to_df,
    sample_generator_translator_tables,
    tmp_path: Path,
):
    # test snapshots at 6 months across two financial years (2023 and 2024):
    snapshots = """
    investment_periods,     snapshots
    2024,                   2023-07-01__12:00:00
    2024,                   2024-01-01__12:00:00
    2025,                   2024-07-01__12:00:00
    2025,                   2025-01-01__12:00:00
    """
    snapshots = csv_str_to_df(snapshots)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    test_generators_csv = """
    name,                      carrier,             isp_fuel_cost_mapping,     isp_heat_rate_gj/mwh,  isp_vom_$/mwh_sent_out,   marginal_cost
    Bairnsdale,                Gas,                 Bairnsdale,                20,                    8,                        bairnsdale
    Eraring,                   Black__Coal,         Eraring,                   10,                    5,                        eraring
    SA__new__CCGT,             Gas,                 SA__new__CCGT,             11,                    10,                       sa_new_ccgt
    Kogan__Gas,                Hyblend,             QLD__new__OCGT,            3,                     20,                       kogan_gas
    Large__Scale__Solar__PV,   Solar,               Large__Scale__Solar__PV,   0,                     0,                        large_scale_solar_pv
    Unserved__Energy,          Unserved__Energy,    Unserved__Energy,          0,                     0,                        STATIC_MARGINAL_COST
    """
    test_generators = csv_str_to_df(test_generators_csv)
    test_generators = test_generators.replace("STATIC_MARGINAL_COST", 10000.0)
    # Extract tables from the fixture

    ispypsa_tables = {
        key: sample_generator_translator_tables[key]
        for key in [
            "gas_prices",
            "coal_prices",
            "hydrogen_prices",
            "biomethane_prices",
            "gpg_emissions_reduction_h2",
            "gpg_emissions_reduction_biomethane",
        ]
    }

    # Call the function with the test data
    create_pypsa_friendly_dynamic_marginal_costs(
        ispypsa_tables, test_generators, snapshots, tmp_path
    )

    expected_marginal_costs = dict(
        bairnsdale="""
        investment_periods,     snapshots,                  marginal_cost
        2024,                   2023-07-01__12:00:00,       484.0
        2024,                   2024-01-01__12:00:00,       484.0
        2025,                   2024-07-01__12:00:00,       496.0
        2025,                   2025-01-01__12:00:00,       496.0
        """,
        eraring="""
        investment_periods,     snapshots,                  marginal_cost
        2024,                   2023-07-01__12:00:00,       25.0
        2024,                   2024-01-01__12:00:00,       25.0
        2025,                   2024-07-01__12:00:00,       35.0
        2025,                   2025-01-01__12:00:00,       35.0
        """,
        sa_new_ccgt="""
        investment_periods,     snapshots,                  marginal_cost
        2024,                   2023-07-01__12:00:00,       271.8
        2024,                   2024-01-01__12:00:00,       271.8
        2025,                   2024-07-01__12:00:00,       278.4
        2025,                   2025-01-01__12:00:00,       278.4
        """,
        kogan_gas="""
        investment_periods,     snapshots,                  marginal_cost
        2024,                   2023-07-01__12:00:00,       84.32
        2024,                   2024-01-01__12:00:00,       84.32
        2025,                   2024-07-01__12:00:00,       88.85
        2025,                   2025-01-01__12:00:00,       88.85
        """,
        large_scale_solar_pv="""
        investment_periods,     snapshots,                  marginal_cost
        2024,                   2023-07-01__12:00:00,       0.0
        2024,                   2024-01-01__12:00:00,       0.0
        2025,                   2024-07-01__12:00:00,       0.0
        2025,                   2025-01-01__12:00:00,       0.0
        """,
    )

    for generator, expected_marginal_cost in expected_marginal_costs.items():
        expected_marginal_cost = csv_str_to_df(expected_marginal_cost)
        expected_marginal_cost["snapshots"] = pd.to_datetime(
            expected_marginal_cost["snapshots"]
        )

        output_file = tmp_path / "marginal_cost_timeseries" / f"{generator}.parquet"
        marginal_costs = pd.read_parquet(output_file)
        marginal_costs["snapshots"] = pd.to_datetime(marginal_costs["snapshots"])

        pd.testing.assert_frame_equal(
            marginal_costs.sort_values(by="snapshots"),
            expected_marginal_cost.sort_values(by="snapshots"),
        )

    # Expect NO marginal costs for the unserved energy generator
    assert len(list(tmp_path.glob("marginal_cost_timeseries/*.parquet"))) == 5

    # Test with same inputs but make one marginal cost value nan to check ValueError raised:
    test_generators = csv_str_to_df(test_generators_csv)
    test_generators = test_generators.replace("STATIC_MARGINAL_COST", np.nan)
    with pytest.raises(ValueError) as excinfo:
        create_pypsa_friendly_dynamic_marginal_costs(
            ispypsa_tables, test_generators, snapshots, tmp_path
        )
    # check message is correct:
    assert (
        str(excinfo.value)
        == "Undefined marginal cost for generator(s): {'Unserved Energy'}"
    )


def test_create_pypsa_friendly_existing_generator_timeseries(tmp_path):
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data/isp_2024")

    ecaa_ispypsa = pd.DataFrame(
        {
            "generator": ["Moree Solar Farm", "Canunda Wind Farm"],
            "fuel_type": ["Solar", "Wind"],
        }
    )

    snapshots = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2026,
        temporal_resolution_min=30,
        year_type="fy",
    )

    snapshots = _add_investment_periods(snapshots, [2025], "fy")

    generator_traces_by_type = create_pypsa_friendly_ecaa_generator_timeseries(
        ecaa_ispypsa,
        parsed_trace_path,
        generator_types=["solar", "wind"],
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
    )

    if generator_traces_by_type is not None:
        # Filter and save generator timeseries by type
        for gen_type, gen_traces in generator_traces_by_type.items():
            if gen_traces:
                _filter_and_save_timeseries(
                    gen_traces,
                    snapshots,
                    tmp_path,
                    f"{gen_type}_traces",
                )

    # Build expected solar trace from consolidated format
    # FY2025 uses RefYear2011, FY2026 uses RefYear2018
    solar_2011 = pd.read_parquet(
        parsed_trace_path / "project/RefYear2011_Moree_Solar_Farm_SAT.parquet"
    )
    solar_2018 = pd.read_parquet(
        parsed_trace_path / "project/RefYear2018_Moree_Solar_Farm_SAT.parquet"
    )
    # Filter FY2025 (Jul 2024 - Jun 2025) from RefYear2011
    solar_fy2025 = solar_2011[
        (solar_2011["datetime"] > "2024-07-01")
        & (solar_2011["datetime"] <= "2025-07-01")
    ]
    # Filter FY2026 (Jul 2025 - Jun 2026) from RefYear2018
    solar_fy2026 = solar_2018[
        (solar_2018["datetime"] > "2025-07-01")
        & (solar_2018["datetime"] <= "2026-07-01")
    ]
    expected_trace = pd.concat([solar_fy2025, solar_fy2026])
    expected_trace["datetime"] = expected_trace["datetime"].astype("datetime64[ns]")
    expected_trace = expected_trace.rename(
        columns={"datetime": "snapshots", "value": "p_max_pu"}
    )
    expected_trace = pd.merge(expected_trace, snapshots, on="snapshots")
    expected_trace = expected_trace.loc[
        :, ["investment_periods", "snapshots", "p_max_pu"]
    ]
    expected_trace = expected_trace.reset_index(drop=True)

    got_trace = pd.read_parquet(
        tmp_path / Path("solar_traces/Moree Solar Farm.parquet")
    )

    pd.testing.assert_frame_equal(expected_trace, got_trace)

    # Build expected wind trace from consolidated format
    wind_2011 = pd.read_parquet(
        parsed_trace_path / "project/RefYear2011_Canunda_Wind_Farm.parquet"
    )
    wind_2018 = pd.read_parquet(
        parsed_trace_path / "project/RefYear2018_Canunda_Wind_Farm.parquet"
    )
    # Filter FY2025 (Jul 2024 - Jun 2025) from RefYear2011
    wind_fy2025 = wind_2011[
        (wind_2011["datetime"] > "2024-07-01") & (wind_2011["datetime"] <= "2025-07-01")
    ]
    # Filter FY2026 (Jul 2025 - Jun 2026) from RefYear2018
    wind_fy2026 = wind_2018[
        (wind_2018["datetime"] > "2025-07-01") & (wind_2018["datetime"] <= "2026-07-01")
    ]
    expected_trace = pd.concat([wind_fy2025, wind_fy2026])
    expected_trace["datetime"] = expected_trace["datetime"].astype("datetime64[ns]")
    expected_trace = expected_trace.rename(
        columns={"datetime": "snapshots", "value": "p_max_pu"}
    )
    expected_trace = pd.merge(expected_trace, snapshots, on="snapshots")
    expected_trace = expected_trace.loc[
        :, ["investment_periods", "snapshots", "p_max_pu"]
    ]
    expected_trace = expected_trace.reset_index(drop=True)

    got_trace = pd.read_parquet(
        tmp_path / Path("wind_traces/Canunda Wind Farm.parquet")
    )

    pd.testing.assert_frame_equal(expected_trace, got_trace)


def test_create_pypsa_friendly_new_entrant_generator_timeseries(tmp_path):
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data/isp_2024")

    new_entrant_ispypsa = pd.DataFrame(
        {
            "generator": ["Large scale Solar PV_N1_SAT", "Wind_Q1_WM"],
            "fuel_type": ["Solar", "Wind"],
            "rez_id": ["N1", "Q1"],
            "isp_resource_type": ["SAT", "WM"],
        }
    )

    snapshots = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2026,
        temporal_resolution_min=30,
        year_type="fy",
    )

    snapshots = _add_investment_periods(snapshots, [2025], "fy")

    create_pypsa_friendly_new_entrant_generator_timeseries(
        new_entrant_ispypsa,
        parsed_trace_path,
        tmp_path,
        generator_types=["solar", "wind"],
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
        snapshots=snapshots,
    )

    # Build expected solar trace from new consolidated format
    # FY2025 uses RefYear2011, FY2026 uses RefYear2018
    solar_2011 = pd.read_parquet(parsed_trace_path / "zone/RefYear2011_N1_SAT.parquet")
    solar_2018 = pd.read_parquet(parsed_trace_path / "zone/RefYear2018_N1_SAT.parquet")
    # Filter FY2025 (Jul 2024 - Jun 2025) from RefYear2011
    # Note: 2024-07-01 00:00:00 belongs to FY2024, first FY2025 timestamp is 2024-07-01 00:30:00
    solar_fy2025 = solar_2011[
        (solar_2011["datetime"] > "2024-07-01")
        & (solar_2011["datetime"] <= "2025-07-01")
    ]
    # Filter FY2026 (Jul 2025 - Jun 2026) from RefYear2018
    solar_fy2026 = solar_2018[
        (solar_2018["datetime"] > "2025-07-01")
        & (solar_2018["datetime"] <= "2026-07-01")
    ]
    expected_trace = pd.concat([solar_fy2025, solar_fy2026])
    expected_trace["datetime"] = expected_trace["datetime"].astype("datetime64[ns]")
    expected_trace = expected_trace.rename(
        columns={"datetime": "snapshots", "value": "p_max_pu"}
    )
    expected_trace = pd.merge(expected_trace, snapshots, on="snapshots")
    expected_trace = expected_trace.loc[
        :, ["investment_periods", "snapshots", "p_max_pu"]
    ]
    expected_trace = expected_trace.reset_index(drop=True)

    got_trace = pd.read_parquet(
        tmp_path / Path("solar_traces/Large scale Solar PV_N1_SAT.parquet")
    )

    pd.testing.assert_frame_equal(expected_trace, got_trace)

    # Build expected wind trace from new consolidated format
    wind_2011 = pd.read_parquet(parsed_trace_path / "zone/RefYear2011_Q1_WM.parquet")
    wind_2018 = pd.read_parquet(parsed_trace_path / "zone/RefYear2018_Q1_WM.parquet")
    # Filter FY2025 (Jul 2024 - Jun 2025) from RefYear2011
    wind_fy2025 = wind_2011[
        (wind_2011["datetime"] > "2024-07-01") & (wind_2011["datetime"] <= "2025-07-01")
    ]
    # Filter FY2026 (Jul 2025 - Jun 2026) from RefYear2018
    wind_fy2026 = wind_2018[
        (wind_2018["datetime"] > "2025-07-01") & (wind_2018["datetime"] <= "2026-07-01")
    ]
    expected_trace = pd.concat([wind_fy2025, wind_fy2026])
    expected_trace["datetime"] = expected_trace["datetime"].astype("datetime64[ns]")
    expected_trace = expected_trace.rename(
        columns={"datetime": "snapshots", "value": "p_max_pu"}
    )
    expected_trace = pd.merge(expected_trace, snapshots, on="snapshots")
    expected_trace = expected_trace.loc[
        :, ["investment_periods", "snapshots", "p_max_pu"]
    ]
    expected_trace = expected_trace.reset_index(drop=True)

    got_trace = pd.read_parquet(tmp_path / Path("wind_traces/Wind_Q1_WM.parquet"))

    pd.testing.assert_frame_equal(expected_trace, got_trace)
