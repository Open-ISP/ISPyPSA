from pathlib import Path

import pandas as pd
from isp_trace_parser.demand_traces import write_new_demand_filepath
from isp_trace_parser.solar_traces import write_output_solar_filepath
from isp_trace_parser.wind_traces import write_output_wind_area_filepath
from numpy import diff

from ispypsa.config import ModelConfig
from ispypsa.model.build import build_pypsa_network
from ispypsa.templater import renewable_energy_zones
from ispypsa.translator.create_pypsa_friendly import (
    create_pypsa_friendly_inputs,
    create_pypsa_friendly_timeseries_inputs,
)

pd.set_option("display.max_columns", None)


def test_vre_build_limit_constraint(csv_str_to_df, tmp_path, monkeypatch):
    """Test that capacity expansion of VRE is limited by custom constraints in a REZ.

    This test creates a simple single region network with one REZ (Q6, Fitzroy) where:
    - New Solar (SAT) and Wind (WM, WH) are all allowed to be built
    - WH generator is 10% more expensive to build and operate than WM (exaggerated).
        Solar SAT is cheaper than both.
    - Separate resource limit constraints are placed on SAT, WM and WH resources
    - Build limits (from land use limits) are set for all Solar and all Wind generation
        capacity. The combined capacity limit is set to 200MW in this test (less than
        peak demand) to ensure that the build limit constraint is met.


    We would expect that in the first investment period 50 MW of expansion occurs and
    in the second investment period another 50 MW of expansion occurs.
    """
    # Create directories
    ispypsa_dir = tmp_path / "ispypsa_inputs"
    ispypsa_dir.mkdir()
    pypsa_dir = tmp_path / "pypsa_inputs"
    pypsa_dir.mkdir()
    traces_dir = tmp_path / "traces"
    traces_dir.mkdir()

    # Create subdirectories for traces
    for subdir in ["demand", "wind", "solar"]:
        (traces_dir / subdir).mkdir()

    # Mock environment variable for trace parser
    monkeypatch.setenv("PATH_TO_PARSED_TRACES", str(traces_dir))

    # Create a mock config
    config_dict = {
        "paths": {
            "ispypsa_run_name": "dummy_value",
            "parsed_traces_directory": "ENV",
            "parsed_workbook_cache": "dummy_value",
            "workbook_path": "dummy_value",
            "run_directory": "dummy_value",
        },
        "scenario": "Step Change",
        "wacc": 0.07,
        "discount_rate": 0.05,
        "network": {
            "transmission_expansion": True,
            "transmission_expansion_limit_override": None,
            "rez_transmission_expansion": False,
            "rez_connection_expansion_limit_override": None,
            "annuitisation_lifetime": 30,
            "nodes": {
                "regional_granularity": "sub_regions",
                "rezs": "discrete_nodes",
            },
            "rez_to_sub_region_transmission_default_limit": 1e5,
        },
        "temporal": {
            "year_type": "fy",
            "range": {
                "start_year": 2026,
                "end_year": 2026,
            },
            "capacity_expansion": {
                "resolution_min": 30,
                "reference_year_cycle": [2018],
                "investment_periods": [2026],
                "aggregation": {
                    "representative_weeks": None,  # Use full year
                },
            },
        },
        "unserved_energy": {
            "cost": 1000000.0,
            "generator_size_mw": 500.0,
        },
        "solver": "highs",
        "iasr_workbook_version": "6.0",
    }

    # Set up some very simplified sample trace profiles:
    date_time_2025_2 = pd.date_range(
        start="2025-07-01 00:00:00", end="2025-12-31 18:00:00", freq="6h"
    )
    days_2025 = date_time_2025_2.dayofyear.nunique()
    date_time_2026_1 = pd.date_range(
        start="2026-01-01 00:00:00", end="2026-06-30 18:00:00", freq="6h"
    )
    days_2026 = date_time_2026_1.dayofyear.nunique()
    solar_profile = [
        0.00,  # 12am - no generation at night
        0.10,  # 6am  - some generation starting
        0.85,  # 12pm - peak generation at noon
        0.20,  # 6pm  - some generation tailing off
    ]
    wind_high_profile = [
        0.55,  # 12am - stronger night winds
        0.48,  # 6am  - moderate morning winds
        0.25,  # 12pm
        0.22,  # 6pm - low evening wind
    ]
    wind_medium_profile = [
        0.26,  # 12am - moderate night winds
        0.26,  # 6am  - low morning winds
        0.35,  # 12pm
        0.35,  # 6pm - moderate evening wind
    ]
    flat_demand_profile = [500.0, 500.0, 500.0, 500.0]

    demand_data_to_write = [
        (date_time_2025_2, flat_demand_profile * days_2025, "CQ", "2025-2"),
        (date_time_2026_1, flat_demand_profile * days_2026, "CQ", "2026-1"),
    ]

    for date_time, demand, subregion, half_year in demand_data_to_write:
        demand_data = pd.DataFrame(
            {"Datetime": date_time, "Value": demand}
        )  # drop the last value to match array length
        demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])
        file_meta_data = {
            "subregion": subregion,
            "scenario": "Step Change",
            "reference_year": 2018,
            "poe": "POE50",
            "demand_type": "OPSO_MODELLING",
            "hy": half_year,
        }
        file_path = Path(
            traces_dir / "demand" / write_new_demand_filepath(file_meta_data)
        )
        file_path.parent.mkdir(parents=True, exist_ok=True)
        demand_data.to_parquet(file_path, index=False)

    solar_data_to_write = [
        (date_time_2025_2, solar_profile * days_2025, "Q6", "2025-2"),
        (date_time_2026_1, solar_profile * days_2026, "Q6", "2026-1"),
    ]
    for date_time, solar, area, half_year in solar_data_to_write:
        solar_data = pd.DataFrame({"Datetime": date_time, "Value": solar})
        solar_data["Datetime"] = pd.to_datetime(solar_data["Datetime"])
        file_meta_data = {
            "name": area,
            "file_type": "area",
            "technology": "SAT",
            "reference_year": 2018,
            "hy": half_year,
        }
        file_path = Path(
            traces_dir / "solar" / write_output_solar_filepath(file_meta_data)
        )
        file_path.parent.mkdir(parents=True, exist_ok=True)
        solar_data.to_parquet(file_path, index=False)

    wind_data_to_write = [
        (date_time_2025_2, wind_medium_profile * days_2025, "Q6", "WM", "2025-2"),
        (date_time_2026_1, wind_medium_profile * days_2026, "Q6", "WM", "2026-1"),
        (date_time_2025_2, wind_high_profile * days_2025, "Q6", "WH", "2025-2"),
        (date_time_2026_1, wind_high_profile * days_2026, "Q6", "WH", "2026-1"),
    ]
    for date_time, wind, area, wind_type, half_year in wind_data_to_write:
        wind_data = pd.DataFrame({"Datetime": date_time, "Value": wind})
        wind_data["Datetime"] = pd.to_datetime(wind_data["Datetime"])
        file_meta_data = {
            "name": area,
            "file_type": "area",
            "resource_quality": wind_type,
            "reference_year": 2018,
            "hy": half_year,
        }
        file_path = Path(
            traces_dir / "wind" / write_output_wind_area_filepath(file_meta_data)
        )
        file_path.parent.mkdir(parents=True, exist_ok=True)
        wind_data.to_parquet(file_path, index=False)

    # Define ISPyPSA input tables

    # Sub-regions table
    sub_regions_csv = """
    isp_sub_region_id, nem_region_id, sub_region_reference_node, sub_region_reference_node_voltage_kv
    CQ,                CQ,            CQ Reference Node,         500
    """
    sub_regions = csv_str_to_df(sub_regions_csv)

    # Flow paths table - set flow path capacity above peak demand
    flow_paths_csv = """
    flow_path,   node_from, node_to, carrier, forward_direction_mw_summer_typical, reverse_direction_mw_summer_typical
    CQ-Q6,       CQ,        Q6,      AC,      250,                                 250
    """
    flow_paths = csv_str_to_df(flow_paths_csv)

    # Renewable energy zones table
    renewable_energy_zones_csv = """
    rez_id,     isp_sub_region_id,  carrier,  wind_generation_total_limits_mw_high,  wind_generation_total_limits_mw_medium,  wind_generation_total_limits_mw_offshore_floating,   wind_generation_total_limits_mw_offshore_fixed,  solar_pv_plus_solar_thermal_limits_mw_solar,  rez_resource_limit_violation_penalty_factor_$/mw,  land_use_limits_mw_wind,     land_use_limits_mw_solar,     rez_transmission_network_limit_summer_typical
    Q6,         CQ,                 AC,       20,                                    30,                                      0,                                                   0,                                               50,                                           10000,                                             100,                         100,                          250
    """
    renewable_energy_zones = csv_str_to_df(renewable_energy_zones_csv)

    # New entrant generators table
    new_entrant_generators_csv = """
    generator_name,           technology_type,              sub_region,     fuel_type,  fuel_cost_mapping,  minimum_stable_level_%,  vom_$/mwh_sent_out,  heat_rate_gj/mwh,  maximum_capacity_mw,  unit_capacity_mw,  lifetime,  connection_cost_technology,  connection_cost_rez/_region_id,  fom_$/kw/annum,  technology_specific_lcf_%,  isp_resource_type,  rez_id,   generator
    Large__scale__Solar__PV,  Large__scale__Solar__PV,      CQ,             Solar,      Solar,              0,                       0,                   0,                 ,                     ,                  30,        Large__scale__Solar__PV,     Fitzroy,                         0.0,             100.0,                      SAT,                Q6,       large_scale_solar_pv_q6_sat
    Wind,                     Wind,                         CQ,             Wind,       Wind,               0,                       0,                   0,                 ,                     ,                  30,        Wind,                        Fitzroy,                         0.0,             100.0,                      WM,                 Q6,       wind_q6_wm
    Wind,                     Wind,                         CQ,             Wind,       Wind,               0,                       0,                   0,                 ,                     ,                  30,        Wind,                        Fitzroy,                         0.0,             100.0,                      WH,                 Q6,       wind_q6_wh
    """
    new_entrant_generators = csv_str_to_df(new_entrant_generators_csv)

    # Additional tables needed for new entrant generators (set to non-zero values to avoid dropping generators)
    new_entrant_build_costs_csv = """
    technology,                 2024_25_$/mw,   2025_26_$/mw
    Large__scale__Solar__PV,    1600000,        1500000
    Wind,                       2800000,        2700000
    """
    new_entrant_build_costs = csv_str_to_df(new_entrant_build_costs_csv)

    new_entrant_wind_and_solar_connection_costs_csv = """
    REZ__names,          2024_25_$/mw,  2025_26_$/mw,  system_strength_connection_cost_$/mw
    Fitzroy,             120000,        120000,        137000
    """
    new_entrant_wind_and_solar_connection_costs = csv_str_to_df(
        new_entrant_wind_and_solar_connection_costs_csv
    )

    # Collect all ISPyPSA tables
    ispypsa_tables = {
        "sub_regions": sub_regions,
        "flow_paths": flow_paths,
        "rez_transmission_expansion_costs": pd.DataFrame(),
        "flow_path_expansion_costs": pd.DataFrame(),
        "ecaa_generators": pd.DataFrame(),
        "new_entrant_generators": new_entrant_generators,
        "renewable_energy_zones": renewable_energy_zones,
        "new_entrant_build_costs": new_entrant_build_costs,
        "new_entrant_wind_and_solar_connection_costs": new_entrant_wind_and_solar_connection_costs,
    }

    # Create a ModelConfig instance
    config = ModelConfig(**config_dict)

    # Translate ISPyPSA tables to PyPSA-friendly format
    pypsa_tables = create_pypsa_friendly_inputs(config, ispypsa_tables)

    # Manually set the marginal and capital costs for the generators:
    generators = pypsa_tables["generators"].copy()

    gen_costs = {
        "large_scale_solar_pv_q6_sat_2026": {
            "marginal_cost": 20.0,
            "capital_cost": 100000.0,
        },
        "wind_q6_wm_2026": {"marginal_cost": 25.0, "capital_cost": 120000.0},
        "wind_q6_wh_2026": {"marginal_cost": 100.0, "capital_cost": 400000.0},
    }
    for gen, costs in gen_costs.items():
        generators.loc[generators["name"] == gen, "marginal_cost"] = costs[
            "marginal_cost"
        ]
        generators.loc[generators["name"] == gen, "capital_cost"] = costs[
            "capital_cost"
        ]

    # and set the USE generator marginal costs:
    for unserved_energy_gen in [gen for gen in generators["name"] if "unserved" in gen]:
        generators.loc[generators["name"] == unserved_energy_gen, "marginal_cost"] = (
            config_dict["unserved_energy"]["cost"]
        )

    pypsa_tables["generators"] = generators

    # Manually create a short hardcoded snapshots so the model works with our short
    # time series data.
    snapshots = pd.DataFrame(
        {
            "investment_periods": [2026, 2026, 2026, 2026],
            "snapshots": [
                "2026-05-01 00:00:00",
                "2026-05-01 06:00:00",
                "2026-05-01 12:00:00",
                "2026-05-01 18:00:00",
            ],
        }
    )
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Override the longer snapshots that would have auto generated.
    pypsa_tables["snapshots"] = snapshots

    # Create timeseries data directory structure for PyPSA inputs
    pypsa_timeseries_dir = pypsa_dir / "timeseries"
    pypsa_timeseries_dir.mkdir(parents=True)

    # Create demand traces for the network model
    create_pypsa_friendly_timeseries_inputs(
        config=config,
        model_phase="capacity_expansion",
        ispypsa_tables=ispypsa_tables,
        snapshots=snapshots,
        generators=generators,
        parsed_traces_directory=traces_dir,
        pypsa_friendly_timeseries_inputs_location=pypsa_timeseries_dir,
    )

    # Build the network model
    network = build_pypsa_network(
        pypsa_friendly_tables=pypsa_tables,
        path_to_pypsa_friendly_timeseries_data=pypsa_timeseries_dir,
    )

    # Solve the optimization problem
    network.optimize.solve_model(
        solver_name=config.solver,
    )

    # Check that nominal generator capacities are as expected - relationally!
    generators = (
        network.generators.reset_index()
        .loc[:, ["Generator", "p_nom_opt"]]
        .set_index("Generator")
    )
    solar_capacity = generators.at["large_scale_solar_pv_q6_sat_2026", "p_nom_opt"]
    wind_medium_capacity = generators.at["wind_q6_wm_2026", "p_nom_opt"]
    wind_high_capacity = generators.at["wind_q6_wh_2026", "p_nom_opt"]

    # dummy gen (relaxation) capacities:
    relax_solar_capacity = generators.at[
        "Q6_Solar_resource_limit_relax_2026", "p_nom_opt"
    ]
    relax_wind_medium_capacity = generators.at[
        "Q6_WM_resource_limit_relax_2026", "p_nom_opt"
    ]
    relax_wind_high_capacity = generators.at[
        "Q6_WH_resource_limit_relax_2026", "p_nom_opt"
    ]

    # Check that if resource limit constraints have been relaxed, the correct capacity
    # for relaxing generators has been built:
    if wind_medium_capacity > 30.0:
        assert wind_medium_capacity - relax_wind_medium_capacity == 30.0
    if wind_high_capacity > 20.0:
        assert wind_high_capacity - relax_wind_high_capacity == 20.0
    if solar_capacity > 50.0:
        assert solar_capacity - relax_solar_capacity == 50.0

    # And check that if constraints haven't been relaxed, the gen capacities don't
    # break resource limits:
    if relax_solar_capacity == 0:
        assert solar_capacity <= 50.0
    if relax_wind_medium_capacity == 0:
        assert wind_medium_capacity <= 30.0
    if relax_wind_high_capacity == 0:
        assert wind_high_capacity <= 20.0

    # FOR THE CURRENT ABOVE INPUTS -> THIS IS EXPECTED OUTPUT
    # expected_generators_csv = """
    # Generator,                              p_nom_opt
    # large_scale_solar_pv_q6_sat_2026,       100.0
    # wind_q6_wm_2026,                        80.0
    # wind_q6_wh_2026,                        20.0
    # Q6_Solar_resource_limit_relax_2026,     50.0
    # Q6_WM_resource_limit_relax_2026,        50.0
    # Q6_WH_resource_limit_relax_2026,        0.0
    # unserved_energy_CQ,                     500.0
    # """
    # expected_generators = csv_str_to_df(expected_generators_csv)
    # expected_generators = expected_generators.set_index("Generator")
    # pd.testing.assert_frame_equal(
    #     generators.sort_index(), expected_generators.sort_index()
    # )
