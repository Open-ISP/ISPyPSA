from pathlib import Path

import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.pypsa_build.build import build_pypsa_network
from ispypsa.translator.create_pypsa_friendly import (
    create_pypsa_friendly_inputs,
    create_pypsa_friendly_timeseries_inputs,
)


def test_flow_path_expansion_limit_respected(csv_str_to_df, tmp_path, monkeypatch):
    """Test that link expansion spanning multiple investment periods respects
    total limit for flow path.

    This test creates a simple two-region network (A and B) where:
    - Region A has an expensive generator and a demand of 100 MW and 200 MW in the two
      investment periods.
    - Region B has a cheap generator and no demand
    - The existing transmission link can only carry 50 MW (half the demand)
    - Link expansion costs are cheap in both years
    - the total allowable expansion for the flow path is 100 MW

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
    for subdir in ["demand", "zone", "project"]:
        (traces_dir / subdir).mkdir()

    # Mock environment variable for trace parser
    monkeypatch.setenv("PATH_TO_PARSED_TRACES", str(traces_dir))

    # Create a mock config
    config_dict = {
        "paths": {
            "ispypsa_run_name": "test",
            "parsed_traces_directory": "ENV",
            "parsed_workbook_cache": "",
            "workbook_path": "",
            "run_directory": "",
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
                "rezs": "attached_to_parent_node",
            },
            "rez_to_sub_region_transmission_default_limit": 1e5,
        },
        "temporal": {
            "year_type": "fy",
            "range": {
                "start_year": 2025,
                "end_year": 2026,
            },
            "capacity_expansion": {
                "resolution_min": 30,
                "reference_year_cycle": [2018],
                "investment_periods": [2025, 2026],
                "aggregation": {
                    "representative_weeks": None,  # Use full year
                },
            },
        },
        "unserved_energy": {
            "cost": 10000.0,
            "max_per_node": 1000.0,
        },
        "solver": "highs",
        "iasr_workbook_version": "6.0",
    }

    # Create demand trace data for regions A and B
    # Region A has demand of 100 MW in 2025 and 200 MW in 2026, Region B has no demand
    demand_csv = """
    datetime,              value,  subregion,  reference_year,  poe,    scenario,     demand_type
    2024-08-01 00:00:00,   0.0,    A,          2018,            POE50,  Step Change,  OPSO_MODELLING
    2025-05-01 00:00:00,   100.0,  A,          2018,            POE50,  Step Change,  OPSO_MODELLING
    2025-08-01 00:00:00,   0.0,    A,          2018,            POE50,  Step Change,  OPSO_MODELLING
    2026-05-01 00:00:00,   200.0,  A,          2018,            POE50,  Step Change,  OPSO_MODELLING
    2024-08-01 00:00:00,   0.0,    B,          2018,            POE50,  Step Change,  OPSO_MODELLING
    2025-05-01 00:00:00,   0.0,    B,          2018,            POE50,  Step Change,  OPSO_MODELLING
    2025-08-01 00:00:00,   0.0,    B,          2018,            POE50,  Step Change,  OPSO_MODELLING
    2026-05-01 00:00:00,   0.0,    B,          2018,            POE50,  Step Change,  OPSO_MODELLING
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["datetime"] = pd.to_datetime(demand_data["datetime"])
    demand_data.to_parquet(traces_dir / "demand" / "demand_data.parquet", index=False)

    # Define ISPyPSA input tables

    # Sub-regions table
    sub_regions_csv = """
    isp_sub_region_id, nem_region_id, sub_region_reference_node, sub_region_reference_node_voltage_kv
    A,                 A,             A Reference Node,          500
    B,                 B,             B Reference Node,          500
    """
    sub_regions = csv_str_to_df(sub_regions_csv)

    # Flow paths table
    flow_paths_csv = """
    flow_path,   node_from, node_to, carrier, forward_direction_mw_summer_typical, reverse_direction_mw_summer_typical
    A-B,         A,         B,       AC,      50,                                  50
    """
    flow_paths = csv_str_to_df(flow_paths_csv)

    # Flow path expansion costs table
    flow_path_expansion_costs_csv = """
    flow_path, option, additional_network_capacity_mw, 2024_25_$/mw, 2025_26_$/mw
    A-B,       Opt1,   100,                            1.0,          1.0
    """
    flow_path_expansion_costs = csv_str_to_df(flow_path_expansion_costs_csv)

    # ECAA Generators table (existing generators)
    # At the moment Brown Coal cost is set to 30 $/MWh and Liquid Fuel to
    # 400 $/MWh. "__" gets converted to a space.
    ecaa_generators_csv = """
    generator,               fuel_type,     sub_region_id, maximum_capacity_mw,     fuel_cost_mapping,      minimum_load_mw,    vom_$/mwh_sent_out,  heat_rate_gj/mwh,      commissioning_date,     closure_year,     rez_id,     technology_type
    expensive_generator_A,   Liquid__Fuel,  A,             200,                     Liquid__Fuel,           0,                  0.0,                 0.0,                   NaN,                    2050,             NaN,        OCGT
    cheap_generator_B,       Brown__Coal,   B,             200,                     cheap_generator_B,      0,                  0.0,                 0.0,                   NaN,                    2050,             NaN,        Steam__Sub__Critical
    """
    ecaa_generators = csv_str_to_df(ecaa_generators_csv)

    # Minimal versions of other required tables

    # Collect all ISPyPSA tables
    ispypsa_tables = {
        "sub_regions": sub_regions,
        "flow_paths": flow_paths,
        "flow_path_expansion_costs": flow_path_expansion_costs,
        "ecaa_generators": ecaa_generators,
        "new_entrant_generators": pd.DataFrame(),
        "renewable_energy_zones": pd.DataFrame(),
        "ecaa_batteries": pd.DataFrame(),
        "new_entrant_batteries": pd.DataFrame(),
    }

    # Create a ModelConfig instance
    config = ModelConfig(**config_dict)

    # Translate ISPyPSA tables to PyPSA-friendly format
    pypsa_tables = create_pypsa_friendly_inputs(config, ispypsa_tables)

    # Manually set the marginal costs for the generators:
    generators = pypsa_tables["generators"].copy()
    generators.loc[generators["name"] == "expensive_generator_A", "marginal_cost"] = (
        400.0
    )
    generators.loc[generators["name"] == "cheap_generator_B", "marginal_cost"] = 30.0
    for unserved_energy_gen in ["unserved_energy_A", "unserved_energy_B"]:
        generators.loc[generators["name"] == unserved_energy_gen, "marginal_cost"] = (
            config_dict["unserved_energy"]["cost"]
        )
    pypsa_tables["generators"] = generators

    # Manually create a short hardcoded snapshots so the model works with our short
    # time series data.
    snapshots = pd.DataFrame(
        {
            "investment_periods": [2025, 2026],
            "snapshots": ["2025-05-01 00:00:00", "2026-05-01 00:00:00"],
        }
    )
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create timeseries data directory structure for PyPSA inputs
    pypsa_timeseries_dir = pypsa_dir / "timeseries"
    pypsa_timeseries_dir.mkdir(parents=True)

    # Create demand traces for the network model with custom snapshots
    returned_snapshots = create_pypsa_friendly_timeseries_inputs(
        config=config,
        model_phase="capacity_expansion",
        ispypsa_tables=ispypsa_tables,
        generators=generators,
        parsed_traces_directory=traces_dir,
        pypsa_friendly_timeseries_inputs_location=pypsa_timeseries_dir,
        snapshots=snapshots,
    )

    # Add snapshots to pypsa_tables for network building
    pypsa_tables["snapshots"] = returned_snapshots

    # Build the network model
    network = build_pypsa_network(
        pypsa_friendly_tables=pypsa_tables,
        path_to_pypsa_friendly_timeseries_data=pypsa_timeseries_dir,
    )

    # Solve the optimization problem
    network.optimize.solve_model(
        solver_name=config.solver,
    )

    # Check that nominal link capacities are as expected.
    links = network.links.reset_index().loc[:, ["Link", "p_nom_opt"]]
    expected_links = """
    Link,          p_nom_opt
    A-B_existing,       50.0
    A-B_exp_2025,       50.0
    A-B_exp_2026,       50.0
    """
    expected_links = csv_str_to_df(expected_links)
    pd.testing.assert_frame_equal(links, expected_links)

    # Check that link dispatch is as expected. In particular that A-B_exp_2026 is only
    # dispatch in the second time interval.
    links_t = network.links_t.p0.reset_index(drop=True)
    links_t.columns.name = None
    expected_links_t = """
    A-B_existing,     A-B_exp_2025,   A-B_exp_2026
    -50.0,            -50.0,          0.0
    -50.0,            -50.0,          -50.0
    """
    expected_links_t = csv_str_to_df(expected_links_t)
    pd.testing.assert_frame_equal(links_t, expected_links_t)
