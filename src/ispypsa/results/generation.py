"""Extract generation/dispatch results from PyPSA network."""

import pandas as pd
import pypsa


def extract_generator_dispatch(network: pypsa.Network) -> pd.DataFrame:
    """Extract generator dispatch data from PyPSA network.

    Args:
        network: PyPSA network with solved optimization results

    Returns:
        DataFrame with columns:
            - generator_name: Name of the generator
            - bus: Bus/sub-region where generator is located
            - carrier: Technology type (Solar, Wind, Gas, etc.)
            - period: Investment period/year
            - timestep: Datetime of dispatch
            - dispatch_mw: Power output in MW
    """
    # Get generator static data
    generators = network.generators[["bus", "carrier"]].copy()
    generators = generators[generators["bus"] != "bus_for_custom_constraint_gens"]

    # Get dispatch time series
    dispatch_t = network.generators_t.p.copy()

    # Reshape dispatch data from wide to long format
    dispatch_long = dispatch_t.stack().reset_index()
    dispatch_long.columns = ["period", "timestep", "generator_name", "dispatch_mw"]

    # Merge with generator static data
    dispatch_long = dispatch_long.merge(
        generators, left_on="generator_name", right_index=True, how="left"
    )

    dispatch_long = dispatch_long.rename(
        columns={
            "generator_name": "generator",
            "bus": "node",
            "period": "investment_period",
        }
    )

    # Reorder columns
    dispatch_long = dispatch_long[
        ["generator", "node", "carrier", "investment_period", "timestep", "dispatch_mw"]
    ]

    return dispatch_long


def extract_demand(network: pypsa.Network) -> pd.DataFrame:
    """Extract demand/load data from PyPSA network.

    Args:
        network: PyPSA network with solved optimization results

    Returns:
        DataFrame with columns:
            - load: Name of the load
            - node: Bus/sub-region where load is located
            - investment_period: Investment period/year
            - timestep: Datetime of load
            - demand_mw: Demand in MW
    """
    # Get load static data
    loads = network.loads[["bus"]].copy()

    # Get demand time series
    demand_t = network.loads_t.p_set.copy()

    # Reshape demand data from wide to long format
    demand_long = demand_t.stack().reset_index()
    demand_long.columns = ["period", "timestep", "load_name", "demand_mw"]

    # Merge with load static data
    demand_long = demand_long.merge(
        loads, left_on="load_name", right_index=True, how="left"
    )

    # Rename columns to match dispatch naming convention
    demand_long = demand_long.rename(
        columns={
            "bus": "node",
            "period": "investment_period",
        }
    )

    # Reorder columns
    demand_long = demand_long.loc[
        :, ["node", "investment_period", "timestep", "demand_mw"]
    ]

    return demand_long
