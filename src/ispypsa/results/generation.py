"""Extract generation/dispatch results from PyPSA network."""

import pandas as pd
import pypsa


def extract_generator_dispatch(network: pypsa.Network) -> pd.DataFrame:
    """Extract generator and storage dispatch data from PyPSA network.

    Combines generator and storage unit dispatch into a single DataFrame.
    For storage units, positive dispatch_mw means discharging (generating power),
    negative means charging (consuming power).

    Args:
        network: PyPSA network with solved optimization results

    Returns:
        DataFrame with columns:
            - generator: Name of the generator or storage unit
            - node: Bus/sub-region where generator/storage is located
            - fuel_type: Technology type (Solar, Wind, Gas, Battery, etc.)
            - technology_type: ISP technology classification
            - investment_period: Investment period/year
            - timestep: Datetime of dispatch
            - dispatch_mw: Power output in MW (negative for storage charging)
    """
    # Get generator static data
    generators = network.generators[["bus", "carrier", "isp_technology_type"]].copy()
    generators = generators[generators["bus"] != "bus_for_custom_constraint_gens"]

    # Get dispatch time series
    dispatch_t = network.generators_t.p.copy()

    # Reshape dispatch data from wide to long format
    dispatch_long = dispatch_t.stack().reset_index()
    dispatch_long.columns = ["period", "timestep", "generator_name", "dispatch_mw"]

    # Merge with generator static data
    dispatch_long = dispatch_long.merge(
        generators, left_on="generator_name", right_index=True, how="inner"
    )

    dispatch_long = dispatch_long.rename(
        columns={
            "generator_name": "generator",
            "bus": "node",
            "carrier": "fuel_type",
            "isp_technology_type": "technology_type",
            "period": "investment_period",
        }
    )

    # Extract storage dispatch and combine
    storage_dispatch = _extract_storage_dispatch(network)

    # Reorder columns
    cols = [
        "generator",
        "node",
        "fuel_type",
        "technology_type",
        "investment_period",
        "timestep",
        "dispatch_mw",
    ]
    dispatch_long = dispatch_long[cols]

    if not storage_dispatch.empty:
        results = pd.concat([dispatch_long, storage_dispatch[cols]], ignore_index=True)
    else:
        results = dispatch_long

    return results


def _extract_storage_dispatch(network: pypsa.Network) -> pd.DataFrame:
    """Extract storage unit dispatch data from PyPSA network.

    Args:
        network: PyPSA network with solved optimization results

    Returns:
        DataFrame with columns matching generator dispatch:
            - generator: Name of the storage unit
            - node: Bus/sub-region where storage is located
            - fuel_type: Carrier (typically "Battery")
            - technology_type: Set to "Battery Storage"
            - investment_period: Investment period/year
            - timestep: Datetime of dispatch
            - dispatch_mw: Power in MW (positive = discharging, negative = charging)
    """
    if network.storage_units.empty or network.storage_units_t.p.empty:
        return pd.DataFrame()

    # Get storage static data
    storage_units = network.storage_units[["bus", "carrier"]].copy()

    # Get dispatch time series
    storage_dispatch_t = network.storage_units_t.p.copy()

    # Reshape dispatch data from wide to long format
    storage_dispatch_long = storage_dispatch_t.stack().reset_index()
    storage_dispatch_long.columns = [
        "period",
        "timestep",
        "storage_unit_name",
        "dispatch_mw",
    ]

    # Merge with storage static data
    storage_dispatch_long = storage_dispatch_long.merge(
        storage_units, left_on="storage_unit_name", right_index=True, how="inner"
    )

    # Rename columns to match generator format
    storage_dispatch_long = storage_dispatch_long.rename(
        columns={
            "storage_unit_name": "generator",
            "bus": "node",
            "carrier": "fuel_type",
            "period": "investment_period",
        }
    )

    # Set technology_type to "Battery Storage"
    storage_dispatch_long["technology_type"] = "Battery Storage"

    return storage_dispatch_long


def extract_generation_expansion_results(network: pypsa.Network) -> pd.DataFrame:
    """Extract generation and storage expansion results from PyPSA network.

    Combines generator and storage unit capacity data into a single DataFrame.

    Args:
        network: PyPSA network with solved optimization results

    Returns:
        DataFrame with columns:
            - generator: Name of the generator or storage unit
            - fuel_type: Fuel type (carrier)
            - technology_type: ISP technology classification
            - node: Bus/sub-region where generator/storage is located
            - capacity_mw: Optimized capacity in MW
            - investment_period: Build year
            - closure_year: Year when generator/storage closes (build_year + lifetime)
    """
    # Extract generator results
    generator_results = network.generators.copy()

    # Filter out constraint dummy generators
    generator_results = generator_results[
        generator_results["bus"] != "bus_for_custom_constraint_gens"
    ]

    # Calculate closure_year
    generator_results["closure_year"] = (
        generator_results["build_year"] + generator_results["lifetime"]
    )

    # Rename columns
    generator_results = generator_results.rename(
        columns={
            "carrier": "fuel_type",
            "isp_technology_type": "technology_type",
            "bus": "node",
            "p_nom_opt": "capacity_mw",
            "build_year": "investment_period",
        }
    )

    # Reset index to get generator name as column
    generator_results = generator_results.reset_index().rename(
        columns={"Generator": "generator"}
    )

    # Extract storage unit results
    storage_results = _extract_storage_expansion_results(network)

    # Combine generator and storage results
    cols = [
        "generator",
        "fuel_type",
        "technology_type",
        "node",
        "capacity_mw",
        "investment_period",
        "closure_year",
    ]

    generator_results = generator_results[cols]

    if not storage_results.empty:
        results = pd.concat(
            [generator_results, storage_results[cols]], ignore_index=True
        )
    else:
        results = generator_results

    return results


def _extract_storage_expansion_results(network: pypsa.Network) -> pd.DataFrame:
    """Extract storage unit expansion results from PyPSA network.

    Args:
        network: PyPSA network with solved optimization results

    Returns:
        DataFrame with columns matching generator expansion results:
            - generator: Name of the storage unit
            - fuel_type: Fuel type (carrier - typically "Battery")
            - technology_type: Set to "Battery Storage"
            - node: Bus/sub-region where storage is located
            - capacity_mw: Optimized capacity in MW (p_nom_opt)
            - investment_period: Build year
            - closure_year: Year when storage closes (build_year + lifetime)
    """
    if network.storage_units.empty:
        return pd.DataFrame()

    storage_results = network.storage_units.copy()

    # Calculate closure_year
    storage_results["closure_year"] = (
        storage_results["build_year"] + storage_results["lifetime"]
    )

    # Rename columns to match generator format
    storage_results = storage_results.rename(
        columns={
            "carrier": "fuel_type",
            "bus": "node",
            "p_nom_opt": "capacity_mw",
            "build_year": "investment_period",
        }
    )

    # Set technology_type to "Battery Storage" since storage units don't have isp_technology_type
    storage_results["technology_type"] = "Battery Storage"

    # Reset index to get storage unit name as column (named "generator" for consistency)
    storage_results = storage_results.reset_index().rename(
        columns={"StorageUnit": "generator"}
    )

    return storage_results


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
