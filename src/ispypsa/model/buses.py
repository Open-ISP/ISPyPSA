from pathlib import Path

import pandas as pd
import pypsa


def _add_bus_to_network(
    bus_name: str, network: pypsa.Network, path_to_demand_traces: Path
) -> None:
    """
    Adds a Bus to the network and if a demand trace for the Bus exists, also adds the
    trace to a Load attached to the Bus.

    Args:
        bus_name: String defining the bus name
        network: The `pypsa.Network` object
        path_to_demand_traces: `pathlib.Path` that points to the
            directory containing demand traces

    Returns: None
    """
    network.add(class_name="Bus", name=bus_name)

    demand_trace_path = path_to_demand_traces / Path(f"{bus_name}.parquet")
    if demand_trace_path.exists():
        demand = pd.read_parquet(demand_trace_path)
        demand = demand.set_index(["investment_periods", "snapshots"])
        network.add(
            class_name="Load",
            name=f"load_{bus_name}",
            bus=bus_name,
            p_set=demand["p_set"],
        )


def _add_buses_to_network(
    network: pypsa.Network, buses: pd.DataFrame, path_to_timeseries_data: Path
) -> None:
    """Adds buses and demand traces to the `pypsa.Network`.

    Args:
        network: The `pypsa.Network` object
        buses: `pd.DataFrame` with `PyPSA` style `Bus` attributes.
        path_to_timeseries_data: `pathlib.Path` that points to the directory containing
            timeseries data

    Returns: None
    """
    path_to_demand_traces = path_to_timeseries_data / Path("demand_traces")
    buses["name"].apply(
        lambda x: _add_bus_to_network(x, network, path_to_demand_traces)
    )


def _add_bus_for_custom_constraints(network: pypsa.Network) -> None:
    """Adds a bus called bus_for_custom_constraint_gens for generators being used to model constraint violation to
    the network.

    Args:
        network: The `pypsa.Network` object

    Returns: None
    """
    network.add(class_name="Bus", name="bus_for_custom_constraint_gens")


def _update_bus_demand_timeseries(
    bus_name: str, network: pypsa.Network, path_to_demand_traces: Path
) -> None:
    """
    Update a Bus's demand timeseries data in the pypsa.Network.

    The function is used to set up the model for operational modelling following
    capacity expansion optimisation. Once the model snapshots are updated then the
    demand timeseries also need to be updated to match.

    Args:
        bus_name: String defining the bus name
        network: The `pypsa.Network` object
        path_to_demand_traces: `pathlib.Path` that points to the
            directory containing demand traces

    Returns: None
    """

    demand_trace_path = path_to_demand_traces / Path(f"{bus_name}.parquet")
    if demand_trace_path.exists():
        demand = pd.read_parquet(demand_trace_path)
        demand = demand.set_index(["investment_periods", "snapshots"])
        network.loads_t.p_set[f"load_{bus_name}"] = demand.loc[:, ["p_set"]]


def _update_buses_demand_timeseries(
    network: pypsa.Network, buses: pd.DataFrame, path_to_timeseries_data: Path
) -> None:
    """Update buses a demand timeseries in the `pypsa.Network`.

    Args:
        network: The `pypsa.Network` object
        buses: `pd.DataFrame` with `PyPSA` style `Bus` attributes.
        path_to_timeseries_data: `pathlib.Path` that points to the directory containing
            timeseries data

    Returns: None
    """
    path_to_demand_traces = path_to_timeseries_data / Path("demand_traces")
    buses["name"].apply(
        lambda x: _update_bus_demand_timeseries(x, network, path_to_demand_traces)
    )
