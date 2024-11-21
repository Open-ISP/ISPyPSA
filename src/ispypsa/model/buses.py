from pathlib import Path

import pandas as pd
import pypsa


def _add_bus_to_network(
    bus_name: str, network: pypsa.Network, path_to_demand_traces: Path
):
    """
    Adds a Bus to the network and if a demand trace for the Bus exists also adds that as a Load attached to the Bus.

    Args:
        bus_name: str defining the buses name
        network: The pypsa.Network object to add the buses to
        path_to_demand_traces: pathlib.Path for the directory containing demand traces

    Returns: None
    """
    network.add(class_name="Bus", name=bus_name)

    demand_trace_path = path_to_demand_traces / Path(f"{bus_name}.parquet")
    if demand_trace_path.exists():
        demand = pd.read_parquet(demand_trace_path)
        demand["Datetime"] = demand["Datetime"].astype("datetime64[ns]")
        demand = demand.set_index("Datetime")
        network.add(
            class_name="Load",
            name=f"load_{bus_name}",
            bus=bus_name,
            p_set=demand["Value"],
        )


def add_buses_to_network(network: pypsa.Network, path_pypsa_inputs: Path):
    """Adds buses from buses.csv in the path_pypsa_inputs directory to the pypsa.Network.

    Args:
         network: The pypsa.Network object
         path_pypsa_inputs: pathlib.Path for directory containing pypsa inputs

    Returns: None
    """
    buses = pd.read_csv(path_pypsa_inputs / Path("buses.csv"))
    path_to_demand_traces = path_pypsa_inputs / Path("demand_traces")
    buses["name"].apply(
        lambda x: _add_bus_to_network(x, network, path_to_demand_traces)
    )
