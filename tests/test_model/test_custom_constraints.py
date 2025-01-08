from datetime import datetime
from pathlib import Path

import pandas as pd

from ispypsa.model import (
    add_buses_for_custom_constraints,
    add_buses_to_network,
    add_carriers_to_network,
    add_custom_constraint_generators_to_network,
    add_custom_constraints,
    add_ecaa_generators_to_network,
    add_lines_to_network,
    initialise_network,
    run,
    save_results,
)


def test_custom_constraints():
    start_date = datetime(year=2025, month=1, day=1, hour=0, minute=0)
    end_date = datetime(year=2025, month=1, day=2, hour=0, minute=0)

    time_index = pd.date_range(
        start=start_date,
        end=end_date,
        freq="30min",
    )

    time_index = pd.DataFrame(index=time_index)
    pypsa_inputs_location = Path("test_pypsa_friendly_inputs/test_custom_constraints")
    time_index.to_csv(pypsa_inputs_location / Path("snapshot.csv"))

    demand_data = time_index.copy()
    demand_data = demand_data.reset_index(names="Datetime")
    demand_data["Value"] = 1000.0
    demand_data.to_parquet(
        pypsa_inputs_location / Path("demand_traces/bus_two.parquet")
    )

    network = initialise_network(pypsa_inputs_location)
    add_carriers_to_network(network, pypsa_inputs_location)
    add_buses_to_network(network, pypsa_inputs_location)
    add_buses_for_custom_constraints(network)
    add_lines_to_network(network, pypsa_inputs_location)
    add_custom_constraint_generators_to_network(network, pypsa_inputs_location)
    add_ecaa_generators_to_network(network, pypsa_inputs_location)
    network.optimize.create_model()
    add_custom_constraints(network, pypsa_inputs_location)
    run(network, solver_name="highs")

    assert network.generators.loc["con_one-EXPANSION", "p_nom_opt"] == 1500.0
