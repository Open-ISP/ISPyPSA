from pathlib import Path

import pandas as pd

from ispypsa.model import (
    add_buses_for_custom_constraints,
    add_buses_to_network,
    add_carriers_to_network,
    add_custom_constraint_generators_to_network,
    add_custom_constraints,
    add_generators_to_network,
    add_lines_to_network,
    initialise_network,
)


def build_pypsa_network(
    pypsa_friendly_tables: pd.DataFrame,
    path_to_pypsa_friendly_timeseries_data: Path,
):
    network = initialise_network(pypsa_friendly_tables["snapshots"])

    add_carriers_to_network(network, pypsa_friendly_tables["generators"])

    add_buses_to_network(
        network, pypsa_friendly_tables["buses"], path_to_pypsa_friendly_timeseries_data
    )

    add_buses_for_custom_constraints(network)

    add_lines_to_network(network, pypsa_friendly_tables["lines"])

    add_custom_constraint_generators_to_network(
        network, pypsa_friendly_tables["custom_constraints_generators"]
    )

    add_generators_to_network(
        network,
        pypsa_friendly_tables["generators"],
        path_to_pypsa_friendly_timeseries_data,
    )

    network.optimize.create_model()

    add_custom_constraints(
        network,
        pypsa_friendly_tables["custom_constraints_rhs"],
        pypsa_friendly_tables["custom_constraints_lhs"],
    )

    return network
