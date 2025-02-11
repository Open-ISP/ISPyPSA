from pathlib import Path

import pandas as pd

from ispypsa.model.buses import _add_buses_for_custom_constraints, _add_buses_to_network
from ispypsa.model.carriers import _add_carriers_to_network
from ispypsa.model.custom_constraints import _add_custom_constraints
from ispypsa.model.generators import (
    _add_custom_constraint_generators_to_network,
    _add_generators_to_network,
)
from ispypsa.model.initialise import _initialise_network
from ispypsa.model.lines import _add_lines_to_network


def build_pypsa_network(
    pypsa_friendly_tables: pd.DataFrame,
    path_to_pypsa_friendly_timeseries_data: Path,
):
    network = _initialise_network(pypsa_friendly_tables["snapshots"])

    _add_carriers_to_network(network, pypsa_friendly_tables["generators"])

    _add_buses_to_network(
        network, pypsa_friendly_tables["buses"], path_to_pypsa_friendly_timeseries_data
    )

    _add_buses_for_custom_constraints(network)

    _add_lines_to_network(network, pypsa_friendly_tables["lines"])

    _add_custom_constraint_generators_to_network(
        network, pypsa_friendly_tables["custom_constraints_generators"]
    )

    _add_generators_to_network(
        network,
        pypsa_friendly_tables["generators"],
        path_to_pypsa_friendly_timeseries_data,
    )

    network.optimize.create_model()

    _add_custom_constraints(
        network,
        pypsa_friendly_tables["custom_constraints_rhs"],
        pypsa_friendly_tables["custom_constraints_lhs"],
    )

    return network
