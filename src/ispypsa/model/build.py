from pathlib import Path

import pandas as pd

from ispypsa.model.buses import _add_bus_for_custom_constraints, _add_buses_to_network
from ispypsa.model.carriers import _add_carriers_to_network
from ispypsa.model.custom_constraints import _add_custom_constraints
from ispypsa.model.generators import (
    _add_custom_constraint_generators_to_network,
    _add_generators_to_network,
)
from ispypsa.model.initialise import _initialise_network
from ispypsa.model.investment_period_weights import _add_investment_period_weights
from ispypsa.model.links import _add_links_to_network


def build_pypsa_network(
    pypsa_friendly_tables: dict[str : pd.DataFrame],
    path_to_pypsa_friendly_timeseries_data: Path,
):
    """Creates a `pypsa.Network` based on set of pypsa friendly input tables.

    Examples:

    # Peform required imports.
    >>> from pathlib import Path
    >>> from ispypsa.data_fetch import read_csvs, write_csvs
    >>> from ispypsa.model import build_pypsa_network

    # Read in PyPSA friendly tables from CSV.
    >>> pypsa_input_tables = read_csvs(Path("pypsa_friendly_inputs_directory"))

    >>> pypsa_friendly_inputs = build_pypsa_network(
    ... pypsa_friendly_tables=pypsa_input_tables,
    ... path_to_pypsa_friendly_timeseries_data=Path("pypsa_friendly_timeseries_data")
    ... )

    # Then the model can be run in PyPSA
    >>> network.optimize.solve_model(solver_name="highs")

    # And the results saved to disk.
    >>> network.export_to_hdf5(Path("model_results.hdf5"))

    Args:
        pypsa_friendly_tables: dictionary of dataframes in the `PyPSA` friendly format.
            (add link to pypsa friendly format table docs)
        path_to_pypsa_friendly_timeseries_data: `Path` to `PyPSA` friendly time series
            data (add link to timeseries data docs.

    """
    network = _initialise_network(pypsa_friendly_tables["snapshots"])

    _add_investment_period_weights(
        network, pypsa_friendly_tables["investment_period_weights"]
    )

    _add_carriers_to_network(network, pypsa_friendly_tables["generators"])

    _add_buses_to_network(
        network, pypsa_friendly_tables["buses"], path_to_pypsa_friendly_timeseries_data
    )

    if "links" in pypsa_friendly_tables.keys():
        _add_links_to_network(network, pypsa_friendly_tables["links"])

    _add_generators_to_network(
        network,
        pypsa_friendly_tables["generators"],
        path_to_pypsa_friendly_timeseries_data,
    )

    if "custom_constraints_generators" in pypsa_friendly_tables.keys():
        _add_bus_for_custom_constraints(network)

        _add_custom_constraint_generators_to_network(
            network, pypsa_friendly_tables["custom_constraints_generators"]
        )

    # The underlying linopy model needs to get built so we can add custom constraints.
    network.optimize.create_model(multi_investment_periods=True)

    if "custom_constraints_rhs" in pypsa_friendly_tables:
        _add_custom_constraints(
            network,
            pypsa_friendly_tables["custom_constraints_rhs"],
            pypsa_friendly_tables["custom_constraints_lhs"],
        )

    return network
