from pathlib import Path

import pandas as pd
import pypsa


def initialise_network(path_pypsa_inputs: Path) -> pypsa.Network:
    """Creates a `pypsa.Network object` with snapshots defined.

    Args:
        path_pypsa_inputs: `pathlib.Path` that points to the directory containing
            PyPSA inputs

    Returns:
        `pypsa.Network` object
    """
    time_index = pd.read_csv(
        path_pypsa_inputs / Path("snapshot.csv"), index_col=0
    ).index
    time_index = pd.to_datetime(time_index)
    network = pypsa.Network(snapshots=time_index)
    return network
