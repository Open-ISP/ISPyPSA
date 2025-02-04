from pathlib import Path

import pandas as pd
import pypsa


def add_lines_to_network(network: pypsa.Network, path_pypsa_inputs: Path) -> None:
    """Adds the Lines defined in `lines.csv` in the `path_pypsa_inputs` directory to the
    `pypsa.Network` object.

    Args:
        network: The `pypsa.Network` object
        path_pypsa_inputs: `pathlib.Path` that points to the directory containing
            PyPSA inputs

    Returns: None
    """
    lines = pd.read_csv(path_pypsa_inputs / Path("lines.csv"))
    line_data["class_name"] = "Line"
    line_data["x"] = 1
    line_data["r"] = 1
    line_data.apply(lambda row: network.add(**row.to_dict()), axis=1)
