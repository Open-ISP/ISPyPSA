from pathlib import Path

import pandas as pd
import pypsa


def _add_lines_to_network(network: pypsa.Network, lines: pd.DataFrame) -> None:
    """Adds the Lines defined in `lines.csv` in the `path_pypsa_inputs` directory to the
    `pypsa.Network` object.

    Args:
        network: The `pypsa.Network` object
        lines: `pd.DataFrame` with `PyPSA` style `Line` attributes.

    Returns: None
    """
    lines["class_name"] = "Line"
    lines["x"] = 1
    lines["r"] = 1
    lines.apply(lambda row: network.add(**row.to_dict()), axis=1)
