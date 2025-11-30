from pathlib import Path

import pandas as pd
import pypsa


def _add_carriers_to_network(network: pypsa.Network, generators: pd.DataFrame) -> None:
    """Adds the Carriers in the generators table, and the AC and DC Carriers to the
    `pypsa.Network`.

    Args:
         network: The `pypsa.Network` object
         generators: `pd.DataFrame` with `PyPSA` style `Generator` attributes.

    Returns: None
    """
    carriers = list(generators["carrier"].unique()) + ["AC", "DC"]
    network.add("Carrier", carriers)
