from pathlib import Path

import pandas as pd
import pypsa


def add_carriers_to_network(network: pypsa.Network, path_pypsa_inputs: Path):
    """Adds the Carriers in the ecaa_generators.csv table (path_pypsa_inputs directory) and the AC Carrier to the
    pypsa.Network.

    Args:
         network: The pypsa.Network object
         path_pypsa_inputs: pathlib.Path for directory containing pypsa inputs

    Returns: None
    """
    ecaa_generators = pd.read_csv(path_pypsa_inputs / Path("generators.csv"))
    carriers = list(ecaa_generators["carrier"].unique()) + ["AC"]
    network.add("Carrier", carriers)
