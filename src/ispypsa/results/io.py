from pathlib import Path
import pypsa


def save_results(network: pypsa.Network, pypsa_outputs_location: Path) -> None:
    network.export_to_hdf5(Path(pypsa_outputs_location, "network.hdf5"))


def load_results(pypsa_outputs_location: str | Path) -> pypsa.Network:
    network = pypsa.Network()
    network.import_from_hdf5(Path(pypsa_outputs_location, "network.hdf5"))
    return network
