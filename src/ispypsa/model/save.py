from pathlib import Path

import pypsa


def save_results(network: pypsa.Network, save_directory: Path, save_name: str) -> None:
    """Save the optimised PyPSA network as a hdf5 file."""
    network.export_to_hdf5(Path(save_directory, f"{save_name}.hdf5"))
