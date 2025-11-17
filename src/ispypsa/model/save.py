from pathlib import Path

import pypsa


def save_pypsa_network(
    network: pypsa.Network, save_directory: Path, save_name: str
) -> None:
    """Save the optimised PyPSA network as a nc file."""
    network.export_to_netcdf(Path(save_directory, f"{save_name}.nc"))
