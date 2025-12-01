from pathlib import Path

import pypsa


def save_pypsa_network(
    network: pypsa.Network, save_directory: Path, save_name: str
) -> None:
    """Save the optimised PyPSA network as a NetCDF file.

    Examples:
        Perform required imports.
        >>> from pathlib import Path
        >>> from ispypsa.pypsa_build import save_pypsa_network

        After running the model optimisation, save the network.
        >>> network.optimize.solve_model(solver_name="highs")
        >>> save_pypsa_network(
        ...     network,
        ...     save_directory=Path("outputs"),
        ...     save_name="capacity_expansion"
        ... )
        # Saves to outputs/capacity_expansion.nc

        Save operational model results.
        >>> save_pypsa_network(
        ...     network,
        ...     save_directory=Path("outputs"),
        ...     save_name="operational"
        ... )
        # Saves to outputs/operational.nc

    Args:
        network: The solved PyPSA network object.
        save_directory: Directory where the network file should be saved.
        save_name: Name for the saved file (without .nc extension).

    Returns:
        None
    """
    network.export_to_netcdf(Path(save_directory, f"{save_name}.nc"))
