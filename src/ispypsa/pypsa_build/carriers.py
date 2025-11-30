import pandas as pd
import pypsa


def _add_carriers_to_network(
    network: pypsa.Network,
    generators: pd.DataFrame | None,
    storage: pd.DataFrame | None,
) -> None:
    """Adds the Carriers in the generators table, and the AC and DC Carriers to the
    `pypsa.Network`.

    Args:
         network: The `pypsa.Network` object
         generators: `pd.DataFrame` with `PyPSA` style `Generator` attributes, or None if no
            such table exists.
         storage: `pd.DataFrame` with `PyPSA` style `StorageUnit` attributes, or None if no such
            table exists. At the moment this comprises batteries only.

    Returns: None
    """
    generator_carriers = []
    storage_carriers = []
    standard_carriers = ["AC", "DC"]

    if generators is not None and not generators.empty:
        generator_carriers = list(generators["carrier"].unique())
    if storage is not None and not storage.empty:
        storage_carriers = list(storage["carrier"].unique())

    carriers = generator_carriers + storage_carriers + standard_carriers
    network.add("Carrier", carriers)
