import pandas as pd
import pypsa


def _add_battery_to_network(
    network: pypsa.Network,
    battery_definition: dict,
) -> None:
    """Adds a battery to a pypsa.Network based on a dict containing `PyPSA` `StorageUnit`
    attributes.

    Args:
        network: The `pypsa.Network` object
        battery_definition: dict containing pypsa `StorageUnit` parameters

    Returns: None
    """
    battery_definition["class_name"] = "StorageUnit"

    pypsa_attributes_only = {
        key: value
        for key, value in battery_definition.items()
        if not key.startswith("isp_")
    }
    network.add(**pypsa_attributes_only)


def _add_batteries_to_network(
    network: pypsa.Network,
    batteries: pd.DataFrame,
) -> None:
    """Adds the batteries in a pypsa-friendly `pd.DataFrame` to the `pypsa.Network` as `PyPSA` `StorageUnit`s.

    Args:
        network: The `pypsa.Network` object
        batteries:  `pd.DataFrame` with `PyPSA` style `StorageUnit` attributes.
    Returns: None
    """

    batteries.apply(
        lambda row: _add_battery_to_network(
            network,
            row.to_dict(),
        ),
        axis=1,
    )
