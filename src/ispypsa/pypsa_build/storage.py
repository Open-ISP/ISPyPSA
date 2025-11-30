import pandas as pd
import pypsa


def _add_battery_to_network(
    battery_definition: dict,
    network: pypsa.Network,
) -> None:
    """Adds a battery to a pypsa.Network based on a dict containing PyPSA StorageUnit
    attributes.

    PyPSA StorageUnits have set power to energy capacity ratio,

    Args:
        battery_definition: dict containing pypsa StorageUnit parameters
        network: The `pypsa.Network` object

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
    batterys: pd.DataFrame,
) -> None:
    """Adds the batterys in a pypsa-friendly `pd.DataFrame` to the `pypsa.Network`.

    Args:
        network: The `pypsa.Network` object
        batterys:  `pd.DataFrame` with `PyPSA` style `StorageUnit` attributes.
    Returns: None
    """

    batterys.apply(
        lambda row: _add_battery_to_network(
            row.to_dict(),
            network,
        ),
        axis=1,
    )
