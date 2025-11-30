import pandas as pd
import pypsa


def _add_links_to_network(network: pypsa.Network, links: pd.DataFrame) -> None:
    """Adds the Links defined in a pypsa-friendly input table called `"links"` to the
    `pypsa.Network` object.

    Args:
        network: The `pypsa.Network` object
        links: `pd.DataFrame` with `PyPSA` style `Link` attributes.

    Returns: None
    """
    links["class_name"] = "Link"
    links.apply(lambda row: network.add(**row.to_dict()), axis=1)
