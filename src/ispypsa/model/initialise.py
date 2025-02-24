import pandas as pd
import pypsa


def _initialise_network(snapshots: pd.DataFrame) -> pypsa.Network:
    """Creates a `pypsa.Network object` with snapshots defined.

    Args:
        snapshots: `pd.DataFrame` specifying the date times (`str`), in column labeled,
         'snapshots', to be used in the `pypsa.Network` snapshots.

    Returns:
        `pypsa.Network` object
    """
    snapshots = pd.to_datetime(snapshots["snapshots"])
    network = pypsa.Network(snapshots=snapshots)
    return network
