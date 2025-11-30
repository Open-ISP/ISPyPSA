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
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])
    snapshots_as_indexes = pd.MultiIndex.from_arrays(
        [snapshots["investment_periods"], snapshots["snapshots"]]
    )
    network = pypsa.Network(
        snapshots=snapshots_as_indexes,
        investment_periods=snapshots["investment_periods"].unique(),
    )

    snapshots = snapshots.rename(
        columns={"investment_periods": "period", "snapshots": "timestep"}
    )
    snapshots = snapshots.set_index(["period", "timestep"])

    network.snapshot_weightings = snapshots

    return network
