from pathlib import Path

import pandas as pd
import pypsa

from ispypsa.feature_flags import FEATURE_FLAGS
from ispypsa.pypsa_build.buses import _update_buses_demand_timeseries
from ispypsa.pypsa_build.custom_constraints import (
    _add_custom_constraints,
    _add_custom_constraints_with_temporal_scope,
)
from ispypsa.pypsa_build.generators import _update_generators_availability_timeseries
from ispypsa.pypsa_build.links import _update_link_limit_timeseries


def update_network_timeseries(
    network: pypsa.Network,
    pypsa_friendly_input_tables: dict[str, pd.DataFrame],
    snapshots: pd.DataFrame,
    pypsa_friendly_timeseries_location: Path,
    # FEATURE_FLAG_CLEANUP[use_new_table_format]: make timeslice_snapshots a
    # required argument. A model with no timeslices passes an empty table.
    timeslice_snapshots: pd.DataFrame | None = None,
) -> None:
    """
    Update the time series data in a pypsa.Network instance.

    Designed to help convert capacity expansion network models into operational models
    but may also be useful in other circumstances, such as when running a capacity
    expansion model with different reference year cycles.

    The network's snapshots are replaced with the ones in `snapshots`, and each new
    snapshot takes its weightings from that table. The weightings the network had
    before the update are discarded, including for snapshots present both before and
    after.

    I/O Example:
        network.snapshot_weightings before the update:
            period  timestep             objective  generators  stores
            2025    2025-01-01 00:30:00  4380.0     4380.0      0.5
            2025    2025-01-01 01:00:00  4380.0     4380.0      0.5

        snapshots:
            investment_periods  snapshots            objective  generators  stores
            2025                2025-01-01 01:00:00  2920.0     2920.0      0.5  # also a snapshot before the update
            2025                2025-01-08 00:30:00  2920.0     2920.0      0.5
            2025                2025-01-08 01:00:00  2920.0     2920.0      0.5

        network.snapshot_weightings after the update:
            period  timestep             objective  generators  stores
            2025    2025-01-01 01:00:00  2920.0     2920.0      0.5
            2025    2025-01-08 00:30:00  2920.0     2920.0      0.5
            2025    2025-01-08 01:00:00  2920.0     2920.0      0.5

    Examples:
        >>> import pandas as pd
        >>> from pathlib import Path
        >>> from ispypsa.data_fetch import read_csvs
        >>> from ispypsa.pypsa_build import update_network_timeseries

        Get PyPSA friendly inputs (inparticular these need to contain the generators and
        buses tables).

        >>> pypsa_friendly_input_tables = read_csvs("path/to/pypsa/friendly/inputs")

        Get the snapshots for the updated time series data.

        >>> snapshots = pd.read_csv("new_snapshots.csv")

        Get the pypsa.Network we want to update the time series data in.

        >>> network = pypsa.Network("existing_network.nc")

        Create pd.Dataframe defining the set of snapshot (time intervals) to be used.

        >>> update_network_timeseries(
        ...     network,
        ...     pypsa_friendly_input_tables,
        ...     snapshots,
        ...     Path("path/to/time/series/data/files")
        ... )

    Args:
        network: pypsa.Network which has set of generators, loads, and buses consistent
            with the updated time series data. i.e. if generator 'Y' exists in the
            existing network it also needs to exist in the updated time series data.
        pypsa_friendly_input_tables: dictionary of dataframes in the `PyPSA` friendly
            format. (add link to pypsa friendly format table docs)
        snapshots: a pd.DataFrame containing the columns 'investment_periods' (int)
            defining the investment period a modelled interval belongs to, 'snapshots'
            (datetime) defining each time interval modelled, and the snapshot
            weightings 'objective', 'generators' and 'stores' (float), as returned by
            `create_pypsa_friendly_timeseries_inputs`. Investment periods are
            referred to by the year (financial or calendar) in which they begin.
        pypsa_friendly_timeseries_location: `Path` to `PyPSA` friendly time series
            data (add link to timeseries data docs).
        timeslice_snapshots: a pd.DataFrame with the columns 'timeslice',
            'investment_periods' and 'snapshots', listing the timeslice active at
            each of the new `snapshots`. Required under the new table format,
            where transmission limits and custom constraints are scoped by
            timeslice: the mapping built for the network's previous snapshots
            doesn't cover the new ones, so both are rebuilt from this table.
            Not used under the old table format.
            FEATURE_FLAG_CLEANUP[use_new_table_format]: make this required.

    Returns: None
    """
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])
    snapshots_as_indexes = pd.MultiIndex.from_arrays(
        [snapshots["investment_periods"], snapshots["snapshots"]]
    )
    network.snapshots = snapshots_as_indexes
    # Replacing the snapshots leaves the old weightings on snapshots the network
    # already had and 1.0 on the rest, so set them all from the new snapshots table,
    # as _initialise_network does.
    network.snapshot_weightings = snapshots.set_index(
        ["investment_periods", "snapshots"]
    )
    # Workaround for pandas behavior where DataFrame.reindex() on a MultiIndex
    # loses the MultiIndex.name attribute when old and new index values are
    # identical (e.g. when capacity expansion and operational snapshots match).
    # pypsa 1.0 requires snapshots.name == "snapshot" for its xarray-based
    # optimization model builder.
    if isinstance(network.snapshots, pd.MultiIndex) and network.snapshots.name is None:
        network._snapshots_data.index.name = "snapshot"
    network.set_investment_periods(snapshots["investment_periods"].unique())
    _update_generators_availability_timeseries(
        network,
        pypsa_friendly_input_tables["generators"],
        pypsa_friendly_timeseries_location,
    )
    _update_buses_demand_timeseries(
        network,
        pypsa_friendly_input_tables["buses"],
        pypsa_friendly_timeseries_location,
    )

    # FEATURE_FLAG_CLEANUP[use_new_table_format]: drop the condition.
    if FEATURE_FLAGS["use_new_table_format"]:
        _update_link_limit_timeseries(
            network,
            pypsa_friendly_input_tables["links"],
            pypsa_friendly_input_tables["link_timeslice_limits"],
            timeslice_snapshots,
        )

    # The underlying linopy model needs to get built again here so that the new time
    # series data is used in the linopy model rather than the old data.
    network.optimize.create_model(multi_investment_periods=True)

    # As we rebuilt the linopy model now we need to re add custom constrains.
    # FEATURE_FLAG_CLEANUP[use_new_table_format]: keep the first branch only.
    if FEATURE_FLAGS["use_new_table_format"]:
        _add_custom_constraints_with_temporal_scope(
            network,
            pypsa_friendly_input_tables["custom_constraints_rhs"],
            pypsa_friendly_input_tables["custom_constraints_lhs"],
            timeslice_snapshots,
        )
    else:
        _add_custom_constraints(
            network,
            pypsa_friendly_input_tables["custom_constraints_rhs"],
            pypsa_friendly_input_tables["custom_constraints_lhs"],
        )
