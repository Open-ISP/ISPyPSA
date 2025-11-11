from pathlib import Path

import pandas as pd
import pypsa

from ispypsa.pypsa_build.buses import _update_buses_demand_timeseries
from ispypsa.pypsa_build.custom_constraints import _add_custom_constraints
from ispypsa.pypsa_build.generators import _update_generators_availability_timeseries


def update_network_timeseries(
    network: pypsa.Network,
    pypsa_friendly_input_tables: dict[str, pd.DataFrame],
    snapshots: pd.DataFrame,
    pypsa_friendly_timeseries_location: Path,
) -> None:
    """
    Update the time series data in a pypsa.Network instance.

    Designed to help convert capacity expansion network models into operational models
    but may also be useful in other circumstances, such as when running a capacity
    expansion model with different reference year cycles.

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

        >>> network = pypsa.Network()
        >>> network.import_from_netcdf("existing_network.nc")

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
            defining the investment a modelled inteval belongs to and 'snapshots'
            (datetime) defining each time interval modelled. 'investment_periods'
            periods are refered to by the year (financial or calander) in which they
            begin.
        pypsa_friendly_timeseries_location: `Path` to `PyPSA` friendly time series
            data (add link to timeseries data docs).

    Returns: None
    """
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])
    snapshots_as_indexes = pd.MultiIndex.from_arrays(
        [snapshots["investment_periods"], snapshots["snapshots"]]
    )
    network.snapshots = snapshots_as_indexes
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

    # The underlying linopy model needs to get built again here so that the new time
    # series data is used in the linopy model rather than the old data.
    network.optimize.create_model()

    # As we rebuilt the linopy model now we need to re add custom constrains.
    _add_custom_constraints(
        network,
        pypsa_friendly_input_tables["custom_constraints_rhs"],
        pypsa_friendly_input_tables["custom_constraints_lhs"],
    )
