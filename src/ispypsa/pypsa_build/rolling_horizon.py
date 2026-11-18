import pandas as pd
import pypsa

from ispypsa.pypsa_build.custom_constraints import (
    _add_custom_constraints,
    _move_fixed_capacity_terms_to_rhs,
)


def optimize_with_rolling_horizon(
    network: pypsa.Network,
    pypsa_friendly_input_tables: dict[str, pd.DataFrame],
    horizon: int,
    overlap: int,
) -> None:
    """Solves the network's dispatch in a rolling horizon, with the custom
    constraints applied in every window.

    The snapshots are solved in consecutive windows of `horizon` snapshots, each
    sharing its first `overlap` snapshots with the window before it. Use this
    rather than calling `network.optimize.optimize_with_rolling_horizon` directly:
    PyPSA builds a new model for each window, so custom constraints added to the
    network's model beforehand never reach the solver. Here they are added again
    to each window's model as it is built.

    Custom constraint terms on capacities that are no longer decision variables
    (all of them, once `network.optimize.fix_optimal_capacities()` has been called)
    are treated as constants, using the component's `p_nom`. Constraints made up of
    only such terms, like expansion limits, are left out: the capacity expansion
    model that chose the capacities already enforced them.

    Examples:
        >>> from ispypsa.pypsa_build import (
        ...     optimize_with_rolling_horizon,
        ...     update_network_timeseries,
        ... )

        Starting from a solved capacity expansion network, switch it to the
        operational snapshots and fix the capacities it built.

        >>> update_network_timeseries(
        ...     network,
        ...     pypsa_friendly_input_tables,
        ...     operational_snapshots,
        ...     Path("path/to/operational/time/series/data/files"),
        ... )
        >>> network.optimize.fix_optimal_capacities()

        Then solve the dispatch in windows of 48 snapshots which overlap by 24.

        >>> optimize_with_rolling_horizon(
        ...     network, pypsa_friendly_input_tables, horizon=48, overlap=24
        ... )

    Args:
        network: the `pypsa.Network` to solve, already carrying the snapshots and
            time series to solve for.
        pypsa_friendly_input_tables: dictionary of dataframes in the `PyPSA` friendly
            format. The 'custom_constraints_rhs' and 'custom_constraints_lhs' tables
            are used.
        horizon: the number of snapshots in each window.
        overlap: the number of snapshots each window shares with the one before it.
            Must be smaller than `horizon`.

    Returns: None
    """
    # Capacities don't change between windows, so this is done once up front.
    rhs, lhs = _move_fixed_capacity_terms_to_rhs(
        network,
        pypsa_friendly_input_tables["custom_constraints_rhs"],
        pypsa_friendly_input_tables["custom_constraints_lhs"],
    )
    # PyPSA calls extra_functionality on each window's model once it is built and
    # before it is solved. That model only has the window's snapshots, so each
    # constraint is added for those snapshots only.
    network.optimize.optimize_with_rolling_horizon(
        horizon=horizon,
        overlap=overlap,
        multi_investment_periods=True,
        extra_functionality=lambda network, snapshots: _add_custom_constraints(
            network, rhs, lhs
        ),
    )
