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

    Storage levels are tracked across windows. Each storage unit starts the first
    window half full and starts every later window at the level it reached just
    before that window. To do this the network's storage units are changed in
    place: `cyclic_state_of_charge` is set to False and `state_of_charge_initial`
    to half of `p_nom` x `max_hours`. Call
    `network.optimize.fix_optimal_capacities()` first, so that `p_nom` holds the
    capacity that was built.

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
    _track_state_of_charge_across_windows(network)
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


def _track_state_of_charge_across_windows(network: pypsa.Network) -> None:
    """Sets the network's storage units up to carry their state of charge from
    one rolling horizon window to the next, starting the first window half full.

    Before each window after the first, PyPSA sets state_of_charge_initial to
    the level the storage unit reached just before that window. A unit with
    cyclic_state_of_charge=True ignores that value: it picks its own starting
    level for each window and returns to it by the window's end. Turning the
    cycling off makes each window start from the carried level. The value set
    here is only used by the first window.

    I/O Example:
        network.storage_units:
            name     p_nom  max_hours  cyclic_state_of_charge  state_of_charge_initial
            battery  5      8          True                    0

        network.storage_units afterwards:
            name     p_nom  max_hours  cyclic_state_of_charge  state_of_charge_initial
            battery  5      8          False                   20                       # 0.5 x 5 x 8
    """
    storage_units = network.storage_units
    energy_capacity = storage_units["p_nom"] * storage_units["max_hours"]
    storage_units["cyclic_state_of_charge"] = False
    storage_units["state_of_charge_initial"] = 0.5 * energy_capacity
