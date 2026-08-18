import pandas as pd
import pypsa

_LIMIT_PER_SNAPSHOT_COLUMNS = [
    "name",
    "attribute",
    "investment_periods",
    "snapshots",
    "value",
]


def _add_links_to_network(
    network: pypsa.Network,
    links: pd.DataFrame,
    link_timeslice_limits: pd.DataFrame | None = None,
    timeslice_snapshots: pd.DataFrame | None = None,
) -> None:
    """Adds the Links defined in a pypsa-friendly input table called `"links"` to the
    `pypsa.Network` object.

    On the new-format path the two timeslice tables are required: each link's
    per-timeslice limits are expanded into per-snapshot p_max_pu / p_min_pu
    series that replace the links table's placeholder values (see
    _build_link_pu_overrides). On the old-format path both are omitted and
    the links table's p_max_pu / p_min_pu are the limits.

    FEATURE_FLAG_CLEANUP[use_new_table_format]: once the flag is retired,
    make link_timeslice_limits and timeslice_snapshots required and drop the
    None handling in _build_link_pu_overrides.

    I/O Example (new-format path):
        links (the p_max_pu / p_min_pu here are placeholders):
            name            bus0  bus1  carrier  p_nom  p_max_pu  p_min_pu  p_nom_extendable
            CQ-NQ_existing  CQ    NQ    AC       1400   1.0       0.0       False

        link_timeslice_limits:
            name            attribute  timeslice        value
            CQ-NQ_existing  p_max_pu   qld_peak_demand  0.857
            CQ-NQ_existing  p_max_pu   ,                1.0     # fallback
            CQ-NQ_existing  p_min_pu   ,                -0.714  # fallback only

        timeslice_snapshots:
            timeslice_id     investment_periods  snapshots
            qld_peak_demand  2025                2025-01-13 12:00

        network.snapshots:
            investment_periods  snapshots
            2025                2025-01-13 12:00
            2025                2025-01-15 12:00

        returns None; network is modified in place. It gains the Link:
            network.links:
                name            bus0  bus1  p_nom  p_nom_extendable
                CQ-NQ_existing  CQ    NQ    1400   False
        with per-snapshot limits in place of the placeholders:
            network.links_t.p_max_pu:
                investment_periods  snapshots         CQ-NQ_existing
                2025                2025-01-13 12:00  0.857
                2025                2025-01-15 12:00  1.0
            network.links_t.p_min_pu:
                investment_periods  snapshots         CQ-NQ_existing
                2025                2025-01-13 12:00  -0.714
                2025                2025-01-15 12:00  -0.714

    I/O Example (old-format path):
        links:
            name            bus0  bus1  carrier  p_nom  p_max_pu  p_min_pu  p_nom_extendable
            CQ-NQ_existing  CQ    NQ    AC       1400   1.0       -0.5      False

        returns None; network is modified in place. It gains the Link with
        the links table's p_max_pu / p_min_pu as static values:
            network.links:
                name            bus0  bus1  p_nom  p_max_pu  p_min_pu  p_nom_extendable
                CQ-NQ_existing  CQ    NQ    1400   1.0       -0.5      False
        and no per-snapshot series (network.links_t.p_max_pu / p_min_pu have
        no CQ-NQ_existing column).
    """
    pu_overrides = _build_link_pu_overrides(
        link_timeslice_limits, timeslice_snapshots, network.snapshots
    )
    links["class_name"] = "Link"
    for _, row in links.iterrows():
        network.add(**(row.to_dict() | pu_overrides.get(row["name"], {})))


def _build_link_pu_overrides(
    link_timeslice_limits: pd.DataFrame | None,
    timeslice_snapshots: pd.DataFrame | None,
    snapshots: pd.MultiIndex,
) -> dict[str, dict[str, pd.Series]]:
    """Expands each link's per-timeslice limits into per-snapshot series.

    The translator emits two kinds of limit row per (link, attribute): rows
    with a named timeslice, which apply at the snapshots that timeslice is
    active, and a row with timeslice = NaN, which is the fallback for every
    snapshot no named timeslice covers. Every snapshot must end up with a
    value from one or the other; a snapshot left without one raises rather
    than silently keeping the links table's placeholder p_max_pu / p_min_pu.

    I/O Example:
        link_timeslice_limits:
            name            attribute  timeslice        value
            CQ-NQ_existing  p_max_pu   qld_peak_demand  0.857
            CQ-NQ_existing  p_max_pu   ,                1.0     # fallback
            CQ-NQ_existing  p_min_pu   ,                -0.714  # fallback only

        timeslice_snapshots:
            timeslice_id     investment_periods  snapshots
            qld_peak_demand  2025                2025-01-13 12:00

        snapshots:
            investment_periods  snapshots
            2025                2025-01-13 12:00
            2025                2025-01-15 12:00

        returns (each series indexed by snapshots):
            {"CQ-NQ_existing": {"p_max_pu": [0.857, 1.0],
                                "p_min_pu": [-0.714, -0.714]}}

        Without the p_max_pu fallback row, p_max_pu would be undefined at the
        second snapshot -> ValueError.
    """
    if link_timeslice_limits is None or link_timeslice_limits.empty:
        return {}
    limits_per_snapshot = _expand_limits_to_snapshots(
        link_timeslice_limits, timeslice_snapshots, snapshots
    )
    _raise_if_snapshots_uncovered(limits_per_snapshot)
    return _series_by_link_and_attribute(limits_per_snapshot, snapshots)


def _expand_limits_to_snapshots(
    link_timeslice_limits: pd.DataFrame,
    timeslice_snapshots: pd.DataFrame,
    snapshots: pd.MultiIndex,
) -> pd.DataFrame:
    """One row per (link, attribute, snapshot): the named timeslice's value where
    one is active there, else the fallback, else NaN.

    I/O Example:
        link_timeslice_limits:
            name            attribute  timeslice        value
            CQ-NQ_existing  p_max_pu   qld_peak_demand  0.857
            CQ-NQ_existing  p_max_pu   ,                1.0
            CQ-NQ_existing  p_min_pu   qld_peak_demand  -0.9    # no fallback

        timeslice_snapshots:
            timeslice_id     investment_periods  snapshots
            qld_peak_demand  2025                2025-01-13 12:00

        snapshots:
            investment_periods  snapshots
            2025                2025-01-13 12:00
            2025                2025-01-15 12:00

        returns:
            name            attribute  investment_periods  snapshots         value
            CQ-NQ_existing  p_max_pu   2025                2025-01-13 12:00  0.857
            CQ-NQ_existing  p_max_pu   2025                2025-01-15 12:00  1.0    # fallback
            CQ-NQ_existing  p_min_pu   2025                2025-01-13 12:00  -0.9
            CQ-NQ_existing  p_min_pu   2025                2025-01-15 12:00         # uncovered
    """
    is_fallback = link_timeslice_limits["timeslice"].isna()
    named = _place_named_limits_at_snapshots(
        link_timeslice_limits[~is_fallback], timeslice_snapshots
    )
    fallback = link_timeslice_limits.loc[is_fallback, ["name", "attribute", "value"]]
    grid = _link_attribute_snapshot_grid(link_timeslice_limits, snapshots)
    grid = grid.merge(named, how="left")
    grid = grid.merge(fallback.rename(columns={"value": "fallback"}), how="left")
    grid["value"] = grid["value"].fillna(grid["fallback"])
    return grid.loc[:, _LIMIT_PER_SNAPSHOT_COLUMNS]


def _link_attribute_snapshot_grid(
    link_timeslice_limits: pd.DataFrame, snapshots: pd.MultiIndex
) -> pd.DataFrame:
    """Every (link, attribute) pair in the limits table at every snapshot.

    I/O Example:
        link_timeslice_limits (only name and attribute are read):
            name            attribute  timeslice        value
            CQ-NQ_existing  p_max_pu   qld_peak_demand  0.857
            CQ-NQ_existing  p_max_pu   ,                1.0     # same pair, once in the grid

        snapshots:
            investment_periods  snapshots
            2025                2025-01-13 12:00
            2025                2025-01-15 12:00

        returns:
            name            attribute  investment_periods  snapshots
            CQ-NQ_existing  p_max_pu   2025                2025-01-13 12:00
            CQ-NQ_existing  p_max_pu   2025                2025-01-15 12:00
    """
    pairs = link_timeslice_limits.loc[:, ["name", "attribute"]].drop_duplicates()
    snapshot_rows = pd.DataFrame(
        snapshots.tolist(), columns=["investment_periods", "snapshots"]
    )
    return pairs.merge(snapshot_rows, how="cross")


def _place_named_limits_at_snapshots(
    named: pd.DataFrame, timeslice_snapshots: pd.DataFrame
) -> pd.DataFrame:
    """Places each named-timeslice limit at the snapshots its timeslice is active.

    A timeslice with no snapshots contributes no rows (the translator has
    already logged it).

    I/O Example:
        named:
            name            attribute  timeslice        value
            CQ-NQ_existing  p_max_pu   qld_peak_demand  0.857

        timeslice_snapshots:
            timeslice_id     investment_periods  snapshots
            qld_peak_demand  2025                2025-01-13 12:00

        returns:
            name            attribute  investment_periods  snapshots         value
            CQ-NQ_existing  p_max_pu   2025                2025-01-13 12:00  0.857
    """
    active_at = timeslice_snapshots.rename(columns={"timeslice_id": "timeslice"})
    active_at["snapshots"] = pd.to_datetime(active_at["snapshots"])
    placed = named.merge(active_at, on="timeslice")
    return placed.loc[:, _LIMIT_PER_SNAPSHOT_COLUMNS]


def _raise_if_snapshots_uncovered(limits_per_snapshot: pd.DataFrame) -> None:
    """Raises if any (link, attribute) has a snapshot with neither a named-timeslice
    nor a fallback value.

    I/O Example:
        limits_per_snapshot:
            name            attribute  investment_periods  snapshots         value
            CQ-NQ_existing  p_max_pu   2025                2025-01-13 12:00  0.857
            CQ-NQ_existing  p_max_pu   2025                2025-01-15 12:00  1.0
        returns None

        limits_per_snapshot:
            name            attribute  investment_periods  snapshots         value
            CQ-NQ_existing  p_min_pu   2025                2025-01-13 12:00  -0.9
            CQ-NQ_existing  p_min_pu   2025                2025-01-15 12:00         # uncovered
        raises ValueError naming (CQ-NQ_existing, p_min_pu) and the
        uncovered snapshot (2025, 2025-01-15 12:00).
    """
    uncovered = limits_per_snapshot[limits_per_snapshot["value"].isna()]
    if uncovered.empty:
        return
    pairs = sorted(set(zip(uncovered["name"], uncovered["attribute"])))
    first = uncovered.head(5)
    first_snapshots = [
        f"({period}, {snapshot})"
        for period, snapshot in zip(first["investment_periods"], first["snapshots"])
    ]
    raise ValueError(
        f"link_timeslice_limits leaves {len(uncovered)} (link, attribute, snapshot) "
        f"combination(s) undefined: no fallback (blank-timeslice) row and no named "
        f"timeslice active there. Affected (link, attribute): {pairs}. "
        f"First uncovered (investment_period, snapshot): {', '.join(first_snapshots)}"
    )


def _series_by_link_and_attribute(
    limits_per_snapshot: pd.DataFrame, snapshots: pd.MultiIndex
) -> dict[str, dict[str, pd.Series]]:
    """Reshapes the long table into {link: {attribute: series indexed by snapshots}}.

    I/O Example:
        limits_per_snapshot:
            name            attribute  investment_periods  snapshots         value
            CQ-NQ_existing  p_max_pu   2025                2025-01-13 12:00  0.857
            CQ-NQ_existing  p_max_pu   2025                2025-01-15 12:00  1.0
            CQ-NQ_existing  p_min_pu   2025                2025-01-13 12:00  -0.714
            CQ-NQ_existing  p_min_pu   2025                2025-01-15 12:00  -0.714

        snapshots:
            investment_periods  snapshots
            2025                2025-01-13 12:00
            2025                2025-01-15 12:00

        returns (each series indexed by snapshots):
            {"CQ-NQ_existing": {"p_max_pu": [0.857, 1.0],
                                "p_min_pu": [-0.714, -0.714]}}
    """
    overrides = {}
    for (name, attribute), rows in limits_per_snapshot.groupby(["name", "attribute"]):
        series = rows.set_index(["investment_periods", "snapshots"])["value"]
        overrides.setdefault(name, {})[attribute] = series.reindex(snapshots)
    return overrides
