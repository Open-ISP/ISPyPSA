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

    On the new-format path the two timeslice tables are required: every
    existing (non-extendable) link's per-timeslice limits are expanded into
    per-snapshot p_max_pu / p_min_pu series that replace the links table's
    placeholder values (see _build_link_pu_overrides). Expansion links keep
    the links table's static p_max_pu / p_min_pu. On the old-format path both
    tables are omitted and every link's p_max_pu / p_min_pu are the limits.

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
        links, link_timeslice_limits, timeslice_snapshots, network.snapshots
    )
    links["class_name"] = "Link"
    for _, row in links.iterrows():
        network.add(**(row.to_dict() | pu_overrides.get(row["name"], {})))


def _build_link_pu_overrides(
    links: pd.DataFrame,
    link_timeslice_limits: pd.DataFrame | None,
    timeslice_snapshots: pd.DataFrame | None,
    snapshots: pd.MultiIndex,
) -> dict[str, dict[str, pd.Series]]:
    """Expands each existing link's per-timeslice limits into per-snapshot series.

    The translator emits two kinds of limit row per (link, attribute): rows
    with a named timeslice, which apply at the snapshots that timeslice is
    active, and a row with timeslice = NaN, which is the fallback for every
    snapshot no named timeslice covers. Every existing (non-extendable) link
    must end up with a value for both p_max_pu and p_min_pu at every snapshot
    from one or the other; a link or snapshot left without one raises rather
    than silently keeping the links table's placeholder p_max_pu / p_min_pu.
    Expansion links are not checked: their p_max_pu / p_min_pu are static.

    I/O Example:
        links (only name and p_nom_extendable are read):
            name            p_nom_extendable
            CQ-NQ_existing  False
            CQ-NQ_option_1  True              # expansion link: not checked

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
        second snapshot -> ValueError. Without any p_min_pu row, p_min_pu would
        be undefined at every snapshot -> ValueError.
    """
    if link_timeslice_limits is None:
        return {}
    limits_per_snapshot = _expand_limits_to_snapshots(
        links, link_timeslice_limits, timeslice_snapshots, snapshots
    )
    _raise_if_snapshots_uncovered(limits_per_snapshot)
    return _series_by_link_and_attribute(limits_per_snapshot, snapshots)


def _expand_limits_to_snapshots(
    links: pd.DataFrame,
    link_timeslice_limits: pd.DataFrame,
    timeslice_snapshots: pd.DataFrame,
    snapshots: pd.MultiIndex,
) -> pd.DataFrame:
    """One row per (existing link, attribute, snapshot): the named timeslice's
    value where one is active there, else the fallback, else NaN.

    I/O Example:
        links (only name and p_nom_extendable are read):
            name            p_nom_extendable
            CQ-NQ_existing  False

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
    grid = _link_attribute_snapshot_grid(links, snapshots)
    grid = grid.merge(named, how="left")
    grid = grid.merge(fallback.rename(columns={"value": "fallback"}), how="left")
    grid["value"] = grid["value"].fillna(grid["fallback"])
    return grid.loc[:, _LIMIT_PER_SNAPSHOT_COLUMNS]


def _link_attribute_snapshot_grid(
    links: pd.DataFrame, snapshots: pd.MultiIndex
) -> pd.DataFrame:
    """Every existing (non-extendable) link, for both p_max_pu and p_min_pu, at
    every snapshot. Expansion links are left out: their limits are static.

    I/O Example:
        links (only name and p_nom_extendable are read):
            name            p_nom_extendable
            CQ-NQ_existing  False
            CQ-NQ_option_1  True              # left out

        snapshots:
            investment_periods  snapshots
            2025                2025-01-13 12:00
            2025                2025-01-15 12:00

        returns:
            name            attribute  investment_periods  snapshots
            CQ-NQ_existing  p_max_pu   2025                2025-01-13 12:00
            CQ-NQ_existing  p_max_pu   2025                2025-01-15 12:00
            CQ-NQ_existing  p_min_pu   2025                2025-01-13 12:00
            CQ-NQ_existing  p_min_pu   2025                2025-01-15 12:00
    """
    existing = links.loc[~links["p_nom_extendable"], ["name"]]
    attributes = pd.DataFrame({"attribute": ["p_max_pu", "p_min_pu"]})
    snapshot_rows = pd.DataFrame(
        snapshots.tolist(), columns=["investment_periods", "snapshots"]
    )
    return existing.merge(attributes, how="cross").merge(snapshot_rows, how="cross")


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

    The message separates pairs undefined at every snapshot (typically a link
    with no limit rows at all) from pairs undefined at only some snapshots
    (named-timeslice rows with no fallback), and samples the uncovered
    snapshots of the latter.

    I/O Example:
        limits_per_snapshot:
            name            attribute  investment_periods  snapshots         value
            CQ-NQ_existing  p_max_pu   2025                2025-01-13 12:00  0.857
            CQ-NQ_existing  p_max_pu   2025                2025-01-15 12:00  1.0
        returns None

        limits_per_snapshot:
            name            attribute  investment_periods  snapshots         value
            CQ-NQ_existing  p_max_pu   2025                2025-01-13 12:00         # no rows at all
            CQ-NQ_existing  p_max_pu   2025                2025-01-15 12:00
            CQ-NQ_existing  p_min_pu   2025                2025-01-13 12:00  -0.9
            CQ-NQ_existing  p_min_pu   2025                2025-01-15 12:00         # no fallback
        raises ValueError: "... Undefined at every snapshot: [('CQ-NQ_existing',
        'p_max_pu')]. Undefined at some snapshots: [('CQ-NQ_existing', 'p_min_pu')],
        first uncovered (investment_period, snapshot): (2025, 2025-01-15 12:00:00)"
    """
    uncovered = limits_per_snapshot[limits_per_snapshot["value"].isna()]
    if uncovered.empty:
        return
    at_every, at_some = _split_pairs_by_uncovered_extent(limits_per_snapshot, uncovered)
    sample = _first_uncovered_snapshots(uncovered, at_some)
    raise ValueError(
        f"link_timeslice_limits leaves p_max_pu / p_min_pu undefined for existing "
        f"links: no fallback (blank-timeslice) row and no named timeslice active "
        f"there. Undefined at every snapshot: {at_every}. "
        f"Undefined at some snapshots: {at_some}{sample}"
    )


def _split_pairs_by_uncovered_extent(
    limits_per_snapshot: pd.DataFrame, uncovered: pd.DataFrame
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Splits the (link, attribute) pairs with uncovered snapshots into those
    uncovered at every snapshot and those uncovered at only some.

    I/O Example:
        limits_per_snapshot: A p_max_pu at 2 snapshots (both NaN),
                             A p_min_pu at 2 snapshots (one NaN)
        uncovered:           the 3 NaN rows

        returns ([("A", "p_max_pu")], [("A", "p_min_pu")])
    """
    keys = ["name", "attribute"]
    n_snapshots = limits_per_snapshot.groupby(keys).size()
    n_uncovered = uncovered.groupby(keys).size()
    at_every = n_uncovered == n_snapshots.loc[n_uncovered.index]
    return sorted(at_every[at_every].index), sorted(at_every[~at_every].index)


def _first_uncovered_snapshots(
    uncovered: pd.DataFrame, pairs: list[tuple[str, str]]
) -> str:
    """An error-message clause sampling the first five uncovered snapshots of the
    given (link, attribute) pairs; empty when there are no pairs to sample.

    I/O Example:
        uncovered:
            name  attribute  investment_periods  snapshots
            A     p_max_pu   2025                2025-01-13 12:00
            A     p_min_pu   2025                2025-01-15 12:00
        pairs: [("A", "p_min_pu")]
        returns ", first uncovered (investment_period, snapshot): (2025, 2025-01-15 12:00:00)"

        pairs: []
        returns ""
    """
    if not pairs:
        return ""
    rows = uncovered.set_index(["name", "attribute"]).loc[pairs].head(5)
    sample = ", ".join(
        f"({period}, {snapshot})"
        for period, snapshot in zip(rows["investment_periods"], rows["snapshots"])
    )
    return f", first uncovered (investment_period, snapshot): {sample}"


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
