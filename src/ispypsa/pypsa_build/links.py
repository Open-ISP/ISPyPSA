import pandas as pd
import pypsa


def _add_links_to_network(
    network: pypsa.Network,
    links: pd.DataFrame,
    link_timeslice_limits: pd.DataFrame | None = None,
    timeslice_snapshots: pd.DataFrame | None = None,
) -> None:
    """Adds the Links defined in a pypsa-friendly input table called `"links"` to the
    `pypsa.Network` object.

    On the new-format path the two limit tables are required: each link's
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

        network.snapshots: (2025, 2025-01-13 12:00), (2025, 2025-01-15 12:00)

        returns None; network is modified in place — it gains the Link
        CQ-NQ_existing with p_nom = 1400 and the per-snapshot series
            network.links_t.p_max_pu["CQ-NQ_existing"] = [0.857, 1.0]
            network.links_t.p_min_pu["CQ-NQ_existing"] = [-0.714, -0.714]

    I/O Example (old-format path):
        _add_links_to_network(network, links)
        returns None; network gains the Link with the links table's static
        p_max_pu / p_min_pu and no per-snapshot series.
    """
    pu_overrides = _build_link_pu_overrides(
        link_timeslice_limits, timeslice_snapshots, links, network.snapshots
    )
    links["class_name"] = "Link"
    for _, row in links.iterrows():
        network.add(**(row.to_dict() | pu_overrides.get(row["name"], {})))


def _build_link_pu_overrides(
    link_timeslice_limits: pd.DataFrame | None,
    timeslice_snapshots: pd.DataFrame | None,
    links: pd.DataFrame,
    snapshots: pd.MultiIndex,
) -> dict[str, dict[str, pd.Series]]:
    """Expands each link's per-timeslice limits into per-snapshot series.

    The translator emits two kinds of limit row per (link, attribute): rows
    with a named timeslice, which apply at the snapshots that timeslice is
    active, and a row with timeslice = NaN, which is the fallback for every
    snapshot no named timeslice covers (the coverage contract is
    Open-ISP/ISPyPSA#123). Each series is seeded with the fallback and the
    named timeslices are then written over it, so a snapshot's value is its
    named timeslice's limit if it has one and the fallback otherwise.

    The links table's static p_max_pu / p_min_pu are placeholders the
    translator sets so the columns exist; they only remain in effect at
    snapshots the limit rows leave uncovered, which the coverage contract
    rules out. A named timeslice with no snapshots leaves the fallback in
    place — the translator has already logged it.

    I/O Example:
        link_timeslice_limits:
            name            attribute  timeslice        value
            CQ-NQ_existing  p_max_pu   qld_peak_demand  0.857
            CQ-NQ_existing  p_max_pu   ,                1.0     # fallback
            CQ-NQ_existing  p_min_pu   ,                -0.714  # fallback only

        timeslice_snapshots: qld_peak_demand active at (2025, 2025-01-13 12:00)
        snapshots: (2025, 2025-01-13 12:00), (2025, 2025-01-15 12:00)

        returns:
            {"CQ-NQ_existing": {"p_max_pu": series [0.857, 1.0],
                                "p_min_pu": series [-0.714, -0.714]}}
    """
    if link_timeslice_limits is None or link_timeslice_limits.empty:
        return {}
    timeslice_labels = _timeslice_snapshot_labels(timeslice_snapshots)
    static_values = links.set_index("name").loc[:, ["p_max_pu", "p_min_pu"]]
    overrides = {}
    for (name, attribute), rows in link_timeslice_limits.groupby(["name", "attribute"]):
        series = pd.Series(static_values.loc[name, attribute], index=snapshots)
        series = _apply_fallback_limit(series, rows)
        series = _apply_named_timeslice_limits(series, rows, timeslice_labels)
        overrides.setdefault(name, {})[attribute] = series
    return overrides


def _apply_fallback_limit(series: pd.Series, rows: pd.DataFrame) -> pd.Series:
    """Sets every snapshot to the timeslice = NaN fallback row's value, if there is one.

    I/O Example:
        series: [1.0, 1.0]
        rows:
            timeslice        value
            qld_peak_demand  0.857
            ,                0.9    # fallback
        -> [0.9, 0.9]

        rows with no fallback row -> series unchanged
    """
    fallback = rows.loc[rows["timeslice"].isna(), "value"]
    if fallback.empty:
        return series
    return pd.Series(fallback.iloc[0], index=series.index)


def _apply_named_timeslice_limits(
    series: pd.Series, rows: pd.DataFrame, timeslice_labels: dict[str, list[tuple]]
) -> pd.Series:
    """Writes each named timeslice's value at the snapshots it is active.

    I/O Example:
        series: [0.9, 0.9, 0.9]  (snapshots s0, s1, s2)
        rows:
            timeslice        value
            qld_peak_demand  0.857
            ,                0.9    # fallback rows are skipped
        timeslice_labels: {"qld_peak_demand": [s1, s2]}
        -> [0.9, 0.857, 0.857]
    """
    series = series.copy()
    for row in rows.loc[rows["timeslice"].notna()].itertuples():
        series.loc[timeslice_labels.get(row.timeslice, [])] = row.value
    return series


def _timeslice_snapshot_labels(
    timeslice_snapshots: pd.DataFrame,
) -> dict[str, list[tuple]]:
    """The (investment_period, snapshot) labels each timeslice is active at.

    I/O Example:
        timeslice_id=qld_peak_demand, investment_periods=2025,
        snapshots=2025-01-13 12:00
        -> {"qld_peak_demand": [(2025, Timestamp("2025-01-13 12:00"))]}
    """
    mapping = timeslice_snapshots.copy()
    mapping["snapshots"] = pd.to_datetime(mapping["snapshots"])
    return {
        timeslice_id: list(zip(rows["investment_periods"], rows["snapshots"]))
        for timeslice_id, rows in mapping.groupby("timeslice_id")
    }
