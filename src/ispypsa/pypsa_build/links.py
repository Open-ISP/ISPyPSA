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

    When the new-format per-timeslice limit tables are given, links with
    timeslice-varying limits get per-snapshot p_max_pu / p_min_pu series
    instead of their static values (see _build_link_pu_overrides).

    Args:
        network: The `pypsa.Network` object
        links: `pd.DataFrame` with `PyPSA` style `Link` attributes.
        link_timeslice_limits: `pd.DataFrame` with per-timeslice per-unit
            limits (columns name, attribute, timeslice, value), or None when
            all link limits are static.
        timeslice_snapshots: `pd.DataFrame` mapping timeslice_ids to the
            snapshots they are active at (columns timeslice_id,
            investment_periods, snapshots). Required when
            link_timeslice_limits is given.

    Returns: None
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

    Each series starts at the link's static value (1.0 for p_max_pu, the
    links table's p_min_pu for p_min_pu — both equivalent to the
    winter_reference limit) and gets each timeslice's value at the snapshots
    that timeslice is active. Timeslices with no snapshots simply leave the
    static value in place — the translator has already logged them.

    I/O Example:
        link_timeslice_limits:
            name            attribute  timeslice        value
            CQ-NQ_existing  p_max_pu   qld_peak_demand  0.857

        timeslice_snapshots: qld_peak_demand active at (2025, 2025-01-13 12:00)
        snapshots: (2025, 2025-01-13 12:00), (2025, 2025-01-15 12:00)

        returns:
            {"CQ-NQ_existing": {"p_max_pu": series [0.857, 1.0]}}
    """
    if link_timeslice_limits is None or link_timeslice_limits.empty:
        return {}
    timeslice_labels = _timeslice_snapshot_labels(timeslice_snapshots)
    static_p_min_pu = links.set_index("name")["p_min_pu"]
    overrides = {}
    for (name, attribute), rows in link_timeslice_limits.groupby(["name", "attribute"]):
        static_value = 1.0 if attribute == "p_max_pu" else static_p_min_pu[name]
        series = pd.Series(static_value, index=snapshots, dtype=float)
        for row in rows.itertuples():
            series.loc[timeslice_labels.get(row.timeslice, [])] = row.value
        overrides.setdefault(name, {})[attribute] = series
    return overrides


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
