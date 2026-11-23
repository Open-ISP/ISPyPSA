"""Translate the new-format network tables into PyPSA buses and links.

This module sits in the translator stage, between the templater that produces
the ISPyPSA network tables and the PyPSA model build. A flow path is a corridor
between two sub-regions and a REZ connection joins a renewable energy zone to its
parent sub-region; both are just transmission paths, so the module routes them
through one pipeline and tells them apart (via isp_type) only where the model
must treat them differently.

The three sparse input tables (limits, expansion options, expansion costs) may
use blank key cells as wildcards; each is first put through _resolve_wildcards
(see translator/helpers.py) to expand those wildcards to concrete rows. Rows
the configuration deliberately excludes (unmodelled REZ paths, disabled
expansions, non-investment-period years) are filtered out before resolution.
For the limits table a path_id unknown to the paths table survives that
filtering, so _resolve_wildcards raises on it as bad input data. The options
and costs filters cannot make that distinction — an expansion_id outside the
enabled set may be a disabled element or a constraint group — so anything
outside the enabled elements or investment periods is filtered rather than
raised on.

_translate_network_to_links is the pipeline orchestrator and reads as the
step-by-step story; each helper's docstring carries an I/O example, and inline
comments mark the non-obvious modelling choices.
"""

import logging

import numpy as np
import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.model_years import _model_year_of_dates
from ispypsa.translator.helpers import (
    _annuitised_investment_costs,
    _resolve_wildcards,
)

_LINK_COLUMNS = [
    "isp_name",
    "name",
    "carrier",
    "bus0",
    "bus1",
    "p_nom",
    "p_min_pu",
    "p_max_pu",
    "build_year",
    "lifetime",
    "capital_cost",
    "p_nom_extendable",
    "isp_type",
]

_LINK_TIMESLICE_LIMIT_COLUMNS = ["name", "attribute", "timeslice", "value"]


def _translate_network_geography_to_buses(
    network_geography: pd.DataFrame, rezs: str
) -> pd.DataFrame:
    """Creates one PyPSA bus per geography in the model.

    REZ buses are only created when REZs are modelled as discrete nodes;
    with ``rezs="attached_to_parent_node"`` REZ-located components connect
    straight to the parent geography's bus.

    I/O Example:
        network_geography:
            geo_id  geo_type   region_id  subregion_id
            NQ      subregion  QLD        NQ
            Q1      rez        QLD        NQ

        rezs = "discrete_nodes" returns:
            name
            NQ
            Q1

        rezs = "attached_to_parent_node" returns:
            name
            NQ
    """
    buses = network_geography
    if rezs != "discrete_nodes":
        buses = buses[buses["geo_type"] != "rez"]
    buses = buses.loc[:, ["geo_id"]].rename(columns={"geo_id": "name"})
    return buses.reset_index(drop=True)


def _translate_network_to_links(
    ispypsa_tables: dict[str, pd.DataFrame],
    config: ModelConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Translates the network tables into PyPSA links and per-timeslice limits.

    Existing links carry the larger of their forward and reverse capacities as
    p_nom; limits for each demand condition are returned per unit of p_nom in
    the link_timeslice_limits table. Each committed capacity change becomes a
    further non-extendable link on its path, in service for the years the
    change applies (see _build_committed_change_links). Expansion links are
    built per investment
    period from the unified expansion options/costs tables (their total
    capacity is capped by the expansion-limit custom constraints built in
    ispypsa.translator.constraints).

    Returns:
        Tuple of (links, link_timeslice_limits) in PyPSA friendly format.
    """
    # Existing
    rez_ids = _rez_geo_ids(ispypsa_tables["network_geography"])
    paths = _drop_rez_paths_if_not_modelled(
        ispypsa_tables["network_transmission_paths"], rez_ids, config.network.nodes.rezs
    )
    limits = _drop_limits_for_unmodelled_paths(
        ispypsa_tables["network_transmission_path_limits"],
        ispypsa_tables["network_transmission_paths"],
        paths,
    )
    limits = _resolve_path_limits(
        limits,
        paths,
        config.network.transmission_default_limit,
    )
    changes = ispypsa_tables["network_transmission_path_capacity_changes"]
    changes = changes[changes["path_id"].isin(paths["path_id"])]
    # A table with no rows reads from CSV as text columns; the arithmetic on
    # capacity_change needs a numeric column either way.
    changes = changes.astype({"capacity_change": "float64"})
    limits = _remove_capacity_that_retires(limits, changes)
    existing_links = _build_existing_links(
        paths,
        limits,
        rez_ids,
        config.temporal.range.start_year,
    )
    existing_limits = _translate_timeslice_limits_to_pu(limits, existing_links)

    # Committed changes
    committed_links = _build_committed_change_links(
        changes,
        existing_links,
        config.temporal.year_type,
        config.temporal.range.start_year,
    )
    committed_limits = _static_limits_as_fallback_rows(committed_links)

    # Expansion
    enabled_ids = _enabled_expansion_element_ids(
        existing_links,
        config.network.transmission_expansion,
        config.network.rez_transmission_expansion,
    )
    options = _resolve_expansion_options(
        ispypsa_tables["network_expansion_options"], enabled_ids
    )
    options = _pair_forward_and_reverse_options(options)
    costs = _prepare_expansion_costs(
        ispypsa_tables["network_transmission_path_expansion_costs"],
        enabled_ids,
        config.temporal.capacity_expansion.investment_periods,
        config.wacc,
        config.network.annuitisation_lifetime,
    )
    expansion_links = _build_expansion_links(existing_links, options, costs)
    links = pd.concat(
        [existing_links, committed_links, expansion_links], ignore_index=True
    )
    link_timeslice_limits = pd.concat(
        [existing_limits, committed_limits], ignore_index=True
    )
    return links.loc[:, _LINK_COLUMNS], link_timeslice_limits


def _rez_geo_ids(network_geography: pd.DataFrame) -> set[str]:
    """geo_ids of the REZ entries in the geography.

    I/O Example:
        geo_id=NQ/geo_type=subregion, geo_id=Q1/geo_type=rez -> {"Q1"}
    """
    return set(network_geography.loc[network_geography["geo_type"] == "rez", "geo_id"])


def _drop_rez_paths_if_not_modelled(
    paths: pd.DataFrame, rez_ids: set[str], rezs: str
) -> pd.DataFrame:
    """Drops REZ-to-parent paths when REZs are not modelled as discrete nodes.

    I/O Example:
        paths:
            path_id  geo_from  geo_to
            CQ-NQ    CQ        NQ
            Q1-NQ    Q1        NQ       # Q1 is a REZ, so geo_from is in rez_ids

        rez_ids = {"Q1"}

        rezs = "attached_to_parent_node" returns:
            path_id  geo_from  geo_to
            CQ-NQ    CQ        NQ

        rezs = "discrete_nodes" returns paths unchanged.
    """
    if rezs == "discrete_nodes":
        return paths
    return paths[~paths["geo_from"].isin(rez_ids)]


def _drop_limits_for_unmodelled_paths(
    limits: pd.DataFrame, all_paths: pd.DataFrame, modelled_paths: pd.DataFrame
) -> pd.DataFrame:
    """Drops limit rows for paths configured out of the model.

    REZ-to-parent paths leave the modelled set when REZs are attached to their
    parent nodes (see _drop_rez_paths_if_not_modelled); their limit rows are
    a config-driven selection, filtered out here (logged at INFO) before wildcard
    resolution. Only rows naming a known-but-unmodelled path drop: blank (wildcard)
    path_ids ride through, and a path_id unknown to the paths table survives to
    _resolve_wildcards, which raises on it.

    I/O Example:
        all_paths:     path_id in {CQ-NQ, Q1-NQ}
        modelled_paths: path_id in {CQ-NQ}

        limits:
            path_id  direction  timeslice  capacity
            CQ-NQ    forward               1400
            Q1-NQ    forward               750    # known but unmodelled: dropped
                                           500    # blank: a wildcard, kept

        returns:
            path_id  direction  timeslice  capacity
            CQ-NQ    forward               1400
                                           500
    """
    unmodelled = set(all_paths["path_id"]) - set(modelled_paths["path_id"])
    dropped = limits["path_id"].isin(unmodelled)
    if dropped.any():
        logging.info(
            f"Filtered limit rows for paths configured out of the model: "
            f"{sorted(set(limits.loc[dropped, 'path_id']))}"
        )
    return limits[~dropped]


def _resolve_path_limits(
    limits: pd.DataFrame, paths: pd.DataFrame, default_limit: float
) -> pd.DataFrame:
    """Resolves the sparse limits table to one row per modelled path, direction
    and timeslice, then fills empty capacities with the system default.

    Blank path_id or direction values are wildcards (see the
    network_transmission_path_limits schema). The limits arrive already filtered
    to the modelled paths (see _drop_limits_for_unmodelled_paths), so
    _resolve_wildcards expands the path_id and direction wildcards against the
    modelled paths and the two directions, raising on any unknown path_id or
    direction; timeslice rides along, so a blank-timeslice row stays the
    snapshot-level fallback that pypsa_build applies later. A blank capacity then
    takes the configured transmission default — the nan_fill the schema declares
    for the capacity column — which is how a no-data path (a single all-wildcard
    row) becomes a default-limit link in both directions.

    I/O Example (blank cells are wildcards):
        limits:
            path_id  direction  timeslice  capacity
            CQ-NQ    forward               1400     # specific to CQ-NQ forward
                                           900      # blank path + direction: a global default
            N1-CNSW                                 # no-data row: blank direction and capacity

        paths: path_id in {CQ-NQ, N1-CNSW};  default_limit = 100000

        returns:
            path_id  direction  timeslice  capacity
            CQ-NQ    forward               1400     # the specific row
            CQ-NQ    reverse               900      # the global default fills CQ-NQ reverse
            N1-CNSW  forward               100000   # the no-data row is more specific than the
            N1-CNSW  reverse               100000   #   global default; its blank capacity -> default
    """
    allowed_values = {
        "path_id": list(paths["path_id"]),
        "direction": ["forward", "reverse"],
    }
    limits = _resolve_wildcards(limits, allowed_values, ["capacity"])
    limits["capacity"] = limits["capacity"].fillna(default_limit)
    return limits


def _build_existing_links(
    paths: pd.DataFrame,
    limits: pd.DataFrame,
    rez_ids: set[str],
    start_year: int,
) -> pd.DataFrame:
    """Builds one existing (non-extendable) PyPSA link per transmission path.

    I/O Example:
        paths:
            path_id  geo_from  geo_to  carrier
            CQ-NQ    CQ        NQ      AC
            N1-CNSW  N1        CNSW    AC      # REZ path, no limit data

        limits:
            path_id  direction  timeslice  capacity
            CQ-NQ    forward               1400
            CQ-NQ    reverse               1910
            N1-CNSW  forward               100000   # defaulted upstream
            N1-CNSW  reverse               100000

        returns (abridged):
            isp_name  name              bus0  bus1  p_nom   p_min_pu  isp_type
            CQ-NQ     CQ-NQ_existing    CQ    NQ    1910    0.0       flow_path  # p_nom = max(1400, 1910)
            N1-CNSW   N1-CNSW_existing  N1    CNSW  100000  0.0       rez
    """
    links = paths.rename(
        columns={"path_id": "isp_name", "geo_from": "bus0", "geo_to": "bus1"}
    )
    links = _add_p_nom(links, limits)
    links["name"] = links["isp_name"] + "_existing"
    links["isp_type"] = np.where(links["bus0"].isin(rez_ids), "rez", "flow_path")
    links["build_year"] = start_year - 1
    links["lifetime"] = np.inf
    links["capital_cost"] = np.nan
    links["p_nom_extendable"] = False
    # Defaults; the limits come from link_timeslice_limits, which overrides them.
    links["p_min_pu"] = 0.0
    links["p_max_pu"] = 1.0
    return links


def _add_p_nom(links: pd.DataFrame, limits: pd.DataFrame) -> pd.DataFrame:
    """Sets p_nom to the largest capacity across every limit row of the path.

    Both directions and all timeslices pool into one max: the timeslice = NaN
    fallback has to be included because at this stage we don't yet know which
    snapshots it will cover, and taking the larger direction keeps every
    per-unit limit in [-1, 1]. The link's reverse limit is not set here — it is
    carried per snapshot in link_timeslice_limits (a named timeslice or the
    timeslice = NaN fallback), which under the coverage contract
    (Open-ISP/ISPyPSA#123) sets p_min_pu on every snapshot, leaving the link's
    static p_min_pu at its inert default. No-data paths arrive already
    defaulted (see _resolve_path_limits), so they get the default limit like
    any other path.

    I/O Example:
        links:
            isp_name
            CQ-NQ
            PAR-NEW     # new parallel corridor, zero capacity

        limits:
            path_id  direction  timeslice        capacity
            CQ-NQ    forward    qld_peak_demand  1200
            CQ-NQ    forward                     1400   # NaN-timeslice fallback
            CQ-NQ    reverse                     1910
            PAR-NEW  forward                     0
            PAR-NEW  reverse                     0

        returns:
            isp_name  p_nom
            CQ-NQ     1910   # the reverse row is the largest
            PAR-NEW   0
    """
    max_capacity = (
        limits.groupby("path_id", as_index=False)["capacity"]
        .max()
        .rename(columns={"path_id": "isp_name", "capacity": "p_nom"})
    )
    return links.merge(max_capacity, on="isp_name", how="left")


def _translate_timeslice_limits_to_pu(
    limits: pd.DataFrame, existing_links: pd.DataFrame
) -> pd.DataFrame:
    """Expresses each limit per unit of its link's p_nom.

    Forward limits become p_max_pu values and reverse limits p_min_pu values.
    Named-timeslice rows carry their timeslice; a timeslice = NaN row is the
    fallback applied to snapshots no named timeslice covers (the coverage
    contract is Open-ISP/ISPyPSA#123). pypsa_build expands these into per-snapshot
    series via the timeslice_snapshots mapping.

    I/O Example:
        limits:
            path_id   direction  timeslice             capacity
            CQ-NQ     forward    qld_peak_demand       1200
            CQ-NQ     forward    qld_winter_reference  1400
            CQ-NQ     reverse    ,                     1000   # NaN-timeslice fallback
            CNSW-SNW  forward    nsw_peak_demand       0      # new parallel corridor
            CNSW-SNW  reverse    nsw_peak_demand       0

        existing_links (abridged):
            isp_name  name               p_nom
            CQ-NQ     CQ-NQ_existing     1400
            CNSW-SNW  CNSW-SNW_existing  0

        returns:
            name               attribute  timeslice             value
            CQ-NQ_existing     p_max_pu   qld_peak_demand       0.857   # 1200/1400
            CQ-NQ_existing     p_max_pu   qld_winter_reference  1.0     # 1400/1400
            CQ-NQ_existing     p_min_pu   ,                     -0.714  # fallback, -1000/1400
            CNSW-SNW_existing  p_max_pu   nsw_peak_demand       0.0     # zero p_nom: 0/0 defined as 0
            CNSW-SNW_existing  p_min_pu   nsw_peak_demand       0.0
    """
    rows = limits.merge(
        existing_links.loc[:, ["isp_name", "name", "p_nom"]],
        left_on="path_id",
        right_on="isp_name",
    )
    rows["attribute"] = rows["direction"].map(
        {"forward": "p_max_pu", "reverse": "p_min_pu"}
    )
    # Reverse flow is negative, so reverse limits become negative p_min_pu values.
    sign = rows["direction"].map({"forward": 1.0, "reverse": -1.0})
    # A zero-p_nom link (new parallel corridor) has only zero limits; 0/0 is
    # undefined, so its per-unit value is defined as 0.
    rows["value"] = np.where(
        rows["p_nom"] > 0, sign * rows["capacity"] / rows["p_nom"], 0.0
    )
    return rows.loc[:, _LINK_TIMESLICE_LIMIT_COLUMNS].reset_index(drop=True)


def _remove_capacity_that_retires(
    limits: pd.DataFrame, changes: pd.DataFrame
) -> pd.DataFrame:
    """Takes the capacity a committed reduction removes off the path's limits.

    The limits table holds a path's capacity as it stands today. Where a
    committed change reduces it, the existing link is left with the capacity
    that stays for the whole horizon, and the part that goes becomes a link of
    its own that retires on the change's date (see
    _build_committed_change_links). The reduction applies in every timeslice
    of its direction.

    I/O Example:
        limits:
            path_id  direction  timeslice        capacity
            SEV-MEL  forward    vic_peak_demand  7100
            SEV-MEL  forward    ,                8170
            SEV-MEL  reverse    ,                8170

        changes:
            path_id  direction  capacity_change  date_from
            SEV-MEL  forward    -1775            2028-07-01T00:00:00
            CQ-NQ    forward    500              2030-07-01T00:00:00   # an addition: ignored

        returns:
            path_id  direction  timeslice        capacity
            SEV-MEL  forward    vic_peak_demand  5325
            SEV-MEL  forward    ,                6395
            SEV-MEL  reverse    ,                8170
    """
    reductions = changes[changes["capacity_change"] < 0.0]
    reduction = reductions.groupby(["path_id", "direction"], as_index=False)[
        "capacity_change"
    ].sum()
    limits = limits.merge(reduction, on=["path_id", "direction"], how="left")
    limits["capacity"] = limits["capacity"] + limits["capacity_change"].fillna(0.0)
    return limits.drop(columns="capacity_change")


def _build_committed_change_links(
    changes: pd.DataFrame,
    existing_links: pd.DataFrame,
    year_type: str,
    start_year: int,
) -> pd.DataFrame:
    """Builds one non-extendable PyPSA link per committed capacity change.

    A committed change is a known, dated change to a path's capacity, such as
    HumeLink adding 2,200 MW to SNSW-CNSW. PyPSA has a link in service for the
    investment periods from its build_year until build_year + lifetime, so
    each change becomes a link that is in service for the years its capacity
    is there:

    - An addition is in service from the model year of its date_from, and
      until the model year of its date_to if it has one.
    - A reduction is the capacity that goes: its link is in service from
      before the model starts until the model year of its date_from. The
      existing link has already had that capacity taken off (see
      _remove_capacity_that_retires).

    A change's forward and reverse rows share a link. p_nom is the larger of
    the two, and p_max_pu and p_min_pu give each direction per unit of it, the
    same in every timeslice.

    I/O Example:
        changes:
            path_id    direction  capacity_change  date_from            date_to
            SNSW-CNSW  forward    2200             2027-12-02T00:00:00
            SNSW-CNSW  reverse    2200             2027-12-02T00:00:00
            SNSW-CNSW  forward    250              2027-08-01T00:00:00  2030-07-01T00:00:00
            SEV-MEL    forward    -1775            2028-07-01T00:00:00

        existing_links (abridged):
            isp_name   bus0  bus1  carrier  isp_type
            SNSW-CNSW  SNSW  CNSW  AC       flow_path
            SEV-MEL    SEV   MEL   AC       flow_path

        year_type = "fy", start_year = 2026

        returns (abridged):
            isp_name   name                            p_nom  p_max_pu  p_min_pu  build_year  lifetime
            SNSW-CNSW  SNSW-CNSW_committed_2027-12-02  2200   1.0       -1.0      2028        inf
            SNSW-CNSW  SNSW-CNSW_committed_2027-08-01  250    1.0       0.0       2028        3     # until FY2031
            SEV-MEL    SEV-MEL_retiring_2028-07-01     1775   1.0       0.0       2025        4     # until FY2029
    """
    links = _pair_forward_and_reverse_changes(changes)
    links = _add_years_in_service(links, year_type, start_year)
    links = links.merge(
        existing_links.loc[:, ["isp_name", "bus0", "bus1", "carrier", "isp_type"]],
        left_on="path_id",
        right_on="isp_name",
    )
    date = pd.to_datetime(links["date_from"]).dt.strftime("%Y-%m-%d")
    kind = np.where(links["is_reduction"], "_retiring_", "_committed_")
    links["name"] = links["path_id"] + kind + date
    capacity = links[["forward_change", "reverse_change"]].abs().max(axis=1)
    links["p_nom"] = capacity
    links["p_max_pu"] = links["forward_change"].abs() / capacity
    links["p_min_pu"] = -1.0 * links["reverse_change"].abs() / capacity
    links["capital_cost"] = np.nan
    links["p_nom_extendable"] = False
    return links


def _pair_forward_and_reverse_changes(changes: pd.DataFrame) -> pd.DataFrame:
    """Pairs each change's forward and reverse rows into one row.

    A change with a row for only one direction leaves the other at zero.

    I/O Example:
        changes:
            path_id    direction  capacity_change  date_from            date_to
            SNSW-CNSW  forward    2200             2027-12-02T00:00:00
            SNSW-CNSW  reverse    2200             2027-12-02T00:00:00
            SEV-MEL    forward    -1775            2028-07-01T00:00:00

        returns:
            path_id    date_from            date_to  forward_change  reverse_change
            SNSW-CNSW  2027-12-02T00:00:00           2200            2200
            SEV-MEL    2028-07-01T00:00:00           -1775           0
    """
    keys = ["path_id", "date_from", "date_to"]
    is_forward = changes["direction"] == "forward"
    forward = changes.loc[is_forward, keys + ["capacity_change"]]
    reverse = changes.loc[~is_forward, keys + ["capacity_change"]]
    forward = forward.rename(columns={"capacity_change": "forward_change"})
    reverse = reverse.rename(columns={"capacity_change": "reverse_change"})
    paired = forward.merge(reverse, on=keys, how="outer")
    return paired.fillna({"forward_change": 0.0, "reverse_change": 0.0})


def _add_years_in_service(
    changes: pd.DataFrame, year_type: str, start_year: int
) -> pd.DataFrame:
    """Adds the build_year and lifetime that put each change's link in service
    for the right years, and is_reduction.

    An addition is built in the model year of its date_from and lasts until
    the model year of its date_to, or forever when date_to is blank. The
    capacity a reduction removes is built the year before the model starts,
    like an existing link, and lasts until the model year of its date_from.

    I/O Example (year_type = "fy", start_year = 2026):
        changes:
            path_id    date_from            date_to              forward_change  reverse_change
            SNSW-CNSW  2027-12-02T00:00:00                       2200            2200
            SNSW-CNSW  2027-08-01T00:00:00  2030-07-01T00:00:00  250             0
            SEV-MEL    2028-07-01T00:00:00                       -1775           0

        returns (the input columns, plus):
            path_id    is_reduction  build_year  lifetime
            SNSW-CNSW  False         2028        inf
            SNSW-CNSW  False         2028        3         # FY2031 - FY2028
            SEV-MEL    True          2025        4         # FY2029 - 2025
    """
    changes = changes.copy()
    date_from = pd.to_datetime(changes["date_from"])
    date_to = pd.to_datetime(changes["date_to"])
    year_from = _model_year_of_dates(year_type, date_from)
    year_to = pd.Series(np.inf, index=changes.index)
    is_temporary = date_to.notna()
    year_to[is_temporary] = _model_year_of_dates(year_type, date_to[is_temporary])
    is_reduction = (changes["forward_change"] < 0.0) | (changes["reverse_change"] < 0.0)
    year_before_model = start_year - 1
    changes["is_reduction"] = is_reduction
    changes["build_year"] = year_from.where(~is_reduction, year_before_model)
    changes["lifetime"] = (year_to - year_from).where(
        ~is_reduction, year_from - year_before_model
    )
    return changes


def _static_limits_as_fallback_rows(links: pd.DataFrame) -> pd.DataFrame:
    """Writes each link's p_max_pu and p_min_pu as blank-timeslice rows of the
    link_timeslice_limits table, so they apply at every snapshot.

    I/O Example:
        links (abridged):
            name                            p_max_pu  p_min_pu
            SNSW-CNSW_committed_2027-12-02  1.0       -1.0

        returns:
            name                            attribute  timeslice  value
            SNSW-CNSW_committed_2027-12-02  p_max_pu   ,          1.0
            SNSW-CNSW_committed_2027-12-02  p_min_pu   ,          -1.0
    """
    limits = links.melt(
        id_vars="name",
        value_vars=["p_max_pu", "p_min_pu"],
        var_name="attribute",
        value_name="value",
    )
    limits["timeslice"] = np.nan
    return limits.loc[:, _LINK_TIMESLICE_LIMIT_COLUMNS]


def _enabled_expansion_element_ids(
    existing_links: pd.DataFrame,
    transmission_expansion: bool,
    rez_transmission_expansion: bool,
) -> list[str]:
    """The isp_names of the existing links whose expansion the config enables.

    ``transmission_expansion`` gates flow paths between (sub)regions;
    ``rez_transmission_expansion`` gates REZ connection paths. The result is the
    enabled expansion_id set the options and costs tables are filtered to (see
    _keep_rows_for_expansion_ids) and then resolved against, so an option or
    cost for a disabled or non-modelled element drops out before resolution.

    I/O Example:
        existing_links:
            isp_name  isp_type
            CQ-NQ     flow_path
            Q1-NQ     rez

        transmission_expansion=True, rez_transmission_expansion=False
            returns ["CQ-NQ"]   # rez path Q1-NQ gated out
    """
    enabled = set()
    if transmission_expansion:
        is_flow_path = existing_links["isp_type"] == "flow_path"
        enabled |= set(existing_links.loc[is_flow_path, "isp_name"])
    if rez_transmission_expansion:
        is_rez = existing_links["isp_type"] == "rez"
        enabled |= set(existing_links.loc[is_rez, "isp_name"])
    return sorted(enabled)


def _resolve_expansion_options(
    options: pd.DataFrame, enabled_ids: list[str]
) -> pd.DataFrame:
    """Resolves the expansion-options wildcards to one row per enabled element
    and physical direction.

    A blank expansion_id is a system-wide default option, and a blank
    expansion_type covers both directions of one element in a single (symmetric)
    row. constraint_relaxation rows are not physical paths — they are set aside
    first and become relaxation generators in ispypsa.translator.constraints.
    Options for disabled or non-modelled elements are config-driven selection,
    filtered out next. _resolve_wildcards then expands the
    blanks against the enabled elements and the two physical directions.

    Each element's forward and reverse must form a coherent pair from one option
    (the schema's expansion_options_pair_forward_and_reverse rule);
    _check_both_directions_defined and _check_forward_reverse_share_an_option
    stand in for that rule once resolved, until schema validation lands.

    I/O Example (blank cells are wildcards; each element's forward and reverse
    share one expansion_option):
        options:
            expansion_id  expansion_type         allowed_expansion  expansion_option
            CQ-NQ         forward                1000               BigLine
            CQ-NQ         reverse                800                BigLine
                          forward                500                Default   # blank id:
                          reverse                400                Default   #   a default pair
            SWQLD1        constraint_relaxation  400                Relax     # standalone, dropped

        enabled_ids = ["CQ-NQ", "Q1-NQ"]

        returns (CQ-NQ keeps its BigLine pair; Q1-NQ inherits the Default pair):
            expansion_id  expansion_type  allowed_expansion  expansion_option
            CQ-NQ         forward         1000               BigLine
            CQ-NQ         reverse         800                BigLine
            Q1-NQ         forward         500                Default
            Q1-NQ         reverse         400                Default
    """
    # constraint_relaxation options are routed to ispypsa.translator.constraints,
    # not dropped, so they are set aside before wildcard resolution (which would
    # otherwise raise on them).
    options = options[options["expansion_type"] != "constraint_relaxation"]
    options = _keep_rows_for_expansion_ids(options, enabled_ids)
    allowed_values = {
        "expansion_id": enabled_ids,
        "expansion_type": ["forward", "reverse"],
    }
    options = _resolve_wildcards(
        options,
        allowed_values,
        ["allowed_expansion", "expansion_option"],
    )
    _check_both_directions_defined(options)
    _check_forward_reverse_share_an_option(options)
    return options


def _keep_rows_for_expansion_ids(
    table: pd.DataFrame, expansion_ids: list[str]
) -> pd.DataFrame:
    """Keeps rows whose expansion_id is one of the given ids or blank (a wildcard).

    Which ids to keep is a config-driven selection made by the caller — the
    enabled paths here, the constraints in the model in
    ispypsa.translator.constraints — so it happens before wildcard resolution
    rather than inside it. Rows for any other id drop out; the blank rows are
    kept because they are wildcards that resolve against the given ids.

    I/O Example:
        table:                                expansion_ids = ["CQ-NQ"]
            expansion_id  year  cost
            CQ-NQ         2026  1000000
            Q1-NQ         2026  500000    # not in expansion_ids: dropped
            SWQLD1        2026  400000    # not in expansion_ids: dropped
                          2026  900000    # blank: a wildcard, kept

        returns:
            expansion_id  year  cost
            CQ-NQ         2026  1000000
                          2026  900000
    """
    ids = table["expansion_id"]
    keep = ids.isna() | ids.isin(expansion_ids)
    return table[keep]


def _check_both_directions_defined(options: pd.DataFrame) -> None:
    """Raises if any element's resolved options define only one direction.

    The schema's expansion_options_pair_forward_and_reverse rule requires an
    expansion_id that defines one direction to define the other; an element
    arriving with only a forward (or only a reverse) row would otherwise produce
    an expansion link with a NaN rating in the missing direction. This stands in
    until schema validation lands.

    I/O Example:
        options with only a CQ-NQ forward row -> raises (no CQ-NQ reverse).
    """
    directions = options.groupby("expansion_id")["expansion_type"].nunique()
    one_directional = sorted(directions[directions < 2].index)
    if one_directional:
        raise ValueError(
            f"Expansion options define only one direction for {one_directional}; "
            "each element needs both forward and reverse (a blank expansion_type "
            "row covers both)."
        )


def _check_forward_reverse_share_an_option(options: pd.DataFrame) -> None:
    """Raises if any element's resolved forward and reverse trace to different
    options.

    Forward and reverse for an expansion_id must come from one physical option, so
    the single cost keyed on expansion_id applies to a coherent pair. Wildcard
    resolution resolves the two directions independently, so a malformed input
    could pair a forward from one option with a reverse from another; this guard
    catches that. A blank expansion_option counts as its own label (nunique with
    dropna=False), so an unlabelled direction only pairs with another unlabelled
    one — nunique's default NaN-dropping would otherwise let a blank/named
    mismatch through. The schema's expansion_options_pair_forward_and_reverse
    rule is the upstream enforcement — this stands in until schema validation
    lands.

    I/O Example:
        options with CQ-NQ forward from "BigLine" and CQ-NQ reverse from "Default"
        -> raises (the two directions disagree on the option).
    """
    options_per_element = options.groupby("expansion_id")["expansion_option"].nunique(
        dropna=False
    )
    mismatched = sorted(options_per_element[options_per_element > 1].index)
    if mismatched:
        raise ValueError(
            f"Forward and reverse expansion options disagree for {mismatched}; "
            "each element's forward and reverse must come from the same option."
        )


def _pair_forward_and_reverse_options(options: pd.DataFrame) -> pd.DataFrame:
    """Pairs each element's forward and reverse expansion rows into one row.

    The options arrive already resolved to physical forward/reverse rows (see
    _resolve_expansion_options), so this only reshapes them.

    I/O Example:
        options:
            expansion_id  expansion_type  allowed_expansion
            CQ-NQ         forward         1000
            CQ-NQ         reverse         900

        returns:
            expansion_id  forward_capacity  reverse_capacity
            CQ-NQ         1000              900
    """
    paired = (
        options.pivot(
            index="expansion_id", columns="expansion_type", values="allowed_expansion"
        )
        .reindex(columns=["forward", "reverse"])
        .rename(columns={"forward": "forward_capacity", "reverse": "reverse_capacity"})
    )
    paired.columns.name = None
    return paired.reset_index()


def _prepare_expansion_costs(
    expansion_costs: pd.DataFrame,
    enabled_ids: list[str],
    investment_periods: list[int],
    wacc: float,
    asset_lifetime: int,
) -> pd.DataFrame:
    """Resolves the expansion-cost wildcards to the given expansion ids and
    investment periods, then annuitises them.

    Blank expansion_id or year cells are wildcards (see the
    network_transmission_path_expansion_costs schema): an empty expansion_id is a
    table-wide default cost, an empty year a static cost across the investment
    periods. The caller decides which ids the costs are for — the enabled paths
    here, the constraints in the model in ispypsa.translator.constraints — so
    rows for any other id, and rows for years that are not investment periods,
    are dropped before resolution: they are selection, not bad data.
    _resolve_wildcards then expands the blanks against the given ids and the
    investment periods. A blank cost then resolves to free — the nan_fill the
    schema declares for the cost column. Year values are labels matched against
    the config's investment periods as ints — no financial vs calendar year
    interpretation happens here; the config's year_type decides what span of
    time the labels denote, so the table just has to label years the same way
    the config does (templated tables carry financial-year ending years).

    I/O Example (a blank year is a static cost across the investment periods):
        expansion_costs:
            expansion_id  year  cost
            CQ-NQ               1000   # blank year: applies to every investment period
            CQ-NQ         2028  1200   # a specific-year override
            Q1-NQ         2025  900    # 2025 is not an investment period -> dropped

        enabled_ids=["CQ-NQ", "Q1-NQ"], investment_periods=[2026, 2028], wacc, asset_lifetime

        returns (cost annuitised; Q1-NQ falls away, its only row was out of period):
            expansion_id  year  capital_cost
            CQ-NQ         2026  annuitise(1000)   # the static row fills 2026
            CQ-NQ         2028  annuitise(1200)   # the 2028 override beats the static row
    """
    costs = _keep_rows_for_expansion_ids(expansion_costs, enabled_ids)
    costs = _keep_rows_for_investment_period_years(costs, investment_periods)
    allowed_values = {"expansion_id": enabled_ids, "year": investment_periods}
    costs = _resolve_wildcards(costs, allowed_values, ["cost"])
    # Every surviving year is now an investment period; cast so expansion link
    # names read e.g. CQ-NQ_exp_2026 rather than CQ-NQ_exp_2026.0.
    costs["year"] = costs["year"].astype("int64")
    costs["cost"] = costs["cost"].fillna(0.0)
    costs["capital_cost"] = _annuitised_investment_costs(
        costs["cost"], wacc, asset_lifetime
    )
    return costs.loc[:, ["expansion_id", "year", "capital_cost"]]


def _keep_rows_for_investment_period_years(
    costs: pd.DataFrame, investment_periods: list[int]
) -> pd.DataFrame:
    """Keeps cost rows whose year is an investment period or blank (a wildcard).

    The costs table spans the IASR horizon while the model only prices the
    configured investment periods, so selecting those years is config-driven and
    happens before wildcard resolution rather than inside it. The filtered years
    are logged at INFO.

    I/O Example:
        costs:                          investment_periods = [2026, 2028]
            expansion_id  year  cost
            CQ-NQ         2025  900000    # not an investment period: dropped
            CQ-NQ         2026  1000000
            CQ-NQ               800000    # blank: a wildcard, kept

        returns:
            expansion_id  year  cost
            CQ-NQ         2026  1000000
            CQ-NQ               800000
    """
    years = costs["year"]
    keep = years.isna() | years.isin(investment_periods)
    if not keep.all():
        logging.info(
            f"Filtered expansion cost rows for years outside the investment "
            f"periods: {sorted(int(year) for year in set(years[~keep]))}"
        )
    return costs[keep]


def _build_expansion_links(
    existing_links: pd.DataFrame,
    options: pd.DataFrame,
    costs: pd.DataFrame,
) -> pd.DataFrame:
    """Builds one extendable PyPSA link per expandable path and investment period.

    p_nom starts at 0 and is unbounded on its own — the expansion-limit custom
    constraints built in ispypsa.translator.constraints cap the p_nom built
    across a path's expansion links at max(forward, reverse) of the selected
    option. p_max_pu and p_min_pu are the forward and reverse capacities per unit
    of that max, so whatever p_nom is built delivers forward and reverse flow in
    the option's proportion, both bounded by [-1, 1]. Using the max as the
    denominator means only a both-zero option divides by zero, which falls back
    to symmetric defaults.

    I/O Example:
        options:
            expansion_id  forward_capacity  reverse_capacity
            CQ-NQ         1000              900

        costs:
            expansion_id  year  capital_cost
            CQ-NQ         2026  81.4

        existing_links (abridged):
            isp_name  bus0  bus1  carrier  isp_type
            CQ-NQ     CQ    NQ    AC       flow_path

        returns (abridged):
            isp_name  name            p_nom  p_nom_extendable  p_max_pu  p_min_pu  build_year  capital_cost
            CQ-NQ     CQ-NQ_exp_2026  0.0    True              1.0       -0.9      2026        81.4
    """
    links = costs.merge(options, on="expansion_id")
    links = links.merge(
        existing_links.loc[:, ["isp_name", "bus0", "bus1", "carrier", "isp_type"]],
        left_on="expansion_id",
        right_on="isp_name",
    )
    links["name"] = links["expansion_id"] + "_exp_" + links["year"].astype(str)
    # Starts at 0 and is unbounded on its own; ispypsa.translator.constraints caps the
    # p_nom built across a path's expansion links at max(forward, reverse) of the option.
    links["p_nom"] = 0.0
    links["p_nom_extendable"] = True
    # Per unit of max(forward, reverse) keeps both ratings in [-1, 1]; only a both-zero
    # option hits the symmetric divide-by-zero fallback.
    capacity = links[["forward_capacity", "reverse_capacity"]].max(axis=1)
    links["p_max_pu"] = np.where(
        capacity > 0, links["forward_capacity"] / capacity, 1.0
    )
    links["p_min_pu"] = np.where(
        capacity > 0, -1.0 * links["reverse_capacity"] / capacity, -1.0
    )
    links["build_year"] = links["year"]
    links["lifetime"] = np.inf
    return links
