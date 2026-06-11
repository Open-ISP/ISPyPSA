"""Translates the new-format network tables into PyPSA friendly buses and links.

Consumes ``network_geography``, ``network_transmission_paths``,
``network_transmission_path_limits``, ``network_expansion_options`` and
``network_transmission_path_expansion_costs``, handling flow paths and REZ
connections through one unified pipeline (both are just paths).
"""

import numpy as np
import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.translator.helpers import _annuitised_investment_costs

_LINK_COLUMNS = [
    "isp_name",
    "name",
    "carrier",
    "bus0",
    "bus1",
    "p_nom",
    "p_min_pu",
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

    Existing links carry the winter_reference limit as p_nom; limits for the
    other demand conditions are returned per unit of p_nom in the
    link_timeslice_limits table. Expansion links are built per investment
    period from the unified expansion options/costs tables (their total
    capacity is capped by the expansion-limit custom constraints built in
    ispypsa.translator.constraints).

    Returns:
        Tuple of (links, link_timeslice_limits) in PyPSA friendly format.
    """
    rez_ids = _rez_geo_ids(ispypsa_tables["network_geography"])
    paths = _drop_rez_paths_if_not_modelled(
        ispypsa_tables["network_transmission_paths"], rez_ids, config.network.nodes.rezs
    )
    limits = ispypsa_tables["network_transmission_path_limits"]
    static_limits = _extract_static_limits(limits)
    existing_links = _build_existing_links(
        paths,
        static_limits,
        rez_ids,
        config.network.rez_to_sub_region_transmission_default_limit,
        config.temporal.range.start_year,
    )
    link_timeslice_limits = _translate_timeslice_limits_to_pu(limits, existing_links)
    options = _pivot_physical_expansion_options(
        ispypsa_tables["network_expansion_options"]
    )
    options = _filter_options_to_enabled_expansion(
        options,
        existing_links,
        config.network.transmission_expansion,
        config.network.rez_transmission_expansion,
    )
    costs = _prepare_expansion_costs(
        ispypsa_tables["network_transmission_path_expansion_costs"],
        config.temporal.capacity_expansion.investment_periods,
        config.temporal.year_type,
        config.wacc,
        config.network.annuitisation_lifetime,
    )
    expansion_links = _build_expansion_links(existing_links, options, costs)
    links = pd.concat([existing_links, expansion_links], ignore_index=True)
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
        paths (path_ids): CQ-NQ, Q1-NQ (Q1 is a REZ)

        rezs = "attached_to_parent_node" returns paths: CQ-NQ
        rezs = "discrete_nodes" returns paths unchanged.
    """
    if rezs == "discrete_nodes":
        return paths
    return paths[~paths["geo_from"].isin(rez_ids)]


def _extract_static_limits(limits: pd.DataFrame) -> pd.DataFrame:
    """Reduces the per-timeslice limits to one static capacity per path and
    direction: the winter_reference limit.

    Winter is used as the link's p_nom because it is the demand condition left
    over when no summer timeslice is active — limits for the other conditions
    are applied per unit of it. Collapsed all-NaN rows (paths with no limit
    data) contribute nothing; paths absent from the result get the default
    limit downstream.

    I/O Example:
        limits:
            path_id  direction  timeslice             capacity
            CQ-NQ    forward    qld_peak_demand       1200
            CQ-NQ    forward    qld_winter_reference  1400
            CQ-NQ    reverse    qld_winter_reference  1910
            N1-CNSW  ,          ,                             # collapsed row

        returns:
            path_id  direction  capacity
            CQ-NQ    forward    1400
            CQ-NQ    reverse    1910
    """
    winter = limits[limits["timeslice"].str.endswith("_winter_reference", na=False)]
    return winter.loc[:, ["path_id", "direction", "capacity"]]


def _build_existing_links(
    paths: pd.DataFrame,
    static_limits: pd.DataFrame,
    rez_ids: set[str],
    default_limit: float,
    start_year: int,
) -> pd.DataFrame:
    """Builds one existing (non-extendable) PyPSA link per transmission path.

    I/O Example:
        paths:
            path_id  geo_from  geo_to  carrier
            CQ-NQ    CQ        NQ      AC
            N1-CNSW  N1        CNSW    AC      # REZ path, no limit data

        static_limits:
            path_id  direction  capacity
            CQ-NQ    forward    1400
            CQ-NQ    reverse    1910

        returns (abridged):
            isp_name  name              bus0  bus1  p_nom   p_min_pu  isp_type
            CQ-NQ     CQ-NQ_existing    CQ    NQ    1400    -1.364    flow_path
            N1-CNSW   N1-CNSW_existing  N1    CNSW  100000  -1.0      rez       # default limit
    """
    links = paths.rename(
        columns={"path_id": "isp_name", "geo_from": "bus0", "geo_to": "bus1"}
    )
    links = _add_static_capacities(links, static_limits, default_limit)
    links["name"] = links["isp_name"] + "_existing"
    links["isp_type"] = np.where(links["bus0"].isin(rez_ids), "rez", "flow_path")
    links["build_year"] = start_year - 1
    links["lifetime"] = np.inf
    links["capital_cost"] = np.nan
    links["p_nom_extendable"] = False
    return links


def _add_static_capacities(
    links: pd.DataFrame, static_limits: pd.DataFrame, default_limit: float
) -> pd.DataFrame:
    """Merges the static forward limit as p_nom and the reverse limit as p_min_pu.

    Paths with no static limit in a direction get the default: ``default_limit``
    as p_nom (the path is constraint-modelled with no explicit physical limit)
    and a symmetric -1.0 as p_min_pu. Zero-capacity paths (new parallel
    corridors) also get a symmetric p_min_pu, avoiding a 0/0 division.

    I/O Example:
        links (isp_name): CQ-NQ, N1-CNSW
        static_limits: CQ-NQ forward 1400, CQ-NQ reverse 1910

        returns:
            isp_name  p_nom   p_min_pu
            CQ-NQ     1400    -1.364     # -1910/1400
            N1-CNSW   100000  -1.0       # defaults
    """
    directions = static_limits.pivot(
        index="path_id", columns="direction", values="capacity"
    ).reindex(columns=["forward", "reverse"])
    links = links.merge(
        directions, left_on="isp_name", right_index=True, how="left"
    ).rename(columns={"forward": "p_nom"})
    links["p_min_pu"] = np.where(
        links["reverse"].notna() & (links["p_nom"] > 0),
        -1.0 * links["reverse"] / links["p_nom"],
        -1.0,
    )
    links["p_nom"] = links["p_nom"].fillna(default_limit)
    return links.drop(columns=["reverse"])


def _translate_timeslice_limits_to_pu(
    limits: pd.DataFrame, existing_links: pd.DataFrame
) -> pd.DataFrame:
    """Expresses each per-timeslice limit per unit of its link's p_nom.

    Forward limits become p_max_pu values and reverse limits p_min_pu values.
    pypsa_build expands them into per-snapshot series via the
    timeslice_snapshots mapping; snapshots outside any of a link's tagged
    timeslices keep the static (winter_reference) limit. Zero-p_nom links
    (new parallel corridors) are skipped — all their limits are zero and the
    per-unit form is undefined.

    I/O Example:
        limits:
            path_id  direction  timeslice             capacity
            CQ-NQ    forward    qld_peak_demand       1200
            CQ-NQ    forward    qld_winter_reference  1400
            CQ-NQ    reverse    qld_peak_demand       1440

        existing_links (abridged):
            isp_name  name            p_nom
            CQ-NQ     CQ-NQ_existing  1400

        returns:
            name            attribute  timeslice             value
            CQ-NQ_existing  p_max_pu   qld_peak_demand       0.857
            CQ-NQ_existing  p_max_pu   qld_winter_reference  1.0
            CQ-NQ_existing  p_min_pu   qld_peak_demand       -1.029
    """
    rows = limits.dropna(subset=["timeslice", "capacity"])
    rows = rows.merge(
        existing_links.loc[:, ["isp_name", "name", "p_nom"]],
        left_on="path_id",
        right_on="isp_name",
    )
    rows = rows[rows["p_nom"] > 0]
    rows["attribute"] = rows["direction"].map(
        {"forward": "p_max_pu", "reverse": "p_min_pu"}
    )
    sign = rows["direction"].map({"forward": 1.0, "reverse": -1.0})
    rows["value"] = sign * rows["capacity"] / rows["p_nom"]
    return rows.loc[:, _LINK_TIMESLICE_LIMIT_COLUMNS].reset_index(drop=True)


def _pivot_physical_expansion_options(expansion_options: pd.DataFrame) -> pd.DataFrame:
    """Pairs each physical path's forward and reverse expansion rows into one row.

    constraint_relaxation rows are not physical paths — they are translated
    into custom-constraint relaxation generators by
    ispypsa.translator.constraints.

    I/O Example:
        expansion_options:
            expansion_id  expansion_type         allowed_expansion  expansion_option
            CQ-NQ         forward                1000               Option 1
            CQ-NQ         reverse                900                Option 1
            SWQLD1        constraint_relaxation  500                Option 2

        returns:
            expansion_id  forward_capacity  reverse_capacity
            CQ-NQ         1000              900
    """
    physical = expansion_options[
        expansion_options["expansion_type"].isin(["forward", "reverse"])
    ]
    options = (
        physical.pivot(
            index="expansion_id", columns="expansion_type", values="allowed_expansion"
        )
        .reindex(columns=["forward", "reverse"])
        .rename(columns={"forward": "forward_capacity", "reverse": "reverse_capacity"})
    )
    options.columns.name = None
    return options.reset_index()


def _filter_options_to_enabled_expansion(
    options: pd.DataFrame,
    existing_links: pd.DataFrame,
    transmission_expansion: bool,
    rez_transmission_expansion: bool,
) -> pd.DataFrame:
    """Keeps the expansion options enabled by the network config flags.

    ``transmission_expansion`` gates paths between (sub)regions;
    ``rez_transmission_expansion`` gates REZ connection paths. Options for
    paths not in the model (e.g. REZ paths dropped under
    attached_to_parent_node) drop out here too, as they match no link.

    I/O Example:
        options (expansion_ids): CQ-NQ, Q1-NQ
        existing_links: CQ-NQ isp_type=flow_path, Q1-NQ isp_type=rez

        transmission_expansion=True, rez_transmission_expansion=False
        returns options: CQ-NQ
    """
    rez_path_ids = set(
        existing_links.loc[existing_links["isp_type"] == "rez", "isp_name"]
    )
    flow_path_ids = set(
        existing_links.loc[existing_links["isp_type"] == "flow_path", "isp_name"]
    )
    enabled = set()
    if transmission_expansion:
        enabled |= flow_path_ids
    if rez_transmission_expansion:
        enabled |= rez_path_ids
    return options[options["expansion_id"].isin(enabled)]


def _prepare_expansion_costs(
    expansion_costs: pd.DataFrame,
    investment_periods: list[int],
    year_type: str,
    wacc: float,
    asset_lifetime: int,
) -> pd.DataFrame:
    """Filters the long-format expansion costs to the investment periods and
    annuitises them.

    The ``year`` column holds financial-year ending years as ints, matching
    the investment period labels used with ``year_type="fy"``.

    I/O Example:
        expansion_costs:
            expansion_id  year  cost
            CQ-NQ         2025  1000
            CQ-NQ         2026  1010

        investment_periods=[2026], wacc=0.07, asset_lifetime=30 returns:
            expansion_id  year  capital_cost
            CQ-NQ         2026  81.4
    """
    if year_type != "fy":
        raise NotImplementedError(
            f"Network expansion costs are not implemented for year_type: {year_type}"
        )
    costs = expansion_costs[expansion_costs["year"].isin(investment_periods)].copy()
    costs["capital_cost"] = costs["cost"].apply(
        lambda cost: _annuitised_investment_costs(cost, wacc, asset_lifetime)
    )
    return costs.loc[:, ["expansion_id", "year", "capital_cost"]]


def _build_expansion_links(
    existing_links: pd.DataFrame,
    options: pd.DataFrame,
    costs: pd.DataFrame,
) -> pd.DataFrame:
    """Builds one extendable PyPSA link per expandable path and investment period.

    Each expansion link is unbounded on its own — the expansion-limit custom
    constraints built in ispypsa.translator.constraints cap the p_nom built
    across a path's expansion links at the selected option's forward capacity.
    Asymmetric options are modelled with p_min_pu = -reverse/forward, so
    however much forward capacity is built, reverse capacity scales in the
    option's proportion.

    I/O Example:
        options:
            expansion_id  forward_capacity  reverse_capacity
            CQ-NQ         1000              900

        costs:
            expansion_id  year  capital_cost
            CQ-NQ         2026  81.4

        existing_links (abridged): CQ-NQ bus0=CQ bus1=NQ carrier=AC isp_type=flow_path

        returns (abridged):
            isp_name  name           p_nom  p_nom_extendable  p_min_pu  build_year  capital_cost
            CQ-NQ     CQ-NQ_exp_2026 0.0    True              -0.9      2026        81.4
    """
    links = costs.merge(options, on="expansion_id")
    links = links.merge(
        existing_links.loc[:, ["isp_name", "bus0", "bus1", "carrier", "isp_type"]],
        left_on="expansion_id",
        right_on="isp_name",
    )
    links["name"] = links["expansion_id"] + "_exp_" + links["year"].astype(str)
    links["p_nom"] = 0.0
    links["p_nom_extendable"] = True
    links["p_min_pu"] = -1.0 * links["reverse_capacity"] / links["forward_capacity"]
    links["build_year"] = links["year"]
    links["lifetime"] = np.inf
    return links
