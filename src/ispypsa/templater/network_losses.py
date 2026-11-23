import numpy as np
import pandas as pd

from ispypsa.templater.helpers import _build_geo_region_lookup
from ispypsa.templater.transmission import (
    _add_flow_path_carrier,
    _aggregate_to_granularity,
    _parse_flow_path_topology,
)

# Loss groups whose expansion is a separate asset in parallel with the existing
# one, so the existing loss curve says nothing about it. TAS-SEV is the only
# one: the existing path is Basslink and its expansion is Marinus Link, a
# second cable. The expansion gets a flat loss factor instead, taken from the
# named option's IASR loss equation at half of the option's capacity (MW).
#
# Every other group's expansion is treated as an upgrade of the existing
# network, which the group's own loss curve covers.
_PARALLEL_EXPANSIONS = pd.DataFrame(
    [("TAS-SEV", "TAS-VIC Option 1 (Marinus Link Stage 1)", 750.0)],
    columns=["loss_group", "option", "capacity"],
)

_LOSS_COLUMNS = [
    "loss_group",
    "direction",
    "loss_constant",
    "loss_linear",
    "loss_quadratic",
    "to_loss_proportion",
    "expansion_loss_factor",
    "date_from",
]


def _template_network_losses(
    loss_equations: pd.DataFrame,
    flow_path_transfer_capability: pd.DataFrame,
    renewable_energy_zones: pd.DataFrame,
    sub_regional_geography: pd.DataFrame,
    regional_granularity: str,
) -> pd.DataFrame:
    """Creates the network_transmission_losses table.

    Power is lost as it flows along a transmission path, and the IASR workbook
    gives the loss on each flow path as a curve: a quadratic in the flow, in MW.
    A loss group is a set of paths that share one curve. Most groups are a
    single path, but the workbook has one CNSW-SNW curve covering the northern
    and southern CNSW-SNW paths together.

    loss_equations is the manual extract of the workbook's three loss equation
    tables: the existing paths, the committed and anticipated projects, and the
    development options. The IASR tables and regional_granularity are only used
    to carry each loss group onto the path ids of the chosen granularity.

    Returns a table with one row per loss group, direction and date:

    - loss_group: the group's id, the sub-regional path id it is named for.
    - direction: forward or reverse, the direction of flow the row's curve is for.
    - loss_constant, loss_linear, loss_quadratic: the loss in MW is
      loss_constant + loss_linear * flow + loss_quadratic * flow^2, where flow is
      the MW flowing in the row's direction (never negative).
    - to_loss_proportion: the share of the loss that falls at the path's geo_to;
      the rest falls at its geo_from.
    - expansion_loss_factor: blank where expanding the group's paths upgrades the
      existing network, so flow on the expansion is added to the flow the curve
      is applied to. Otherwise the fraction of the flow on the expansion that is
      lost.
    - date_from: the date the row's curve takes over from the group's earlier
      one; blank for the curve that applies from the start.

    How an equation becomes two rows depends on what its flow is measured in:

    - An AC path's equation is in signed flow, positive in the forward
      direction. The reverse row is the same curve seen from the other side, so
      its loss_linear has the opposite sign.
    - A DC link's equation is in the size of the flow, so both rows are the
      same.

    Only the existing paths and the committed projects become rows. A
    development option's equation is used only for the groups listed in
    _PARALLEL_EXPANSIONS, to work out their expansion_loss_factor.

    I/O Example:
        loss_equations (abbreviated):
            status     loss_group  option     flow_basis  loss_constant  loss_linear  loss_quadratic  to_loss_proportion  date_from
            existing   CQ-NQ                  signed      0              0.0298       0.000060892     0.7533
            existing   TAS-SEV                magnitude   4              -0.00392     0.00010393      0.5
            option     TAS-SEV     TAS-VI...  magnitude   9              0.01         0.0000285645    0.5

        regional_granularity = "sub_regions" returns:
            loss_group  direction  loss_constant  loss_linear  loss_quadratic  to_loss_proportion  expansion_loss_factor  date_from
            CQ-NQ       forward    0              0.0298       0.000060892     0.7533
            CQ-NQ       reverse    0              -0.0298      0.000060892     0.7533
            TAS-SEV     forward    4              -0.00392     0.00010393      0.5                 0.0447
            TAS-SEV     reverse    4              -0.00392     0.00010393      0.5                 0.0447

        regional_granularity = "nem_regions" returns the TAS-SEV rows with a
        loss_group of TAS-VIC, and no CQ-NQ rows: the path sits inside QLD.
    """
    is_option = loss_equations["status"] == "option"
    losses = _split_into_directions(loss_equations[~is_option])
    expansion_loss_factors = _calculate_expansion_loss_factors(
        loss_equations[is_option]
    )
    losses = losses.merge(expansion_loss_factors, on="loss_group", how="left")
    losses = _rekey_to_granularity(
        losses,
        flow_path_transfer_capability,
        renewable_energy_zones,
        sub_regional_geography,
        regional_granularity,
    )
    losses = losses.sort_values(["loss_group", "date_from", "direction"])
    return losses.loc[:, _LOSS_COLUMNS].reset_index(drop=True)


def _split_into_directions(equations: pd.DataFrame) -> pd.DataFrame:
    """Writes each loss equation as a forward row and a reverse row, both in
    the size of the flow in that direction.

    An equation in signed flow, a * F + b * F^2, gives a loss of
    -a * R + b * R^2 for a reverse flow of size R, so the reverse row's
    loss_linear changes sign. An equation already in the size of the flow is
    the same in both directions.

    I/O Example:
        equations (abbreviated):
            loss_group  flow_basis  loss_constant  loss_linear  loss_quadratic
            CQ-NQ       signed      0              0.0298       0.000060892
            TAS-SEV     magnitude   4              -0.00392     0.00010393

        returns:
            loss_group  direction  loss_constant  loss_linear  loss_quadratic
            CQ-NQ       forward    0              0.0298       0.000060892
            TAS-SEV     forward    4              -0.00392     0.00010393
            CQ-NQ       reverse    0              -0.0298      0.000060892
            TAS-SEV     reverse    4              -0.00392     0.00010393
    """
    forward = equations.assign(direction="forward")
    is_signed = equations["flow_basis"] == "signed"
    reverse = equations.assign(
        direction="reverse",
        loss_linear=equations["loss_linear"].where(
            ~is_signed, -1.0 * equations["loss_linear"]
        ),
    )
    return pd.concat([forward, reverse], ignore_index=True)


def _calculate_expansion_loss_factors(options: pd.DataFrame) -> pd.DataFrame:
    """Works out the flat loss factor for the expansion of each group in
    _PARALLEL_EXPANSIONS.

    The factor is the loss the option's equation gives at half of the option's
    capacity, as a fraction of that flow. The equation's constant term, a fixed
    loss the link has at any flow, is included, so that it is averaged into the
    factor instead of being lost: a fixed loss can't be applied to a link the
    model may never build.

    I/O Example:
        options (abbreviated):
            loss_group  option                                   loss_constant  loss_linear  loss_quadratic
            TAS-SEV     TAS-VIC Option 1 (Marinus Link Stage 1)  9              0.01         0.0000285645
            TAS-SEV     TAS-VIC Option 2 (Marinus Link Stage 2)  18             0.01         0.0000142822   # not the named option
            CQ-NQ       CQ-NQ Option 1                           0              0.0282       0.0000574      # not a parallel expansion

        returns (9 / 375 + 0.01 + 0.0000285645 * 375, for half of 750 MW):
            loss_group  expansion_loss_factor
            TAS-SEV     0.0447
    """
    parallel = options.merge(_PARALLEL_EXPANSIONS, on=["loss_group", "option"])
    half_flow = parallel["capacity"] / 2.0
    parallel["expansion_loss_factor"] = (
        parallel["loss_constant"] / half_flow
        + parallel["loss_linear"]
        + parallel["loss_quadratic"] * half_flow
    )
    return parallel.loc[:, ["loss_group", "expansion_loss_factor"]]


def _rekey_to_granularity(
    losses: pd.DataFrame,
    flow_path_transfer_capability: pd.DataFrame,
    renewable_energy_zones: pd.DataFrame,
    sub_regional_geography: pd.DataFrame,
    regional_granularity: str,
) -> pd.DataFrame:
    """Carries the loss groups from sub-regional path ids onto the path ids of
    the chosen granularity, dropping groups whose paths that granularity doesn't
    model.

    This is the re-keying network_transmission_path_limits goes through (see
    _aggregate_to_granularity), applied to the flow paths alone: no REZ
    connection has a loss curve. A group named for several paths (CNSW-SNW) is
    not itself a path id, which is fine: its paths sit inside one NEM region,
    so it is only kept at the sub-regional granularity, where nothing is
    re-keyed.

    I/O Example:
        losses (abbreviated):
            loss_group  direction  loss_linear
            NNSW-SQ     forward    0.039
            CQ-NQ       forward    0.0298

        regional_granularity = "nem_regions" returns:
            loss_group  direction  loss_linear
            NSW-QLD     forward    0.039         # CQ-NQ is inside QLD: dropped
    """
    topology = _parse_flow_path_topology(flow_path_transfer_capability["Flow Paths"])
    flow_paths = _add_flow_path_carrier(topology)
    region_lookup = _build_geo_region_lookup(sub_regional_geography)
    losses = losses.rename(columns={"loss_group": "path_id"})
    _, losses = _aggregate_to_granularity(
        flow_paths, losses, regional_granularity, renewable_energy_zones, region_lookup
    )
    return losses.rename(columns={"path_id": "loss_group"})


def _add_loss_group_column(paths: pd.DataFrame, losses: pd.DataFrame) -> pd.DataFrame:
    """Adds the loss_group column to the network_transmission_paths table.

    A path belongs to the loss group named for it. Failing that it belongs to
    the group named for its corridor, the path id without its parallel-path
    suffix: the workbook gives one CNSW-SNW curve for the northern and southern
    paths together. A path with neither, such as a REZ connection, is left
    blank and has no losses.

    I/O Example:
        paths (abbreviated):
            path_id
            CQ-NQ
            NNSW-SQ_Terranora
            CNSW-SNW_NTH
            Q1-NQ

        losses (abbreviated):
            loss_group
            CQ-NQ
            NNSW-SQ
            NNSW-SQ_Terranora
            CNSW-SNW

        returns:
            path_id            loss_group
            CQ-NQ              CQ-NQ
            NNSW-SQ_Terranora  NNSW-SQ_Terranora   # has a group of its own
            CNSW-SNW_NTH       CNSW-SNW            # shares its corridor's group
            Q1-NQ              (NaN)               # a REZ connection: no losses
    """
    loss_groups = set(losses["loss_group"])
    corridor = paths["path_id"].str.split("_").str[0]
    own_group = paths["path_id"].where(paths["path_id"].isin(loss_groups))
    corridor_group = corridor.where(corridor.isin(loss_groups))
    return paths.assign(loss_group=own_group.combine_first(corridor_group))
