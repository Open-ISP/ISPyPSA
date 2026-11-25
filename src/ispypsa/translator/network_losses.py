import numpy as np
import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.model_years import _model_year_of_dates
from ispypsa.translator.constraints import _resolve_path_expansion_caps

# The carrier that marks a generator as a transmission loss, so the results can
# tell losses apart from generation.
TRANSMISSION_LOSS_CARRIER = "Transmission losses"

# Each direction of a loss curve is approximated by this many straight
# segments. Four matches the tranche count AEMO's PLEXOS model uses.
_SEGMENTS_PER_DIRECTION = 4

# The ends of each segment, as fractions of a loss group's largest flow:
# reverse flow is negative, so the segments run from -1 to 1.
_SEGMENT_ENDS = pd.DataFrame(
    {
        "segment": range(2 * _SEGMENTS_PER_DIRECTION),
        "from_fraction": np.linspace(-1.0, 1.0, 2 * _SEGMENTS_PER_DIRECTION + 1)[:-1],
        "to_fraction": np.linspace(-1.0, 1.0, 2 * _SEGMENTS_PER_DIRECTION + 1)[1:],
    }
)

_LOSS_GENERATOR_COLUMNS = [
    "name",
    "isp_name",
    "bus",
    "carrier",
    "sign",
    "p_nom",
    "p_nom_extendable",
    "p_min_pu",
    "p_max_pu",
    "build_year",
    "lifetime",
    "capital_cost",
    "marginal_cost",
]

_LHS_COLUMNS = [
    "constraint_name",
    "investment_period",
    "variable_name",
    "component",
    "attribute",
    "coefficient",
]

_RHS_COLUMNS = [
    "constraint_name",
    "investment_period",
    "timeslice",
    "rhs",
    "constraint_type",
]


def _translate_network_losses(
    ispypsa_tables: dict[str, pd.DataFrame],
    links: pd.DataFrame,
    config: ModelConfig,
) -> dict[str, pd.DataFrame]:
    """Translates the transmission loss curves into loss generators and the
    custom constraints that tie them to link flows.

    Power is lost as it flows along a transmission path. The
    network_transmission_losses table gives that loss as a curve, a quadratic
    in the flow, for each loss group: a set of paths between the same two
    geographies that share one curve. A PyPSA link can't lose power this way,
    so the loss is modelled beside the links. Each end of a loss group gets a
    loss generator, a generator that takes power off its bus, and custom
    constraints hold the power it takes at or above the loss the curve gives
    for the flow on the group's links. Losses cost energy, so the model keeps
    them down on the curve.

    A constraint has to be linear, so each direction of a curve is replaced by
    _SEGMENTS_PER_DIRECTION straight segments between points on it, and the
    loss is held above every segment's line. This only gives the curve's loss
    when the curve is convex, which the network_transmission_losses schema
    requires.

    links is the translated links table. The curves and the paths' loss groups
    come from ispypsa_tables, and config supplies the investment periods and
    year type.

    Returns a dictionary of three tables:

    - "transmission_loss_generators": two generators per loss group, one at
      each end, splitting the loss by the curve's to_loss_proportion. The
      generator's sign is -1, so its output p is the MW of loss at its bus.
    - "custom_constraints_lhs" and "custom_constraints_rhs": the rows that hold
      each generator's p above each segment's line, in each investment period.
      They are in the format of the PyPSA-friendly tables of the same names,
      for appending to them.

    How a group's expansion links enter depends on the curve's
    expansion_loss_factor:

    - Blank: expansion upgrades the existing network, so the flow on the
      expansion links is added to the flow the curve is applied to.
    - Set: the expansion is a separate asset, so the curve is applied to the
      flow on the other links only, and the expansion links lose that fraction
      of their own flow, in whichever direction it goes.

    A loss group with no links in the model, or whose links have no capacity,
    gets no generators or constraints. Where a curve changes over time, the
    row whose date_from falls in the latest model year up to the investment
    period applies.

    I/O Example:
        ispypsa_tables["network_transmission_losses"]:
            loss_group  direction  loss_constant  loss_linear  loss_quadratic  to_loss_proportion  expansion_loss_factor  date_from
            CQ-NQ       forward    0              0.03         0.00005         0.75
            CQ-NQ       reverse    0              -0.03        0.00005         0.75

        ispypsa_tables["network_transmission_paths"] (abbreviated):
            path_id  geo_from  geo_to  loss_group
            CQ-NQ    CQ        NQ      CQ-NQ

        links (abbreviated; no expansion links):
            isp_name  name            p_nom  p_nom_extendable  build_year  lifetime
            CQ-NQ     CQ-NQ_existing  2000   False             2025        inf

        config: investment period 2026

        returns["transmission_loss_generators"] (abbreviated; the loss is at
        most 260 MW, at 2000 MW forward):
            name              isp_name  bus  sign  p_nom  p_min_pu  p_max_pu
            CQ-NQ_loss_at_NQ  CQ-NQ     NQ   -1    195    -0.0096   1.0     # 75% of the loss
            CQ-NQ_loss_at_CQ  CQ-NQ     CQ   -1    65     -0.0096   1.0     # 25% of the loss

        returns["custom_constraints_rhs"] (the first of 8 segments, 2 ends):
            constraint_name      investment_period  timeslice  rhs      constraint_type
            CQ-NQ_loss_at_NQ_0   2026                          -112.5   >=
            CQ-NQ_loss_at_CQ_0   2026                          -37.5    >=

        returns["custom_constraints_lhs"] (for the same two constraints):
            constraint_name      investment_period  variable_name     component  attribute  coefficient
            CQ-NQ_loss_at_NQ_0   2026               CQ-NQ_loss_at_NQ  Generator  p          1.0
            CQ-NQ_loss_at_NQ_0   2026               CQ-NQ_existing    Link       p          0.10875
            CQ-NQ_loss_at_CQ_0   2026               CQ-NQ_loss_at_CQ  Generator  p          1.0
            CQ-NQ_loss_at_CQ_0   2026               CQ-NQ_existing    Link       p          0.03625

        Segment 0 runs from 2000 MW to 1500 MW of reverse flow (a flow of
        -2000 to -1500), where the loss falls from 140 MW to 67.5 MW: the line
        -150 - 0.145 * flow. The NQ generator takes 75% of it, so its
        constraint is p + 0.10875 * flow >= -112.5.
    """
    group_links = _links_by_loss_group(
        ispypsa_tables["network_transmission_paths"], links
    )
    curves = _curves_in_each_investment_period(
        ispypsa_tables["network_transmission_losses"],
        config.temporal.capacity_expansion.investment_periods,
        config.temporal.year_type,
    )
    expansion_caps = _resolve_path_expansion_caps(
        ispypsa_tables["network_expansion_options"], links
    )
    curves = _add_flow_ranges(curves, group_links, expansion_caps)
    lines = _linearise_curves(curves)
    lines = _split_lines_between_the_two_ends(lines, group_links)
    return {
        "transmission_loss_generators": _build_loss_generators(
            lines, config.temporal.range.start_year
        ),
        "custom_constraints_lhs": _build_constraint_lhs(lines, group_links),
        "custom_constraints_rhs": _build_constraint_rhs(lines),
    }


def _links_by_loss_group(paths: pd.DataFrame, links: pd.DataFrame) -> pd.DataFrame:
    """Lists the links of each loss group, with the group's two end buses.

    A path with a blank loss_group has no losses, and a path the model doesn't
    include has no links; neither contributes a row.

    I/O Example:
        paths (abbreviated):
            path_id       geo_from  geo_to  loss_group
            CNSW-SNW_NTH  CNSW      SNW     CNSW-SNW
            CNSW-SNW_STH  CNSW      SNW     CNSW-SNW
            Q1-NQ         Q1        NQ                  # no loss group

        links (abbreviated):
            isp_name      name                   p_nom  p_nom_extendable  build_year  lifetime
            CNSW-SNW_NTH  CNSW-SNW_NTH_existing  4730   False             2025        inf
            CNSW-SNW_STH  CNSW-SNW_STH_existing  2720   False             2025        inf
            Q1-NQ         Q1-NQ_existing         750    False             2025        inf

        returns:
            loss_group  geo_from  geo_to  name                   p_nom  p_nom_extendable  build_year  lifetime
            CNSW-SNW    CNSW      SNW     CNSW-SNW_NTH_existing  4730   False             2025        inf
            CNSW-SNW    CNSW      SNW     CNSW-SNW_STH_existing  2720   False             2025        inf
    """
    grouped_paths = paths.dropna(subset="loss_group")
    grouped_paths = grouped_paths.loc[
        :, ["path_id", "geo_from", "geo_to", "loss_group"]
    ]
    group_links = grouped_paths.merge(links, left_on="path_id", right_on="isp_name")
    return group_links.loc[
        :,
        [
            "loss_group",
            "geo_from",
            "geo_to",
            "name",
            "p_nom",
            "p_nom_extendable",
            "build_year",
            "lifetime",
        ],
    ]


def _curves_in_each_investment_period(
    losses: pd.DataFrame, investment_periods: list[int], year_type: str
) -> pd.DataFrame:
    """Picks the curve of each loss group that applies in each investment
    period, as one row holding both directions.

    A dated row applies from the model year its date_from falls in, and an
    undated row from the start. For each investment period the row with the
    latest such year, up to the period, is kept. This is the rule committed
    capacity changes follow, so a project's capacity and its loss curve arrive
    together.

    I/O Example (year_type = "fy", investment_periods = [2026, 2028]):
        losses (abbreviated to loss_linear):
            loss_group  direction  loss_linear  to_loss_proportion  expansion_loss_factor  date_from
            SNSW-CNSW   forward    -0.05        0.7
            SNSW-CNSW   reverse    0.05         0.7
            SNSW-CNSW   forward    -0.04        0.7                                        2027-12-02T00:00:00
            SNSW-CNSW   reverse    0.04         0.7                                        2027-12-02T00:00:00

        returns (abbreviated; 2 December 2027 is in FY2028):
            loss_group  investment_period  to_loss_proportion  expansion_loss_factor  loss_linear_forward  loss_linear_reverse
            SNSW-CNSW   2026               0.7                                        -0.05                0.05
            SNSW-CNSW   2028               0.7                                        -0.04                0.04
    """
    losses = losses.copy()
    is_dated = losses["date_from"].notna()
    dates = pd.to_datetime(losses.loc[is_dated, "date_from"])
    losses["year_from"] = -np.inf
    losses.loc[is_dated, "year_from"] = _model_year_of_dates(year_type, dates)
    periods = pd.DataFrame({"investment_period": investment_periods})
    candidates = losses.merge(periods, how="cross")
    candidates = candidates[candidates["year_from"] <= candidates["investment_period"]]
    candidates = candidates.sort_values("year_from")
    active = candidates.groupby(
        ["loss_group", "direction", "investment_period"], as_index=False
    ).tail(1)
    return _pair_forward_and_reverse_curves(active)


def _pair_forward_and_reverse_curves(curves: pd.DataFrame) -> pd.DataFrame:
    """Pairs each loss group's forward and reverse rows into one row, with
    the three coefficients of each direction side by side.

    I/O Example:
        curves (abbreviated):
            loss_group  direction  investment_period  loss_constant  loss_linear  loss_quadratic  to_loss_proportion  expansion_loss_factor
            CQ-NQ       forward    2026               0              0.03         0.00005         0.75
            CQ-NQ       reverse    2026               0              -0.03        0.00005         0.75

        returns:
            loss_group  investment_period  to_loss_proportion  expansion_loss_factor  loss_constant_forward  loss_linear_forward  loss_quadratic_forward  loss_constant_reverse  loss_linear_reverse  loss_quadratic_reverse
            CQ-NQ       2026               0.75                                       0                      0.03                 0.00005                 0                      -0.03                0.00005
    """
    keys = ["loss_group", "investment_period"]
    coefficients = ["loss_constant", "loss_linear", "loss_quadratic"]
    shared = ["to_loss_proportion", "expansion_loss_factor"]
    is_forward = curves["direction"] == "forward"
    forward = curves.loc[is_forward, keys + shared + coefficients]
    reverse = curves.loc[~is_forward, keys + coefficients]
    return forward.merge(reverse, on=keys, suffixes=("_forward", "_reverse"))


def _add_flow_ranges(
    curves: pd.DataFrame, group_links: pd.DataFrame, expansion_caps: pd.DataFrame
) -> pd.DataFrame:
    """Adds the largest flow each curve has to cover, and the largest flow on
    the group's expansion links.

    max_flow is the combined p_nom of the links the curve is applied to. That
    is the non-extendable links, plus the expansion the model may build
    (expansion_cap) where the expansion is an upgrade. A link's p_nom is its
    larger direction, so the same size covers both directions. Groups with no
    links, or no capacity, are dropped.

    I/O Example:
        curves (abbreviated):
            loss_group  investment_period  expansion_loss_factor
            CQ-NQ       2026
            TAS-SEV     2026               0.045
            MN-SA       2026                                      # no links in the model

        group_links (abbreviated):
            loss_group  name               p_nom  p_nom_extendable
            CQ-NQ       CQ-NQ_existing     1910   False
            CQ-NQ       CQ-NQ_exp_2026     0      True
            TAS-SEV     TAS-SEV_existing   594    False
            TAS-SEV     TAS-SEV_exp_2026   0      True

        expansion_caps:
            expansion_id  allowed_expansion
            CQ-NQ         1000
            TAS-SEV       750

        returns:
            loss_group  investment_period  expansion_loss_factor  expansion_cap  max_flow
            CQ-NQ       2026                                      1000           2910     # an upgrade: 1910 + 1000
            TAS-SEV     2026               0.045                  750            594      # in parallel: Basslink only
    """
    is_fixed = ~group_links["p_nom_extendable"].astype(bool)
    fixed_capacity = (
        group_links[is_fixed].groupby("loss_group", as_index=False)["p_nom"].sum()
    )
    fixed_capacity = fixed_capacity.rename(columns={"p_nom": "fixed_capacity"})
    group_caps = _expansion_cap_per_loss_group(group_links, expansion_caps)
    curves = curves.merge(fixed_capacity, on="loss_group")
    curves = curves.merge(group_caps, on="loss_group", how="left")
    curves["expansion_cap"] = curves["expansion_cap"].fillna(0.0)
    is_upgrade = curves["expansion_loss_factor"].isna()
    curves["max_flow"] = curves["fixed_capacity"] + curves["expansion_cap"].where(
        is_upgrade, 0.0
    )
    return curves[curves["max_flow"] > 0.0].drop(columns="fixed_capacity")


def _expansion_cap_per_loss_group(
    group_links: pd.DataFrame, expansion_caps: pd.DataFrame
) -> pd.DataFrame:
    """Adds up, for each loss group, the most expansion the model may build
    on its paths.

    I/O Example:
        group_links (abbreviated):
            loss_group  name
            CQ-NQ       CQ-NQ_existing
            CQ-NQ       CQ-NQ_exp_2026
            CQ-NQ       CQ-NQ_exp_2028

        expansion_caps:
            expansion_id  allowed_expansion
            CQ-NQ         1000

        returns:
            loss_group  expansion_cap
            CQ-NQ       1000
    """
    # An expansion link is named <path_id>_exp_<year>, so the path id is the
    # name up to the last "_exp_".
    path_id = group_links["name"].str.rsplit("_exp_", n=1).str[0]
    path_groups = group_links.assign(expansion_id=path_id)
    path_groups = path_groups.loc[
        group_links["p_nom_extendable"].astype(bool), ["loss_group", "expansion_id"]
    ].drop_duplicates()
    caps = path_groups.merge(expansion_caps, on="expansion_id")
    caps = caps.groupby("loss_group", as_index=False)["allowed_expansion"].sum()
    return caps.rename(columns={"allowed_expansion": "expansion_cap"})


def _linearise_curves(curves: pd.DataFrame) -> pd.DataFrame:
    """Replaces each loss curve with straight lines the loss must stay above.

    What the result looks like, for one direction of one curve:

        loss |                         *
             |                     *  /
             |                 *    /        * the curve
             |            *      /           / a segment's line
             |      *        /
             |*______/___________________ flow
             0      1/4     1/2    3/4    max_flow

    How the lines are built:

    1. The flow range, from max_flow in reverse to max_flow forward, is cut
       into 2 * _SEGMENTS_PER_DIRECTION equal segments (see _SEGMENT_ENDS).
       Flow is signed here: reverse flow is negative.
    2. The curve is evaluated at both ends of each segment, using the forward
       coefficients where the flow is positive and the reverse ones, on the
       size of the flow, where it is negative.
    3. Each segment's line passes through its two end points. For CQ-NQ with a
       max_flow of 2000 MW and a loss of 0.03 * F + 0.00005 * F^2, the last
       segment runs from 1500 MW (157.5 MW lost) to 2000 MW (260 MW lost): a
       slope of 0.205 and an intercept of -150.
    4. Where the group's expansion is a separate asset, each line is repeated
       for the two directions the expansion links can flow in:
       expansion_slope is plus and minus the expansion_loss_factor. Otherwise
       the expansion links share the line's slope.

    Holding the loss above every line gives the largest of the lines at any
    flow. Because the curve is convex, that is the segment the flow is in, so
    the loss follows the curve to within the gap between a segment and the
    curve. A dip in the curve narrower than a segment, as Basslink has near
    zero flow, disappears because a segment spans it.

    I/O Example:
        curves (abbreviated to the columns of one direction):
            loss_group  investment_period  to_loss_proportion  expansion_loss_factor  expansion_cap  max_flow  loss_constant_forward  loss_linear_forward  loss_quadratic_forward
            CQ-NQ       2026               0.75                                       0              2000      0                      0.03                 0.00005

        returns (the last of the 8 lines):
            loss_group  investment_period  to_loss_proportion  expansion_cap  line  slope  intercept  expansion_slope  highest_loss  lowest_loss
            CQ-NQ       2026               0.75                0              7     0.205  -150       0.205            260           157.5
    """
    segments = curves.merge(_SEGMENT_ENDS, how="cross")
    flow_from = segments["from_fraction"] * segments["max_flow"]
    flow_to = segments["to_fraction"] * segments["max_flow"]
    loss_from = _loss_at(segments, flow_from)
    loss_to = _loss_at(segments, flow_to)
    segments["slope"] = (loss_to - loss_from) / (flow_to - flow_from)
    segments["intercept"] = loss_from - segments["slope"] * flow_from
    segments["highest_loss"] = np.maximum(loss_from, loss_to)
    segments["lowest_loss"] = np.minimum(loss_from, loss_to)
    lines = _add_expansion_slopes(segments)
    lines["line"] = lines.groupby(["loss_group", "investment_period"]).cumcount()
    return lines.loc[
        :,
        [
            "loss_group",
            "investment_period",
            "to_loss_proportion",
            "expansion_cap",
            "line",
            "slope",
            "intercept",
            "expansion_slope",
            "highest_loss",
            "lowest_loss",
        ],
    ]


def _loss_at(curves: pd.DataFrame, flow: pd.Series) -> pd.Series:
    """The loss each curve gives at a signed flow: the forward curve for a
    positive flow, and the reverse curve, on the size of the flow, for a
    negative one.

    I/O Example:
        curves (abbreviated):
            loss_constant_forward  loss_linear_forward  loss_quadratic_forward  loss_constant_reverse  loss_linear_reverse  loss_quadratic_reverse
            0                      0.03                 0.00005                 0                      -0.03                0.00005
            0                      0.03                 0.00005                 0                      -0.03                0.00005

        flow: [1000, -1000]

        returns: [80, 20]   # 30 + 50 forward, -30 + 50 in reverse
    """
    size = flow.abs()
    forward = (
        curves["loss_constant_forward"]
        + curves["loss_linear_forward"] * size
        + curves["loss_quadratic_forward"] * size**2
    )
    reverse = (
        curves["loss_constant_reverse"]
        + curves["loss_linear_reverse"] * size
        + curves["loss_quadratic_reverse"] * size**2
    )
    return forward.where(flow >= 0.0, reverse)


def _add_expansion_slopes(segments: pd.DataFrame) -> pd.DataFrame:
    """Adds expansion_slope, the loss per MW of flow on the group's expansion
    links, to each segment's line.

    Where the expansion is an upgrade its flow is part of the flow the curve
    is applied to, so it shares the line's slope. Where it is a separate asset
    it loses expansion_loss_factor of its flow in either direction: the line is
    repeated with the factor positive (forward flow) and negative (reverse
    flow), and the loss stays above both.

    I/O Example:
        segments (abbreviated):
            loss_group  segment  slope  expansion_loss_factor
            CQ-NQ       7        0.205
            TAS-SEV     7        0.14   0.045

        returns:
            loss_group  segment  slope  expansion_loss_factor  expansion_slope
            CQ-NQ       7        0.205                         0.205
            TAS-SEV     7        0.14   0.045                  0.045
            TAS-SEV     7        0.14   0.045                  -0.045
    """
    is_upgrade = segments["expansion_loss_factor"].isna()
    upgrade = segments[is_upgrade]
    upgrade = upgrade.assign(expansion_slope=upgrade["slope"])
    parallel = segments[~is_upgrade]
    forward = parallel.assign(expansion_slope=parallel["expansion_loss_factor"])
    reverse = parallel.assign(expansion_slope=-1.0 * parallel["expansion_loss_factor"])
    lines = pd.concat([upgrade, forward, reverse], ignore_index=True)
    return lines.sort_values(
        ["loss_group", "investment_period", "segment"], kind="stable"
    )


def _split_lines_between_the_two_ends(
    lines: pd.DataFrame, group_links: pd.DataFrame
) -> pd.DataFrame:
    """Gives each end of a loss group its share of every line, and names the
    end's loss generator and constraints.

    The geo_to end takes to_loss_proportion of the loss and the geo_from end
    the rest.

    I/O Example:
        lines (abbreviated):
            loss_group  investment_period  to_loss_proportion  line  slope  intercept
            CQ-NQ       2026               0.75                7     0.205  -150

        group_links (abbreviated):
            loss_group  geo_from  geo_to
            CQ-NQ       CQ        NQ

        returns (abbreviated):
            loss_group  investment_period  line  slope  intercept  bus  share  generator         constraint_name
            CQ-NQ       2026               7     0.205  -150       NQ   0.75   CQ-NQ_loss_at_NQ  CQ-NQ_loss_at_NQ_7
            CQ-NQ       2026               7     0.205  -150       CQ   0.25   CQ-NQ_loss_at_CQ  CQ-NQ_loss_at_CQ_7
    """
    ends = group_links.loc[:, ["loss_group", "geo_from", "geo_to"]].drop_duplicates()
    lines = lines.merge(ends, on="loss_group")
    at_to = lines.assign(bus=lines["geo_to"], share=lines["to_loss_proportion"])
    at_from = lines.assign(
        bus=lines["geo_from"], share=1.0 - lines["to_loss_proportion"]
    )
    lines = pd.concat([at_to, at_from], ignore_index=True)
    lines["generator"] = lines["loss_group"] + "_loss_at_" + lines["bus"]
    lines["constraint_name"] = lines["generator"] + "_" + lines["line"].astype(str)
    return lines.drop(columns=["geo_from", "geo_to", "to_loss_proportion"])


def _build_loss_generators(lines: pd.DataFrame, start_year: int) -> pd.DataFrame:
    """Builds the loss generator at each end of each loss group.

    A loss generator has a sign of -1, so its output p is power taken off its
    bus: the loss. It costs nothing to run. Its output is bounded by its share
    of the smallest and largest loss its lines can give over the whole
    horizon, plus what the expansion links can lose where they are a separate
    asset. The lower bound can be negative, where flow reduces losses
    elsewhere. The upper bound stops the model from using the generator to
    dump unwanted energy beyond the largest loss the path could have.

    I/O Example:
        lines (abbreviated):
            loss_group  generator         bus  share  highest_loss  lowest_loss  expansion_slope  expansion_cap
            CQ-NQ       CQ-NQ_loss_at_NQ  NQ   0.75   260           157.5        0.205            0
            CQ-NQ       CQ-NQ_loss_at_NQ  NQ   0.75   20            -4.5         0.02             0

        start_year = 2026

        returns (abbreviated):
            name              isp_name  bus  carrier              sign  p_nom  p_min_pu  p_max_pu  build_year  lifetime
            CQ-NQ_loss_at_NQ  CQ-NQ     NQ   Transmission losses  -1    195    -0.0173   1.0       2025        inf
    """
    lines = lines.copy()
    # An expansion in parallel adds up to its loss factor times its capacity.
    expansion_loss = lines["expansion_slope"].abs() * lines["expansion_cap"]
    is_parallel = lines["expansion_slope"] != lines["slope"]
    lines["highest_loss"] = lines["highest_loss"] + expansion_loss.where(
        is_parallel, 0.0
    )
    generators = lines.groupby(
        ["loss_group", "generator", "bus", "share"], as_index=False
    ).agg(highest_loss=("highest_loss", "max"), lowest_loss=("lowest_loss", "min"))
    highest = generators["share"] * generators["highest_loss"]
    lowest = generators["share"] * generators["lowest_loss"]
    generators["p_nom"] = np.maximum(highest.abs(), lowest.abs())
    # An end with none of the loss has a p_nom of zero: its bounds are zero too.
    has_loss = generators["p_nom"] > 0.0
    generators["p_max_pu"] = (highest / generators["p_nom"]).where(has_loss, 0.0)
    generators["p_min_pu"] = (lowest / generators["p_nom"]).where(has_loss, 0.0)
    generators = generators.rename(
        columns={"generator": "name", "loss_group": "isp_name"}
    )
    generators["carrier"] = TRANSMISSION_LOSS_CARRIER
    generators["sign"] = -1.0
    generators["p_nom_extendable"] = False
    generators["build_year"] = start_year - 1
    generators["lifetime"] = np.inf
    generators["capital_cost"] = 0.0
    generators["marginal_cost"] = 0.0
    return generators.loc[:, _LOSS_GENERATOR_COLUMNS]


def _build_constraint_rhs(lines: pd.DataFrame) -> pd.DataFrame:
    """Builds the right-hand side of each line's constraint: the end's share of
    the line's intercept. The constraint applies at every snapshot of its
    investment period.

    I/O Example:
        lines (abbreviated):
            constraint_name      investment_period  share  intercept
            CQ-NQ_loss_at_NQ_7   2026               0.75   -150

        returns:
            constraint_name      investment_period  timeslice  rhs      constraint_type
            CQ-NQ_loss_at_NQ_7   2026                          -112.5   >=
    """
    rhs = lines.loc[:, ["constraint_name", "investment_period"]].copy()
    rhs["timeslice"] = np.nan
    rhs["rhs"] = lines["share"] * lines["intercept"]
    rhs["constraint_type"] = ">="
    return rhs.loc[:, _RHS_COLUMNS].reset_index(drop=True)


def _build_constraint_lhs(
    lines: pd.DataFrame, group_links: pd.DataFrame
) -> pd.DataFrame:
    """Builds the left-hand side of each line's constraint: the loss
    generator's output, less the end's share of the line's slope times the
    flow on the group's links.

    A link only has a term in the investment periods it is in service
    (build_year <= period < build_year + lifetime). An expansion link's flow
    is multiplied by the line's expansion_slope and every other link's by its
    slope.

    I/O Example:
        lines (abbreviated):
            loss_group  constraint_name      investment_period  generator         share  slope  expansion_slope
            CQ-NQ       CQ-NQ_loss_at_NQ_7   2028               CQ-NQ_loss_at_NQ  0.75   0.205  0.205

        group_links (abbreviated):
            loss_group  name            p_nom_extendable  build_year  lifetime
            CQ-NQ       CQ-NQ_existing  False             2025        inf
            CQ-NQ       CQ-NQ_exp_2028  True              2028        inf
            CQ-NQ       CQ-NQ_exp_2030  True              2030        inf       # not built yet in 2028

        returns:
            constraint_name      investment_period  variable_name     component  attribute  coefficient
            CQ-NQ_loss_at_NQ_7   2028               CQ-NQ_loss_at_NQ  Generator  p          1.0
            CQ-NQ_loss_at_NQ_7   2028               CQ-NQ_existing    Link       p          -0.15375
            CQ-NQ_loss_at_NQ_7   2028               CQ-NQ_exp_2028    Link       p          -0.15375
    """
    generator_terms = lines.loc[:, ["constraint_name", "investment_period"]].copy()
    generator_terms["variable_name"] = lines["generator"]
    generator_terms["component"] = "Generator"
    generator_terms["coefficient"] = 1.0

    link_terms = lines.merge(group_links, on="loss_group")
    retirement_year = link_terms["build_year"] + link_terms["lifetime"]
    in_service = (link_terms["build_year"] <= link_terms["investment_period"]) & (
        link_terms["investment_period"] < retirement_year
    )
    link_terms = link_terms[in_service].copy()
    is_expansion = link_terms["p_nom_extendable"].astype(bool)
    slope = link_terms["expansion_slope"].where(is_expansion, link_terms["slope"])
    link_terms["variable_name"] = link_terms["name"]
    link_terms["component"] = "Link"
    link_terms["coefficient"] = -1.0 * link_terms["share"] * slope

    lhs = pd.concat([generator_terms, link_terms], ignore_index=True)
    lhs["attribute"] = "p"
    return lhs.loc[:, _LHS_COLUMNS]
