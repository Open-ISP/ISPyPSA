import logging

import numpy as np
import pandas as pd

from ispypsa.config import ModelConfig

_CARBON_BUDGET_COLUMNS = ["constraint_name", "year_from", "year_to", "rhs"]

# How far one emissions rate may sit above the one before it, in Mt a year,
# before the trajectory is treated as rising. Allows for float rounding.
_RISING_RATE_TOLERANCE = 1e-9


def _translate_carbon_budgets(
    carbon_budgets: pd.DataFrame, config: ModelConfig
) -> pd.DataFrame:
    """Adjusts each carbon budget to the years the model covers.

    A carbon budget is a cap, in Mt, on the NEM's total emissions over a
    window of years. The model may cover only some of those years
    (config.temporal.range), so the cap has to be adjusted to match.

    The adjusted budgets are returned as a table with one row per budget and
    the columns:

    - constraint_name: the budget's budget_id.
    - year_from, year_to: the first and last year of the budget's window,
      both inclusive and unchanged from the input.
    - rhs: the adjusted cap, in Mt.

    ispypsa.pypsa_build.carbon_budgets adds one constraint to the model per
    row: the emissions of the modelled years in the window must be no more
    than rhs.

    How a budget is adjusted depends on how much of its window is modelled:

    1. All of it: the rhs is the budget, unchanged.
    2. None of it: the budget is dropped, as there is nothing for it to
       constrain.
    3. Part of it: config.carbon_budget_scaling decides.

    The carbon_budget_scaling options for a partly modelled window are:

    - "none": raises a ValueError naming the budgets.
    - "prorata": every year of the window gets an equal share of the budget,
      and the rhs is the sum of the shares of the modelled years.
    - "linear_trajectory_to_zero": the same, but the yearly shares are not
      equal. They follow a trajectory of emissions that falls in straight
      lines to zero at the end of the last budget (see
      _build_emissions_trajectory).

    The budgets are applied to the whole model whatever regions it covers. A
    warning is logged when the model is filtered to some regions only.

    I/O Example (config: modelled years 2031 to 2050, carbon_budget_scaling
    "prorata"):
        carbon_budgets:
            budget_id         year_from  year_to  budget
            nem_2027_to_2030  2027       2030     290.0   # no modelled years: dropped
            nem_2027_to_2050  2027       2050     583.0   # 20 of its 24 years modelled
            nem_2035_to_2035  2035       2035     23.0

        returns:
            constraint_name   year_from  year_to  rhs
            nem_2027_to_2050  2027       2050     485.833333   # 583 x 20 / 24
            nem_2035_to_2035  2035       2035     23.0
    """
    filtered_to = config.filter_by_nem_regions or config.filter_by_isp_sub_regions
    if filtered_to and not carbon_budgets.empty:
        logging.warning(
            f"The carbon budgets are for the whole NEM but the model is filtered "
            f"to {sorted(filtered_to)}: the budgets are applied to it unscaled."
        )
    return _scale_budgets_to_modelled_years(
        carbon_budgets,
        config.temporal.range.start_year,
        config.temporal.range.end_year,
        config.carbon_budget_scaling,
    )


def _scale_budgets_to_modelled_years(
    carbon_budgets: pd.DataFrame, start_year: int, end_year: int, scaling: str
) -> pd.DataFrame:
    """Scales each budget to the part of its window that is modelled.

    The modelled years are every year from start_year to end_year, both
    inclusive. Each budget is shared out over the years of its window in the way scaling
    says (the carbon_budget_scaling options listed in
    _translate_carbon_budgets). The scaled budget, returned as rhs, is the sum
    of the shares of the modelled years.

    I/O Example (modelled years 2029 to 2050):
        carbon_budgets:
            budget_id         year_from  year_to  budget
            nem_2025_to_2025  2025       2025     60.0    # no modelled years: dropped
            nem_2027_to_2030  2027       2030     290.0   # 2 of its 4 years modelled
            nem_2027_to_2050  2027       2050     583.0   # 22 of its 24 years modelled
            nem_2035_to_2035  2035       2035     23.0

        returns (scaling "prorata"):
            constraint_name   year_from  year_to  rhs
            nem_2027_to_2030  2027       2030     145.0        # 290 x 2 / 4
            nem_2027_to_2050  2027       2050     534.416667   # 583 x 22 / 24
            nem_2035_to_2035  2035       2035     23.0

        returns (scaling "linear_trajectory_to_zero"):
            constraint_name   year_from  year_to  rhs
            nem_2027_to_2030  2027       2030     101.8   # the trajectory in 2029 and 2030
            nem_2027_to_2050  2027       2050     394.8   # 101.8 + 293 for 2031 to 2050
            nem_2035_to_2035  2035       2035     23.0

        raises (scaling "none"), as two windows are only partly modelled
    """
    budgets = _drop_budgets_with_no_modelled_years(carbon_budgets, start_year, end_year)
    if scaling == "none":
        _raise_on_partly_modelled_windows(budgets, start_year, end_year)

    budget_years = _pair_budgets_with_modelled_years(budgets, start_year, end_year)

    if scaling == "linear_trajectory_to_zero":
        # The trajectory depends on every budget, so it is built from
        # carbon_budgets: the table before the budgets with no modelled years
        # were dropped.
        budget_years = _share_budgets_along_trajectory(budget_years, carbon_budgets)
    else:
        # Equal shares, for "prorata" and for "none". With "none" every window
        # left is modelled in full, so its shares sum back to the whole budget.
        window_length = budget_years["year_to"] - budget_years["year_from"] + 1
        budget_years["budget_share"] = budget_years["budget"] / window_length

    return _sum_shares_into_rhs(budget_years)


def _sum_shares_into_rhs(budget_years: pd.DataFrame) -> pd.DataFrame:
    """Sums each budget's yearly shares into its rhs.

    The result has the columns of the pypsa-friendly carbon_budgets table:
    budget_id becomes constraint_name, and rhs is rounded to the nearest
    tonne.

    I/O Example:
        budget_years:
            budget_id         year_from  year_to  year  budget_share
            nem_2027_to_2030  2027       2030     2029  72.5
            nem_2027_to_2030  2027       2030     2030  72.5
            nem_2035_to_2035  2035       2035     2035  23.0

        returns:
            constraint_name   year_from  year_to  rhs
            nem_2027_to_2030  2027       2030     145.0
            nem_2035_to_2035  2035       2035     23.0
    """
    budgets = budget_years.groupby(
        ["budget_id", "year_from", "year_to"], as_index=False
    )["budget_share"].sum()
    budgets = budgets.rename(columns={"budget_id": "constraint_name"})
    # To the nearest tonne, which also clears the float noise left by adding up
    # the yearly shares. A table with no rows has no numeric dtype to round.
    budgets["rhs"] = budgets["budget_share"].astype("float64").round(6)
    return budgets[_CARBON_BUDGET_COLUMNS]


def _drop_budgets_with_no_modelled_years(
    carbon_budgets: pd.DataFrame, start_year: int, end_year: int
) -> pd.DataFrame:
    """Drops the budgets with no modelled year in their window.

    The modelled years are every year from start_year to end_year, both
    inclusive. The dropped budgets are logged, as they are in the
    carbon_budgets table but put no constraint on the model.

    I/O Example (start_year 2031, end_year 2050):
        carbon_budgets:
            budget_id         year_from  year_to
            nem_2027_to_2030  2027       2030      # ends before 2031: dropped
            nem_2027_to_2050  2027       2050
            nem_2055_to_2055  2055       2055      # starts after 2050: dropped

        returns:
            budget_id         year_from  year_to
            nem_2027_to_2050  2027       2050

        logs: "Carbon budgets with no modelled year (2031 to 2050) in their
        window (not applied): ['nem_2027_to_2030', 'nem_2055_to_2055']"
    """
    has_modelled_years = (carbon_budgets["year_from"] <= end_year) & (
        carbon_budgets["year_to"] >= start_year
    )
    dropped = sorted(carbon_budgets.loc[~has_modelled_years, "budget_id"])
    if dropped:
        logging.info(
            f"Carbon budgets with no modelled year ({start_year} to {end_year}) "
            f"in their window (not applied): {dropped}"
        )
    return carbon_budgets[has_modelled_years]


def _raise_on_partly_modelled_windows(
    budgets: pd.DataFrame, start_year: int, end_year: int
) -> None:
    """Raises when only part of a budget's window is modelled.

    A window is partly modelled when it starts before start_year or ends after
    end_year. Every such budget is named in the error. This is the check for
    carbon_budget_scaling "none", where a budget is applied in full or not at
    all.

    The budgets with no modelled year in their window must already have been
    dropped, as they would be named too.

    I/O Example (start_year 2029, end_year 2050):
        budgets:
            budget_id         year_from  year_to
            nem_2027_to_2030  2027       2030      # 2027 and 2028 aren't modelled
            nem_2035_to_2035  2035       2035

        raises ValueError: "Carbon budgets with only part of their window in
        the modelled years (2029 to 2050): ['nem_2027_to_2030']. Set
        carbon_budget_scaling to 'prorata' or 'linear_trajectory_to_zero' in
        the config to scale them to the modelled years, or remove them from
        the carbon_budgets table."
    """
    is_partly_modelled = (budgets["year_from"] < start_year) | (
        budgets["year_to"] > end_year
    )
    partly_modelled = sorted(budgets.loc[is_partly_modelled, "budget_id"])
    if partly_modelled:
        raise ValueError(
            f"Carbon budgets with only part of their window in the modelled "
            f"years ({start_year} to {end_year}): {partly_modelled}. Set "
            f"carbon_budget_scaling to 'prorata' or 'linear_trajectory_to_zero' "
            f"in the config to scale them to the modelled years, or remove them "
            f"from the carbon_budgets table."
        )


def _pair_budgets_with_modelled_years(
    budgets: pd.DataFrame, start_year: int, end_year: int
) -> pd.DataFrame:
    """Pairs each budget with every modelled year in its window.

    The result has one row per budget and year. The modelled years are every
    year from start_year to end_year, both inclusive.

    I/O Example (start_year 2029, end_year 2050):
        budgets:
            budget_id         year_from  year_to  budget
            nem_2027_to_2030  2027       2030     290.0   # 2027 and 2028 aren't modelled
            nem_2035_to_2035  2035       2035     23.0

        returns:
            budget_id         year_from  year_to  budget  year
            nem_2027_to_2030  2027       2030     290.0   2029
            nem_2027_to_2030  2027       2030     290.0   2030
            nem_2035_to_2035  2035       2035     23.0    2035
    """
    modelled_years = pd.DataFrame({"year": range(start_year, end_year + 1)})
    budget_years = budgets.merge(modelled_years, how="cross")
    is_in_window = budget_years["year"].between(
        budget_years["year_from"], budget_years["year_to"]
    )
    return budget_years[is_in_window].reset_index(drop=True)


def _share_budgets_along_trajectory(
    budget_years: pd.DataFrame, carbon_budgets: pd.DataFrame
) -> pd.DataFrame:
    """Shares each budget out over the years of its window along the
    trajectory, adding the budget_share column.

    The share depends on the length of the budget's window:

    - More than one year: the share for a year is the trajectory's emissions
      in that year (see _build_emissions_trajectory).
    - A single year: there is nothing to share out, so that year takes the
      whole budget, as it would with equal shares.

    The trajectory is built from every multi-year budget in carbon_budgets,
    including those with no modelled years, as they still shape it.

    I/O Example (rows for two of the modelled years, 2030 and 2035):
        carbon_budgets:
            budget_id         year_from  year_to  budget
            nem_2027_to_2030  2027       2030     290.0
            nem_2027_to_2050  2027       2050     583.0
            nem_2035_to_2035  2035       2035     23.0

        budget_years, each budget above paired with those two years where
        they are in its window:
            budget_id         year_from  year_to  budget  year
            nem_2027_to_2030  2027       2030     290.0   2030
            nem_2027_to_2050  2027       2050     583.0   2030
            nem_2027_to_2050  2027       2050     583.0   2035
            nem_2035_to_2035  2035       2035     23.0    2035

        returns, where the trajectory's emissions are 40.1 Mt in 2030 and
        22.7075 Mt in 2035:
            budget_id         year_from  year_to  budget  year  budget_share
            nem_2027_to_2030  2027       2030     290.0   2030  40.1
            nem_2027_to_2050  2027       2050     583.0   2030  40.1
            nem_2027_to_2050  2027       2050     583.0   2035  22.7075
            nem_2035_to_2035  2035       2035     23.0    2035  23.0      # one year: its budget
    """
    modelled_years = budget_years[["year"]].drop_duplicates()
    trajectory = _build_emissions_trajectory(carbon_budgets, modelled_years)
    budget_years = budget_years.merge(trajectory, on="year", how="left")
    # A budget on a single year has nothing to share out: its year takes all
    # of it, as it would with equal shares.
    follows_trajectory = budget_years["year_from"] < budget_years["year_to"]
    budget_years["budget_share"] = budget_years["emissions"].where(
        follows_trajectory, budget_years["budget"]
    )
    return budget_years.drop(columns="emissions")


def _build_emissions_trajectory(
    carbon_budgets: pd.DataFrame, modelled_years: pd.DataFrame
) -> pd.DataFrame:
    """Returns the emissions assumed for each modelled year under the
    linear_trajectory_to_zero scaling option.

    The trajectory is a line of assumed yearly emissions over the years the
    multi-year carbon budgets cover. It is drawn so that:

    - The emissions over each budget's window add up to exactly that budget.
    - Emissions fall in a straight line from one budget's end to the next, and
      reach zero at the end of the last budget.

    For example, budgets of 290 Mt for 2027 to 2030 and 583 Mt for 2027 to
    2050 give this trajectory:

        Mt a year
            115.7 *
                  |*
                  | *
                  |  *
             29.3 |   *
                  |          *
                  |                *
                0 +---+-------------------*
                2027 2030                2050
                  |290|      293 Mt       |
                  | Mt|    (583 - 290)    |

    The trajectory starts at 115.7 Mt a year at the start of 2027, falls to
    29.3 by the end of 2030 and to zero by the end of 2050. The budgets are
    the areas under it: 290 Mt up to the end of 2030 and 583 Mt up to the end
    of 2050.

    A year's emissions are the area under the trajectory for that year.
    Modelled years outside the multi-year budgets aren't returned. Budgets on
    a single year take no part in the trajectory, as they never need scaling.

    How the trajectory is built:

    1. A knot is placed at the start of the budgets and at the end of each
       budget's window, with the emissions allowed up to it: 0 at the start of
       2027, 290 Mt by the end of 2030 and 583 Mt by the end of 2050.
    2. The rate of emissions at each knot is worked out from the last knot
       back to the first, starting from zero (see
       _emissions_rate_at_each_knot). Above, that gives 29.3 Mt a year at the
       end of 2030 and 115.7 at the start of 2027.
    3. The rate between two knots is a straight line from one to the other, so
       a year's emissions are the rate half way through it.

    The multi-year budgets must fit together into a single falling
    trajectory. A ValueError is raised unless:

    - They all start in the same year and end in different years, so that each
      sits inside the next longer one. 2027 to 2030 alongside 2027 to 2050 is
      fine. 2027 to 2030 alongside 2031 to 2050 is not.
    - No budget is smaller than a shorter one inside it. A 2027 to 2050 budget
      of 200 Mt alongside 290 Mt for 2027 to 2030 would leave less than
      nothing for 2031 to 2050.
    - Emissions never have to rise to use the budgets in full. In the chart,
      2031 to 2050 starts at 29.3 Mt a year, and 2027 to 2030 falls to meet
      it. Had the longer budget been 1100 Mt, 2031 to 2050 would start at 81
      Mt a year. 2027 to 2030 is only allowed 72.5 Mt a year on average, so
      it would have to start at 64 and rise to 81.

    I/O Example:
        carbon_budgets:
            budget_id         year_from  year_to  budget
            nem_2027_to_2030  2027       2030     290.0
            nem_2027_to_2050  2027       2050     583.0
            nem_2035_to_2035  2035       2035     23.0    # one year: not on the trajectory

        modelled_years:
            year
            2026    # before the trajectory starts: not returned
            2027
            2030
            2031
            2050

        returns:
            year  emissions
            2027  104.9      # falling from 115.7 to 29.3 over 2027 to 2030
            2030  40.1
            2031  28.5675    # falling from 29.3 to zero over 2031 to 2050
            2050  0.7325
    """
    is_multi_year = carbon_budgets["year_from"] < carbon_budgets["year_to"]
    budgets = carbon_budgets[is_multi_year].sort_values("year_to")
    _raise_if_budgets_are_not_nested(budgets)
    first_year = budgets["year_from"].min()
    # A knot is the end of a year: the end of the year before the budgets
    # start, then the end of each budget's window.
    knot_years = np.array([first_year - 1, *budgets["year_to"]], dtype="float64")
    # Every budget's window starts in first_year, so a budget is also the total
    # emissions allowed from first_year to the end of its window. For 290 Mt
    # over 2027 to 2030 and 583 Mt over 2027 to 2050 this is [0, 290, 583].
    cumulative_budget = np.array([0.0, *budgets["budget"]], dtype="float64")
    rates = _emissions_rate_at_each_knot(knot_years, cumulative_budget)
    _raise_if_emissions_would_rise(knot_years, rates)

    # The trajectory only covers the years the multi-year budgets do: from the
    # year they all start in to the year the longest one ends.
    is_on_trajectory = modelled_years["year"].between(
        first_year, budgets["year_to"].max()
    )
    # Keep the modelled years on the trajectory, and number the rows from zero
    # again so the emissions worked out below line up with them row by row.
    trajectory = modelled_years[is_on_trajectory].reset_index(drop=True)
    # The rate half way through a year is the area under the trajectory for
    # that year. A year ends at its own number on the knots' scale, so half
    # way through it is half a year before that.
    trajectory["emissions"] = np.interp(trajectory["year"] - 0.5, knot_years, rates)
    return trajectory


def _raise_if_budgets_are_not_nested(budgets: pd.DataFrame) -> None:
    """Raises unless the budgets are nested.

    Budgets are nested when they all start in the same year and end in
    different years, so that each sits inside the next longer one. Only then
    is each budget the total emissions allowed from the shared first year to
    the end of its window, which is what the trajectory is drawn through.

    Every budget that starts later than the earliest, or ends in the same year
    as another, is named in the error.

    I/O Example:
        budgets:
            budget_id         year_from  year_to
            nem_2027_to_2030  2027       2030
            nem_2031_to_2050  2031       2050      # starts later than 2027

        raises ValueError: "Carbon budgets that start later than the earliest
        budget, or end in the same year as another: ['nem_2031_to_2050']. With
        carbon_budget_scaling 'linear_trajectory_to_zero', the multi-year
        budgets must all start in the same year and end in different years."
    """
    starts_later = budgets["year_from"] != budgets["year_from"].min()
    ends_with_another = budgets["year_to"].duplicated(keep=False)
    not_nested = sorted(budgets.loc[starts_later | ends_with_another, "budget_id"])
    if not_nested:
        raise ValueError(
            f"Carbon budgets that start later than the earliest budget, or end in "
            f"the same year as another: {not_nested}. With carbon_budget_scaling "
            f"'linear_trajectory_to_zero', the multi-year budgets must all start "
            f"in the same year and end in different years."
        )


def _emissions_rate_at_each_knot(
    knot_years: np.ndarray, cumulative_budget: np.ndarray
) -> np.ndarray:
    """Works out the rate of emissions, in Mt a year, at each knot of the
    trajectory.

    A knot is the end of a year at which the trajectory starts, ends or
    changes slope. knot_years holds those years in order, and
    cumulative_budget the total emissions allowed from the first knot
    up to each one. The trajectory is then the straight lines joining
    the rates returned here.

    The rate at the last knot is zero. Each earlier rate is worked out from
    the one after it: between two knots the rate is a straight line, so its
    average is half way between its two ends, and that average is fixed by
    the emissions allowed between the knots. So the rate at the earlier knot
    is twice the average, less the rate at the later knot.

    I/O Example:
        knot_years:         [2026, 2030, 2050]    # each the end of that year
        cumulative_budget:  [0.0, 290.0, 583.0]   # total Mt allowed up to each knot

        returns:            [115.7, 29.3, 0.0]

        2030 to 2050 is allowed 293 Mt over 20 years, an average of 14.65. It
        ends at zero, so it starts at 2 x 14.65 - 0 = 29.3. 2026 to 2030 is
        allowed 290 Mt over 4 years, an average of 72.5. It ends at 29.3, so
        it starts at 2 x 72.5 - 29.3 = 115.7.
    """
    rates = np.zeros(len(knot_years))
    for later in range(len(knot_years) - 1, 0, -1):
        earlier = later - 1
        years_between = knot_years[later] - knot_years[earlier]
        allowed_between = cumulative_budget[later] - cumulative_budget[earlier]
        average_rate = allowed_between / years_between
        rates[earlier] = 2 * average_rate - rates[later]
    return rates


def _raise_if_emissions_would_rise(knot_years: np.ndarray, rates: np.ndarray) -> None:
    """Raises when emissions would have to rise between two knots of the
    trajectory.

    They would when the rate at a knot is higher than the rate at the knot
    before it. The trajectory is meant to fall all the way to zero, so budgets
    that need a rise can't be scaled this way. Each stretch between two knots
    over which emissions would rise is named in the error as (first year,
    last year).

    I/O Example:
        knot_years:  [2026, 2030, 2040, 2050]
        rates:       [153.0, -8.0, 20.0, 0.0]    # rises from 2030 to 2040

        raises ValueError: "With carbon_budget_scaling
        'linear_trajectory_to_zero', emissions would have to rise over the
        years [(2031, 2040)] to use the carbon budgets in full. Either the
        budgets allow too many emissions after those years for the trajectory
        to keep falling, or a longer budget is smaller than a shorter one."
    """
    rises = rates[:-1] < rates[1:] - _RISING_RATE_TOLERANCE
    if rises.any():
        first_years = (knot_years[:-1][rises] + 1).astype(int).tolist()
        last_years = knot_years[1:][rises].astype(int).tolist()
        raise ValueError(
            f"With carbon_budget_scaling 'linear_trajectory_to_zero', emissions "
            f"would have to rise over the years {list(zip(first_years, last_years))} "
            f"to use the carbon budgets in full. Either the budgets allow too "
            f"many emissions after those years for the trajectory to keep "
            f"falling, or a longer budget is smaller than a shorter one."
        )
