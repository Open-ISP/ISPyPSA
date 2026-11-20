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
    """Translates the carbon budgets into the limits
    ispypsa.pypsa_build.carbon_budgets adds to the model as constraints: one
    row per budget, with its limit on the emissions of the modelled years.

    Only budgets with a modelled year (config.temporal.range) in their window
    are returned. The rhs is the budget itself when every year of its window
    is modelled. Otherwise config.carbon_budget_scaling decides:

    - "none": raises a ValueError naming the budgets.
    - "prorata": every year of the budget window gets an equal share of the budget,
      and the rhs is the modelled years' shares added up.
    - "linear_trajectory_to_zero": the yearly shares follow a path of emissions
      that falls in straight lines to zero at the end of the last budget (see
      _build_emissions_trajectory), and the rhs is again the modelled years'
      shares added up.

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
    """Works out each budget's limit on the emissions of the modelled years in
    its window.

    Each budget is shared out over the years of its window, and its limit is
    the shares of the modelled years added up. So a budget whose whole window
    is modelled keeps its full size whatever the scaling. Budgets with no
    modelled years are dropped, and with scaling "none" a budget with only part
    of its window modelled raises.

    I/O Example (modelled years 2029 to 2050):
        carbon_budgets:
            budget_id         year_from  year_to  budget
            nem_2020_to_2025  2020       2025     600.0   # no modelled years: dropped
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
            nem_2027_to_2030  2027       2030     101.8   # the path's 2029 and 2030
            nem_2027_to_2050  2027       2050     394.8   # 101.8 + 293 for 2031 to 2050
            nem_2035_to_2035  2035       2035     23.0

        raises (scaling "none"), as two windows are only partly modelled
    """
    budgets = _drop_budgets_with_no_modelled_years(carbon_budgets, start_year, end_year)
    if scaling == "none":
        _raise_on_partly_modelled_windows(budgets, start_year, end_year)

    # One row per budget and modelled year in its window.
    modelled_years = pd.DataFrame({"year": range(start_year, end_year + 1)})
    yearly = budgets.merge(modelled_years, how="cross")
    yearly = yearly[yearly["year"].between(yearly["year_from"], yearly["year_to"])]

    if scaling == "linear_trajectory_to_zero":
        yearly = _add_shares_along_trajectory(yearly, carbon_budgets, modelled_years)
    else:
        window_length = yearly["year_to"] - yearly["year_from"] + 1
        yearly["budget_share"] = yearly["budget"] / window_length

    return _sum_shares_into_limits(yearly)


def _sum_shares_into_limits(yearly: pd.DataFrame) -> pd.DataFrame:
    """Adds up each budget's yearly shares into its limit, named and laid out
    as the pypsa-friendly carbon_budgets table.

    I/O Example:
        yearly:
            budget_id         year_from  year_to  year  budget_share
            nem_2027_to_2030  2027       2030     2029  72.5
            nem_2027_to_2030  2027       2030     2030  72.5
            nem_2035_to_2035  2035       2035     2035  23.0

        returns:
            constraint_name   year_from  year_to  rhs
            nem_2027_to_2030  2027       2030     145.0
            nem_2035_to_2035  2035       2035     23.0
    """
    limits = yearly.groupby(["budget_id", "year_from", "year_to"], as_index=False)[
        "budget_share"
    ].sum()
    limits = limits.rename(columns={"budget_id": "constraint_name"})
    # To the nearest tonne, which also clears the float noise left by adding up
    # the yearly shares. A table with no rows has no numeric dtype to round.
    limits["rhs"] = limits["budget_share"].astype("float64").round(6)
    return limits[_CARBON_BUDGET_COLUMNS]


def _drop_budgets_with_no_modelled_years(
    carbon_budgets: pd.DataFrame, start_year: int, end_year: int
) -> pd.DataFrame:
    """Drops the budgets whose window has no year between start_year and
    end_year, and logs them.

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
    """Raises when a budget's window starts before start_year or ends after
    end_year.

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


def _add_shares_along_trajectory(
    yearly: pd.DataFrame, carbon_budgets: pd.DataFrame, modelled_years: pd.DataFrame
) -> pd.DataFrame:
    """Adds each budget's share for each year: the emissions of that year on
    the linear_trajectory_to_zero path, or the whole budget when it is on a
    single year.

    The path is built from every multi-year budget in carbon_budgets, including
    those with no modelled years, as they still shape it.

    I/O Example:
        yearly:
            budget_id         year_from  year_to  budget  year
            nem_2027_to_2030  2027       2030     290.0   2030
            nem_2027_to_2050  2027       2050     583.0   2030
            nem_2035_to_2035  2035       2035     23.0    2035

        carbon_budgets:
            budget_id         year_from  year_to  budget
            nem_2027_to_2030  2027       2030     290.0
            nem_2027_to_2050  2027       2050     583.0
            nem_2035_to_2035  2035       2035     23.0

        modelled_years:
            year
            2030
            2035

        returns:
            budget_id         year_from  year_to  budget  year  budget_share
            nem_2027_to_2030  2027       2030     290.0   2030  40.1    # the path in 2030
            nem_2027_to_2050  2027       2050     583.0   2030  40.1
            nem_2035_to_2035  2035       2035     23.0    2035  23.0    # one year: its budget
    """
    trajectory = _build_emissions_trajectory(carbon_budgets, modelled_years)
    yearly = yearly.merge(trajectory, on="year", how="left")
    # A single-year budget's share is the whole budget. Every other row takes
    # the path's emissions for its year.
    is_single_year = yearly["year_from"] == yearly["year_to"]
    yearly["budget_share"] = yearly["budget"].where(is_single_year, yearly["emissions"])
    return yearly.drop(columns="emissions")


def _build_emissions_trajectory(
    carbon_budgets: pd.DataFrame, modelled_years: pd.DataFrame
) -> pd.DataFrame:
    """Returns the emissions assumed for each modelled year under the
    linear_trajectory_to_zero scaling option.

    The trajectory is an assumed path of yearly emissions over the years the
    multi-year carbon budgets cover. It is drawn so that:

    - The emissions over each budget's window add up to exactly that budget.
    - Emissions fall in a straight line from one budget's end to the next, and
      reach zero at the end of the last budget.

    For example, budgets of 290 Mt for 2027 to 2030 and 583 Mt for 2027 to
    2050 give this path:

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

    The path starts at 115.7 Mt a year at the start of 2027, falls to 29.3 by
    the end of 2030 and to zero by the end of 2050. The budgets are the areas
    under it: 290 Mt up to the end of 2030 and 583 Mt up to the end of 2050.

    A year's emissions are the area under the path for that year. Modelled
    years outside the multi-year budgets aren't returned. Budgets on a single
    year take no part in the path, as they never need scaling.

    How the path is built:

    1. A knot is placed at the start of the budgets and at the end of each
       budget's window, with the emissions allowed up to it: 0 at the start of
       2027, 290 Mt by the end of 2030 and 583 Mt by the end of 2050.
    2. The rate of emissions at each knot is worked out from the last knot
       back to the first, starting from zero (see
       _emissions_rate_at_each_knot). Above, that gives 29.3 Mt a year at the
       end of 2030 and 115.7 at the start of 2027.
    3. The rate between two knots is a straight line from one to the other, so
       a year's emissions are the rate half way through it.

    The multi-year budgets must fit together into a single falling path. A
    ValueError is raised unless:

    - They all start in the same year and end in different years, so that each
      sits inside the next longer one. 2027 to 2030 alongside 2027 to 2050 is
      fine. 2027 to 2030 alongside 2031 to 2050 is not.
    - Emissions never have to rise to use the budgets in full. They would if
      a later run of years were allowed more emissions a year than the years
      before it, or if a longer budget were smaller than a shorter one.

    I/O Example:
        carbon_budgets:
            budget_id         year_from  year_to  budget
            nem_2027_to_2030  2027       2030     290.0
            nem_2027_to_2050  2027       2050     583.0
            nem_2035_to_2035  2035       2035     23.0    # one year: not part of the path

        modelled_years:
            year
            2026    # before the path starts: not returned
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

    # The path only covers the years the multi-year budgets do: from the year
    # they all start in to the year the longest one ends.
    is_on_path = modelled_years["year"].between(first_year, budgets["year_to"].max())
    # Keep the modelled years on the path, and number the rows from zero again
    # so the emissions worked out below line up with them row by row.
    trajectory = modelled_years[is_on_path].reset_index(drop=True)
    # The rate half way through a year is the area under the path for that
    # year. A year ends at its own number on the knots' scale, so half way
    # through it is half a year before that.
    trajectory["emissions"] = np.interp(trajectory["year"] - 0.5, knot_years, rates)
    return trajectory


def _raise_if_budgets_are_not_nested(budgets: pd.DataFrame) -> None:
    """Raises unless the budgets all start in the same year and end in
    different years, so that each sits inside the next longer one.

    Only then is each budget the emissions allowed from that shared first year
    to the end of its window, which is what the trajectory is drawn through.

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
    """The rate of emissions, in Mt a year, at each knot of the trajectory.

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
    """Raises when the rate of emissions at a knot is higher than at the knot
    before it, as the trajectory is meant to fall all the way to zero.

    I/O Example:
        knot_years:  [2026, 2030, 2040, 2050]
        rates:       [153.0, -8.0, 20.0, 0.0]    # rises from 2030 to 2040

        raises ValueError: "With carbon_budget_scaling
        'linear_trajectory_to_zero', emissions would have to rise over the
        years [(2031, 2040)] to use the carbon budgets in full. Either the
        budgets allow more emissions a year after those years than during
        them, or a longer budget is smaller than a shorter one."
    """
    rises = rates[:-1] < rates[1:] - _RISING_RATE_TOLERANCE
    if rises.any():
        first_years = (knot_years[:-1][rises] + 1).astype(int).tolist()
        last_years = knot_years[1:][rises].astype(int).tolist()
        raise ValueError(
            f"With carbon_budget_scaling 'linear_trajectory_to_zero', emissions "
            f"would have to rise over the years {list(zip(first_years, last_years))} "
            f"to use the carbon budgets in full. Either the budgets allow more "
            f"emissions a year after those years than during them, or a longer "
            f"budget is smaller than a shorter one."
        )
