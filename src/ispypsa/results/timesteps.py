import pandas as pd

from ispypsa.pypsa_build.carbon_budgets import _weight_snapshots_by_model_year


def extract_timesteps(snapshots: pd.DataFrame) -> pd.DataFrame:
    """Extracts the length of each timestep the model was solved on, and the
    hours of its model year it stands for.

    The time-indexed results (generator_dispatch, demand, transmission_flows
    and the three regional flow tables) give a value in MW for each timestep.
    Turning those into energy needs two things the tables don't hold. The first
    is how long each timestep lasts: half an hour at a 30 minute resolution,
    but with tsam segments each timestep is a run of merged intervals, and the
    runs differ in length. The second is how much of the year each timestep
    stands for: a model year solved on two representative weeks covers 336
    hours, and each of those hours stands for about 26 hours of that year.

    snapshots is the pypsa-friendly snapshots table of the model that was
    solved: the snapshots table for capacity expansion, operational_snapshots
    for the operational model. Its investment_periods, snapshots, model_year
    and stores columns are used.

    Returns a table with one row per timestep, which can be merged onto any
    time-indexed results table on investment_period and timestep. Its columns
    are:

    - investment_period: the investment period the timestep belongs to.
    - timestep: the time the timestep ends, as in the other results tables.
    - model_year: the model year the timestep falls in. An investment period
      can cover several: a 2030 investment period that runs to 2034 has
      timesteps in five model years.
    - duration_hours: the length of the timestep in hours (the snapshots
      table's stores column), so that a value in MW times this gives the
      energy in MWh over the timestep itself. Use it for what happens inside
      the modelled time, such as the energy a battery charges in a timestep.
    - hours_represented: the hours of its model year that the timestep stands
      for, so that a value in MW times this, summed over a model year's
      timesteps, gives that year's energy in MWh. It is duration_hours scaled
      so that the timesteps of each model year sum to 8760, and it is the
      weight to average by: an annual capacity factor is the average of
      dispatch over capacity weighted by this column. On a model solved on
      every interval of the year it equals duration_hours. It is the weight
      the carbon budgets count emissions with.

    hours_represented is not PyPSA's "generators" or "objective" snapshot
    weighting. Those are scaled to 8760 hours per investment period, not per
    model year, so over an investment period of several model years they give
    the energy of an average year, and no single year's energy can be worked
    out from them.

    I/O Example:
        snapshots:
            investment_periods  snapshots            model_year  stores
            2030                2029-07-01 00:30:00  2030        0.5
            2030                2029-07-01 02:00:00  2030        1.5   # a segment of three half-hours
            2030                2030-07-01 00:30:00  2031        0.5   # the only timestep in 2031

        returns:
            investment_period  timestep             model_year  duration_hours  hours_represented
            2030               2029-07-01 00:30:00  2030        0.5             2190.0   # 8760 x 0.5 / 2
            2030               2029-07-01 02:00:00  2030        1.5             6570.0   # 8760 x 1.5 / 2
            2030               2030-07-01 00:30:00  2031        0.5             8760.0
    """
    hours_represented = _weight_snapshots_by_model_year(snapshots).reset_index()
    timesteps = snapshots.rename(
        columns={
            "investment_periods": "period",
            "snapshots": "timestep",
            "stores": "duration_hours",
        }
    )
    timesteps["timestep"] = pd.to_datetime(timesteps["timestep"])
    timesteps = timesteps.merge(
        hours_represented, on=["period", "timestep", "model_year"]
    )
    timesteps = timesteps.rename(columns={"period": "investment_period"})
    return timesteps[
        [
            "investment_period",
            "timestep",
            "model_year",
            "duration_hours",
            "hours_represented",
        ]
    ]
