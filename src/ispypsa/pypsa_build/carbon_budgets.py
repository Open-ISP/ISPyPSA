import linopy
import pandas as pd
import pypsa

_TONNES_PER_MT = 1e6
_HOURS_PER_YEAR = 8760


def _add_carbon_budgets(
    network: pypsa.Network,
    carbon_budgets: pd.DataFrame,
    snapshots: pd.DataFrame,
    generators: pd.DataFrame,
) -> None:
    """Adds one constraint per carbon budget to the network's linopy model:
    the emissions of every generator, added up over the snapshots whose model
    year is in the budget's window, must not exceed the budget's rhs.

    The linopy model must already be built. carbon_budgets is the table
    written by ispypsa.translator.carbon_budgets. snapshots is the
    pypsa-friendly snapshots table, of which investment_periods, snapshots,
    model_year and stores (each snapshot's length in hours) are used.
    generators is the pypsa-friendly generators table, of which only name and
    emissions_intensity (t CO2-e/MWh) are used.

    A generator's emissions at a snapshot are its output times its
    emissions_intensity times the hours of the model year the snapshot stands
    for: its length, scaled so the snapshots of each model year add up to 8760
    hours. They are added up in Mt, like the rhs, which keeps the constraint
    well scaled: in tonnes a limit would be in the hundreds of millions.

    - A carbon budget's window is matched on model years, not investment
      periods, so it can start or end part way through an investment period.
    - Generators with a zero or blank emissions_intensity are left out.
    - A generator that isn't built yet, or has retired, in a snapshot's
      investment period adds nothing there: PyPSA masks its output variables
      and linopy leaves masked variables out of the sum.
    - No constraints are added when no generator has an emissions_intensity
      above zero, as there are no emissions to limit.

    I/O Example:
        carbon_budgets:
            constraint_name   year_from  year_to  rhs
            nem_2027_to_2030  2027       2030     290.0

        snapshots:
            investment_periods  snapshots            model_year  stores
            2027                2026-07-01 12:00:00  2027        12.0   # t1
            2027                2026-07-02 00:00:00  2027        12.0   # t2
            2031                2030-07-01 12:00:00  2031        12.0   # after the budget's window

        generators:
            name   emissions_intensity
            BW01   0.88
            Q1G1   0.0                   # emits nothing: left out

        adds constraint (t1 and t2 each stand for half of 2027, 4380 hours, so
        a MW of BW01's output at either emits 4380 x 0.88 = 3854.4 t, or
        0.0038544 Mt):
            nem_2027_to_2030:
                0.0038544 Generator-p[t1, BW01] + 0.0038544 Generator-p[t2, BW01] <= 290
    """
    emitters = generators[generators["emissions_intensity"] > 0]
    if emitters.empty:
        return
    snapshots = _weight_snapshots_by_model_year(snapshots)
    for budget in carbon_budgets.itertuples():
        is_in_window = snapshots["model_year"].between(budget.year_from, budget.year_to)
        hours_represented = snapshots.loc[is_in_window, "hours_represented"]
        emissions = _sum_emissions(network.model, emitters, hours_represented)
        network.model.add_constraints(
            emissions, "<=", budget.rhs, name=budget.constraint_name
        )


def _weight_snapshots_by_model_year(snapshots: pd.DataFrame) -> pd.DataFrame:
    """Weights each snapshot by the hours of its model year that it represents
    (hours_represented), so that output in MW times the weight gives energy in
    MWh.

    The weight is the snapshot's length (stores), scaled so that the weights of
    each model year's snapshots sum to 8760 hours. A model year with a few
    representative weeks is then counted as a whole year.

    The result is indexed by (period, timestep), as the network's snapshots
    are, so its rows can be matched to the model's variables.

    I/O Example:
        snapshots:
            investment_periods  snapshots            model_year  stores
            2027                2026-07-01 01:00:00  2027        1.0
            2027                2026-07-01 04:00:00  2027        3.0    # a longer snapshot
            2027                2027-07-01 12:00:00  2028        12.0   # the only one in 2028

        returns:
            period  timestep             model_year  hours_represented
            2027    2026-07-01 01:00:00  2027        2190.0   # 8760 x 1 / 4
            2027    2026-07-01 04:00:00  2027        6570.0   # 8760 x 3 / 4
            2027    2027-07-01 12:00:00  2028        8760.0
    """
    # PyPSA names the two levels of the network's snapshots "period" and
    # "timestep", and linopy matches data to the model's variables by those
    # names.
    snapshots = snapshots.rename(
        columns={"investment_periods": "period", "snapshots": "timestep"}
    )
    snapshots["timestep"] = pd.to_datetime(snapshots["timestep"])
    # transform gives every snapshot its model year's total, not one row per
    # model year.
    hours_modelled_in_year = snapshots.groupby("model_year")["stores"].transform("sum")
    snapshots["hours_represented"] = (
        _HOURS_PER_YEAR * snapshots["stores"] / hours_modelled_in_year
    )
    snapshots = snapshots.set_index(["period", "timestep"])
    return snapshots[["model_year", "hours_represented"]]


def _sum_emissions(
    model: linopy.Model, emitters: pd.DataFrame, hours_represented: pd.Series
) -> linopy.LinearExpression:
    """The total emissions, in Mt, of the emitters over the snapshots in
    hours_represented: each generator's output at each snapshot times its
    emissions intensity and the hours the snapshot represents.

    I/O Example:
        emitters:
            name     emissions_intensity
            BW01     0.88
            TALWA1   0.37

        hours_represented:
            period  timestep
            2027    2026-07-01 12:00:00    4380.0   # t1
            2027    2026-07-02 00:00:00    4380.0   # t2

        returns:
            0.0038544 Generator-p[t1, BW01] + 0.0038544 Generator-p[t2, BW01]
            + 0.0016206 Generator-p[t1, TALWA1] + 0.0016206 Generator-p[t2, TALWA1]
    """
    intensity = emitters.set_index("name")["emissions_intensity"]
    output = model.variables["Generator-p"].sel(
        snapshot=hours_represented.index, name=list(intensity.index)
    )
    # linopy lines up hours_represented with the snapshots and intensity with
    # the generators, going by the names of their indexes.
    energy = output * hours_represented
    emissions = energy * intensity / _TONNES_PER_MT
    return emissions.sum()
