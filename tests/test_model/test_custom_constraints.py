from datetime import datetime
from pathlib import Path

import pandas as pd

from ispypsa.data_fetch import read_csvs
from ispypsa.model import build_pypsa_network


def test_custom_constraints():
    start_date = datetime(year=2025, month=1, day=1, hour=0, minute=0)
    end_date = datetime(year=2025, month=1, day=2, hour=0, minute=0)

    snapshots = pd.date_range(
        start=start_date, end=end_date, freq="30min", name="snapshots"
    )

    snapshots = pd.DataFrame(
        {
            "investment_periods": 2025,
            "snapshots": snapshots,
        }
    )
    pypsa_friendly_inputs_location = Path(
        "tests/test_model/test_pypsa_friendly_inputs/test_custom_constraints"
    )
    snapshots.to_csv(pypsa_friendly_inputs_location / Path("snapshots.csv"))

    pypsa_friendly_inputs = read_csvs(pypsa_friendly_inputs_location)

    demand_data = snapshots.copy()
    demand_data["p_set"] = 1000.0
    demand_data.to_parquet(
        pypsa_friendly_inputs_location / Path("demand_traces/bus_two.parquet")
    )

    network = build_pypsa_network(pypsa_friendly_inputs, pypsa_friendly_inputs_location)

    network.optimize.solve_model()

    assert network.generators.loc["con_one-EXPANSION", "p_nom_opt"] == 1500.0
