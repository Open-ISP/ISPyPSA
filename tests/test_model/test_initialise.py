from pathlib import Path

import pandas as pd

from ispypsa.pypsa_build.initialise import _initialise_network
from ispypsa.translator.snapshots import (
    _add_investment_periods,
    _create_complete_snapshots_index,
)


def test_network_initialisation(tmp_path):
    snapshots = _create_complete_snapshots_index(
        start_year=2020,
        end_year=2020,
        temporal_resolution_min=30,
        year_type="fy",
    )
    snapshots = _add_investment_periods(snapshots, [2020], "fy")
    network = _initialise_network(snapshots)
    snapshots = snapshots.rename(
        columns={"investment_periods": "period", "snapshots": "timestep"}
    )
    pd.testing.assert_index_equal(
        network.snapshots,
        pd.MultiIndex.from_arrays([snapshots["period"], snapshots["timestep"]]),
    )
    assert network.investment_periods == [2020]
