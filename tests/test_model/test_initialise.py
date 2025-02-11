from pathlib import Path

from ispypsa.model.initialise import initialise_network
from ispypsa.translator.snapshot import create_complete_snapshots_index
from ispypsa.translator.time_series_checker import check_time_series


def test_network_initialisation(tmp_path):
    snapshots = create_complete_snapshots_index(
        start_year=2020,
        end_year=2020,
        operational_temporal_resolution_min=30,
        year_type="fy",
    )
    snapshots = snapshots.reset_index()
    network = initialise_network(snapshots)
    check_time_series(network.snapshots, snapshots["snapshots"], "", "")
