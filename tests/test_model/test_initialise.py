from pathlib import Path


from ispypsa.model.initialise import initialise_network
from ispypsa.translator.snapshot import create_complete_snapshot_index
from ispypsa.translator.time_series_checker import check_time_series


def test_network_initialisation(tmp_path):
    snapshot = create_complete_snapshot_index(
        start_year=2020,
        end_year=2020,
        operational_temporal_resolution_min=30,
        year_type="fy",
    )
    snapshot.to_csv(Path(tmp_path, "snapshot.csv"))
    network = initialise_network(Path(tmp_path))
    check_time_series(network.snapshots, snapshot.index, "", "")
