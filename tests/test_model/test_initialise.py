import pandas as pd

from ispypsa.pypsa_build.initialise import _initialise_network


def test_network_initialisation(csv_str_to_df):
    snapshots = csv_str_to_df("""
        investment_periods,  snapshots,            objective,  generators,  stores,  model_year
        2020,                2019-07-01 00:30:00,  4380.0,     4380.0,      0.5,     2020
        2020,                2019-07-01 01:00:00,  4380.0,     4380.0,      0.5,     2020
        2021,                2020-07-01 00:30:00,  8760.0,     8760.0,      0.5,     2021
    """)

    network = _initialise_network(snapshots)

    expected_snapshots = pd.MultiIndex.from_arrays(
        [
            [2020, 2020, 2021],
            pd.to_datetime(
                ["2019-07-01 00:30:00", "2019-07-01 01:00:00", "2020-07-01 00:30:00"]
            ),
        ]
    )
    pd.testing.assert_index_equal(
        network.snapshots, expected_snapshots, check_names=False
    )
    assert list(network.investment_periods) == [2020, 2021]
    # Only the three weighting columns become snapshot weightings: model_year
    # is a property of the snapshot, not a weighting.
    expected_weightings = csv_str_to_df("""
        objective,  generators,  stores
        4380.0,     4380.0,      0.5
        4380.0,     4380.0,      0.5
        8760.0,     8760.0,      0.5
    """)
    pd.testing.assert_frame_equal(
        network.snapshot_weightings.reset_index(drop=True),
        expected_weightings,
        check_names=False,
    )
