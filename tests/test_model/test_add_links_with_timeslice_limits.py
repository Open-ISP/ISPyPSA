import pandas as pd
import pypsa
import pytest

from ispypsa.pypsa_build.links import _add_links_to_network


def _network() -> pypsa.Network:
    snapshots = pd.date_range("2025-01-01", periods=4, freq="h")
    index = pd.MultiIndex.from_arrays([[2025] * 4, list(snapshots)])
    network = pypsa.Network(snapshots=index, investment_periods=[2025])
    network.add("Bus", "bus1")
    network.add("Bus", "bus2")
    return network


def _links(csv_str_to_df) -> pd.DataFrame:
    return csv_str_to_df("""
        name,            bus0,  bus1,  carrier,  p_nom,  p_min_pu,  p_nom_extendable
        CQ-NQ_existing,  bus1,  bus2,  AC,       1400,   -1.364286, False
    """)


def test_links_get_per_snapshot_pu_series(csv_str_to_df):
    network = _network()
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,        value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,  0.857143
        CQ-NQ_existing,  p_min_pu,   qld_peak_demand,  -1.028571
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice_id,     investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
        qld_peak_demand,  2025,                2025-01-01 02:00:00
    """)

    _add_links_to_network(
        network, _links(csv_str_to_df), link_timeslice_limits, timeslice_snapshots
    )

    p_max_pu = network.links_t.p_max_pu["CQ-NQ_existing"]
    p_min_pu = network.links_t.p_min_pu["CQ-NQ_existing"]
    # The static (winter) limit holds outside the tagged snapshots.
    expected_p_max_pu = [1.0, 0.857143, 0.857143, 1.0]
    expected_p_min_pu = [-1.364286, -1.028571, -1.028571, -1.364286]
    assert p_max_pu.tolist() == pytest.approx(expected_p_max_pu)
    assert p_min_pu.tolist() == pytest.approx(expected_p_min_pu)


def test_links_without_timeslice_limits_stay_static(csv_str_to_df):
    network = _network()
    link_timeslice_limits = csv_str_to_df("""
        name,  attribute,  timeslice,  value
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice_id,  investment_periods,  snapshots
    """)

    _add_links_to_network(
        network, _links(csv_str_to_df), link_timeslice_limits, timeslice_snapshots
    )

    assert "CQ-NQ_existing" not in network.links_t.p_max_pu.columns
    assert network.links.loc["CQ-NQ_existing", "p_min_pu"] == pytest.approx(-1.364286)


def test_links_old_format_call_without_limit_tables(csv_str_to_df):
    network = _network()

    _add_links_to_network(network, _links(csv_str_to_df))

    assert network.links.loc["CQ-NQ_existing", "p_nom"] == 1400
