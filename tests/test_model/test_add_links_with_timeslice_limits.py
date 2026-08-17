import pandas as pd
import pypsa

from ispypsa.pypsa_build.links import _add_links_to_network


def _network() -> pypsa.Network:
    snapshots = pd.date_range("2025-01-01", periods=4, freq="h")
    index = pd.MultiIndex.from_arrays([[2025] * 4, list(snapshots)])
    network = pypsa.Network(snapshots=index, investment_periods=[2025])
    network.add("Bus", "bus1")
    network.add("Bus", "bus2")
    return network


def _links(csv_str_to_df) -> pd.DataFrame:
    # p_max_pu / p_min_pu are the translator's placeholders; the real limits
    # come from link_timeslice_limits.
    return csv_str_to_df("""
        name,            bus0,  bus1,  carrier,  p_nom,  p_max_pu,  p_min_pu,  p_nom_extendable
        CQ-NQ_existing,  bus1,  bus2,  AC,       1400,   1.0,       0.0,       False
    """)


def _link_pu_limits(network: pypsa.Network, name: str) -> pd.DataFrame:
    """The per-snapshot p_max_pu / p_min_pu of one link, one row per snapshot."""
    limits = pd.DataFrame(
        {
            "p_max_pu": network.links_t.p_max_pu[name],
            "p_min_pu": network.links_t.p_min_pu[name],
        }
    )
    limits.index = limits.index.set_names(["investment_periods", "snapshots"])
    return limits.reset_index()


def test_named_timeslices_overlay_the_fallback(csv_str_to_df):
    network = _network()
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,        value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,  0.857
        CQ-NQ_existing,  p_max_pu,   ,                 1.0
        CQ-NQ_existing,  p_min_pu,   qld_peak_demand,  -0.9
        CQ-NQ_existing,  p_min_pu,   ,                 -0.714
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice_id,     investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
        qld_peak_demand,  2025,                2025-01-01 02:00:00
    """)

    _add_links_to_network(
        network, _links(csv_str_to_df), link_timeslice_limits, timeslice_snapshots
    )

    expected = csv_str_to_df("""
        investment_periods,  snapshots,            p_max_pu,  p_min_pu
        2025,                2025-01-01 00:00:00,  1.0,       -0.714
        2025,                2025-01-01 01:00:00,  0.857,     -0.9
        2025,                2025-01-01 02:00:00,  0.857,     -0.9
        2025,                2025-01-01 03:00:00,  1.0,       -0.714
    """)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])
    pd.testing.assert_frame_equal(_link_pu_limits(network, "CQ-NQ_existing"), expected)


def test_fallback_only_attribute_gets_the_fallback_at_every_snapshot(csv_str_to_df):
    # The static p_min_pu placeholder (0.0) must not survive at uncovered
    # snapshots — that would silently disable reverse flow.
    network = _network()
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,        value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,  0.857
        CQ-NQ_existing,  p_max_pu,   ,                 1.0
        CQ-NQ_existing,  p_min_pu,   ,                 -0.714
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice_id,     investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
    """)

    _add_links_to_network(
        network, _links(csv_str_to_df), link_timeslice_limits, timeslice_snapshots
    )

    expected = csv_str_to_df("""
        investment_periods,  snapshots,            p_max_pu,  p_min_pu
        2025,                2025-01-01 00:00:00,  1.0,       -0.714
        2025,                2025-01-01 01:00:00,  0.857,     -0.714
        2025,                2025-01-01 02:00:00,  1.0,       -0.714
        2025,                2025-01-01 03:00:00,  1.0,       -0.714
    """)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])
    pd.testing.assert_frame_equal(_link_pu_limits(network, "CQ-NQ_existing"), expected)


def test_named_timeslices_that_tile_the_snapshots_need_no_fallback(csv_str_to_df):
    network = _network()
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,             value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,       0.857
        CQ-NQ_existing,  p_max_pu,   qld_winter_reference,  1.0
        CQ-NQ_existing,  p_min_pu,   ,                      -0.714
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice_id,          investment_periods,  snapshots
        qld_winter_reference,  2025,                2025-01-01 00:00:00
        qld_peak_demand,       2025,                2025-01-01 01:00:00
        qld_peak_demand,       2025,                2025-01-01 02:00:00
        qld_winter_reference,  2025,                2025-01-01 03:00:00
    """)

    _add_links_to_network(
        network, _links(csv_str_to_df), link_timeslice_limits, timeslice_snapshots
    )

    expected = csv_str_to_df("""
        investment_periods,  snapshots,            p_max_pu,  p_min_pu
        2025,                2025-01-01 00:00:00,  1.0,       -0.714
        2025,                2025-01-01 01:00:00,  0.857,     -0.714
        2025,                2025-01-01 02:00:00,  0.857,     -0.714
        2025,                2025-01-01 03:00:00,  1.0,       -0.714
    """)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])
    pd.testing.assert_frame_equal(_link_pu_limits(network, "CQ-NQ_existing"), expected)


def test_named_timeslice_with_no_snapshots_leaves_the_fallback(csv_str_to_df):
    network = _network()
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,        value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,  0.857
        CQ-NQ_existing,  p_max_pu,   ,                 1.0
        CQ-NQ_existing,  p_min_pu,   ,                 -0.714
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice_id,  investment_periods,  snapshots
    """)

    _add_links_to_network(
        network, _links(csv_str_to_df), link_timeslice_limits, timeslice_snapshots
    )

    expected = csv_str_to_df("""
        investment_periods,  snapshots,            p_max_pu,  p_min_pu
        2025,                2025-01-01 00:00:00,  1.0,       -0.714
        2025,                2025-01-01 01:00:00,  1.0,       -0.714
        2025,                2025-01-01 02:00:00,  1.0,       -0.714
        2025,                2025-01-01 03:00:00,  1.0,       -0.714
    """)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])
    pd.testing.assert_frame_equal(_link_pu_limits(network, "CQ-NQ_existing"), expected)


def test_links_without_timeslice_limits_keep_their_static_values(csv_str_to_df):
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
    assert "CQ-NQ_existing" not in network.links_t.p_min_pu.columns
    assert network.links.loc["CQ-NQ_existing", "p_max_pu"] == 1.0
    assert network.links.loc["CQ-NQ_existing", "p_min_pu"] == 0.0


def test_old_format_call_without_limit_tables(csv_str_to_df):
    network = _network()

    _add_links_to_network(network, _links(csv_str_to_df))

    assert network.links.loc["CQ-NQ_existing", "p_nom"] == 1400
    assert "CQ-NQ_existing" not in network.links_t.p_max_pu.columns
