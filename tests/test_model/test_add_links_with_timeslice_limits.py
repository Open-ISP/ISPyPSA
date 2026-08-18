import pandas as pd
import pypsa
import pytest

from ispypsa.pypsa_build.links import (
    _add_links_to_network,
    _expand_limits_to_snapshots,
)


def _network() -> pypsa.Network:
    snapshots = pd.date_range("2025-01-01", periods=4, freq="h")
    index = pd.MultiIndex.from_arrays([[2025] * 4, list(snapshots)])
    network = pypsa.Network(snapshots=index, investment_periods=[2025])
    network.add("Bus", "bus1")
    network.add("Bus", "bus2")
    return network


def _links(csv_str_to_df) -> pd.DataFrame:
    # p_max_pu / p_min_pu are the translator's placeholders, never read on the
    # new-format path; the real limits come from link_timeslice_limits.
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


def test_snapshot_covered_by_neither_named_timeslice_nor_fallback_raises(csv_str_to_df):
    network = _network()
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,        value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,  0.857
        CQ-NQ_existing,  p_min_pu,   ,                 -0.714
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice_id,     investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
    """)

    with pytest.raises(ValueError) as excinfo:
        _add_links_to_network(
            network, _links(csv_str_to_df), link_timeslice_limits, timeslice_snapshots
        )

    assert str(excinfo.value) == (
        "link_timeslice_limits leaves 3 (link, attribute, snapshot) combination(s) "
        "undefined: no fallback (blank-timeslice) row and no named timeslice active "
        "there. Affected (link, attribute): [('CQ-NQ_existing', 'p_max_pu')]. "
        "First uncovered (investment_period, snapshot): (2025, 2025-01-01 00:00:00), "
        "(2025, 2025-01-01 02:00:00), (2025, 2025-01-01 03:00:00)"
    )


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


def test_expand_limits_to_snapshots(csv_str_to_df):
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,        value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,  0.857
        CQ-NQ_existing,  p_max_pu,   ,                 1.0
        CQ-NQ_existing,  p_min_pu,   qld_peak_demand,  -0.9
        NQ-CQ_other,     p_max_pu,   no_snapshots,     0.5
        NQ-CQ_other,     p_max_pu,   ,                 0.8
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice_id,     investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
    """)
    snapshots = pd.MultiIndex.from_arrays(
        [[2025, 2025], pd.to_datetime(["2025-01-01 00:00", "2025-01-01 01:00"])]
    )

    result = _expand_limits_to_snapshots(
        link_timeslice_limits, timeslice_snapshots, snapshots
    )

    expected = csv_str_to_df("""
        name,            attribute,  investment_periods,  snapshots,            value
        CQ-NQ_existing,  p_max_pu,   2025,                2025-01-01 00:00:00,  1.0
        CQ-NQ_existing,  p_max_pu,   2025,                2025-01-01 01:00:00,  0.857
        CQ-NQ_existing,  p_min_pu,   2025,                2025-01-01 00:00:00,
        CQ-NQ_existing,  p_min_pu,   2025,                2025-01-01 01:00:00,  -0.9
        NQ-CQ_other,     p_max_pu,   2025,                2025-01-01 00:00:00,  0.8
        NQ-CQ_other,     p_max_pu,   2025,                2025-01-01 01:00:00,  0.8
    """)
    # Rows: named value where active, fallback elsewhere, NaN where neither
    # (CQ-NQ_existing p_min_pu at 00:00); a named timeslice with no snapshots
    # (no_snapshots) contributes nothing so NQ-CQ_other takes its fallback.
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])
    pd.testing.assert_frame_equal(result, expected)
