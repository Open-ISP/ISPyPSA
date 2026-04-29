import pandas as pd

from ispypsa.templater.transmission import _template_network_transmission

_FLOW_PATH_COLUMNS = [
    "Flow Paths",
    "Forward direction capability approximation (MW)_Peak demand",
    "Forward direction capability approximation (MW)_Summer typical",
    "Forward direction capability approximation (MW)_Winter reference",
    "Reverse direction capability approximation (MW)_Peak demand",
    "Reverse direction capability approximation (MW)_Summer typical",
    "Reverse direction capability approximation (MW)_Winter reference",
]

_REZ_LIMIT_COLUMNS = [
    "REZ ID",
    "REZ transmission network limit_Peak demand",
    "REZ transmission network limit_Summer typical",
    "REZ transmission network limit_Winter reference",
]

_REZ_COLUMNS = ["ID", "Name", "NEM region", "ISP sub-region"]

_GEOGRAPHY_COLUMNS = ["geo_id", "geo_type", "region_id", "subregion_id"]


def test_single_region_drops_flow_paths_and_rekeys_rez(csv_str_to_df):
    """Flow paths are dropped entirely; REZ paths point at the single 'NEM' geo."""
    flow_paths = csv_str_to_df("""
        Flow Paths,  Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter reference,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter reference
        CQ-NQ,       1200,  1200,  1400,  1440,  1440,  1910
        NNSW-SQ,     950,   950,   950,   1450,  1450,  1450
    """)

    initial_limits = csv_str_to_df("""
        REZ ID,  REZ transmission network limit_Peak demand,  REZ transmission network limit_Summer typical,  REZ transmission network limit_Winter reference
        Q1,      750,   750,   750
    """)

    renewable_energy_zones = csv_str_to_df("""
        ID,  Name,            NEM region,  ISP sub-region
        Q1,  Far North QLD,   QLD,         NQ
        N1,  North West NSW,  NSW,         NNSW
    """)

    sub_regional_geography = pd.DataFrame(columns=_GEOGRAPHY_COLUMNS)

    paths, limits = _template_network_transmission(
        flow_paths,
        initial_limits,
        renewable_energy_zones,
        sub_regional_geography,
        "single_region",
    )

    expected_paths = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        Q1-NEM,   Q1,        NEM,     AC
        N1-NEM,   N1,        NEM,     AC
    """)
    pd.testing.assert_frame_equal(
        paths.sort_values("path_id").reset_index(drop=True),
        expected_paths.sort_values("path_id").reset_index(drop=True),
    )

    # CQ-NQ and NNSW-SQ flow path limits are dropped; only REZ limits remain.
    assert "CQ-NQ" not in set(limits["path_id"])
    assert "NNSW-SQ" not in set(limits["path_id"])
    assert set(limits["path_id"]) == {"Q1-NEM", "N1-NEM"}

    q1_rows = limits[limits["path_id"] == "Q1-NEM"]
    assert len(q1_rows) == 6
    assert (q1_rows["capacity"] == 750).all()


def test_single_region_empty_inputs():
    flow_paths = pd.DataFrame(columns=_FLOW_PATH_COLUMNS)
    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)
    renewable_energy_zones = pd.DataFrame(columns=_REZ_COLUMNS)
    sub_regional_geography = pd.DataFrame(columns=_GEOGRAPHY_COLUMNS)

    paths, limits = _template_network_transmission(
        flow_paths,
        initial_limits,
        renewable_energy_zones,
        sub_regional_geography,
        "single_region",
    )

    assert list(paths.columns) == ["path_id", "geo_from", "geo_to", "carrier"]
    assert paths.empty
    assert list(limits.columns) == ["path_id", "direction", "timeslice", "capacity"]
    assert limits.empty


def test_single_region_rez_with_no_initial_limit_keeps_collapsed_row(csv_str_to_df):
    flow_paths = pd.DataFrame(columns=_FLOW_PATH_COLUMNS)
    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)

    renewable_energy_zones = csv_str_to_df("""
        ID,  Name,           NEM region,  ISP sub-region
        N1,  North West NSW, NSW,         NNSW
    """)

    sub_regional_geography = pd.DataFrame(columns=_GEOGRAPHY_COLUMNS)

    paths, limits = _template_network_transmission(
        flow_paths,
        initial_limits,
        renewable_energy_zones,
        sub_regional_geography,
        "single_region",
    )

    expected_paths = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        N1-NEM,   N1,        NEM,     AC
    """)
    pd.testing.assert_frame_equal(
        paths.reset_index(drop=True), expected_paths.reset_index(drop=True)
    )

    assert len(limits) == 1
    assert limits.iloc[0]["path_id"] == "N1-NEM"
    assert pd.isna(limits.iloc[0]["direction"])
