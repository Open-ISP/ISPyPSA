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

    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CQ,      subregion,  QLD,        CQ
        NQ,      subregion,  QLD,        NQ
        SQ,      subregion,  QLD,        SQ
        NNSW,    subregion,  NSW,        NNSW
        Q1,      rez,        QLD,        NQ
        N1,      rez,        NSW,        NNSW
    """)

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

    # CQ-NQ and NNSW-SQ flow path limits are dropped; only REZ limits remain. The
    # REZ keeps its own region prefix (qld) even though geo_to is now the NEM geo —
    # the prefix is fixed at the sub-regional level before aggregation.
    expected_limits = csv_str_to_df("""
        path_id,  direction,  timeslice,             capacity
        Q1-NEM,   forward,    qld_peak_demand,       750
        Q1-NEM,   forward,    qld_summer_typical,    750
        Q1-NEM,   forward,    qld_winter_reference,  750
        Q1-NEM,   reverse,    qld_peak_demand,       750
        Q1-NEM,   reverse,    qld_summer_typical,    750
        Q1-NEM,   reverse,    qld_winter_reference,  750
        N1-NEM,   ,           ,
    """)
    pd.testing.assert_frame_equal(
        limits.sort_values(["path_id", "direction", "timeslice"]).reset_index(
            drop=True
        ),
        expected_limits.sort_values(["path_id", "direction", "timeslice"]).reset_index(
            drop=True
        ),
        check_exact=False,
        check_dtype=False,
    )


def test_single_region_empty_inputs(csv_str_to_df):
    flow_paths = pd.DataFrame(columns=_FLOW_PATH_COLUMNS)
    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)
    renewable_energy_zones = csv_str_to_df("""
        ID,  Name,  NEM region,  ISP sub-region
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,  region_id,  subregion_id
    """)

    paths, limits = _template_network_transmission(
        flow_paths,
        initial_limits,
        renewable_energy_zones,
        sub_regional_geography,
        "single_region",
    )

    expected_paths = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
    """)
    pd.testing.assert_frame_equal(
        paths.reset_index(drop=True),
        expected_paths.reset_index(drop=True),
        check_dtype=False,
    )

    expected_limits = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
    """)
    pd.testing.assert_frame_equal(
        limits.reset_index(drop=True),
        expected_limits.reset_index(drop=True),
        check_dtype=False,
    )


def test_single_region_rez_with_no_initial_limit_keeps_collapsed_row(csv_str_to_df):
    flow_paths = pd.DataFrame(columns=_FLOW_PATH_COLUMNS)
    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)

    renewable_energy_zones = csv_str_to_df("""
        ID,  Name,           NEM region,  ISP sub-region
        N1,  North West NSW, NSW,         NNSW
    """)

    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        NNSW,    subregion,  NSW,        NNSW
        N1,      rez,        NSW,        NNSW
    """)

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

    expected_limits = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        N1-NEM,   ,           ,
    """)
    pd.testing.assert_frame_equal(
        limits.reset_index(drop=True),
        expected_limits.reset_index(drop=True),
        check_exact=False,
        check_dtype=False,
    )
