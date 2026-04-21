import pandas as pd

from ispypsa.templater.transmission import (
    _collapse_paths_with_no_limits,
    _template_network_transmission,
)

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

_PATHS_COLUMNS = ["path_id", "geo_from", "geo_to", "carrier"]
_LIMITS_COLUMNS = ["path_id", "direction", "timeslice", "capacity"]


def test_template_network_transmission(csv_str_to_df):
    flow_paths = csv_str_to_df("""
        Flow Paths,  Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter reference,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter reference
        CQ-NQ,       1200,  1200,  1400,  1440,  1440,  1910
        NNSW-SQ,     950,   950,   950,   1450,  1450,  1450
        NNSW-SQ (Terranora),  0,  50,  50,  130,  150,  200
        TAS-SEV,     594,   594,   594,   478,   478,   478
        WNV-CSA (Murraylink),  165,  220,  220,  100,  200,  200
        CNSW-SNW-NTH,  4490,  4490,  4730,  4490,  4490,  4730
        SA-VIC,      ,      ,      ,      ,      ,
    """)

    initial_limits = csv_str_to_df("""
        REZ ID,  REZ transmission network limit_Peak demand,  REZ transmission network limit_Summer typical,  REZ transmission network limit_Winter reference
        Q1,      750,   750,   750
        Q3,      ,      ,
    """)

    renewable_energy_zones = csv_str_to_df("""
        ID,  Name,               NEM region,  ISP sub-region
        Q1,  Far North QLD,      QLD,         NQ
        Q3,  Northern Qld,       QLD,         NQ
        N3,  Central-West Orana, NSW,         CNSW
    """)

    paths, limits = _template_network_transmission(
        flow_paths, initial_limits, renewable_energy_zones
    )

    expected_paths = csv_str_to_df("""
        path_id,             geo_from,  geo_to,  carrier
        CQ-NQ,               CQ,        NQ,      AC
        NNSW-SQ,             NNSW,      SQ,      AC
        NNSW-SQ_Terranora,   NNSW,      SQ,      DC
        TAS-SEV,             TAS,       SEV,     DC
        WNV-CSA_Murraylink,  WNV,       CSA,     DC
        CNSW-SNW_NTH,        CNSW,      SNW,     AC
        SA-VIC,              SA,        VIC,     AC
        Q1-NQ,               Q1,        NQ,      AC
        Q3-NQ,               Q3,        NQ,      AC
        N3-CNSW,             N3,        CNSW,    AC
    """)
    pd.testing.assert_frame_equal(
        paths.sort_values("path_id").reset_index(drop=True),
        expected_paths.sort_values("path_id").reset_index(drop=True),
    )

    expected_limits = csv_str_to_df("""
        path_id,  direction,  timeslice,         capacity
        CQ-NQ,    forward,    peak_demand,       1200
        CQ-NQ,    forward,    summer_typical,    1200
        CQ-NQ,    forward,    winter_reference,  1400
        CQ-NQ,    reverse,    peak_demand,       1440
        CQ-NQ,    reverse,    summer_typical,    1440
        CQ-NQ,    reverse,    winter_reference,  1910
        NNSW-SQ,  forward,    peak_demand,       950
        NNSW-SQ,  forward,    summer_typical,    950
        NNSW-SQ,  forward,    winter_reference,  950
        NNSW-SQ,  reverse,    peak_demand,       1450
        NNSW-SQ,  reverse,    summer_typical,    1450
        NNSW-SQ,  reverse,    winter_reference,  1450
        NNSW-SQ_Terranora,  forward,  peak_demand,       0
        NNSW-SQ_Terranora,  forward,  summer_typical,    50
        NNSW-SQ_Terranora,  forward,  winter_reference,  50
        NNSW-SQ_Terranora,  reverse,  peak_demand,       130
        NNSW-SQ_Terranora,  reverse,  summer_typical,    150
        NNSW-SQ_Terranora,  reverse,  winter_reference,  200
        TAS-SEV,  forward,    peak_demand,       594
        TAS-SEV,  forward,    summer_typical,    594
        TAS-SEV,  forward,    winter_reference,  594
        TAS-SEV,  reverse,    peak_demand,       478
        TAS-SEV,  reverse,    summer_typical,    478
        TAS-SEV,  reverse,    winter_reference,  478
        WNV-CSA_Murraylink,  forward,  peak_demand,       165
        WNV-CSA_Murraylink,  forward,  summer_typical,    220
        WNV-CSA_Murraylink,  forward,  winter_reference,  220
        WNV-CSA_Murraylink,  reverse,  peak_demand,       100
        WNV-CSA_Murraylink,  reverse,  summer_typical,    200
        WNV-CSA_Murraylink,  reverse,  winter_reference,  200
        CNSW-SNW_NTH,  forward,  peak_demand,       4490
        CNSW-SNW_NTH,  forward,  summer_typical,    4490
        CNSW-SNW_NTH,  forward,  winter_reference,  4730
        CNSW-SNW_NTH,  reverse,  peak_demand,       4490
        CNSW-SNW_NTH,  reverse,  summer_typical,    4490
        CNSW-SNW_NTH,  reverse,  winter_reference,  4730
        Q1-NQ,    forward,    peak_demand,       750
        Q1-NQ,    forward,    summer_typical,    750
        Q1-NQ,    forward,    winter_reference,  750
        Q1-NQ,    reverse,    peak_demand,       750
        Q1-NQ,    reverse,    summer_typical,    750
        Q1-NQ,    reverse,    winter_reference,  750
        SA-VIC,   ,           ,
        Q3-NQ,    ,           ,
        N3-CNSW,  ,           ,
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


def test_typo_in_column_names(csv_str_to_df):
    """Handles the 'refernce' typo in IASR workbook v7.5 flow path columns."""
    flow_paths = csv_str_to_df("""
        Flow Paths,  Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter refernce,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter refernce
        CQ-NQ,       100,  200,  300,  400,  500,  600
    """)

    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)
    renewable_energy_zones = pd.DataFrame(columns=_REZ_COLUMNS)

    _, limits = _template_network_transmission(
        flow_paths, initial_limits, renewable_energy_zones
    )

    expected_limits = csv_str_to_df("""
        path_id,  direction,  timeslice,         capacity
        CQ-NQ,    forward,    peak_demand,       100
        CQ-NQ,    forward,    summer_typical,    200
        CQ-NQ,    forward,    winter_reference,  300
        CQ-NQ,    reverse,    peak_demand,       400
        CQ-NQ,    reverse,    summer_typical,    500
        CQ-NQ,    reverse,    winter_reference,  600
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


def test_empty_flow_paths(csv_str_to_df):
    flow_paths = pd.DataFrame(columns=_FLOW_PATH_COLUMNS)

    initial_limits = csv_str_to_df("""
        REZ ID,  REZ transmission network limit_Peak demand,  REZ transmission network limit_Summer typical,  REZ transmission network limit_Winter reference
        Q1,      750,   750,   750
    """)

    renewable_energy_zones = csv_str_to_df("""
        ID,  Name,             NEM region,  ISP sub-region
        Q1,  Far North QLD,    QLD,         NQ
    """)

    paths, limits = _template_network_transmission(
        flow_paths, initial_limits, renewable_energy_zones
    )

    expected_paths = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        Q1-NQ,    Q1,        NQ,      AC
    """)
    pd.testing.assert_frame_equal(
        paths.reset_index(drop=True),
        expected_paths.reset_index(drop=True),
    )

    expected_limits = csv_str_to_df("""
        path_id,  direction,  timeslice,         capacity
        Q1-NQ,    forward,    peak_demand,       750
        Q1-NQ,    forward,    summer_typical,    750
        Q1-NQ,    forward,    winter_reference,  750
        Q1-NQ,    reverse,    peak_demand,       750
        Q1-NQ,    reverse,    summer_typical,    750
        Q1-NQ,    reverse,    winter_reference,  750
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


def test_empty_rez(csv_str_to_df):
    flow_paths = csv_str_to_df("""
        Flow Paths,  Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter reference,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter reference
        CQ-NQ,       100,  200,  300,  400,  500,  600
    """)

    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)
    renewable_energy_zones = pd.DataFrame(columns=_REZ_COLUMNS)

    paths, limits = _template_network_transmission(
        flow_paths, initial_limits, renewable_energy_zones
    )

    expected_paths = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
    """)
    pd.testing.assert_frame_equal(
        paths.reset_index(drop=True),
        expected_paths.reset_index(drop=True),
    )

    expected_limits = csv_str_to_df("""
        path_id,  direction,  timeslice,         capacity
        CQ-NQ,    forward,    peak_demand,       100
        CQ-NQ,    forward,    summer_typical,    200
        CQ-NQ,    forward,    winter_reference,  300
        CQ-NQ,    reverse,    peak_demand,       400
        CQ-NQ,    reverse,    summer_typical,    500
        CQ-NQ,    reverse,    winter_reference,  600
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


def test_both_empty():
    flow_paths = pd.DataFrame(columns=_FLOW_PATH_COLUMNS)
    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)
    renewable_energy_zones = pd.DataFrame(columns=_REZ_COLUMNS)

    paths, limits = _template_network_transmission(
        flow_paths, initial_limits, renewable_energy_zones
    )

    expected_paths = pd.DataFrame(columns=_PATHS_COLUMNS)
    pd.testing.assert_frame_equal(paths, expected_paths)

    expected_limits = pd.DataFrame(columns=_LIMITS_COLUMNS)
    pd.testing.assert_frame_equal(limits, expected_limits, check_dtype=False)


# --- _collapse_paths_with_no_limits edge cases ---


def test_collapse_keeps_path_with_all_capacity_present(csv_str_to_df):
    limits = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      forward,    peak_demand,     100
        A-B,      reverse,    peak_demand,     200
    """)

    result = _collapse_paths_with_no_limits(limits)

    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      forward,    peak_demand,     100
        A-B,      reverse,    peak_demand,     200
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_collapse_collapses_path_with_all_nan_capacity(csv_str_to_df):
    limits = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      forward,    peak_demand,
        A-B,      forward,    summer_typical,
        A-B,      reverse,    peak_demand,
    """)

    result = _collapse_paths_with_no_limits(limits)

    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      ,           ,
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_collapse_keeps_path_with_partial_nan_capacity(csv_str_to_df):
    """A path with any non-NaN capacity is kept in full, NaN rows included."""
    limits = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      forward,    peak_demand,     100
        A-B,      forward,    summer_typical,
        A-B,      reverse,    peak_demand,
    """)

    result = _collapse_paths_with_no_limits(limits)

    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      forward,    peak_demand,     100
        A-B,      forward,    summer_typical,
        A-B,      reverse,    peak_demand,
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_collapse_treats_zero_capacity_as_real_value(csv_str_to_df):
    """capacity=0 is a valid limit — not NaN — so the path is kept in full."""
    limits = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      forward,    peak_demand,     0
        A-B,      reverse,    peak_demand,     0
    """)

    result = _collapse_paths_with_no_limits(limits)

    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      forward,    peak_demand,     0
        A-B,      reverse,    peak_demand,     0
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_collapse_empty_dataframe():
    limits = pd.DataFrame(columns=_LIMITS_COLUMNS)

    result = _collapse_paths_with_no_limits(limits)

    expected = pd.DataFrame(columns=_LIMITS_COLUMNS)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_collapse_mixed_paths(csv_str_to_df):
    """Mix of path states: full data, all-NaN, and partial NaN."""
    limits = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      forward,    peak_demand,     100
        A-B,      reverse,    peak_demand,     200
        C-D,      forward,    peak_demand,
        C-D,      reverse,    peak_demand,
        E-F,      forward,    peak_demand,     500
        E-F,      reverse,    peak_demand,
    """)

    result = _collapse_paths_with_no_limits(limits)

    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      forward,    peak_demand,     100
        A-B,      reverse,    peak_demand,     200
        E-F,      forward,    peak_demand,     500
        E-F,      reverse,    peak_demand,
        C-D,      ,           ,
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_collapse_multiple_all_nan_paths(csv_str_to_df):
    """Each all-NaN path gets its own collapsed row."""
    limits = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      forward,    peak_demand,
        A-B,      reverse,    peak_demand,
        C-D,      forward,    peak_demand,
        C-D,      reverse,    peak_demand,
    """)

    result = _collapse_paths_with_no_limits(limits)

    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,       capacity
        A-B,      ,           ,
        C-D,      ,           ,
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("path_id").reset_index(drop=True),
        expected.sort_values("path_id").reset_index(drop=True),
        check_dtype=False,
    )
