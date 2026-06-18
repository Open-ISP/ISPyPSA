import pandas as pd
import pytest

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


def test_nem_regions_filters_intra_region_paths_and_rekeys(csv_str_to_df):
    """Intra-region flow paths drop, cross-region ones re-key to NEM regions."""
    flow_paths = csv_str_to_df("""
        Flow Paths,  Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter reference,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter reference
        CQ-NQ,       1200,  1200,  1400,  1440,  1440,  1910
        NNSW-SQ,     950,   950,   950,   1450,  1450,  1450
        NNSW-SQ (Terranora),  0,  50,  50,  130,  150,  200
        TAS-SEV,     594,   594,   594,   478,   478,   478
    """)

    initial_limits = csv_str_to_df("""
        REZ ID,  REZ transmission network limit_Peak demand,  REZ transmission network limit_Summer typical,  REZ transmission network limit_Winter reference
        Q1,      750,   750,   750
    """)

    renewable_energy_zones = csv_str_to_df("""
        ID,  Name,            NEM region,  ISP sub-region
        Q1,  Far North QLD,   QLD,         NQ
    """)

    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        NQ,      subregion,  QLD,        NQ
        CQ,      subregion,  QLD,        CQ
        SQ,      subregion,  QLD,        SQ
        NNSW,    subregion,  NSW,        NNSW
        SEV,     subregion,  VIC,        SEV
        TAS,     subregion,  TAS,        TAS
        Q1,      rez,        QLD,        NQ
    """)

    paths, limits = _template_network_transmission(
        flow_paths,
        initial_limits,
        renewable_energy_zones,
        sub_regional_geography,
        "nem_regions",
    )

    expected_paths = csv_str_to_df("""
        path_id,            geo_from,  geo_to,  carrier
        NSW-QLD,            NSW,       QLD,     AC
        NSW-QLD_Terranora,  NSW,       QLD,     DC
        TAS-VIC,            TAS,       VIC,     DC
        Q1-QLD,             Q1,        QLD,     AC
    """)
    pd.testing.assert_frame_equal(
        paths.sort_values("path_id").reset_index(drop=True),
        expected_paths.sort_values("path_id").reset_index(drop=True),
    )

    # CQ-NQ is dropped (intra-QLD); NNSW-SQ becomes NSW-QLD; etc. Timeslices are
    # region-prefixed: forward carries the destination region, reverse the origin.
    # TAS-VIC is the asymmetric cross-region case (forward into VIC, reverse into TAS).
    expected_limits = csv_str_to_df("""
        path_id,            direction,  timeslice,             capacity
        NSW-QLD,            forward,    qld_peak_demand,       950
        NSW-QLD,            forward,    qld_summer_typical,    950
        NSW-QLD,            forward,    qld_winter_reference,  950
        NSW-QLD,            reverse,    nsw_peak_demand,       1450
        NSW-QLD,            reverse,    nsw_summer_typical,    1450
        NSW-QLD,            reverse,    nsw_winter_reference,  1450
        NSW-QLD_Terranora,  forward,    qld_peak_demand,       0
        NSW-QLD_Terranora,  forward,    qld_summer_typical,    50
        NSW-QLD_Terranora,  forward,    qld_winter_reference,  50
        NSW-QLD_Terranora,  reverse,    nsw_peak_demand,       130
        NSW-QLD_Terranora,  reverse,    nsw_summer_typical,    150
        NSW-QLD_Terranora,  reverse,    nsw_winter_reference,  200
        TAS-VIC,            forward,    vic_peak_demand,       594
        TAS-VIC,            forward,    vic_summer_typical,    594
        TAS-VIC,            forward,    vic_winter_reference,  594
        TAS-VIC,            reverse,    tas_peak_demand,       478
        TAS-VIC,            reverse,    tas_summer_typical,    478
        TAS-VIC,            reverse,    tas_winter_reference,  478
        Q1-QLD,             forward,    qld_peak_demand,       750
        Q1-QLD,             forward,    qld_summer_typical,    750
        Q1-QLD,             forward,    qld_winter_reference,  750
        Q1-QLD,             reverse,    qld_peak_demand,       750
        Q1-QLD,             reverse,    qld_summer_typical,    750
        Q1-QLD,             reverse,    qld_winter_reference,  750
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


def test_nem_regions_all_flow_paths_filtered_out_returns_empty(csv_str_to_df):
    """All flow paths are intra-region and there are no REZs -> empty outputs."""
    flow_paths = csv_str_to_df("""
        Flow Paths,  Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter reference,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter reference
        CNSW-NNSW-NTH,  4490,  4490,  4730,  4490,  4490,  4730
    """)

    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)
    renewable_energy_zones = csv_str_to_df("""
        ID,  Name,  NEM region,  ISP sub-region
    """)

    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CNSW,    subregion,  NSW,        CNSW
        NNSW,    subregion,  NSW,        NNSW
    """)

    paths, limits = _template_network_transmission(
        flow_paths,
        initial_limits,
        renewable_energy_zones,
        sub_regional_geography,
        "nem_regions",
    )

    # Both endpoints in NSW -> intra-region -> dropped from output paths.
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


def test_nem_regions_empty_inputs(csv_str_to_df):
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
        "nem_regions",
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


def test_nem_regions_rez_with_no_initial_limit_keeps_collapsed_row(csv_str_to_df):
    """A REZ absent from initial_transmission_limits keeps a single path_id-only row."""
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
        "nem_regions",
    )

    expected_paths = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        N1-NSW,   N1,        NSW,     AC
    """)
    pd.testing.assert_frame_equal(
        paths.reset_index(drop=True), expected_paths.reset_index(drop=True)
    )

    # Collapsed row: path_id only, no direction/timeslice/capacity.
    expected_limits = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        N1-NSW,   ,           ,
    """)
    pd.testing.assert_frame_equal(
        limits.reset_index(drop=True),
        expected_limits.reset_index(drop=True),
        check_exact=False,
        check_dtype=False,
    )


def test_nem_regions_raises_when_flow_path_geo_missing_from_geography(csv_str_to_df):
    """A flow-path endpoint absent from sub_regional_geography raises ValueError."""
    flow_paths = csv_str_to_df("""
        Flow Paths,  Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter reference,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter reference
        MN-SA,       100,  100,  100,  100,  100,  100
    """)
    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)
    renewable_energy_zones = csv_str_to_df("""
        ID,  Name,  NEM region,  ISP sub-region
    """)

    # MN and SA are not in the sub_regional_geography below.
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        NQ,      subregion,  QLD,        NQ
    """)

    with pytest.raises(
        ValueError, match="Path geos missing from sub_regional_geography"
    ):
        _template_network_transmission(
            flow_paths,
            initial_limits,
            renewable_energy_zones,
            sub_regional_geography,
            "nem_regions",
        )


def test_nem_regions_raises_when_rez_parent_subregion_missing_from_geography(
    csv_str_to_df,
):
    """A REZ whose parent sub-region is absent from sub_regional_geography raises ValueError."""
    flow_paths = pd.DataFrame(columns=_FLOW_PATH_COLUMNS)
    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)

    # REZ N1 sits in sub-region NNSW, but NNSW is not in the geography below.
    renewable_energy_zones = csv_str_to_df("""
        ID,  Name,           NEM region,  ISP sub-region
        N1,  North West NSW, NSW,         NNSW
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        NQ,      subregion,  QLD,        NQ
    """)

    with pytest.raises(
        ValueError, match="Path geos missing from sub_regional_geography"
    ):
        _template_network_transmission(
            flow_paths,
            initial_limits,
            renewable_energy_zones,
            sub_regional_geography,
            "nem_regions",
        )


def test_nem_regions_new_parallel_corridor_gets_region_prefix(csv_str_to_df):
    """A parallel corridor injected after aggregation is still region-prefixed.

    ``_append_new_parallel_paths`` runs after the paths have been re-keyed to NEM
    regions, so the injected corridor's endpoints are already regions (NSW, QLD),
    not sub-regions. Prefixing them relies on the region->region identities in the
    geo lookup. Here only the suffixed sibling (NNSW-SQ Terranora -> NSW-QLD_Terranora)
    has a base path, so the un-suffixed ``NSW-QLD`` augmentation key has no match and
    is injected as a zero-capacity corridor — and must still read forward=qld,
    reverse=nsw.
    """
    flow_paths = csv_str_to_df("""
        Flow Paths,  Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter reference,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter reference
        NNSW-SQ (Terranora),  50,  50,  50,  50,  50,  50
    """)
    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)
    renewable_energy_zones = csv_str_to_df("""
        ID,  Name,  NEM region,  ISP sub-region
    """)
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        NNSW,    subregion,  NSW,        NNSW
        SQ,      subregion,  QLD,        SQ
    """)
    # Region-keyed augmentation corridor (as create_template's granularity filter
    # produces at nem_regions) with no matching aggregated path.
    flow_path_options = {"NSW-QLD": pd.DataFrame()}

    paths, limits = _template_network_transmission(
        flow_paths,
        initial_limits,
        renewable_energy_zones,
        sub_regional_geography,
        "nem_regions",
        flow_path_options,
    )

    expected_paths = csv_str_to_df("""
        path_id,            geo_from,  geo_to,  carrier
        NSW-QLD_Terranora,  NSW,       QLD,     DC
        NSW-QLD,            NSW,       QLD,     AC
    """)
    pd.testing.assert_frame_equal(
        paths.sort_values("path_id").reset_index(drop=True),
        expected_paths.sort_values("path_id").reset_index(drop=True),
    )

    expected_limits = csv_str_to_df("""
        path_id,            direction,  timeslice,             capacity
        NSW-QLD_Terranora,  forward,    qld_peak_demand,       50
        NSW-QLD_Terranora,  forward,    qld_summer_typical,    50
        NSW-QLD_Terranora,  forward,    qld_winter_reference,  50
        NSW-QLD_Terranora,  reverse,    nsw_peak_demand,       50
        NSW-QLD_Terranora,  reverse,    nsw_summer_typical,    50
        NSW-QLD_Terranora,  reverse,    nsw_winter_reference,  50
        NSW-QLD,            forward,    qld_peak_demand,       0
        NSW-QLD,            forward,    qld_summer_typical,    0
        NSW-QLD,            forward,    qld_winter_reference,  0
        NSW-QLD,            reverse,    nsw_peak_demand,       0
        NSW-QLD,            reverse,    nsw_summer_typical,    0
        NSW-QLD,            reverse,    nsw_winter_reference,  0
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
