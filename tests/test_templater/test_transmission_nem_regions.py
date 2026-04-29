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

    # CQ-NQ is dropped (intra-QLD); NNSW-SQ becomes NSW-QLD; etc.
    nsw_qld_rows = limits[limits["path_id"] == "NSW-QLD"]
    assert len(nsw_qld_rows) == 6
    assert set(nsw_qld_rows["direction"]) == {"forward", "reverse"}
    assert "CQ-NQ" not in set(limits["path_id"])
    assert set(limits["path_id"]) == {
        "NSW-QLD",
        "NSW-QLD_Terranora",
        "TAS-VIC",
        "Q1-QLD",
    }


def test_nem_regions_preserves_dash_suffix(csv_str_to_df):
    """Dash-separated parallel-path suffixes (CNSW-SNW-NTH) collapse intra-region paths."""
    flow_paths = csv_str_to_df("""
        Flow Paths,  Forward direction capability approximation (MW)_Peak demand,  Forward direction capability approximation (MW)_Summer typical,  Forward direction capability approximation (MW)_Winter reference,  Reverse direction capability approximation (MW)_Peak demand,  Reverse direction capability approximation (MW)_Summer typical,  Reverse direction capability approximation (MW)_Winter reference
        CNSW-NNSW-NTH,  4490,  4490,  4730,  4490,  4490,  4730
    """)

    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)
    renewable_energy_zones = pd.DataFrame(columns=_REZ_COLUMNS)

    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CNSW,    subregion,  NSW,        CNSW
        NNSW,    subregion,  NSW,        NNSW
    """)

    paths, _ = _template_network_transmission(
        flow_paths,
        initial_limits,
        renewable_energy_zones,
        sub_regional_geography,
        "nem_regions",
    )

    # Both endpoints in NSW -> intra-region -> dropped from output paths.
    assert paths.empty


def test_nem_regions_empty_inputs():
    flow_paths = pd.DataFrame(columns=_FLOW_PATH_COLUMNS)
    initial_limits = pd.DataFrame(columns=_REZ_LIMIT_COLUMNS)
    renewable_energy_zones = pd.DataFrame(columns=_REZ_COLUMNS)
    sub_regional_geography = pd.DataFrame(columns=_GEOGRAPHY_COLUMNS)

    paths, limits = _template_network_transmission(
        flow_paths,
        initial_limits,
        renewable_energy_zones,
        sub_regional_geography,
        "nem_regions",
    )

    assert list(paths.columns) == ["path_id", "geo_from", "geo_to", "carrier"]
    assert paths.empty
    assert list(limits.columns) == ["path_id", "direction", "timeslice", "capacity"]
    assert limits.empty


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
    assert len(limits) == 1
    assert limits.iloc[0]["path_id"] == "N1-NSW"
    assert pd.isna(limits.iloc[0]["direction"])
