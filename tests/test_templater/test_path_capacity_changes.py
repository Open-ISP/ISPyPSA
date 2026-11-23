import pandas as pd

from ispypsa.templater.path_capacity_changes import (
    _drop_expansion_costs_before_prerequisite_projects,
    _template_path_capacity_changes,
)


def _iasr_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """The IASR tables the templater uses to carry changes onto the path ids of
    the chosen granularity: two NSW sub-regions and one SA sub-region, joined by
    an intra-region path (SNSW-CNSW) and a cross-region path (SNSW-CSA)."""
    tables = {}
    tables["flow_path_transfer_capability"] = csv_str_to_df("""
        Flow Paths
        SNSW-CNSW
        SNSW-CSA
    """)
    tables["renewable_energy_zones"] = csv_str_to_df("""
        ID,  ISP sub-region
        N1,  CNSW
    """)
    tables["sub_regional_geography"] = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        SNSW,    subregion,  NSW
        CNSW,    subregion,  NSW
        CSA,     subregion,  SA
        N1,      rez,        NSW
    """)
    return tables


def _line_limits(csv_str_to_df) -> pd.DataFrame:
    """PLEXOS limits for the SNSW-CNSW line (in two timeslices) and the
    EnergyConnect line, which is the SNSW-CSA path."""
    return csv_str_to_df("""
        object_class,  object_name,    property,  value,    date_from,            date_to,  tags
        Line,          SNSW-CNSW,      Max Flow,  2700.0,   ,                     ,         NSW Hot Day
        Line,          SNSW-CNSW,      Max Flow,  2950.0,   2027-08-01T00:00:00,  ,         NSW Hot Day
        Line,          SNSW-CNSW,      Max Flow,  5150.0,   2027-12-02T00:00:00,  ,         NSW Hot Day
        Line,          SNSW-CNSW,      Max Flow,  5600.0,   2029-07-01T00:00:00,  ,         NSW Hot Day
        Line,          SNSW-CNSW,      Max Flow,  2950.0,   ,                     ,         NSW Winter
        Line,          SNSW-CNSW,      Max Flow,  3200.0,   2027-08-01T00:00:00,  ,         NSW Winter
        Line,          SNSW-CNSW,      Max Flow,  5400.0,   2027-12-02T00:00:00,  ,         NSW Winter
        Line,          SNSW-CNSW,      Max Flow,  5850.0,   2029-07-01T00:00:00,  ,         NSW Winter
        Line,          SNSW-CNSW,      Min Flow,  -2320.0,  ,                     ,         NSW Hot Day
        Line,          SNSW-CNSW,      Min Flow,  -2320.0,  2027-08-01T00:00:00,  ,         NSW Hot Day
        Line,          SNSW-CNSW,      Min Flow,  -4520.0,  2027-12-02T00:00:00,  ,         NSW Hot Day
        Line,          EnergyConnect,  Max Flow,  150.0,    ,                     ,         SA Hot Day
        Line,          EnergyConnect,  Max Flow,  800.0,    2027-11-30T00:00:00,  ,         SA Hot Day
        Line,          EnergyConnect,  Min Flow,  -150.0,   ,                     ,         SA Hot Day
        Line,          EnergyConnect,  Min Flow,  -800.0,   2027-11-30T00:00:00,  ,         SA Hot Day
    """)


def test_template_path_capacity_changes_sub_regions(csv_str_to_df):
    iasr_tables = _iasr_tables(csv_str_to_df)
    line_limits = _line_limits(csv_str_to_df)

    result = _template_path_capacity_changes(
        line_limits,
        iasr_tables["flow_path_transfer_capability"],
        iasr_tables["renewable_energy_zones"],
        iasr_tables["sub_regional_geography"],
        "sub_regions",
    )

    # SNSW-CNSW steps three times. The 2027-08-01 step (+250 forward, reverse
    # unchanged) is the temporary SIPS uplift, 2027-12-02 (+2200 both ways) is
    # HumeLink, and the 2029-07-01 step isn't a committed project so it is left
    # out. Each step is the same size in both timeslices, so it gets one row.
    expected = csv_str_to_df("""
        path_id,    direction,  capacity_change,  date_from,            date_to,              project
        SNSW-CNSW,  forward,    250.0,            2027-08-01T00:00:00,  2030-07-01T00:00:00,  Waratah Super Battery SIPS control
        SNSW-CNSW,  forward,    2200.0,           2027-12-02T00:00:00,  ,                     HumeLink
        SNSW-CNSW,  reverse,    2200.0,           2027-12-02T00:00:00,  ,                     HumeLink
        SNSW-CSA,   forward,    650.0,            2027-11-30T00:00:00,  ,                     Project EnergyConnect Stage 2
        SNSW-CSA,   reverse,    650.0,            2027-11-30T00:00:00,  ,                     Project EnergyConnect Stage 2
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_template_path_capacity_changes_nem_regions(csv_str_to_df):
    iasr_tables = _iasr_tables(csv_str_to_df)
    line_limits = _line_limits(csv_str_to_df)

    result = _template_path_capacity_changes(
        line_limits,
        iasr_tables["flow_path_transfer_capability"],
        iasr_tables["renewable_energy_zones"],
        iasr_tables["sub_regional_geography"],
        "nem_regions",
    )

    # SNSW-CSA crosses from NSW to SA, so it is re-keyed to NSW-SA. SNSW-CNSW
    # sits inside NSW and isn't modelled, so its changes are dropped.
    expected = csv_str_to_df("""
        path_id,  direction,  capacity_change,  date_from,            date_to,  project
        NSW-SA,   forward,    650.0,            2027-11-30T00:00:00,  ,         Project EnergyConnect Stage 2
        NSW-SA,   reverse,    650.0,            2027-11-30T00:00:00,  ,         Project EnergyConnect Stage 2
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_template_path_capacity_changes_single_region(csv_str_to_df):
    iasr_tables = _iasr_tables(csv_str_to_df)
    line_limits = _line_limits(csv_str_to_df)

    result = _template_path_capacity_changes(
        line_limits,
        iasr_tables["flow_path_transfer_capability"],
        iasr_tables["renewable_energy_zones"],
        iasr_tables["sub_regional_geography"],
        "single_region",
    )

    # No flow path is modelled in a single region.
    expected = csv_str_to_df("""
        path_id,  direction,  capacity_change,  date_from,  date_to,  project
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_template_path_capacity_changes_no_line_limits(csv_str_to_df):
    iasr_tables = _iasr_tables(csv_str_to_df)
    line_limits = pd.DataFrame(
        columns=[
            "object_class",
            "object_name",
            "property",
            "value",
            "date_from",
            "date_to",
            "tags",
        ]
    )

    result = _template_path_capacity_changes(
        line_limits,
        iasr_tables["flow_path_transfer_capability"],
        iasr_tables["renewable_energy_zones"],
        iasr_tables["sub_regional_geography"],
        "sub_regions",
    )

    expected = csv_str_to_df("""
        path_id,  direction,  capacity_change,  date_from,  date_to,  project
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_drop_expansion_costs_before_prerequisite_projects(csv_str_to_df):
    expansion_costs = csv_str_to_df("""
        expansion_id,  year,  cost
        SNSW-CNSW,     2027,  480000
        SNSW-CNSW,     2028,  490000
        CQ-NQ,         2027,  410000
    """)

    result = _drop_expansion_costs_before_prerequisite_projects(expansion_costs)

    # HumeLink is in service on 2 December 2027, in FY2028, so SNSW-CNSW can't
    # be expanded in 2027. CQ-NQ has no prerequisite project.
    expected = csv_str_to_df("""
        expansion_id,  year,  cost
        SNSW-CNSW,     2028,  490000
        CQ-NQ,         2027,  410000
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_drop_expansion_costs_before_prerequisite_projects_no_costs(csv_str_to_df):
    expansion_costs = pd.DataFrame(columns=["expansion_id", "year", "cost"])

    result = _drop_expansion_costs_before_prerequisite_projects(expansion_costs)

    expected = csv_str_to_df("""
        expansion_id,  year,  cost
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
