import pandas as pd

from ispypsa.templater.network_losses import (
    _add_loss_group_column,
    _template_network_losses,
)

_LOSS_EQUATION_COLUMNS = [
    "status",
    "loss_group",
    "option",
    "flow_basis",
    "loss_constant",
    "loss_linear",
    "loss_quadratic",
    "from_loss_proportion",
    "to_loss_proportion",
    "date_from",
]


def _iasr_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """The IASR tables the templater uses to carry loss groups onto the path
    ids of the chosen granularity: a path inside QLD (CQ-NQ), a path inside NSW
    (SNSW-CNSW) and a path from TAS to VIC (TAS-SEV)."""
    tables = {}
    tables["flow_path_transfer_capability"] = csv_str_to_df("""
        Flow Paths
        CQ-NQ
        SNSW-CNSW
        TAS-SEV
    """)
    tables["renewable_energy_zones"] = csv_str_to_df("""
        ID,  ISP sub-region
        Q1,  NQ
    """)
    tables["sub_regional_geography"] = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CQ,      subregion,  QLD
        NQ,      subregion,  QLD
        SNSW,    subregion,  NSW
        CNSW,    subregion,  NSW
        TAS,     subregion,  TAS
        SEV,     subregion,  VIC
        Q1,      rez,        QLD
    """)
    return tables


def _loss_equations(csv_str_to_df) -> pd.DataFrame:
    """An AC path (CQ-NQ), an AC path whose curve changes with a committed
    project (SNSW-CNSW) and a DC link with a parallel expansion (TAS-SEV)."""
    return csv_str_to_df("""
        status,     loss_group,  option,                                   flow_basis,  loss_constant,  loss_linear,  loss_quadratic,  from_loss_proportion,  to_loss_proportion,  date_from
        existing,   CQ-NQ,       ,                                         signed,      0.0,            0.03,         0.00006,         0.25,                  0.75,
        existing,   SNSW-CNSW,   ,                                         signed,      0.0,            -0.05,        0.00003,         0.3,                   0.7,
        committed,  SNSW-CNSW,   SNSW-CNSW Option 1 (HumeLink),            signed,      0.0,            -0.04,        0.00002,         0.3,                   0.7,                 2027-12-02T00:00:00
        existing,   TAS-SEV,     ,                                         magnitude,   4.0,            -0.004,       0.0001,          0.5,                   0.5,
        option,     TAS-SEV,     TAS-VIC Option 1 (Marinus Link Stage 1),  magnitude,   9.0,            0.01,         0.00004,         0.5,                   0.5,
        option,     TAS-SEV,     TAS-VIC Option 2 (Marinus Link Stage 2),  magnitude,   18.0,           0.01,         0.00002,         0.5,                   0.5,
        option,     CQ-NQ,       CQ-NQ Option 1,                           signed,      0.0,            0.028,        0.00005,         0.25,                  0.75,
    """)


def test_template_network_losses_sub_regions(csv_str_to_df):
    iasr_tables = _iasr_tables(csv_str_to_df)
    loss_equations = _loss_equations(csv_str_to_df)

    result = _template_network_losses(
        loss_equations,
        iasr_tables["flow_path_transfer_capability"],
        iasr_tables["renewable_energy_zones"],
        iasr_tables["sub_regional_geography"],
        "sub_regions",
    )

    # The AC paths (signed flow) flip the sign of loss_linear in reverse; the DC
    # link TAS-SEV (flow magnitude) is the same both ways. HumeLink's equation
    # is a dated curve for SNSW-CNSW. TAS-SEV's expansion loss factor comes from
    # Marinus Link Stage 1 at half of its 750 MW:
    # 9 / 375 + 0.01 + 0.00004 * 375 = 0.049. CQ-NQ Option 1 is not a parallel
    # expansion, so CQ-NQ gets no factor.
    expected = csv_str_to_df("""
        loss_group,  direction,  loss_constant,  loss_linear,  loss_quadratic,  to_loss_proportion,  expansion_loss_factor,  date_from
        CQ-NQ,       forward,    0.0,            0.03,         0.00006,         0.75,                ,
        CQ-NQ,       reverse,    0.0,            -0.03,        0.00006,         0.75,                ,
        SNSW-CNSW,   forward,    0.0,            -0.04,        0.00002,         0.7,                 ,                       2027-12-02T00:00:00
        SNSW-CNSW,   reverse,    0.0,            0.04,         0.00002,         0.7,                 ,                       2027-12-02T00:00:00
        SNSW-CNSW,   forward,    0.0,            -0.05,        0.00003,         0.7,                 ,
        SNSW-CNSW,   reverse,    0.0,            0.05,         0.00003,         0.7,                 ,
        TAS-SEV,     forward,    4.0,            -0.004,       0.0001,          0.5,                 0.049,
        TAS-SEV,     reverse,    4.0,            -0.004,       0.0001,          0.5,                 0.049,
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_template_network_losses_nem_regions(csv_str_to_df):
    iasr_tables = _iasr_tables(csv_str_to_df)
    loss_equations = _loss_equations(csv_str_to_df)

    result = _template_network_losses(
        loss_equations,
        iasr_tables["flow_path_transfer_capability"],
        iasr_tables["renewable_energy_zones"],
        iasr_tables["sub_regional_geography"],
        "nem_regions",
    )

    # TAS-SEV crosses from TAS to VIC, so its group is re-keyed to TAS-VIC.
    # CQ-NQ and SNSW-CNSW each sit inside one region and aren't modelled.
    expected = csv_str_to_df("""
        loss_group,  direction,  loss_constant,  loss_linear,  loss_quadratic,  to_loss_proportion,  expansion_loss_factor,  date_from
        TAS-VIC,     forward,    4.0,            -0.004,       0.0001,          0.5,                 0.049,
        TAS-VIC,     reverse,    4.0,            -0.004,       0.0001,          0.5,                 0.049,
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_template_network_losses_single_region(csv_str_to_df):
    iasr_tables = _iasr_tables(csv_str_to_df)
    loss_equations = _loss_equations(csv_str_to_df)

    result = _template_network_losses(
        loss_equations,
        iasr_tables["flow_path_transfer_capability"],
        iasr_tables["renewable_energy_zones"],
        iasr_tables["sub_regional_geography"],
        "single_region",
    )

    # No flow path is modelled in a single region.
    expected = csv_str_to_df("""
        loss_group,  direction,  loss_constant,  loss_linear,  loss_quadratic,  to_loss_proportion,  expansion_loss_factor,  date_from
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_template_network_losses_no_equations(csv_str_to_df):
    iasr_tables = _iasr_tables(csv_str_to_df)
    loss_equations = pd.DataFrame(columns=_LOSS_EQUATION_COLUMNS)

    result = _template_network_losses(
        loss_equations,
        iasr_tables["flow_path_transfer_capability"],
        iasr_tables["renewable_energy_zones"],
        iasr_tables["sub_regional_geography"],
        "sub_regions",
    )

    expected = csv_str_to_df("""
        loss_group,  direction,  loss_constant,  loss_linear,  loss_quadratic,  to_loss_proportion,  expansion_loss_factor,  date_from
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_add_loss_group_column(csv_str_to_df):
    paths = csv_str_to_df("""
        path_id,            geo_from,  geo_to,  carrier
        CQ-NQ,              CQ,        NQ,      AC
        NNSW-SQ,            NNSW,      SQ,      AC
        NNSW-SQ_Terranora,  NNSW,      SQ,      DC
        CNSW-SNW_NTH,       CNSW,      SNW,     AC
        CNSW-SNW_STH,       CNSW,      SNW,     AC
        CNSW-SNW,           CNSW,      SNW,     AC
        Q1-NQ,              Q1,        NQ,      AC
    """)
    losses = csv_str_to_df("""
        loss_group,         direction
        CQ-NQ,              forward
        CQ-NQ,              reverse
        NNSW-SQ,            forward
        NNSW-SQ_Terranora,  forward
        CNSW-SNW,           forward
    """)

    result = _add_loss_group_column(paths, losses)

    # Terranora has a group of its own, so it doesn't fall back to its
    # corridor's (NNSW-SQ). The two CNSW-SNW paths with a suffix share the
    # corridor's group. The REZ connection Q1-NQ has no group.
    expected = csv_str_to_df("""
        path_id,            geo_from,  geo_to,  carrier,  loss_group
        CQ-NQ,              CQ,        NQ,      AC,       CQ-NQ
        NNSW-SQ,            NNSW,      SQ,      AC,       NNSW-SQ
        NNSW-SQ_Terranora,  NNSW,      SQ,      DC,       NNSW-SQ_Terranora
        CNSW-SNW_NTH,       CNSW,      SNW,     AC,       CNSW-SNW
        CNSW-SNW_STH,       CNSW,      SNW,     AC,       CNSW-SNW
        CNSW-SNW,           CNSW,      SNW,     AC,       CNSW-SNW
        Q1-NQ,              Q1,        NQ,      AC,
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_add_loss_group_column_no_losses(csv_str_to_df):
    paths = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
    """)
    losses = pd.DataFrame(columns=["loss_group", "direction"])

    result = _add_loss_group_column(paths, losses)

    expected = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier,  loss_group
        CQ-NQ,    CQ,        NQ,      AC,
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_add_loss_group_column_no_paths(csv_str_to_df):
    paths = pd.DataFrame(columns=["path_id", "geo_from", "geo_to", "carrier"])
    losses = csv_str_to_df("""
        loss_group,  direction
        CQ-NQ,       forward
    """)

    result = _add_loss_group_column(paths, losses)

    expected = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier,  loss_group
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
