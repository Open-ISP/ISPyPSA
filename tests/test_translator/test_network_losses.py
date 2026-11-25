import pandas as pd

from ispypsa.translator.network_losses import _translate_network_losses


def _one_period(sample_model_config, investment_periods=(2026,)):
    """sample_model_config (financial years, starting in 2026) with the given
    investment periods."""
    config = sample_model_config.model_copy(deep=True)
    config.temporal.capacity_expansion.investment_periods = list(investment_periods)
    return config


def _loss_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """One flow path with a loss curve (CQ-NQ, losing 0.03 * F + 0.00005 * F^2
    with 75% of it at NQ), one REZ connection with none, and no expansion."""
    tables = {}
    tables["network_transmission_losses"] = csv_str_to_df("""
        loss_group,  direction,  loss_constant,  loss_linear,  loss_quadratic,  to_loss_proportion,  expansion_loss_factor,  date_from
        CQ-NQ,       forward,    0,              0.03,         0.00005,         0.75,                ,
        CQ-NQ,       reverse,    0,              -0.03,        0.00005,         0.75,                ,
    """)
    tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier,  loss_group
        CQ-NQ,    CQ,        NQ,      AC,       CQ-NQ
        Q1-NQ,    Q1,        NQ,      AC,
    """)
    tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)
    return tables


def _links(csv_str_to_df) -> pd.DataFrame:
    return csv_str_to_df("""
        isp_name,  name,            p_nom,  p_nom_extendable,  build_year,  lifetime
        CQ-NQ,     CQ-NQ_existing,  2000,   False,             2025,        inf
        Q1-NQ,     Q1-NQ_existing,  750,    False,             2025,        inf
    """)


def test_translate_network_losses(csv_str_to_df, sample_model_config):
    ispypsa_tables = _loss_tables(csv_str_to_df)
    links = _links(csv_str_to_df)

    result = _translate_network_losses(
        ispypsa_tables, links, _one_period(sample_model_config)
    )

    # The curve covers 2000 MW each way. Its largest loss is 260 MW, at 2000 MW
    # forward (0.03 * 2000 + 0.00005 * 2000^2), and its smallest is -2.5 MW, at
    # 500 MW in reverse (-0.03 * 500 + 0.00005 * 500^2). NQ takes 75% of the
    # loss, so its generator can take up to 195 MW.
    expected_generators = csv_str_to_df("""
        name,              isp_name,  bus,  carrier,              sign,  p_nom,  p_nom_extendable,  p_min_pu,   p_max_pu,  build_year,  lifetime,  capital_cost,  marginal_cost
        CQ-NQ_loss_at_CQ,  CQ-NQ,     CQ,   Transmission losses,  -1.0,  65.0,   False,             -0.009615,  1.0,       2025,        inf,       0.0,           0.0
        CQ-NQ_loss_at_NQ,  CQ-NQ,     NQ,   Transmission losses,  -1.0,  195.0,  False,             -0.009615,  1.0,       2025,        inf,       0.0,           0.0
    """)
    pd.testing.assert_frame_equal(
        result["transmission_loss_generators"],
        expected_generators,
        check_dtype=False,
        rtol=1e-4,
    )

    # Eight segments of 500 MW, from 2000 MW in reverse to 2000 MW forward. The
    # last (line 7) runs from 1500 MW (157.5 MW lost) to 2000 MW (260 MW lost):
    # a slope of 0.205 and an intercept of -150, of which NQ's share is -112.5
    # and CQ's -37.5. The curve is symmetric about its lowest point, so the
    # intercepts mirror.
    expected_rhs = csv_str_to_df("""
        constraint_name,     investment_period,  timeslice,  rhs,     constraint_type
        CQ-NQ_loss_at_NQ_0,  2026,               ,           -112.5,  >=
        CQ-NQ_loss_at_NQ_1,  2026,               ,           -56.25,  >=
        CQ-NQ_loss_at_NQ_2,  2026,               ,           -18.75,  >=
        CQ-NQ_loss_at_NQ_3,  2026,               ,           0.0,     >=
        CQ-NQ_loss_at_NQ_4,  2026,               ,           0.0,     >=
        CQ-NQ_loss_at_NQ_5,  2026,               ,           -18.75,  >=
        CQ-NQ_loss_at_NQ_6,  2026,               ,           -56.25,  >=
        CQ-NQ_loss_at_NQ_7,  2026,               ,           -112.5,  >=
        CQ-NQ_loss_at_CQ_0,  2026,               ,           -37.5,   >=
        CQ-NQ_loss_at_CQ_1,  2026,               ,           -18.75,  >=
        CQ-NQ_loss_at_CQ_2,  2026,               ,           -6.25,   >=
        CQ-NQ_loss_at_CQ_3,  2026,               ,           0.0,     >=
        CQ-NQ_loss_at_CQ_4,  2026,               ,           0.0,     >=
        CQ-NQ_loss_at_CQ_5,  2026,               ,           -6.25,   >=
        CQ-NQ_loss_at_CQ_6,  2026,               ,           -18.75,  >=
        CQ-NQ_loss_at_CQ_7,  2026,               ,           -37.5,   >=
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"], expected_rhs, check_dtype=False
    )

    # Each constraint is p - share * slope * flow >= share * intercept. On line
    # 7 NQ's link coefficient is -0.75 * 0.205 = -0.15375. The REZ connection
    # Q1-NQ has no loss group, so its link appears nowhere.
    expected_lhs = csv_str_to_df("""
        constraint_name,     investment_period,  variable_name,     component,  attribute,  coefficient
        CQ-NQ_loss_at_NQ_0,  2026,               CQ-NQ_loss_at_NQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_NQ_1,  2026,               CQ-NQ_loss_at_NQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_NQ_2,  2026,               CQ-NQ_loss_at_NQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_NQ_3,  2026,               CQ-NQ_loss_at_NQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_NQ_4,  2026,               CQ-NQ_loss_at_NQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_NQ_5,  2026,               CQ-NQ_loss_at_NQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_NQ_6,  2026,               CQ-NQ_loss_at_NQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_NQ_7,  2026,               CQ-NQ_loss_at_NQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_CQ_0,  2026,               CQ-NQ_loss_at_CQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_CQ_1,  2026,               CQ-NQ_loss_at_CQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_CQ_2,  2026,               CQ-NQ_loss_at_CQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_CQ_3,  2026,               CQ-NQ_loss_at_CQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_CQ_4,  2026,               CQ-NQ_loss_at_CQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_CQ_5,  2026,               CQ-NQ_loss_at_CQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_CQ_6,  2026,               CQ-NQ_loss_at_CQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_CQ_7,  2026,               CQ-NQ_loss_at_CQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_NQ_0,  2026,               CQ-NQ_existing,    Link,       p,          0.10875
        CQ-NQ_loss_at_NQ_1,  2026,               CQ-NQ_existing,    Link,       p,          0.07125
        CQ-NQ_loss_at_NQ_2,  2026,               CQ-NQ_existing,    Link,       p,          0.03375
        CQ-NQ_loss_at_NQ_3,  2026,               CQ-NQ_existing,    Link,       p,          -0.00375
        CQ-NQ_loss_at_NQ_4,  2026,               CQ-NQ_existing,    Link,       p,          -0.04125
        CQ-NQ_loss_at_NQ_5,  2026,               CQ-NQ_existing,    Link,       p,          -0.07875
        CQ-NQ_loss_at_NQ_6,  2026,               CQ-NQ_existing,    Link,       p,          -0.11625
        CQ-NQ_loss_at_NQ_7,  2026,               CQ-NQ_existing,    Link,       p,          -0.15375
        CQ-NQ_loss_at_CQ_0,  2026,               CQ-NQ_existing,    Link,       p,          0.03625
        CQ-NQ_loss_at_CQ_1,  2026,               CQ-NQ_existing,    Link,       p,          0.02375
        CQ-NQ_loss_at_CQ_2,  2026,               CQ-NQ_existing,    Link,       p,          0.01125
        CQ-NQ_loss_at_CQ_3,  2026,               CQ-NQ_existing,    Link,       p,          -0.00125
        CQ-NQ_loss_at_CQ_4,  2026,               CQ-NQ_existing,    Link,       p,          -0.01375
        CQ-NQ_loss_at_CQ_5,  2026,               CQ-NQ_existing,    Link,       p,          -0.02625
        CQ-NQ_loss_at_CQ_6,  2026,               CQ-NQ_existing,    Link,       p,          -0.03875
        CQ-NQ_loss_at_CQ_7,  2026,               CQ-NQ_existing,    Link,       p,          -0.05125
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"], expected_lhs, check_dtype=False
    )


def test_translate_network_losses_expansion_as_upgrade(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _loss_tables(csv_str_to_df)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         forward,         1000,               CQ-NQ Option 1
        CQ-NQ,         reverse,         1000,               CQ-NQ Option 1
    """)
    links = csv_str_to_df("""
        isp_name,  name,            p_nom,  p_nom_extendable,  build_year,  lifetime
        CQ-NQ,     CQ-NQ_existing,  1000,   False,             2025,        inf
        CQ-NQ,     CQ-NQ_exp_2026,  0,      True,              2026,        inf
        CQ-NQ,     CQ-NQ_exp_2028,  0,      True,              2028,        inf
    """)

    result = _translate_network_losses(
        ispypsa_tables, links, _one_period(sample_model_config)
    )

    # The curve covers the existing 1000 MW plus the 1000 MW that may be built,
    # so the lines are those of a 2000 MW path, and the expansion link's flow
    # is added to the existing link's with the same coefficient. The 2028
    # expansion link isn't in service in 2026, so it has no term.
    lhs = result["custom_constraints_lhs"]
    line_7_at_nq = lhs[lhs["constraint_name"] == "CQ-NQ_loss_at_NQ_7"]
    expected = csv_str_to_df("""
        constraint_name,     investment_period,  variable_name,     component,  attribute,  coefficient
        CQ-NQ_loss_at_NQ_7,  2026,               CQ-NQ_loss_at_NQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_NQ_7,  2026,               CQ-NQ_existing,    Link,       p,          -0.15375
        CQ-NQ_loss_at_NQ_7,  2026,               CQ-NQ_exp_2026,    Link,       p,          -0.15375
    """)
    pd.testing.assert_frame_equal(
        line_7_at_nq.reset_index(drop=True), expected, check_dtype=False
    )


def test_translate_network_losses_expansion_in_parallel(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _loss_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_losses"] = csv_str_to_df("""
        loss_group,  direction,  loss_constant,  loss_linear,  loss_quadratic,  to_loss_proportion,  expansion_loss_factor,  date_from
        CQ-NQ,       forward,    0,              0.03,         0.00005,         1.0,                 0.04,
        CQ-NQ,       reverse,    0,              -0.03,        0.00005,         1.0,                 0.04,
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         forward,         1000,               CQ-NQ Option 1
        CQ-NQ,         reverse,         1000,               CQ-NQ Option 1
    """)
    links = csv_str_to_df("""
        isp_name,  name,            p_nom,  p_nom_extendable,  build_year,  lifetime
        CQ-NQ,     CQ-NQ_existing,  2000,   False,             2025,        inf
        CQ-NQ,     CQ-NQ_exp_2026,  0,      True,              2026,        inf
    """)

    result = _translate_network_losses(
        ispypsa_tables, links, _one_period(sample_model_config)
    )

    # The curve covers the existing 2000 MW only. Each of its eight lines is
    # repeated for the expansion link losing 4% of a forward flow and of a
    # reverse flow, so the last segment is lines 14 and 15. All of the loss is
    # at NQ (to_loss_proportion of 1).
    lhs = result["custom_constraints_lhs"]
    last_segment_at_nq = lhs[
        lhs["constraint_name"].isin(["CQ-NQ_loss_at_NQ_14", "CQ-NQ_loss_at_NQ_15"])
    ]
    expected = csv_str_to_df("""
        constraint_name,      investment_period,  variable_name,     component,  attribute,  coefficient
        CQ-NQ_loss_at_NQ_14,  2026,               CQ-NQ_loss_at_NQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_NQ_15,  2026,               CQ-NQ_loss_at_NQ,  Generator,  p,          1.0
        CQ-NQ_loss_at_NQ_14,  2026,               CQ-NQ_existing,    Link,       p,          -0.205
        CQ-NQ_loss_at_NQ_14,  2026,               CQ-NQ_exp_2026,    Link,       p,          -0.04
        CQ-NQ_loss_at_NQ_15,  2026,               CQ-NQ_existing,    Link,       p,          -0.205
        CQ-NQ_loss_at_NQ_15,  2026,               CQ-NQ_exp_2026,    Link,       p,          0.04
    """)
    pd.testing.assert_frame_equal(
        last_segment_at_nq.reset_index(drop=True), expected, check_dtype=False
    )

    # NQ's generator can take the curve's largest loss (260 MW) plus 4% of the
    # 1000 MW of expansion. CQ has none of the loss, so its generator is fixed
    # at zero.
    expected_generators = csv_str_to_df("""
        name,              isp_name,  bus,  carrier,              sign,  p_nom,  p_nom_extendable,  p_min_pu,   p_max_pu,  build_year,  lifetime,  capital_cost,  marginal_cost
        CQ-NQ_loss_at_CQ,  CQ-NQ,     CQ,   Transmission losses,  -1.0,  0.0,    False,             0.0,        0.0,       2025,        inf,       0.0,           0.0
        CQ-NQ_loss_at_NQ,  CQ-NQ,     NQ,   Transmission losses,  -1.0,  300.0,  False,             -0.008333,  1.0,       2025,        inf,       0.0,           0.0
    """)
    pd.testing.assert_frame_equal(
        result["transmission_loss_generators"],
        expected_generators,
        check_dtype=False,
        rtol=1e-4,
    )


def test_translate_network_losses_dated_curve(csv_str_to_df, sample_model_config):
    ispypsa_tables = _loss_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_losses"] = csv_str_to_df("""
        loss_group,  direction,  loss_constant,  loss_linear,  loss_quadratic,  to_loss_proportion,  expansion_loss_factor,  date_from
        CQ-NQ,       forward,    0,              0.03,         0.00005,         1.0,                 ,
        CQ-NQ,       reverse,    0,              -0.03,        0.00005,         1.0,                 ,
        CQ-NQ,       forward,    0,              0.02,         0.00004,         1.0,                 ,                       2027-12-02T00:00:00
        CQ-NQ,       reverse,    0,              -0.02,        0.00004,         1.0,                 ,                       2027-12-02T00:00:00
    """)
    links = _links(csv_str_to_df)

    result = _translate_network_losses(
        ispypsa_tables, links, _one_period(sample_model_config, (2026, 2028))
    )

    # 2 December 2027 is in FY2028, so the dated curve applies in 2028. On the
    # last segment (1500 to 2000 MW) the first curve loses 157.5 and 260 MW, an
    # intercept of -150; the dated one loses 120 and 200 MW, an intercept of
    # -120.
    rhs = result["custom_constraints_rhs"]
    line_7_at_nq = rhs[rhs["constraint_name"] == "CQ-NQ_loss_at_NQ_7"]
    expected = csv_str_to_df("""
        constraint_name,     investment_period,  timeslice,  rhs,     constraint_type
        CQ-NQ_loss_at_NQ_7,  2026,               ,           -150.0,  >=
        CQ-NQ_loss_at_NQ_7,  2028,               ,           -120.0,  >=
    """)
    pd.testing.assert_frame_equal(
        line_7_at_nq.reset_index(drop=True), expected, check_dtype=False
    )


def test_translate_network_losses_links_out_of_service(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _loss_tables(csv_str_to_df)
    links = csv_str_to_df("""
        isp_name,  name,                        p_nom,  p_nom_extendable,  build_year,  lifetime
        CQ-NQ,     CQ-NQ_existing,              1000,   False,             2025,        inf
        CQ-NQ,     CQ-NQ_committed_2027-12-02,  600,    False,             2028,        inf
        CQ-NQ,     CQ-NQ_retiring_2027-07-01,   400,    False,             2025,        3
    """)

    result = _translate_network_losses(
        ispypsa_tables, links, _one_period(sample_model_config, (2026, 2028))
    )

    # The curve covers all 2000 MW of links, whenever they are in service. The
    # committed link has a term from 2028, when it is built, and the retiring
    # link (2025 + 3) only before 2028.
    lhs = result["custom_constraints_lhs"]
    line_7_at_nq = lhs[
        (lhs["constraint_name"] == "CQ-NQ_loss_at_NQ_7") & (lhs["component"] == "Link")
    ]
    expected = csv_str_to_df("""
        constraint_name,     investment_period,  variable_name,               component,  attribute,  coefficient
        CQ-NQ_loss_at_NQ_7,  2026,               CQ-NQ_existing,              Link,       p,          -0.15375
        CQ-NQ_loss_at_NQ_7,  2026,               CQ-NQ_retiring_2027-07-01,   Link,       p,          -0.15375
        CQ-NQ_loss_at_NQ_7,  2028,               CQ-NQ_existing,              Link,       p,          -0.15375
        CQ-NQ_loss_at_NQ_7,  2028,               CQ-NQ_committed_2027-12-02,  Link,       p,          -0.15375
    """)
    pd.testing.assert_frame_equal(
        line_7_at_nq.reset_index(drop=True), expected, check_dtype=False
    )


def _assert_no_losses(result: dict[str, pd.DataFrame], csv_str_to_df) -> None:
    expected_generators = csv_str_to_df("""
        name,  isp_name,  bus,  carrier,  sign,  p_nom,  p_nom_extendable,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  marginal_cost
    """)
    pd.testing.assert_frame_equal(
        result["transmission_loss_generators"], expected_generators, check_dtype=False
    )
    expected_rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"], expected_rhs, check_dtype=False
    )
    expected_lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"], expected_lhs, check_dtype=False
    )


def test_translate_network_losses_no_curves(csv_str_to_df, sample_model_config):
    ispypsa_tables = _loss_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_losses"] = pd.DataFrame(
        columns=ispypsa_tables["network_transmission_losses"].columns
    )
    links = _links(csv_str_to_df)

    result = _translate_network_losses(
        ispypsa_tables, links, _one_period(sample_model_config)
    )

    _assert_no_losses(result, csv_str_to_df)


def test_translate_network_losses_no_links(csv_str_to_df, sample_model_config):
    ispypsa_tables = _loss_tables(csv_str_to_df)
    links = pd.DataFrame(columns=_links(csv_str_to_df).columns)

    result = _translate_network_losses(
        ispypsa_tables, links, _one_period(sample_model_config)
    )

    _assert_no_losses(result, csv_str_to_df)


def test_translate_network_losses_no_curves_and_no_links(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _loss_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_losses"] = pd.DataFrame(
        columns=ispypsa_tables["network_transmission_losses"].columns
    )
    links = pd.DataFrame(columns=_links(csv_str_to_df).columns)

    result = _translate_network_losses(
        ispypsa_tables, links, _one_period(sample_model_config)
    )

    _assert_no_losses(result, csv_str_to_df)
