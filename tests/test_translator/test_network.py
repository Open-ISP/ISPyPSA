import pandas as pd
import pytest

from ispypsa.translator.helpers import _annuitised_investment_costs
from ispypsa.translator.network import (
    _drop_limits_for_unmodelled_paths,
    _resolve_expansion_options,
    _translate_network_geography_to_buses,
    _translate_network_to_links,
)

# Annuitised $1/MW at the sample_model_config's wacc (0.06) and annuitisation
# lifetime (25) — expansion cost expectations are multiples of this.
_ANNUITY_PER_DOLLAR = _annuitised_investment_costs(1.0, 0.06, 25)


def test_translate_network_geography_to_buses_discrete_nodes(csv_str_to_df):
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        NQ,      subregion,  QLD
        CNSW,    subregion,  NSW
        Q1,      rez,        QLD
    """)

    result = _translate_network_geography_to_buses(network_geography, "discrete_nodes")

    expected = csv_str_to_df("""
        name
        NQ
        CNSW
        Q1
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_translate_network_geography_to_buses_attached_to_parent_node(csv_str_to_df):
    network_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        NQ,      subregion,  QLD
        Q1,      rez,        QLD
    """)

    result = _translate_network_geography_to_buses(
        network_geography, "attached_to_parent_node"
    )

    expected = csv_str_to_df("""
        name
        NQ
    """)
    pd.testing.assert_frame_equal(result, expected)


def _network_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """New-format network tables with one flow path (CQ-NQ, asymmetric winter
    limits, expandable), one REZ path with limits (Q1-NQ) and one collapsed
    REZ path with no limit data (N1-CNSW)."""
    tables = {}
    tables["network_geography"] = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        NQ,      subregion,  QLD
        CQ,      subregion,  QLD
        CNSW,    subregion,  NSW
        Q1,      rez,        QLD
        N1,      rez,        NSW
    """)
    tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
        Q1-NQ,    Q1,        NQ,      AC
        N1-CNSW,  N1,        CNSW,    AC
    """)
    tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,             capacity
        CQ-NQ,    forward,    qld_peak_demand,       1200
        CQ-NQ,    forward,    qld_summer_typical,    1300
        CQ-NQ,    forward,    qld_winter_reference,  1400
        CQ-NQ,    reverse,    qld_peak_demand,       1440
        CQ-NQ,    reverse,    qld_summer_typical,    1600
        CQ-NQ,    reverse,    qld_winter_reference,  1910
        Q1-NQ,    forward,    qld_winter_reference,  750
        Q1-NQ,    reverse,    qld_winter_reference,  750
        N1-CNSW,  ,           ,
    """)
    tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        CQ-NQ,         forward,                1000,               CQ-NQ Option 1
        CQ-NQ,         reverse,                900,                CQ-NQ Option 1
        Q1-NQ,         forward,                500,                Q1 Option 1
        Q1-NQ,         reverse,                500,                Q1 Option 1
        SWQLD1,        constraint_relaxation,  400,                SWQLD1 Option 2
    """)
    # 2025 is outside the sample config's investment periods (2026, 2028) and
    # is expected to be filtered out.
    tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         2025,  900000
        CQ-NQ,         2026,  1000000
        Q1-NQ,         2026,  500000
        SWQLD1,        2026,  400000
    """)
    return tables


def test_translate_network_to_links(csv_str_to_df, sample_model_config):
    ispypsa_tables = _network_tables(csv_str_to_df)

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    expected_links = csv_str_to_df(f"""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,    p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,                       p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_existing,  AC,       CQ,    NQ,    1910,     0.0,       1.0,       2025,        inf,       ,                                   False,             flow_path
        Q1-NQ,     Q1-NQ_existing,  AC,       Q1,    NQ,    750,      0.0,       1.0,       2025,        inf,       ,                                   False,             rez
        N1-CNSW,   N1-CNSW_existing,AC,       N1,    CNSW,  1000000,  0.0,       1.0,       2025,        inf,       ,                                   False,             rez
        CQ-NQ,     CQ-NQ_exp_2026,  AC,       CQ,    NQ,    0.0,      -0.9,      1.0,       2026,        inf,       {1000000 * _ANNUITY_PER_DOLLAR},   True,              flow_path
        Q1-NQ,     Q1-NQ_exp_2026,  AC,       Q1,    NQ,    0.0,      -1.0,      1.0,       2026,        inf,       {500000 * _ANNUITY_PER_DOLLAR},    True,              rez
    """)
    pd.testing.assert_frame_equal(
        links.sort_values("name").reset_index(drop=True),
        expected_links.sort_values("name").reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )

    expected_limits = csv_str_to_df("""
        name,            attribute,  timeslice,             value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,       0.628272
        CQ-NQ_existing,  p_max_pu,   qld_summer_typical,    0.680628
        CQ-NQ_existing,  p_max_pu,   qld_winter_reference,  0.732984
        CQ-NQ_existing,  p_min_pu,   qld_peak_demand,       -0.753927
        CQ-NQ_existing,  p_min_pu,   qld_summer_typical,    -0.837696
        CQ-NQ_existing,  p_min_pu,   qld_winter_reference,  -1.0
        N1-CNSW_existing,p_max_pu,   ,                      1.0
        N1-CNSW_existing,p_min_pu,   ,                      -1.0
        Q1-NQ_existing,  p_max_pu,   qld_winter_reference,  1.0
        Q1-NQ_existing,  p_min_pu,   qld_winter_reference,  -1.0
    """)
    pd.testing.assert_frame_equal(
        link_timeslice_limits.sort_values(
            ["name", "attribute", "timeslice"]
        ).reset_index(drop=True),
        expected_limits.sort_values(["name", "attribute", "timeslice"]).reset_index(
            drop=True
        ),
        rtol=1e-5,
    )


def test_translate_network_to_links_p_nom_when_forward_exceeds_reverse(
    csv_str_to_df, sample_model_config
):
    """p_nom is the larger of the two directions. With forward (peak 1500) above
    reverse, p_nom takes it, so a forward limit above winter stays at p_max_pu
    1.0 rather than over-crediting the link above its rating
    (Open-ISP/ISPyPSA#123, item 3)."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
    """)
    # Forward peak (1500) exceeds winter (1400): winter is no longer the max.
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,             capacity
        CQ-NQ,    forward,    qld_peak_demand,       1500
        CQ-NQ,    forward,    qld_winter_reference,  1400
        CQ-NQ,    reverse,    qld_winter_reference,  1000
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    expected_links = csv_str_to_df("""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_existing,  AC,       CQ,    NQ,    1500,   0.0,       1.0,       2025,        inf,       ,              False,             flow_path
    """)
    pd.testing.assert_frame_equal(links, expected_links, check_dtype=False, rtol=1e-5)

    expected_limits = csv_str_to_df("""
        name,            attribute,  timeslice,             value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,       1.0
        CQ-NQ_existing,  p_max_pu,   qld_winter_reference,  0.933333
        CQ-NQ_existing,  p_min_pu,   qld_winter_reference,  -0.666667
    """)
    pd.testing.assert_frame_equal(
        link_timeslice_limits.sort_values(
            ["name", "attribute", "timeslice"]
        ).reset_index(drop=True),
        expected_limits.sort_values(["name", "attribute", "timeslice"]).reset_index(
            drop=True
        ),
        rtol=1e-5,
    )


def test_translate_network_to_links_p_nom_when_reverse_exceeds_forward(
    csv_str_to_df, sample_model_config
):
    """p_nom is the larger of the two directions. With reverse (1400) above
    forward (1000), p_nom takes reverse, so the forward limit is p_max_pu < 1 and
    the reverse limit reaches p_min_pu -1.0 in link_timeslice_limits — both
    per-unit limits within [-1, 1]."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
    """)
    # Reverse winter (1400) exceeds forward winter (1000): reverse sets p_nom.
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,             capacity
        CQ-NQ,    forward,    qld_winter_reference,  1000
        CQ-NQ,    reverse,    qld_winter_reference,  1400
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    expected_links = csv_str_to_df("""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_existing,  AC,       CQ,    NQ,    1400,   0.0,       1.0,       2025,        inf,       ,              False,             flow_path
    """)
    pd.testing.assert_frame_equal(links, expected_links, check_dtype=False, rtol=1e-5)

    expected_limits = csv_str_to_df("""
        name,            attribute,  timeslice,             value
        CQ-NQ_existing,  p_max_pu,   qld_winter_reference,  0.714286
        CQ-NQ_existing,  p_min_pu,   qld_winter_reference,  -1.0
    """)
    pd.testing.assert_frame_equal(
        link_timeslice_limits.sort_values(["attribute"]).reset_index(drop=True),
        expected_limits.sort_values(["attribute"]).reset_index(drop=True),
        rtol=1e-5,
    )


def test_translate_network_to_links_nan_timeslice_fallback(
    csv_str_to_df, sample_model_config
):
    """A timeslice = NaN row is a fallback limit: it counts towards p_nom (we
    don't yet know which snapshots it covers) and flows into link_timeslice_limits
    carrying a NaN timeslice (Open-ISP/ISPyPSA#123)."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
    """)
    # Forward: a named peak row plus a NaN-timeslice fallback (1400, the larger).
    # Reverse: only a NaN-timeslice fallback (1000).
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,        capacity
        CQ-NQ,    forward,    qld_peak_demand,  1200
        CQ-NQ,    forward,    ,                 1400
        CQ-NQ,    reverse,    ,                 1000
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    # p_nom = max(forward 1400, reverse 1000) = 1400; the reverse fallback is in
    # link_timeslice_limits, so the static p_min_pu stays at 0.0.
    expected_links = csv_str_to_df("""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_existing,  AC,       CQ,    NQ,    1400,   0.0,       1.0,       2025,        inf,       ,              False,             flow_path
    """)
    pd.testing.assert_frame_equal(links, expected_links, check_dtype=False, rtol=1e-5)

    # The fallback rows keep their NaN timeslice (blank field after the comma).
    expected_limits = csv_str_to_df("""
        name,            attribute,  timeslice,        value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,  0.857143
        CQ-NQ_existing,  p_max_pu,   ,                 1.0
        CQ-NQ_existing,  p_min_pu,   ,                 -0.714286
    """)
    pd.testing.assert_frame_equal(
        link_timeslice_limits.sort_values(["attribute", "value"]).reset_index(
            drop=True
        ),
        expected_limits.sort_values(["attribute", "value"]).reset_index(drop=True),
        rtol=1e-5,
    )


def test_translate_network_to_links_zero_forward_keeps_reverse(
    csv_str_to_df, sample_model_config
):
    """A link with a zero forward limit and a non-zero reverse limit takes its
    reverse capacity as p_nom, so the reverse capability is preserved rather than
    collapsing the link to zero flow in both directions."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
    """)
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,             capacity
        CQ-NQ,    forward,    qld_winter_reference,  0
        CQ-NQ,    reverse,    qld_winter_reference,  1000
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    # p_nom = max(0, 1000) = 1000; reverse fully available (via the timeslice
    # limit), forward pinned to 0. Static p_min_pu stays at 0.0.
    expected_links = csv_str_to_df("""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_existing,  AC,       CQ,    NQ,    1000,   0.0,       1.0,       2025,        inf,       ,              False,             flow_path
    """)
    pd.testing.assert_frame_equal(links, expected_links, check_dtype=False, rtol=1e-5)

    expected_limits = csv_str_to_df("""
        name,            attribute,  timeslice,             value
        CQ-NQ_existing,  p_max_pu,   qld_winter_reference,  0.0
        CQ-NQ_existing,  p_min_pu,   qld_winter_reference,  -1.0
    """)
    pd.testing.assert_frame_equal(
        link_timeslice_limits.sort_values(["attribute"]).reset_index(drop=True),
        expected_limits.sort_values(["attribute"]).reset_index(drop=True),
        rtol=1e-5,
    )


def test_translate_network_to_links_no_data_path_gets_default_fallback(
    csv_str_to_df, sample_model_config
):
    """A path with no limit data (a collapsed all-NaN row) is normalised to a
    symmetric timeslice = NaN fallback at the default limit, so it tiles the year
    the same way a data path does (Open-ISP/ISPyPSA#123)."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        N1-CNSW,  N1,        CNSW,    AC
    """)
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        N1-CNSW,  ,           ,
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    # No-data path takes the default limit (1000000) in both directions.
    expected_links = csv_str_to_df("""
        isp_name,  name,              carrier,  bus0,  bus1,  p_nom,    p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
        N1-CNSW,   N1-CNSW_existing,  AC,       N1,    CNSW,  1000000,  0.0,       1.0,       2025,        inf,       ,              False,             rez
    """)
    pd.testing.assert_frame_equal(links, expected_links, check_dtype=False, rtol=1e-5)

    # Both directions get a NaN-timeslice fallback per unit of p_nom.
    expected_limits = csv_str_to_df("""
        name,              attribute,  timeslice,  value
        N1-CNSW_existing,  p_max_pu,   ,           1.0
        N1-CNSW_existing,  p_min_pu,   ,           -1.0
    """)
    pd.testing.assert_frame_equal(
        link_timeslice_limits.sort_values(["attribute"]).reset_index(drop=True),
        expected_limits.sort_values(["attribute"]).reset_index(drop=True),
        rtol=1e-5,
    )


def test_translate_network_to_links_expansion_disabled(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _network_tables(csv_str_to_df)
    sample_model_config.network.transmission_expansion = False

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    # The flow path's expansion link is gone; the REZ path's remains. The
    # existing links' content is pinned by test_translate_network_to_links.
    expansion = links[links["p_nom_extendable"]].reset_index(drop=True)
    expected_expansion = csv_str_to_df(f"""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,                     p_nom_extendable,  isp_type
        Q1-NQ,     Q1-NQ_exp_2026,  AC,       Q1,    NQ,    0.0,    -1.0,      1.0,       2026,        inf,       {500000 * _ANNUITY_PER_DOLLAR},  True,              rez
    """)
    pd.testing.assert_frame_equal(
        expansion, expected_expansion, check_dtype=False, rtol=1e-5
    )


def test_translate_network_to_links_rez_expansion_disabled(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _network_tables(csv_str_to_df)
    sample_model_config.network.rez_transmission_expansion = False

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    # The REZ path's expansion link is gone; the flow path's remains. The
    # existing links' content is pinned by test_translate_network_to_links.
    expansion = links[links["p_nom_extendable"]].reset_index(drop=True)
    expected_expansion = csv_str_to_df(f"""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,                      p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_exp_2026,  AC,       CQ,    NQ,    0.0,    -0.9,      1.0,       2026,        inf,       {1000000 * _ANNUITY_PER_DOLLAR},  True,              flow_path
    """)
    pd.testing.assert_frame_equal(
        expansion, expected_expansion, check_dtype=False, rtol=1e-5
    )


def test_translate_network_to_links_all_expansion_disabled(
    csv_str_to_df, sample_model_config
):
    """With both expansion flags off no element is enabled, so the populated
    options and costs tables resolve against an empty allowed set and no
    expansion links are built."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    sample_model_config.network.transmission_expansion = False
    sample_model_config.network.rez_transmission_expansion = False

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    # No expansion links; the existing links' content is pinned by
    # test_translate_network_to_links.
    expansion = links[links["p_nom_extendable"]].reset_index(drop=True)
    expected_expansion = csv_str_to_df("""
        isp_name,  name,  carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
    """)
    pd.testing.assert_frame_equal(expansion, expected_expansion, check_dtype=False)


def test_translate_network_to_links_rezs_attached_to_parent_node(
    csv_str_to_df, sample_model_config
):
    """With REZs attached to their parent nodes the REZ paths leave the model,
    and their limit, option and cost rows are filtered out before wildcard
    resolution rather than raising as out-of-set values."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    sample_model_config.network.nodes.rezs = "attached_to_parent_node"

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    # REZ paths (and their expansion links and timeslice limits) are dropped.
    expected_links = csv_str_to_df(f"""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,                      p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_existing,  AC,       CQ,    NQ,    1910,   0.0,       1.0,       2025,        inf,       ,                                  False,             flow_path
        CQ-NQ,     CQ-NQ_exp_2026,  AC,       CQ,    NQ,    0.0,    -0.9,      1.0,       2026,        inf,       {1000000 * _ANNUITY_PER_DOLLAR},  True,              flow_path
    """)
    pd.testing.assert_frame_equal(
        links.sort_values("name").reset_index(drop=True),
        expected_links.sort_values("name").reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )

    expected_limits = csv_str_to_df("""
        name,            attribute,  timeslice,             value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,       0.628272
        CQ-NQ_existing,  p_max_pu,   qld_summer_typical,    0.680628
        CQ-NQ_existing,  p_max_pu,   qld_winter_reference,  0.732984
        CQ-NQ_existing,  p_min_pu,   qld_peak_demand,       -0.753927
        CQ-NQ_existing,  p_min_pu,   qld_summer_typical,    -0.837696
        CQ-NQ_existing,  p_min_pu,   qld_winter_reference,  -1.0
    """)
    pd.testing.assert_frame_equal(
        link_timeslice_limits.sort_values(
            ["name", "attribute", "timeslice"]
        ).reset_index(drop=True),
        expected_limits.sort_values(["name", "attribute", "timeslice"]).reset_index(
            drop=True
        ),
        rtol=1e-5,
    )


def test_translate_network_to_links_zero_capacity_parallel_path(
    csv_str_to_df, sample_model_config
):
    """A new parallel corridor has zero existing capacity, given as a
    timeslice = NaN fallback of 0 in both directions. It becomes an inert
    existing link at p_nom 0 (its buildable capacity is modelled by expansion
    links), and the zero-p_nom link is skipped when translating per-timeslice
    limits, so it yields no link_timeslice_limits rows and no 0/0 division."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,   geo_from,  geo_to,  carrier
        CNSW-SNW,  CNSW,      SNW,     AC
    """)
    # Zero existing capacity as a timeslice = NaN fallback (covers the year).
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,   direction,  timeslice,  capacity
        CNSW-SNW,  forward,    ,           0
        CNSW-SNW,  reverse,    ,           0
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    expected_links = csv_str_to_df("""
        isp_name,  name,                carrier, bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
        CNSW-SNW,  CNSW-SNW_existing,   AC,      CNSW,  SNW,   0,      0.0,       1.0,       2025,        inf,       ,              False,             flow_path
    """)
    pd.testing.assert_frame_equal(links, expected_links, check_dtype=False)

    expected_limits = csv_str_to_df("""
        name,  attribute,  timeslice,  value
    """)
    pd.testing.assert_frame_equal(
        link_timeslice_limits, expected_limits, check_dtype=False
    )


def test_translate_network_to_links_empty_expansion_tables(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)
    ispypsa_tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
    """)

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    # No expansion links; the existing links' content is pinned by
    # test_translate_network_to_links.
    expansion = links[links["p_nom_extendable"]].reset_index(drop=True)
    expected_expansion = csv_str_to_df("""
        isp_name,  name,  carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
    """)
    pd.testing.assert_frame_equal(expansion, expected_expansion, check_dtype=False)


def test_translate_network_to_links_options_populated_costs_empty(
    csv_str_to_df, sample_model_config
):
    """A populated options table with no matching costs is invalid per the
    schema (costs_cover_every_element_and_investment_period), but until schema
    validation lands it silently produces no expansion links."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
    """)

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    # No expansion links; the existing links' content is pinned by
    # test_translate_network_to_links.
    expansion = links[links["p_nom_extendable"]].reset_index(drop=True)
    expected_expansion = csv_str_to_df("""
        isp_name,  name,  carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
    """)
    pd.testing.assert_frame_equal(expansion, expected_expansion, check_dtype=False)


def test_translate_network_to_links_options_empty_costs_populated(
    csv_str_to_df, sample_model_config
):
    """Costs without options mean no element is expandable, so no expansion
    links are built. This input is actually invalid per the schema — costs
    declare expansion_id allowed_values_from network_expansion_options, and an
    empty options table leaves no allowed values — so once the validation layer
    lands it will be rejected upstream rather than silently building nothing."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    # No expansion links; the existing links' content is pinned by
    # test_translate_network_to_links.
    expansion = links[links["p_nom_extendable"]].reset_index(drop=True)
    expected_expansion = csv_str_to_df("""
        isp_name,  name,  carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
    """)
    pd.testing.assert_frame_equal(expansion, expected_expansion, check_dtype=False)


def test_translate_network_to_links_empty_paths(csv_str_to_df, sample_model_config):
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
    """)
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)
    ispypsa_tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
    """)

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    expected_links = csv_str_to_df("""
        isp_name,  name,  carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
    """)
    pd.testing.assert_frame_equal(links, expected_links, check_dtype=False)

    expected_limits = csv_str_to_df("""
        name,  attribute,  timeslice,  value
    """)
    pd.testing.assert_frame_equal(
        link_timeslice_limits, expected_limits, check_dtype=False
    )


def test_translate_network_to_links_path_id_wildcard_default(
    csv_str_to_df, sample_model_config
):
    """A blank path_id limit row is a table-wide default: a path with no rows of
    its own (Q1-NQ) inherits it in both directions, while a path with its own
    rows (CQ-NQ) keeps them, being more specific."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
        Q1-NQ,    Q1,        NQ,      AC
    """)
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    ,           1500
        CQ-NQ,    reverse,    ,           1000
        ,         ,           ,           800
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    expected_links = csv_str_to_df("""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_existing,  AC,       CQ,    NQ,    1500,   0.0,       1.0,       2025,        inf,       ,              False,             flow_path
        Q1-NQ,     Q1-NQ_existing,  AC,       Q1,    NQ,    800,    0.0,       1.0,       2025,        inf,       ,              False,             rez
    """)
    pd.testing.assert_frame_equal(
        links.sort_values("name").reset_index(drop=True),
        expected_links.sort_values("name").reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )

    # CQ-NQ keeps its own per-unit limits; Q1-NQ takes the global default both ways.
    expected_limits = csv_str_to_df("""
        name,            attribute,  timeslice,  value
        CQ-NQ_existing,  p_max_pu,   ,           1.0
        CQ-NQ_existing,  p_min_pu,   ,           -0.666667
        Q1-NQ_existing,  p_max_pu,   ,           1.0
        Q1-NQ_existing,  p_min_pu,   ,           -1.0
    """)
    pd.testing.assert_frame_equal(
        link_timeslice_limits.sort_values(["name", "attribute"]).reset_index(drop=True),
        expected_limits.sort_values(["name", "attribute"]).reset_index(drop=True),
        rtol=1e-5,
    )


def test_translate_network_to_links_static_cost_across_investment_periods(
    csv_str_to_df, sample_model_config
):
    """A blank year cost row is a static cost across every investment period, so
    an expansion link is built for each (2026 and 2028 in the sample config)."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
    """)
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    ,           1000
        CQ-NQ,    reverse,    ,           1000
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         forward,         500,                Opt
        CQ-NQ,         reverse,         500,                Opt
    """)
    ispypsa_tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         ,      1000000
    """)

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    expansion = links[links["p_nom_extendable"]].sort_values("name")
    expected = csv_str_to_df(f"""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,                       p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_exp_2026,  AC,       CQ,    NQ,    0.0,    -1.0,      1.0,       2026,        inf,       {1000000 * _ANNUITY_PER_DOLLAR},   True,              flow_path
        CQ-NQ,     CQ-NQ_exp_2028,  AC,       CQ,    NQ,    0.0,    -1.0,      1.0,       2028,        inf,       {1000000 * _ANNUITY_PER_DOLLAR},   True,              flow_path
    """)
    pd.testing.assert_frame_equal(
        expansion.reset_index(drop=True),
        expected.sort_values("name").reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )


def test_translate_network_to_links_year_override_beats_static_cost(
    csv_str_to_df, sample_model_config
):
    """A concrete-year cost row overrides the blank-year static row for that
    investment period; the static row still fills the other periods."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
    """)
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    ,           1000
        CQ-NQ,    reverse,    ,           1000
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         ,                500,                Opt
    """)
    ispypsa_tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         ,      1000000
        CQ-NQ,         2028,  1200000
    """)

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    expansion = links[links["p_nom_extendable"]].sort_values("name")
    expected = csv_str_to_df(f"""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,                       p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_exp_2026,  AC,       CQ,    NQ,    0.0,    -1.0,      1.0,       2026,        inf,       {1000000 * _ANNUITY_PER_DOLLAR},   True,              flow_path
        CQ-NQ,     CQ-NQ_exp_2028,  AC,       CQ,    NQ,    0.0,    -1.0,      1.0,       2028,        inf,       {1200000 * _ANNUITY_PER_DOLLAR},   True,              flow_path
    """)
    pd.testing.assert_frame_equal(
        expansion.reset_index(drop=True),
        expected.sort_values("name").reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )


def test_translate_network_to_links_expansion_id_wildcard_default_cost(
    csv_str_to_df, sample_model_config
):
    """A blank expansion_id cost row is a table-wide default: an element with no
    cost rows of its own (Q1-NQ) inherits it, while CQ-NQ keeps its own cost. It
    also fans out to enabled elements with no expansion options (N1-CNSW), which
    still build no expansion link."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         2026,  1000000
        ,              2026,  500000
    """)

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    expansion = links[links["p_nom_extendable"]].sort_values("name")
    expected = csv_str_to_df(f"""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,                       p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_exp_2026,  AC,       CQ,    NQ,    0.0,    -0.9,      1.0,       2026,        inf,       {1000000 * _ANNUITY_PER_DOLLAR},   True,              flow_path
        Q1-NQ,     Q1-NQ_exp_2026,  AC,       Q1,    NQ,    0.0,    -1.0,      1.0,       2026,        inf,       {500000 * _ANNUITY_PER_DOLLAR},    True,              rez
    """)
    pd.testing.assert_frame_equal(
        expansion.reset_index(drop=True),
        expected.sort_values("name").reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )


def test_translate_network_to_links_expansion_type_wildcard(
    csv_str_to_df, sample_model_config
):
    """A blank expansion_type option applies to both directions, so the expansion
    link gets symmetric forward and reverse capability."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
    """)
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    ,           1000
        CQ-NQ,    reverse,    ,           1000
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         ,                700,                Opt
    """)
    ispypsa_tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         2026,  1000000
    """)

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    expansion = links[links["p_nom_extendable"]].reset_index(drop=True)
    expected = csv_str_to_df(f"""
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,  p_min_pu,  p_max_pu,  build_year,  lifetime,  capital_cost,                       p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_exp_2026,  AC,       CQ,    NQ,    0.0,    -1.0,      1.0,       2026,        inf,       {1000000 * _ANNUITY_PER_DOLLAR},   True,              flow_path
    """)
    pd.testing.assert_frame_equal(expansion, expected, check_dtype=False, rtol=1e-5)


def test_translate_network_to_links_raises_on_unknown_limits_path_id(
    csv_str_to_df, sample_model_config
):
    """A limits path_id absent from the paths table is bad input data — unlike a
    config-driven drop, it survives the pre-resolution filtering and the wildcard
    resolver halts the run on it. In the long run the validation layer will guard
    against this example.

    Run with REZs attached to their parent nodes so the filtering is active: the
    raise message lists every disallowed value, so asserting it names only the
    typo also proves the known-but-unmodelled Q1-NQ row was filtered out and the
    blank wildcard row rode through."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    sample_model_config.network.nodes.rezs = "attached_to_parent_node"
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    ,           1200
        Q1-NQ,    forward,    ,           750
        ,         ,           ,           500
        CQ-NQQ,   forward,    ,           1200
    """)

    with pytest.raises(
        ValueError,
        match=r"path_id contains values outside the allowed set: \['CQ-NQQ'\]",
    ):
        _translate_network_to_links(ispypsa_tables, sample_model_config)


def test_translate_network_to_links_logs_config_filtered_rows(
    csv_str_to_df, sample_model_config, caplog
):
    """Rows filtered out by config-driven selection — limits for unmodelled REZ
    paths, options and costs for elements that are not enabled, and cost years
    outside the investment periods — are logged at INFO so their absence from
    the model can be audited."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    sample_model_config.network.nodes.rezs = "attached_to_parent_node"

    with caplog.at_level("INFO"):
        _translate_network_to_links(ispypsa_tables, sample_model_config)

    assert (
        "Filtered limit rows for paths configured out of the model: "
        "['N1-CNSW', 'Q1-NQ']"
    ) in caplog.text
    # The options table's filtered ids (SWQLD1 was already set aside as a
    # constraint_relaxation row)...
    assert (
        "Filtered rows whose expansion_id is not an enabled expansion element: "
        "['Q1-NQ']"
    ) in caplog.text
    # ...and the costs table's, where SWQLD1 is filtered rather than set aside.
    assert (
        "Filtered rows whose expansion_id is not an enabled expansion element: "
        "['Q1-NQ', 'SWQLD1']"
    ) in caplog.text
    assert (
        "Filtered expansion cost rows for years outside the investment periods: [2025]"
    ) in caplog.text


def test_translate_network_to_links_no_filter_log_when_all_rows_used(
    csv_str_to_df, sample_model_config, caplog
):
    """When every input row survives into the model, no filter log lines fire."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
    """)
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    ,           1000
        CQ-NQ,    reverse,    ,           1000
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         ,                700,                Opt
    """)
    ispypsa_tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         2026,  1000000
    """)

    with caplog.at_level("INFO"):
        _translate_network_to_links(ispypsa_tables, sample_model_config)

    assert "Filtered" not in caplog.text


def test_drop_limits_for_unmodelled_paths_limits_empty(csv_str_to_df):
    limits = pd.DataFrame(columns=["path_id", "direction", "timeslice", "capacity"])
    paths = csv_str_to_df("""
        path_id
        CQ-NQ
    """)

    result = _drop_limits_for_unmodelled_paths(limits, paths, paths)

    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_drop_limits_for_unmodelled_paths_paths_empty(csv_str_to_df):
    """With no paths at all nothing is 'known but unmodelled', so every limit row
    rides through to _resolve_wildcards, which raises on the out-of-set path_id."""
    limits = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    ,           1200
    """)
    paths = pd.DataFrame(columns=["path_id"])

    result = _drop_limits_for_unmodelled_paths(limits, paths, paths)

    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
        CQ-NQ,    forward,    ,           1200
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_drop_limits_for_unmodelled_paths_both_empty(csv_str_to_df):
    limits = pd.DataFrame(columns=["path_id", "direction", "timeslice", "capacity"])
    paths = pd.DataFrame(columns=["path_id"])

    result = _drop_limits_for_unmodelled_paths(limits, paths, paths)

    expected = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_resolve_expansion_options_pairs_forward_and_reverse(csv_str_to_df):
    """Each option defines both directions; a blank-expansion_id default pair fills
    elements with no option of their own, keeping forward and reverse coupled."""
    options = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        CQ-NQ,         forward,                1000,               BigLine
        CQ-NQ,         reverse,                800,                BigLine
        ,              forward,                500,                Default
        ,              reverse,                400,                Default
        SWQLD1,        constraint_relaxation,  400,                Relax
    """)

    result = _resolve_expansion_options(options, ["CQ-NQ", "Q1-NQ"])

    # CQ-NQ keeps its BigLine pair; Q1-NQ inherits the Default pair; SWQLD1 drops.
    expected = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         forward,         1000,               BigLine
        CQ-NQ,         reverse,         800,                BigLine
        Q1-NQ,         forward,         500,                Default
        Q1-NQ,         reverse,         400,                Default
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["expansion_id", "expansion_type"]).reset_index(drop=True),
        expected.sort_values(["expansion_id", "expansion_type"]).reset_index(drop=True),
        check_dtype=False,
    )


def test_resolve_expansion_options_raises_on_missing_direction(csv_str_to_df):
    """An option defining only one direction raises rather than producing an
    expansion link with a NaN rating in the missing direction. The check stands
    in for the schema's expansion_options_pair_forward_and_reverse rule, which
    will reject this input upstream once the validation layer lands."""
    options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         forward,         1000,               BigLine
    """)

    with pytest.raises(
        ValueError, match=r"Expansion options define only one direction"
    ):
        _resolve_expansion_options(options, ["CQ-NQ"])


def test_resolve_expansion_options_raises_on_mismatched_pair(csv_str_to_df):
    """A forward from one option paired with a reverse from another (here a
    blank-expansion_id default) is incoherent and raises. The check stands in
    for the schema's expansion_options_pair_forward_and_reverse rule, which will
    reject this input upstream once the validation layer lands."""
    options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         forward,         1000,               BigLine
        ,              reverse,         400,                Default
    """)

    with pytest.raises(
        ValueError, match="Forward and reverse expansion options disagree"
    ):
        _resolve_expansion_options(options, ["CQ-NQ"])


def test_resolve_expansion_options_raises_on_blank_option_mismatched_pair(
    csv_str_to_df,
):
    """A blank expansion_option only pairs with another blank: a forward from an
    unlabelled option and a reverse from a named one (here a blank-expansion_id
    default) is just as incoherent as two differing names, and nunique's default
    NaN-dropping would otherwise let it through."""
    options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         forward,         1000,
        ,              reverse,         400,                Default
    """)

    with pytest.raises(
        ValueError, match="Forward and reverse expansion options disagree"
    ):
        _resolve_expansion_options(options, ["CQ-NQ"])
