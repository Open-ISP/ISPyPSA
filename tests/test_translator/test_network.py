import pandas as pd
import pytest

from ispypsa.translator.helpers import _annuitised_investment_costs
from ispypsa.translator.network import (
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
        isp_name,  name,            carrier,  bus0,  bus1,  p_nom,    p_min_pu,  build_year,  lifetime,  capital_cost,                       p_nom_extendable,  isp_type
        CQ-NQ,     CQ-NQ_existing,  AC,       CQ,    NQ,    1400,     -1.364286, 2025,        inf,       ,                                   False,             flow_path
        Q1-NQ,     Q1-NQ_existing,  AC,       Q1,    NQ,    750,      -1.0,      2025,        inf,       ,                                   False,             rez
        N1-CNSW,   N1-CNSW_existing,AC,       N1,    CNSW,  1000000,  -1.0,      2025,        inf,       ,                                   False,             rez
        CQ-NQ,     CQ-NQ_exp_2026,  AC,       CQ,    NQ,    0.0,      -0.9,      2026,        inf,       {1000000 * _ANNUITY_PER_DOLLAR},   True,              flow_path
        Q1-NQ,     Q1-NQ_exp_2026,  AC,       Q1,    NQ,    0.0,      -1.0,      2026,        inf,       {500000 * _ANNUITY_PER_DOLLAR},    True,              rez
    """)
    pd.testing.assert_frame_equal(
        links.sort_values("name").reset_index(drop=True),
        expected_links.sort_values("name").reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )

    expected_limits = csv_str_to_df("""
        name,            attribute,  timeslice,             value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,       0.857143
        CQ-NQ_existing,  p_max_pu,   qld_summer_typical,    0.928571
        CQ-NQ_existing,  p_max_pu,   qld_winter_reference,  1.0
        CQ-NQ_existing,  p_min_pu,   qld_peak_demand,       -1.028571
        CQ-NQ_existing,  p_min_pu,   qld_summer_typical,    -1.142857
        CQ-NQ_existing,  p_min_pu,   qld_winter_reference,  -1.364286
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


def test_translate_network_to_links_expansion_disabled(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _network_tables(csv_str_to_df)
    sample_model_config.network.transmission_expansion = False

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    # The flow path's expansion link is gone; the REZ path's remains.
    assert sorted(links["name"]) == [
        "CQ-NQ_existing",
        "N1-CNSW_existing",
        "Q1-NQ_existing",
        "Q1-NQ_exp_2026",
    ]


def test_translate_network_to_links_rez_expansion_disabled(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _network_tables(csv_str_to_df)
    sample_model_config.network.rez_transmission_expansion = False

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    # The REZ path's expansion link is gone; the flow path's remains.
    assert sorted(links["name"]) == [
        "CQ-NQ_existing",
        "CQ-NQ_exp_2026",
        "N1-CNSW_existing",
        "Q1-NQ_existing",
    ]


def test_translate_network_to_links_rezs_attached_to_parent_node(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _network_tables(csv_str_to_df)
    sample_model_config.network.nodes.rezs = "attached_to_parent_node"

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    # REZ paths (and their expansion links and timeslice limits) are dropped.
    assert sorted(links["name"]) == ["CQ-NQ_existing", "CQ-NQ_exp_2026"]
    assert set(link_timeslice_limits["name"]) == {"CQ-NQ_existing"}


def test_translate_network_to_links_zero_capacity_parallel_path(
    csv_str_to_df, sample_model_config
):
    """New parallel corridors arrive as zero-capacity paths: p_nom 0 with a
    symmetric p_min_pu (no 0/0 division), and no per-unit timeslice rows."""
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,   geo_from,  geo_to,  carrier
        CNSW-SNW,  CNSW,      SNW,     AC
    """)
    ispypsa_tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,   direction,  timeslice,             capacity
        CNSW-SNW,  forward,    nsw_winter_reference,  0
        CNSW-SNW,  reverse,    nsw_winter_reference,  0
    """)
    ispypsa_tables["network_expansion_options"] = pd.DataFrame(
        columns=[
            "expansion_id",
            "expansion_type",
            "allowed_expansion",
            "expansion_option",
        ]
    )

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    expected_links = csv_str_to_df("""
        isp_name,  name,                carrier, bus0,  bus1,  p_nom,  p_min_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
        CNSW-SNW,  CNSW-SNW_existing,   AC,      CNSW,  SNW,   0,      -1.0,      2025,        inf,       ,              False,             flow_path
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
    ispypsa_tables["network_expansion_options"] = pd.DataFrame(
        columns=[
            "expansion_id",
            "expansion_type",
            "allowed_expansion",
            "expansion_option",
        ]
    )
    ispypsa_tables["network_transmission_path_expansion_costs"] = pd.DataFrame(
        columns=["expansion_id", "year", "cost"]
    )

    links, _ = _translate_network_to_links(ispypsa_tables, sample_model_config)

    assert sorted(links["name"]) == [
        "CQ-NQ_existing",
        "N1-CNSW_existing",
        "Q1-NQ_existing",
    ]


def test_translate_network_to_links_empty_paths(csv_str_to_df, sample_model_config):
    ispypsa_tables = _network_tables(csv_str_to_df)
    ispypsa_tables["network_transmission_paths"] = pd.DataFrame(
        columns=["path_id", "geo_from", "geo_to", "carrier"]
    )
    ispypsa_tables["network_transmission_path_limits"] = pd.DataFrame(
        columns=["path_id", "direction", "timeslice", "capacity"]
    )
    ispypsa_tables["network_expansion_options"] = pd.DataFrame(
        columns=[
            "expansion_id",
            "expansion_type",
            "allowed_expansion",
            "expansion_option",
        ]
    )
    ispypsa_tables["network_transmission_path_expansion_costs"] = pd.DataFrame(
        columns=["expansion_id", "year", "cost"]
    )

    links, link_timeslice_limits = _translate_network_to_links(
        ispypsa_tables, sample_model_config
    )

    expected_links = csv_str_to_df("""
        isp_name,  name,  carrier,  bus0,  bus1,  p_nom,  p_min_pu,  build_year,  lifetime,  capital_cost,  p_nom_extendable,  isp_type
    """)
    pd.testing.assert_frame_equal(links, expected_links, check_dtype=False)

    expected_limits = csv_str_to_df("""
        name,  attribute,  timeslice,  value
    """)
    pd.testing.assert_frame_equal(
        link_timeslice_limits, expected_limits, check_dtype=False
    )
