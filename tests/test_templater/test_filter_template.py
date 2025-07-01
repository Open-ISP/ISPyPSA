import logging

import pandas as pd
import pytest

from ispypsa.templater.filter_template import (
    _determine_selected_regions,
    _filter_custom_constraints,
    _filter_expansion_costs,
    _filter_generator_dependent_tables,
    _filter_generators,
    _filter_other_tables,
    _filter_policy_tables,
    _filter_region_tables,
    _filter_template,
    _get_selected_flow_paths,
    _get_selected_rezs,
    _infer_link_names,
)


def test_determine_selected_regions_with_nem_regions(csv_str_to_df):
    """Test determining selected regions when filtering by NEM regions."""
    # Input data
    sub_regions_csv = """
    isp_sub_region_id,    nem_region_id
    CNSW,                 NSW
    SNSW,                 NSW
    VIC,                  VIC
    TAS,                  TAS
    """
    sub_regions_df = csv_str_to_df(sub_regions_csv)

    # Test filtering by NEM regions
    selected_sub, selected_nem = _determine_selected_regions(
        sub_regions_df, nem_regions=["NSW"], isp_sub_regions=None
    )

    assert set(selected_sub) == {"CNSW", "SNSW"}
    assert selected_nem == ["NSW"]


def test_determine_selected_regions_with_isp_sub_regions(csv_str_to_df):
    """Test determining selected regions when filtering by ISP sub-regions."""
    # Input data
    sub_regions_csv = """
    isp_sub_region_id,    nem_region_id
    CNSW,                 NSW
    SNSW,                 NSW
    VIC,                  VIC
    TAS,                  TAS
    """
    sub_regions_df = csv_str_to_df(sub_regions_csv)

    # Test filtering by ISP sub-regions
    selected_sub, selected_nem = _determine_selected_regions(
        sub_regions_df, nem_regions=None, isp_sub_regions=["CNSW", "VIC"]
    )

    assert selected_sub == ["CNSW", "VIC"]
    assert set(selected_nem) == {"NSW", "VIC"}


def test_filter_region_tables(csv_str_to_df):
    """Test filtering of basic region tables.

    Note: This tests the _filter_region_tables subfunction directly,
    which is called internally after region validation in the main filter_regions function.
    """
    # Input data
    template = {
        "sub_regions": csv_str_to_df("""
            isp_sub_region_id,    nem_region_id,    sub_region_reference_node
            CNSW,                 NSW,              Wellington
            SNSW,                 NSW,              Canberra
            VIC,                  VIC,              Thomastown
            TAS,                  TAS,              George Town
        """),
        "nem_regions": csv_str_to_df("""
            nem_region_id,    reference_node
            NSW,              Sydney
            VIC,              Melbourne
            TAS,              Hobart
        """),
        "renewable_energy_zones": csv_str_to_df("""
            rez_id,    isp_sub_region_id,    capacity_mw
            N1,        CNSW,                 1000
            N2,        SNSW,                 500
            V1,        VIC,                  800
            T1,        TAS,                  300
        """),
        "flow_paths": csv_str_to_df("""
            flow_path,    node_from,    node_to,    capacity_mw
            NSW-VIC,           NSW,          VIC,        1500
            VIC-TAS,           VIC,          TAS,        500
            CNSW-SNSW,         CNSW,         SNSW,       1000
        """),
    }

    # Filter to NSW only
    filtered = _filter_region_tables(template, ["CNSW", "SNSW"], ["NSW"])

    # Expected results
    expected_sub_regions = csv_str_to_df("""
        isp_sub_region_id,    nem_region_id,    sub_region_reference_node
        CNSW,                 NSW,              Wellington
        SNSW,                 NSW,              Canberra
    """)

    expected_nem_regions = csv_str_to_df("""
        nem_region_id,    reference_node
        NSW,              Sydney
    """)

    expected_rez = csv_str_to_df("""
        rez_id,    isp_sub_region_id,    capacity_mw
        N1,        CNSW,                 1000
        N2,        SNSW,                 500
    """)

    expected_flow_paths = csv_str_to_df("""
        flow_path,    node_from,    node_to,    capacity_mw
        CNSW-SNSW,         CNSW,         SNSW,       1000
    """)

    # Compare results
    pd.testing.assert_frame_equal(
        filtered["sub_regions"].sort_values("isp_sub_region_id").reset_index(drop=True),
        expected_sub_regions.sort_values("isp_sub_region_id").reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        filtered["nem_regions"].reset_index(drop=True),
        expected_nem_regions.reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        filtered["renewable_energy_zones"].sort_values("rez_id").reset_index(drop=True),
        expected_rez.sort_values("rez_id").reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        filtered["flow_paths"].reset_index(drop=True),
        expected_flow_paths.reset_index(drop=True),
    )


def test_filter_generators(csv_str_to_df):
    """Test filtering of generator tables."""
    # Input data
    template = {
        "ecaa_generators": csv_str_to_df("""
            generator,          sub_region_id,    region_id,    capacity_mw
            Bayswater,          CNSW,             NSW,          2640
            Eraring,            SNSW,             NSW,          2880
            LoyYangA,           VIC,              VIC,          2210
        """),
        "new_entrant_generators": csv_str_to_df("""
            generator,          sub_region_id,    region_id,    technology_type
            Solar_CNSW,         CNSW,             NSW,          Solar
            Wind_VIC,           VIC,              VIC,          Wind
        """),
    }

    # Filter to NSW sub-regions only
    filtered, selected_generators = _filter_generators(template, ["CNSW", "SNSW"])

    # Expected results
    expected_ecaa = csv_str_to_df("""
        generator,          sub_region_id,    region_id,    capacity_mw
        Bayswater,          CNSW,             NSW,          2640
        Eraring,            SNSW,             NSW,          2880
    """)

    expected_new_entrant = csv_str_to_df("""
        generator,          sub_region_id,    region_id,    technology_type
        Solar_CNSW,         CNSW,             NSW,          Solar
    """)

    # Compare results
    pd.testing.assert_frame_equal(
        filtered["ecaa_generators"].sort_values("generator").reset_index(drop=True),
        expected_ecaa.sort_values("generator").reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        filtered["new_entrant_generators"], expected_new_entrant
    )
    assert selected_generators == {"Bayswater", "Eraring", "Solar_CNSW"}


def test_filter_generator_dependent_tables(csv_str_to_df):
    """Test filtering of tables that depend on generator names."""
    # Input data
    template = {
        "closure_years": csv_str_to_df("""
            generator,          duid,       expected_closure_year_calendar_year
            Bayswater,          BW01,       2033
            Eraring,            ER01,       2025
            LoyYangA,           LYA1,       2045
        """),
        "coal_prices": csv_str_to_df("""
            generator,          FY,         price_$/GJ
            Bayswater,          2024_25,    3.5
            Eraring,            2024_25,    3.2
            LoyYangA,           2024_25,    2.8
        """),
        "seasonal_ratings": csv_str_to_df("""
            generator,          season,     rating_mw
            Bayswater,          summer,     2640
            Eraring,            summer,     2880
            LoyYangA,           summer,     2210
        """),
    }

    # Filter to selected generators only
    selected_generators = {"Bayswater", "Eraring"}
    filtered = _filter_generator_dependent_tables(template, selected_generators)

    # Expected results
    expected_closure = csv_str_to_df("""
        generator,          duid,       expected_closure_year_calendar_year
        Bayswater,          BW01,       2033
        Eraring,            ER01,       2025
    """)

    expected_coal_prices = csv_str_to_df("""
        generator,          FY,         price_$/GJ
        Bayswater,          2024_25,    3.5
        Eraring,            2024_25,    3.2
    """)

    expected_ratings = csv_str_to_df("""
        generator,          season,     rating_mw
        Bayswater,          summer,     2640
        Eraring,            summer,     2880
    """)

    # Compare results
    pd.testing.assert_frame_equal(
        filtered["closure_years"].sort_values("generator").reset_index(drop=True),
        expected_closure.sort_values("generator").reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        filtered["coal_prices"].sort_values("generator").reset_index(drop=True),
        expected_coal_prices.sort_values("generator").reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        filtered["seasonal_ratings"].sort_values("generator").reset_index(drop=True),
        expected_ratings.sort_values("generator").reset_index(drop=True),
    )


def test_get_selected_rezs_and_flow_paths(csv_str_to_df):
    """Test extraction of REZ IDs and flow path names from filtered tables."""
    # Input data
    filtered_tables = {
        "renewable_energy_zones": csv_str_to_df("""
            rez_id,    isp_sub_region_id,    capacity_mw
            N1,        CNSW,                 1000
            N2,        SNSW,                 500
        """),
        "flow_paths": csv_str_to_df("""
            flow_path,    node_from,    node_to,    capacity_mw
            NSW-VIC,           NSW,          VIC,        1500
            CNSW-SNSW,         CNSW,         SNSW,       1000
        """),
    }

    # Test extraction
    rezs = _get_selected_rezs(filtered_tables)
    flow_paths = _get_selected_flow_paths(filtered_tables)

    assert rezs == {"N1", "N2"}
    assert flow_paths == {"NSW-VIC", "CNSW-SNSW"}


def test_infer_link_names(csv_str_to_df):
    """Test inference of link names from flow paths and REZs."""
    # Input data
    flow_paths = csv_str_to_df("""
        flow_path,    node_from,    node_to
        NSW-VIC,           NSW,          VIC
        CNSW-SNSW,         CNSW,         SNSW
    """)

    rez = csv_str_to_df("""
        rez_id,    isp_sub_region_id
        N1,        CNSW
        N2,        SNSW
    """)

    # Test inference
    link_names = _infer_link_names(flow_paths, rez)

    expected_links = {"NSW-VIC", "CNSW-SNSW", "N1-CNSW", "N2-SNSW"}
    assert link_names == expected_links


def test_filter_custom_constraints(csv_str_to_df):
    """Test filtering of custom constraints tables."""
    # Input data
    template = {
        "custom_constraints_lhs": csv_str_to_df("""
            constraint_id,    term_type,           term_id,        coefficient
            C1,               generator_output,    Bayswater,      1.0
            C1,               generator_output,    Eraring,        1.0
            C2,               link_flow,           NSW-VIC,        1.0
            C2,               generator_output,    LoyYangA,       -1.0
            C3,               link_flow,           VIC-TAS,        1.0
        """),
        "custom_constraints_rhs": csv_str_to_df("""
            constraint_id,    rhs
            C1,               5000
            C2,               0
            C3,               500
        """),
    }

    # Only Bayswater, Eraring, and NSW-VIC are selected
    selected_generators = {"Bayswater", "Eraring"}
    selected_links = {"NSW-VIC"}

    filtered, constraint_ids = _filter_custom_constraints(
        template, selected_generators, selected_links
    )

    # Expected results - only C1 has all terms valid
    expected_lhs = csv_str_to_df("""
        constraint_id,    term_type,           term_id,        coefficient
        C1,               generator_output,    Bayswater,      1.0
        C1,               generator_output,    Eraring,        1.0
    """)

    expected_rhs = csv_str_to_df("""
        constraint_id,    rhs
        C1,               5000
    """)

    # Compare results
    pd.testing.assert_frame_equal(
        filtered["custom_constraints_lhs"]
        .sort_values(["constraint_id", "term_id"])
        .reset_index(drop=True),
        expected_lhs.sort_values(["constraint_id", "term_id"]).reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(filtered["custom_constraints_rhs"], expected_rhs)
    assert constraint_ids == {"C1"}


def test_filter_expansion_costs(csv_str_to_df):
    """Test filtering of expansion cost tables."""
    # Input data
    template = {
        "flow_path_expansion_costs": csv_str_to_df("""
            flow_path,    FY,         cost_$/MW
            NSW-VIC,           2024_25,    100000
            VIC-TAS,           2024_25,    150000
            CNSW-SNSW,         2024_25,    80000
        """),
        "rez_transmission_expansion_costs": csv_str_to_df("""
            rez_constraint_id,    rez,    option,    additional_network_capacity_mw
            N1,                   N1,     Option 1,  1000
            N2,                   N2,     Option 1,  500
            C1,                   NaN,    Option 1,  0
            V1,                   V1,     Option 1,  800
        """),
    }

    selected_flow_paths = {"NSW-VIC", "CNSW-SNSW"}
    selected_rezs = {"N1", "N2"}
    constraint_ids = {"C1"}

    filtered = _filter_expansion_costs(
        template, selected_flow_paths, selected_rezs, constraint_ids
    )

    # Expected results
    expected_flow_costs = csv_str_to_df("""
        flow_path,    FY,         cost_$/MW
        NSW-VIC,           2024_25,    100000
        CNSW-SNSW,         2024_25,    80000
    """)

    expected_rez_costs = csv_str_to_df("""
        rez_constraint_id,    rez,    option,    additional_network_capacity_mw
        N1,                   N1,     Option 1,  1000
        N2,                   N2,     Option 1,  500
        C1,                   NaN,    Option 1,  0
    """)

    # Compare results
    pd.testing.assert_frame_equal(
        filtered["flow_path_expansion_costs"]
        .sort_values("flow_path")
        .reset_index(drop=True),
        expected_flow_costs.sort_values("flow_path").reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        filtered["rez_transmission_expansion_costs"]
        .sort_values("rez_constraint_id")
        .reset_index(drop=True),
        expected_rez_costs.sort_values("rez_constraint_id").reset_index(drop=True),
    )


def test_filter_policy_tables(csv_str_to_df):
    """Test filtering of policy-related tables."""
    # Input data
    template = {
        "renewable_generation_targets": csv_str_to_df("""
            region_id,    policy_id,           FY,         target_gwh
            NSW,          renewable_gen_nsw,   2024_25,    20000
            VIC,          renewable_gen_vic,   2024_25,    15000
            NEM,          renewable_gen_nem,   2024_25,    50000
        """),
        "technology_capacity_targets": csv_str_to_df("""
            region_id,    policy_id,        FY,         capacity_mw
            NSW,          cis_generator,    2024_25,    1000
            VIC,          cis_generator,    2024_25,    800
            NEM,          cis_storage,      2024_25,    500
        """),
        "renewable_share_targets": csv_str_to_df("""
            region_id,    policy_id,          FY,         target_percent
            NSW,          renewable_share,    2024_25,    50
            VIC,          renewable_share,    2024_25,    45
            NEM,          renewable_share,    2024_25,    60
        """),
        "policy_generator_types": csv_str_to_df("""
            policy_id,            generator
            cis_generator,        Solar
            cis_generator,        Wind
            cis_storage,          Battery
            renewable_gen_nsw,    Solar
            renewable_gen_nem,    Wind
            renewable_share,      Solar
            other_policy,         Hydro
        """),
    }

    # Filter to NSW only
    filtered, policy_ids = _filter_policy_tables(template, ["NSW"])

    # Expected results
    expected_renewable = csv_str_to_df("""
        region_id,    policy_id,           FY,         target_gwh
        NSW,          renewable_gen_nsw,   2024_25,    20000
        NEM,          renewable_gen_nem,   2024_25,    50000
    """)

    expected_capacity = csv_str_to_df("""
        region_id,    policy_id,        FY,         capacity_mw
        NSW,          cis_generator,    2024_25,    1000
        NEM,          cis_storage,      2024_25,    500
    """)

    expected_share = csv_str_to_df("""
        region_id,    policy_id,          FY,         target_percent
        NSW,          renewable_share,    2024_25,    50
        NEM,          renewable_share,    2024_25,    60
    """)

    expected_policy_types = csv_str_to_df("""
        policy_id,            generator
        cis_generator,        Solar
        cis_generator,        Wind
        cis_storage,          Battery
        renewable_gen_nsw,    Solar
        renewable_gen_nem,    Wind
        renewable_share,      Solar
    """)

    # Compare results
    pd.testing.assert_frame_equal(
        filtered["renewable_generation_targets"]
        .sort_values("region_id")
        .reset_index(drop=True),
        expected_renewable.sort_values("region_id").reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        filtered["technology_capacity_targets"]
        .sort_values(["region_id", "policy_id"])
        .reset_index(drop=True),
        expected_capacity.sort_values(["region_id", "policy_id"]).reset_index(
            drop=True
        ),
    )
    pd.testing.assert_frame_equal(
        filtered["renewable_share_targets"]
        .sort_values(["region_id", "policy_id"])
        .reset_index(drop=True),
        expected_share.sort_values(["region_id", "policy_id"]).reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        filtered["policy_generator_types"]
        .sort_values(["policy_id", "generator"])
        .reset_index(drop=True),
        expected_policy_types.sort_values(["policy_id", "generator"]).reset_index(
            drop=True
        ),
    )
    assert policy_ids == {
        "cis_generator",
        "cis_storage",
        "renewable_gen_nsw",
        "renewable_gen_nem",
        "renewable_share",
    }


def test_filter_other_tables(csv_str_to_df):
    """Test filtering of miscellaneous tables."""
    # Input data
    template = {
        "new_entrant_non_vre_connection_costs": csv_str_to_df("""
            technology_type,    Region,    cost_$/kW
            CCGT,               NSW,       100
            CCGT,               VIC,       120
            OCGT,               NSW,       80
        """),
    }

    # Filter to NSW only
    filtered = _filter_other_tables(template, ["NSW"])

    # Expected results
    expected = csv_str_to_df("""
        technology_type,    Region,    cost_$/kW
        CCGT,               NSW,       100
        OCGT,               NSW,       80
    """)

    # Compare results
    pd.testing.assert_frame_equal(
        filtered["new_entrant_non_vre_connection_costs"]
        .sort_values("technology_type")
        .reset_index(drop=True),
        expected.sort_values("technology_type").reset_index(drop=True),
    )


def test_filter_regions_integration_nem_regions(csv_str_to_df):
    """Integration test: filter entire template by NEM regions."""
    # Create a minimal but complete template
    template = {
        "sub_regions": csv_str_to_df("""
            isp_sub_region_id,    nem_region_id
            CNSW,                 NSW
            SNSW,                 NSW
            VIC,                  VIC
        """),
        "ecaa_generators": csv_str_to_df("""
            generator,          sub_region_id,    region_id
            Bayswater,          CNSW,             NSW
            LoyYangA,           VIC,              VIC
        """),
        "closure_years": csv_str_to_df("""
            generator,          duid,       expected_closure_year_calendar_year
            Bayswater,          BW01,       2033
            LoyYangA,           LYA1,       2045
        """),
        "flow_paths": csv_str_to_df("""
            flow_path,    node_from,    node_to
            NSW-VIC,           NSW,          VIC
            CNSW-SNSW,         CNSW,         SNSW
        """),
        "renewable_energy_zones": csv_str_to_df("""
            rez_id,    isp_sub_region_id
            N1,        CNSW
            V1,        VIC
        """),
        "build_costs": csv_str_to_df("""
            technology_type,    FY,         cost_$/kW
            Solar,              2024_25,    1000
            Wind,               2024_25,    1500
        """),
    }

    # Filter to NSW only
    filtered = _filter_template(template, nem_regions=["NSW"])

    # Check key results
    assert set(filtered["sub_regions"]["isp_sub_region_id"]) == {"CNSW", "SNSW"}
    assert filtered["ecaa_generators"]["generator"].tolist() == ["Bayswater"]
    assert filtered["closure_years"]["generator"].tolist() == ["Bayswater"]
    assert filtered["flow_paths"]["flow_path"].tolist() == ["CNSW-SNSW"]
    assert filtered["renewable_energy_zones"]["rez_id"].tolist() == ["N1"]
    # Build costs should be unchanged
    assert len(filtered["build_costs"]) == 2


def test_filter_regions_integration_sub_regions(csv_str_to_df):
    """Integration test: filter entire template by ISP sub-regions."""
    # Create a minimal but complete template
    template = {
        "sub_regions": csv_str_to_df("""
            isp_sub_region_id,    nem_region_id
            CNSW,                 NSW
            SNSW,                 NSW
            VIC,                  VIC
        """),
        "ecaa_generators": csv_str_to_df("""
            generator,          sub_region_id,    region_id
            Bayswater,          CNSW,             NSW
            Eraring,            SNSW,             NSW
            LoyYangA,           VIC,              VIC
        """),
        "custom_constraints_lhs": csv_str_to_df("""
            constraint_id,    term_type,           term_id,        coefficient
            C1,               generator_output,    Bayswater,      1.0
            C1,               link_flow,           CNSW-SNSW,      -1.0
            C2,               generator_output,    LoyYangA,       1.0
        """),
        "custom_constraints_rhs": csv_str_to_df("""
            constraint_id,    rhs
            C1,               1000
            C2,               2000
        """),
        "flow_paths": csv_str_to_df("""
            flow_path,    node_from,    node_to
            CNSW-SNSW,         CNSW,         SNSW
            VIC-NSW,           VIC,          NSW
        """),
    }

    # Filter to CNSW and VIC only
    filtered = _filter_template(template, isp_sub_regions=["CNSW", "VIC"])

    # Check results
    assert set(filtered["sub_regions"]["isp_sub_region_id"]) == {"CNSW", "VIC"}
    assert set(filtered["ecaa_generators"]["generator"]) == {"Bayswater", "LoyYangA"}
    # Only C2 should be kept (C1 has invalid link CNSW-SNSW because SNSW is not selected)
    assert set(filtered["custom_constraints_lhs"]["constraint_id"]) == {"C2"}
    assert set(filtered["custom_constraints_rhs"]["constraint_id"]) == {"C2"}


def test_filter_regions_edge_cases(csv_str_to_df):
    """Test edge cases for filter_regions function."""
    # Test with empty sub_regions
    template = {"sub_regions": pd.DataFrame()}
    with pytest.raises(ValueError, match="No sub_regions found"):
        _filter_template(template, nem_regions=["NSW"])

    # Test with both parameters provided
    template = {
        "sub_regions": csv_str_to_df("isp_sub_region_id,nem_region_id\nCNSW,NSW")
    }
    with pytest.raises(ValueError, match="Exactly one of"):
        _filter_template(template, nem_regions=["NSW"], isp_sub_regions=["CNSW"])

    # Test with neither parameter provided
    with pytest.raises(ValueError, match="Exactly one of"):
        _filter_template(template)

    # Test with invalid regions
    template = {
        "sub_regions": csv_str_to_df("""
            isp_sub_region_id,    nem_region_id
            CNSW,                 NSW
        """)
    }
    with pytest.raises(ValueError, match="No sub_regions after filtering"):
        _filter_template(template, nem_regions=["QLD"])  # QLD doesn't exist


def test_filter_regions_with_extra_tables(csv_str_to_df):
    """Test that unhandled tables raise an error."""
    template = {
        "sub_regions": csv_str_to_df("""
            isp_sub_region_id,    nem_region_id
            CNSW,                 NSW
            VIC,                  VIC
        """),
        "ecaa_generators": csv_str_to_df("""
            generator,          sub_region_id,    region_id
            Bayswater,          CNSW,             NSW
        """),
        # Add an unknown table that should raise an error
        "unknown_table": csv_str_to_df("""
            id,    value
            1,     100
            2,     200
        """),
    }

    # Should raise ValueError for unknown table
    with pytest.raises(
        ValueError,
        match="The following tables have no known filtering method: \\['unknown_table'\\]",
    ):
        _filter_template(template, nem_regions=["NSW"])


def test_filter_regions_with_multiple_unknown_tables(csv_str_to_df):
    """Test that multiple unhandled tables are all reported in the error."""
    template = {
        "sub_regions": csv_str_to_df("""
            isp_sub_region_id,    nem_region_id
            CNSW,                 NSW
        """),
        # Add multiple unknown tables
        "unknown_table_1": csv_str_to_df("id,value\n1,100"),
        "unknown_table_2": csv_str_to_df("name,type\ntest,A"),
        "another_unknown": csv_str_to_df("x,y\n1,2"),
    }

    # Should raise ValueError listing all unknown tables
    with pytest.raises(ValueError) as exc_info:
        _filter_template(template, nem_regions=["NSW"])

    error_message = str(exc_info.value)
    assert "another_unknown" in error_message
    assert "unknown_table_1" in error_message
    assert "unknown_table_2" in error_message
    assert "no known filtering method" in error_message


def test_filter_regions_with_no_filtering_tables(csv_str_to_df):
    """Test that tables in the no-filtering list are copied unchanged."""
    template = {
        "sub_regions": csv_str_to_df("""
            isp_sub_region_id,    nem_region_id
            CNSW,                 NSW
            VIC,                  VIC
        """),
        "ecaa_generators": csv_str_to_df("""
            generator,          sub_region_id,    region_id
            Bayswater,          CNSW,             NSW
        """),
        # Add tables that don't need filtering
        "build_costs": csv_str_to_df("""
            technology_type,    FY,         cost_$/kW
            Solar,              2024_25,    1000
            Wind,               2024_25,    1500
        """),
        "new_entrant_build_costs": csv_str_to_df("""
            technology_type,    region,    cost_$/kW
            Battery,            NSW,       500
        """),
    }

    filtered = _filter_template(template, nem_regions=["NSW"])

    # Check that no-filtering tables are copied unchanged
    assert "build_costs" in filtered
    pd.testing.assert_frame_equal(filtered["build_costs"], template["build_costs"])
    assert "new_entrant_build_costs" in filtered
    pd.testing.assert_frame_equal(
        filtered["new_entrant_build_costs"], template["new_entrant_build_costs"]
    )


def test_determine_selected_regions_with_invalid_regions(csv_str_to_df, caplog):
    """Test region determination with some invalid region names."""
    sub_regions_df = csv_str_to_df("""
        isp_sub_region_id,    nem_region_id
        CNSW,                 NSW
        VIC,                  VIC
    """)

    # Test with invalid NEM region
    with caplog.at_level(logging.WARNING):
        selected_sub, selected_nem = _determine_selected_regions(
            sub_regions_df, nem_regions=["NSW", "INVALID"], isp_sub_regions=None
        )

    assert "NEM region 'INVALID' not found" in caplog.text
    assert selected_sub == ["CNSW"]
    assert selected_nem == ["NSW"]

    # Clear the log capture for the next test
    caplog.clear()

    # Test with invalid ISP sub-region
    with caplog.at_level(logging.WARNING):
        selected_sub, selected_nem = _determine_selected_regions(
            sub_regions_df, nem_regions=None, isp_sub_regions=["CNSW", "INVALID"]
        )

    assert "ISP sub-region 'INVALID' not found" in caplog.text
    assert selected_sub == ["CNSW"]
    assert selected_nem == ["NSW"]


def test_get_selected_entities_empty_tables(csv_str_to_df):
    """Test extraction functions with empty tables."""
    # Empty tables
    filtered_tables = {
        "renewable_energy_zones": pd.DataFrame(),
        "flow_paths": pd.DataFrame(),
    }

    rezs = _get_selected_rezs(filtered_tables)
    flow_paths = _get_selected_flow_paths(filtered_tables)

    assert rezs == set()
    assert flow_paths == set()

    # Missing tables
    filtered_tables = {}

    rezs = _get_selected_rezs(filtered_tables)
    flow_paths = _get_selected_flow_paths(filtered_tables)

    assert rezs == set()
    assert flow_paths == set()


def test_filter_custom_constraints_unknown_term_type(csv_str_to_df):
    """Test that unknown term types in custom constraints raise an error."""
    template = {
        "custom_constraints_lhs": csv_str_to_df("""
            constraint_id,    term_type,           term_id,        coefficient
            C1,               generator_output,    Bayswater,      1.0
            C2,               unknown_type,        Something,      1.0
        """),
        "custom_constraints_rhs": csv_str_to_df("""
            constraint_id,    rhs
            C1,               5000
            C2,               1000
        """),
    }

    selected_generators = {"Bayswater"}
    selected_links = set()

    # Should raise ValueError for unknown term type
    with pytest.raises(
        ValueError, match="Cannot filter unknown term types: {'unknown_type'}"
    ):
        _filter_custom_constraints(template, selected_generators, selected_links)


def test_filter_policy_tables_empty_policy_ids(csv_str_to_df):
    """Test filtering policy tables when no matching regions result in empty policy_ids."""
    template = {
        # Policy tables with policy_id columns
        "technology_capacity_targets": csv_str_to_df("""
            region_id,    policy_id,        FY,         capacity_mw
            QLD,          qld_policy,       2024_25,    1000
            QLD,          qld_policy2,      2024_25,    800
        """),
        # policy_generator_types exists
        "policy_generator_types": csv_str_to_df("""
            policy_id,        generator
            qld_policy,       Solar
            qld_policy2,      Wind
            other_policy,     Battery
        """),
    }

    # Filter to NSW only (no QLD data will match)
    filtered, policy_ids = _filter_policy_tables(template, ["NSW"])

    # Should have empty results since NSW has no data
    assert "technology_capacity_targets" in filtered
    assert len(filtered["technology_capacity_targets"]) == 0

    # policy_generator_types should be empty since no policy_ids matched
    assert "policy_generator_types" in filtered
    assert len(filtered["policy_generator_types"]) == 0

    # No policy IDs should have been collected
    assert policy_ids == set()
