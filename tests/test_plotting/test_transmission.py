import pandas as pd
import plotly.graph_objects as go

from ispypsa.plotting.transmission import (
    plot_aggregate_transmission_capacity,
    plot_flows,
    plot_regional_capacity_expansion,
    prepare_flow_data,
    prepare_transmission_capacity_by_region,
)


def test_prepare_transmission_capacity_by_region(csv_str_to_df):
    """
    Test transmission capacity preparation with different aggregation methods.

    Data Setup:
    - Link1: Intra-region (N1->N2, both in R1), 100 MW in 2030
    - Link2: Inter-region (N1->N3, R1->R2), 200 MW in 2030, 400 MW in 2040
    - Link3: Other type (should be filtered out), 50 MW
    """

    # Shared Input Data
    transmission_expansion_csv = """
    isp_name, isp_type,  investment_period, node_from, node_to, forward_capacity_mw
    Link1,    flow_path, 2030,              N1,        N2,      100
    Link2,    flow_path, 2030,              N1,        N3,      200
    Link2,    flow_path, 2040,              N1,        N3,      400
    Link3,    other,     2030,              N1,        N2,      50
    """

    regions_mapping_csv = """
    nem_region_id, isp_sub_region_id
    R1,            N1
    R1,            N2
    R2,            N3
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    # Case 1: Aggregate True, Split Evenly
    # 2030:
    #   R1: Link1 (100) + Link2 (200/2) = 200
    #   R2: Link2 (200/2) = 100
    # 2040:
    #   R1: Link2 (400/2) = 200
    #   R2: Link2 (400/2) = 200
    result_split = prepare_transmission_capacity_by_region(
        transmission_expansion,
        regions_mapping,
        isp_types=["flow_path"],
        aggregate=True,
        inter_region_capacity_aggregate_method="split_evenly",
    )

    expected_split = csv_str_to_df("""
    nem_region_id, investment_period, capacity_mw
    R1,            2030,              200
    R1,            2040,              200
    R2,            2030,              100
    R2,            2040,              200
    """)

    pd.testing.assert_frame_equal(
        result_split.sort_values(["nem_region_id", "investment_period"]).reset_index(
            drop=True
        ),
        expected_split.sort_values(["nem_region_id", "investment_period"]).reset_index(
            drop=True
        ),
        check_dtype=False,
    )

    # Case 2: Aggregate True, Keep All
    # 2030:
    #   R1: Link1 (100) + Link2 (200) = 300
    #   R2: Link2 (200) = 200
    # 2040:
    #   R1: Link2 (400) = 400
    #   R2: Link2 (400) = 400
    result_keep = prepare_transmission_capacity_by_region(
        transmission_expansion,
        regions_mapping,
        isp_types=["flow_path"],
        aggregate=True,
        inter_region_capacity_aggregate_method="keep_all",
    )

    expected_keep = csv_str_to_df("""
    nem_region_id, investment_period, capacity_mw
    R1,            2030,              300
    R1,            2040,              400
    R2,            2030,              200
    R2,            2040,              400
    """)

    pd.testing.assert_frame_equal(
        result_keep.sort_values(["nem_region_id", "investment_period"]).reset_index(
            drop=True
        ),
        expected_keep.sort_values(["nem_region_id", "investment_period"]).reset_index(
            drop=True
        ),
        check_dtype=False,
    )

    # Case 3: Aggregate False
    # Returns rows for each flow path in each region
    # 2030:
    #   R1: Link1 (100), Link2 (200)
    #   R2: Link2 (200)
    # 2040:
    #   R1: Link2 (400)
    #   R2: Link2 (400)
    result_no_agg = prepare_transmission_capacity_by_region(
        transmission_expansion,
        regions_mapping,
        isp_types=["flow_path"],
        aggregate=False,
        inter_region_capacity_aggregate_method="keep_all",
    )

    expected_no_agg = csv_str_to_df("""
    nem_region_id, investment_period, isp_name, capacity_mw
    R1,            2030,              Link1,    100
    R1,            2030,              Link2,    200
    R1,            2040,              Link2,    400
    R2,            2030,              Link2,    200
    R2,            2040,              Link2,    400
    """)

    pd.testing.assert_frame_equal(
        result_no_agg.sort_values(
            ["nem_region_id", "investment_period", "isp_name"]
        ).reset_index(drop=True),
        expected_no_agg.sort_values(
            ["nem_region_id", "investment_period", "isp_name"]
        ).reset_index(drop=True),
        check_dtype=False,
    )


def test_prepare_transmission_capacity_by_region_rez(csv_str_to_df):
    """
    Test basic REZ capacity preparation.

    REZ nodes (e.g., REZ1) are typically connected to a sub-region node (e.g., N1).
    If both are in the same region (R1), it counts as intra-region capacity.
    """

    transmission_expansion_csv = """
    isp_name, isp_type, investment_period, node_from, node_to, forward_capacity_mw
    REZ1,     rez,      2030,              REZ1_Node, N1,      500
    """

    regions_mapping_csv = """
    nem_region_id, isp_sub_region_id
    R1,            N1
    R1,            REZ1_Node
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    # Case: Basic REZ extraction
    result = prepare_transmission_capacity_by_region(
        transmission_expansion,
        regions_mapping,
        isp_types=["rez"],
        aggregate=False,  # Typically False for REZ charts as we want per-REZ data
    )

    expected = csv_str_to_df("""
    nem_region_id, investment_period, isp_name, capacity_mw
    R1,            2030,              REZ1,     500
    """)

    pd.testing.assert_frame_equal(
        result.sort_values(["nem_region_id", "isp_name"]).reset_index(drop=True),
        expected.sort_values(["nem_region_id", "isp_name"]).reset_index(drop=True),
        check_dtype=False,
    )


def test_plot_aggregate_transmission_capacity(csv_str_to_df):
    """
    Test the high-level plotting function.

    Ensures that:
    1. Data is correctly prepared and returned.
    2. A Plotly figure object is created.
    """

    transmission_expansion_csv = """
    isp_name, isp_type,  investment_period, node_from, node_to, forward_capacity_mw
    Link1,    flow_path, 2030,              N1,        N2,      100
    Link2,    flow_path, 2030,              N1,        N3,      200
    """

    regions_mapping_csv = """
    nem_region_id, isp_sub_region_id
    R1,            N1
    R1,            N2
    R2,            N3
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    result = plot_aggregate_transmission_capacity(
        transmission_expansion, regions_mapping
    )

    # Check 1: Return structure
    assert "plot" in result
    assert "data" in result
    assert isinstance(result["plot"], go.Figure)
    assert isinstance(result["data"], pd.DataFrame)

    # Check 2: Data correctness (uses split_evenly by default)
    # R1: Link1 (100) + Link2 (200/2) = 200
    # R2: Link2 (200/2) = 100
    expected_data = csv_str_to_df("""
    nem_region_id, investment_period, capacity_mw
    R1,            2030,              200
    R2,            2030,              100
    """)

    pd.testing.assert_frame_equal(
        result["data"].sort_values("nem_region_id").reset_index(drop=True),
        expected_data.sort_values("nem_region_id").reset_index(drop=True),
        check_dtype=False,
    )

    # Check 3: Plot traces exist (one per region)
    assert len(result["plot"].data) == 2
    trace_names = {trace.name for trace in result["plot"].data}
    assert trace_names == {"R1", "R2"}


def test_prepare_flow_data(csv_str_to_df):
    """
    Test prepare_flow_data function logic: merge, timestep conversion, week_starting calculation.
    """

    # Input Flows
    flows_csv = """
    isp_name, investment_period, timestep,            flow
    Link1,    2030,              2030-01-01 00:00:00, 100
    Link1,    2030,              2030-01-08 00:00:00, 150
    Link1,    2040,              2040-01-01 00:00:00, 200
    Link2,    2030,              2030-01-01 00:00:00, 50
    """

    # Input Transmission Expansion
    transmission_expansion_csv = """
    isp_name, isp_type,  investment_period, forward_capacity_mw, reverse_capacity_mw
    Link1,    flow_path, 2030,              500,                 450
    Link1,    flow_path, 2040,              600,                 550
    """

    flows = csv_str_to_df(flows_csv)
    transmission_expansion = csv_str_to_df(transmission_expansion_csv)

    # Execute
    result = prepare_flow_data(flows, transmission_expansion)

    # Expected Output
    # week_starting logic: 2030-01-01 (Tue) -> 2029-12-31 (Mon)
    #                      2030-01-08 (Tue) -> 2030-01-07 (Mon)
    #                      2040-01-01 (Sun) -> 2039-12-26 (Mon)
    expected_csv = """
    isp_name, investment_period, timestep,            flow, isp_type,  forward_capacity_mw, reverse_capacity_mw, week_starting
    Link1,    2030,              2030-01-01 00:00:00, 100,  flow_path, 500,                 450,                 2029-12-31
    Link1,    2030,              2030-01-08 00:00:00, 150,  flow_path, 500,                 450,                 2030-01-07
    Link1,    2040,              2040-01-01 00:00:00, 200,  flow_path, 600,                 550,                 2039-12-26
    Link2,    2030,              2030-01-01 00:00:00, 50,   nan,       nan,                 nan,                 2029-12-31
    """
    expected = csv_str_to_df(expected_csv)

    # Type conversions for expected data to match result
    expected["timestep"] = pd.to_datetime(expected["timestep"])
    expected["week_starting"] = pd.to_datetime(expected["week_starting"]).dt.date

    # Sort both to ensure consistent order for comparison
    result = result.sort_values(["isp_name", "timestep"]).reset_index(drop=True)
    expected = expected.sort_values(["isp_name", "timestep"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected, check_dtype=False, check_like=True)


def test_plot_flows(csv_str_to_df):
    """
    Test the plot_flows function.

    Verifies:
    1. Correct structure of the returned dictionary.
    2. Correct data content in the returned dictionary.
    3. Correct Plotly figure creation.
    4. Grouping of 'rez_no_limit' under 'rez'.
    """

    # Input Flows
    # Note: Function expects 'flow_mw' column for plotting, although prepare_flow_data docstring mentions 'flow'
    flows_csv = """
    isp_name, investment_period, timestep,            flow_mw
    Link1,    2030,              2030-01-01 00:00:00, 100
    Link1,    2030,              2030-01-02 00:00:00, 150
    REZ1,     2030,              2030-01-01 00:00:00, 50
    REZ2,     2030,              2030-01-01 00:00:00, 75
    """

    # Input Transmission Expansion
    transmission_expansion_csv = """
    isp_name, isp_type,     investment_period, forward_capacity_mw, reverse_capacity_mw
    Link1,    flow_path,    2030,              500,                 450
    REZ1,     rez,          2030,              200,                 0
    REZ2,     rez_no_limit, 2030,              0,                   0
    """

    flows = csv_str_to_df(flows_csv)
    transmission_expansion = csv_str_to_df(transmission_expansion_csv)

    result = plot_flows(flows, transmission_expansion)

    # Structure check
    # Link1 2030-01-01 is Tuesday, so week starting is Monday 2029-12-31
    assert "flow_path" in result
    assert "Link1" in result["flow_path"]

    # Check that 'rez' key exists and contains both REZ types
    assert "rez" in result
    assert "rez_no_limit" not in result  # Should be merged into 'rez'

    assert "REZ1" in result["rez"]
    assert "REZ2" in result["rez"]

    # Verify Link1 data
    week_key = "2029-12-31"
    plot_entry_link = result["flow_path"]["Link1"]["2030"][week_key]
    assert isinstance(plot_entry_link["plot"], go.Figure)

    expected_data_link = csv_str_to_df("""
    timestep,            flow_mw, forward_capacity_mw, reverse_capacity_mw
    2030-01-01 00:00:00, 100,     500,                 450
    2030-01-02 00:00:00, 150,     500,                 450
    """)
    expected_data_link["timestep"] = pd.to_datetime(expected_data_link["timestep"])

    pd.testing.assert_frame_equal(
        plot_entry_link["data"].reset_index(drop=True),
        expected_data_link.reset_index(drop=True),
        check_dtype=False,
    )

    # Verify REZ2 (rez_no_limit) data
    plot_entry_rez2 = result["rez"]["REZ2"]["2030"][week_key]
    expected_data_rez2 = csv_str_to_df("""
    timestep,            flow_mw, forward_capacity_mw, reverse_capacity_mw
    2030-01-01 00:00:00, 75,      0,                   0
    """)
    expected_data_rez2["timestep"] = pd.to_datetime(expected_data_rez2["timestep"])

    pd.testing.assert_frame_equal(
        plot_entry_rez2["data"].reset_index(drop=True),
        expected_data_rez2.reset_index(drop=True),
        check_dtype=False,
    )


def test_plot_regional_capacity_expansion(csv_str_to_df):
    """
    Test plot_regional_capacity_expansion.

    Verifies:
    1. Dictionary structure (by region, then by entity type).
    2. Separation of REZ and Flow Path data.
    3. Data content for intra-region and inter-region flows.
    """

    transmission_expansion_csv = """
    isp_name, isp_type,  investment_period, node_from, node_to,   forward_capacity_mw
    Link1,    flow_path, 2030,              N1,        N2,        100
    Link2,    flow_path, 2030,              N1,        N3,        200
    REZ1,     rez,       2030,              REZ1_Node, N1,        50
    """

    regions_mapping_csv = """
    nem_region_id, isp_sub_region_id
    R1,            N1
    R1,            N2
    R1,            REZ1_Node
    R2,            N3
    """

    transmission_expansion = csv_str_to_df(transmission_expansion_csv)
    regions_mapping = csv_str_to_df(regions_mapping_csv)

    result = plot_regional_capacity_expansion(transmission_expansion, regions_mapping)

    # Check R1 Data
    assert "R1" in result
    r1_rez = result["R1"]["rez_capacity"]
    r1_flow = result["R1"]["flow_path_capacity"]

    # R1 REZ should contain REZ1
    expected_r1_rez = csv_str_to_df("""
    nem_region_id, investment_period, isp_name, capacity_mw
    R1,            2030,              REZ1,     50
    """)
    pd.testing.assert_frame_equal(
        r1_rez["data"].sort_values("isp_name").reset_index(drop=True),
        expected_r1_rez.sort_values("isp_name").reset_index(drop=True),
        check_dtype=False,
    )

    # R1 Flow Path should contain Link1 (intra) and Link2 (inter-from)
    expected_r1_flow = csv_str_to_df("""
    nem_region_id, investment_period, isp_name, capacity_mw
    R1,            2030,              Link1,    100
    R1,            2030,              Link2,    200
    """)
    pd.testing.assert_frame_equal(
        r1_flow["data"].sort_values("isp_name").reset_index(drop=True),
        expected_r1_flow.sort_values("isp_name").reset_index(drop=True),
        check_dtype=False,
    )

    # Check R2 Data
    assert "R2" in result
    r2_flow = result["R2"]["flow_path_capacity"]

    # R2 Flow Path should contain Link2 (inter-to)
    expected_r2_flow = csv_str_to_df("""
    nem_region_id, investment_period, isp_name, capacity_mw
    R2,            2030,              Link2,    200
    """)
    pd.testing.assert_frame_equal(
        r2_flow["data"].reset_index(drop=True),
        expected_r2_flow.reset_index(drop=True),
        check_dtype=False,
    )

    # Check Plot objects exist
    assert isinstance(r1_rez["plot"], go.Figure)
    assert isinstance(r1_flow["plot"], go.Figure)
