from datetime import timedelta
from typing import Literal

import pandas as pd
import plotly.graph_objects as go

from ispypsa.helpers import csv_str_to_df
from ispypsa.plotting.style import create_plotly_professional_layout
from ispypsa.plotting.utils import calculate_week_starting
from ispypsa.results.transmission import _build_node_to_geography_mapping


def prepare_transmission_capacity_by_region(
    transmission_expansion: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
    isp_types: list[str],
    aggregate: bool,
    inter_region_capacity_aggregate_method: Literal[
        "split_evenly", "keep_all"
    ] = "keep_all",
) -> pd.DataFrame:
    """Prepare transmission capacity expansion data organized by NEM region.

    Include inter region and intra region capacity. Flow paths connecting two regions appear in both regions' data.

    Examples:
    >>> transmission_expansion = csv_str_to_df('''
    ... isp_name,  isp_type,    investment_period,  node_from,  node_to,  forward_capacity_mw
    ... CNSW-SNSW, flow_path,                2030,       CNSW,     SNSW,     1000
    ... CNSW-SNSW, flow_path,                2031,       CNSW,     SNSW,     2000
    ... CNSW-SNSW, flow_path,                2032,       CNSW,     SNSW,     3000
    ... CNSW-VIC,  flow_path,                2030,       CNSW,     VIC,      1000
    ... CNSW-VIC,  flow_path,                2031,       CNSW,     VIC,      2000
    ... CNSW-VIC,  flow_path,                2032,       CNSW,     VIC,      3000
    ... ''')

    >>> regions_and_zones_mapping = csv_str_to_df('''
    ... nem_region_id,  isp_sub_region_id
    ... NSW,            CNSW
    ... NSW,            SNSW
    ... VIC,            VIC
    ... ''')

    >>> prepare_transmission_capacity_by_region(
    ...     transmission_expansion,
    ...     regions_and_zones_mapping,
    ...     isp_types=["flow_path"],
    ...     aggregate=True,
    ...     inter_region_capacity_aggregate_method="keep_all"
    ... )
      nem_region_id  investment_period  capacity_mw
    0           NSW               2030         2000
    1           NSW               2031         4000
    2           NSW               2032         6000
    3           VIC               2030         1000
    4           VIC               2031         2000
    5           VIC               2032         3000

    Args:
        transmission_expansion: DataFrame with columns: isp_name, isp_type, investment_period,
            node_from, node_to, forward_capacity_mw. This data should be the
            cumulative installed capacity by investment period.
        regions_and_zones_mapping: DataFrame with columns: nem_region_id, isp_sub_region_id
        isp_types: list of isp_types to include in the output
        inter_region_capacity_aggregate_method: method to allocate inter region capacity
            "split_evenly": split the inter region capacity evenly between the two regions.
            "keep_all": keep the inter region capacity in both regions.
        aggregate: whether to aggregate the capacity by region and investment period

    Returns:
        DataFrame with columns: nem_region_id, flow_path, investment_period, capacity_mw
        Contains cumulative capacity by region and investment period.
    """
    # Filter for flow paths only
    paths = transmission_expansion[
        transmission_expansion["isp_type"].isin(isp_types)
    ].copy()

    # Create mapping from node to NEM region
    node_to_region = _build_node_to_geography_mapping(
        regions_and_zones_mapping, "region"
    )

    # Map nodes to regions
    paths["region_from"] = paths["node_from"].map(node_to_region)
    paths["region_to"] = paths["node_to"].map(node_to_region)

    # Change and shorten some names.
    paths = paths.rename(columns={"forward_capacity_mw": "capacity_mw"})

    # Get intra region capacity
    intra_region_paths = paths[paths["region_from"] == paths["region_to"]].copy()
    intra_region_paths["nem_region_id"] = paths["region_from"]

    # Get inter region capacity.
    inter_region_paths = paths[paths["region_to"] != paths["region_from"]].copy()

    # Assign inter region capacity to the from region.
    inter_region_paths_from_regions = inter_region_paths.copy()
    inter_region_paths_from_regions["nem_region_id"] = inter_region_paths["region_from"]

    # Assign inter region capacity to the to region.
    inter_region_paths_to_regions = inter_region_paths.copy()
    inter_region_paths_to_regions["nem_region_id"] = inter_region_paths["region_to"]

    # Apply inter region capacity method.
    if aggregate and inter_region_capacity_aggregate_method == "split_evenly":
        inter_region_paths_from_regions["capacity_mw"] = (
            inter_region_paths_from_regions["capacity_mw"] / 2
        )
        inter_region_paths_to_regions["capacity_mw"] = (
            inter_region_paths_to_regions["capacity_mw"] / 2
        )

    # Combine intra region and inter region capacity.
    flow_df = pd.concat(
        [
            intra_region_paths,
            inter_region_paths_from_regions,
            inter_region_paths_to_regions,
        ]
    )

    if aggregate:
        flow_df = (
            flow_df.groupby(["nem_region_id", "investment_period"])["capacity_mw"]
            .sum()
            .reset_index()
        )
    else:
        flow_df = (
            flow_df.groupby(["nem_region_id", "investment_period", "isp_name"])[
                "capacity_mw"
            ]
            .sum()
            .reset_index()
        )
    return flow_df


def plot_aggregate_transmission_capacity(
    transmission_expansion: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
) -> dict:
    """Plot aggregate transmission capacity by NEM region as a stacked bar chart.

    Args:
        transmission_expansion: DataFrame with transmission expansion data.
            Expected columns: isp_name, isp_type, investment_period, node_from, node_to, forward_capacity_mw
        regions_and_zones_mapping: DataFrame mapping nodes to NEM regions.
            Expected columns: nem_region_id, isp_sub_region_id, rez_id

    Returns:
        A dictionary with keys:
            - "plot": Plotly Figure with stacked bar chart showing cumulative capacity by region and year
            - "data": DataFrame with the underlying data (investment_period, nem_region_id, capacity_gw)
    """
    # Prepare data aggregated by region
    capacity_by_region = prepare_transmission_capacity_by_region(
        transmission_expansion,
        regions_and_zones_mapping,
        isp_types=["flow_path", "rez"],
        aggregate=True,
        inter_region_capacity_aggregate_method="split_evenly",
    )

    fig = go.Figure()

    capacity_by_region = capacity_by_region.sort_values(
        ["nem_region_id", "investment_period"]
    )

    capacity_by_region_grouped = capacity_by_region.groupby("nem_region_id")

    # Add bar for each region
    for region, region_data in capacity_by_region_grouped:
        fig.add_trace(
            go.Bar(
                name=region,
                x=region_data["investment_period"].astype(str),
                y=region_data["capacity_mw"],
                hovertemplate=f"<b>{region}</b><br>%{{y:.2f}} GW<extra></extra>",
            )
        )

    # Apply professional styling
    layout = create_plotly_professional_layout(
        title="Aggregate Transmission Capacity by NEM Region"
    )
    layout["barmode"] = "stack"
    layout["yaxis_title"] = {"text": "Cumulative Capacity (GW)", "font": {"size": 14}}
    layout["xaxis_title"] = {"text": "Build Year", "font": {"size": 14}}
    layout["xaxis"]["tickformat"] = None  # Years are categorical, not datetime
    fig.update_layout(**layout)

    return {"plot": fig, "data": capacity_by_region}


def prepare_flow_data(
    flows: pd.DataFrame,
    transmission_expansion: pd.DataFrame,
) -> pd.DataFrame:
    """Prepare flow data with capacity limits for plotting.

    Args:
        flows: DataFrame with columns: isp_name, investment_period, timestep, flow
        transmission_expansion: DataFrame with columns: isp_name, isp_type, investment_period,
            forward_capacity_mw, reverse_capacity_mw

    Returns:
        DataFrame with columns: isp_type, isp_name, investment_period, week_starting,
            timestep, flow, forward_limit, reverse_limit
    """
    # Prepare the flow data with capacity limits
    # flows = prepare_flow_data(flows, transmission_expansion)
    flows = pd.merge(
        flows,
        transmission_expansion,
        how="left",
        on=["isp_name", "investment_period"],
    )

    # Convert timestep to datetime if it's not already
    flows["timestep"] = pd.to_datetime(flows["timestep"])

    # Calculate week_starting as the Monday of each week
    flows["week_starting"] = calculate_week_starting(flows["timestep"])
    return flows


def plot_flows(
    flows: pd.DataFrame, transmission_expansion: pd.DataFrame
) -> dict[str, dict]:
    """Plot the time varying flows for all transmission lines.

    Plot the flows for each transmission line for each week separately.

    Args:
        flows: The flows dataframe.
            Expected columns: isp_name, investment_period, timestep, flow
        transmission_expansion: The transmission expansion dataframe.
            Expected columns: isp_name, isp_type, investment_period, forward_capacity_mw, reverse_capacity_mw

    Returns:
        A dictionary of plots with the structure:
        {
            isp_type=<flow_path/rez>: {
                isp_name=<isp_name>: {
                    investment_period=<investment_period>: {
                        week_starting=<date of first day of week>: {"plot": plotly.Figure, "data": pd.DataFrame},
                    }
                }
            }

        }
    """
    # Initialize the nested dictionary structure
    plots = {}

    flows = prepare_flow_data(flows, transmission_expansion)

    # Group by isp_type, isp_name, investment_period, and week_starting
    grouped = flows.groupby(
        ["isp_type", "isp_name", "investment_period", "week_starting"]
    )

    for (isp_type, isp_name, investment_period, week_starting), week_data in grouped:
        # Sort by timestep to ensure chronological order
        week_data = week_data.sort_values("timestep")

        # Create Plotly figure
        fig = go.Figure()

        # Add the flow data
        fig.add_trace(
            go.Scatter(
                x=week_data["timestep"],
                y=week_data["flow_mw"],
                name="Flow",
                mode="lines",
                line=dict(color="#1f77b4", width=2),
                hovertemplate="<b>Flow</b><br>%{y:.2f} MW<extra></extra>",
            )
        )

        # Add capacity limit lines (except for rez_no_limit and when limits are not available)
        if isp_type != "rez_no_limit":
            # Get limits from prepared data (same for all rows in this group)
            forward_limit = week_data["forward_capacity_mw"].iloc[0]
            reverse_limit = week_data["reverse_capacity_mw"].iloc[0]

            # Add horizontal lines for capacity limits
            fig.add_trace(
                go.Scatter(
                    x=[
                        week_data["timestep"].iloc[0],
                        week_data["timestep"].iloc[-1],
                    ],
                    y=[forward_limit, forward_limit],
                    name="Forward Limit",
                    mode="lines",
                    line=dict(color="red", width=2, dash="dash"),
                    hovertemplate=f"<b>Forward Limit</b><br>{forward_limit:.2f} MW<extra></extra>",
                )
            )

            fig.add_trace(
                go.Scatter(
                    x=[
                        week_data["timestep"].iloc[0],
                        week_data["timestep"].iloc[-1],
                    ],
                    y=[reverse_limit, reverse_limit],
                    name="Reverse Limit",
                    mode="lines",
                    line=dict(color="orange", width=2, dash="dash"),
                    hovertemplate=f"<b>Reverse Limit</b><br>{reverse_limit:.2f} MW<extra></extra>",
                )
            )

        # Apply professional styling
        layout = create_plotly_professional_layout(
            title=f"{isp_name} - Week {week_starting} (Investment Period {investment_period})"
        )
        layout["yaxis_title"] = {"text": "Flow (MW)", "font": {"size": 14}}
        layout["xaxis_title"] = {"text": "Timestep", "font": {"size": 14}}
        fig.update_layout(**layout)

        # Build the nested dictionary structure
        # Group rez_no_limit under rez
        output_isp_type = "rez" if isp_type == "rez_no_limit" else isp_type

        if output_isp_type not in plots:
            plots[output_isp_type] = {}
        if isp_name not in plots[output_isp_type]:
            plots[output_isp_type][isp_name] = {}
        if investment_period not in plots[output_isp_type][isp_name]:
            plots[output_isp_type][isp_name][str(investment_period)] = {}

        # Store the chart and data with week_starting as string for consistency
        # Keep only relevant columns for the CSV output
        plot_data = week_data[
            ["timestep", "flow_mw", "forward_capacity_mw", "reverse_capacity_mw"]
        ].copy()
        plots[output_isp_type][isp_name][str(investment_period)][str(week_starting)] = {
            "plot": fig,
            "data": plot_data,
        }

    return plots


def _create_entity_capacity_chart(
    region_data: pd.DataFrame,
    region: str,
    chart_type: str,
) -> dict:
    """Create a line chart showing capacity expansion for entities in a region.

    Args:
        region_data: DataFrame filtered for a specific region with columns:
            investment_period, capacity_mw, and entity_column
        region: Region name for chart title
        chart_type: Type of entity for chart title ("REZ" or "Flow Path")

    Returns:
        Dictionary with keys "plot" (Plotly Figure) and "data" (DataFrame)
    """
    fig = go.Figure()

    if not region_data.empty:
        investment_periods = sorted(region_data["investment_period"].unique())

        # Reshape data: years as rows, entities as columns
        pivot_data = region_data.pivot(
            index="investment_period", columns="isp_name", values="capacity_mw"
        )

        # Add each entity as a line series
        for entity_id in sorted(pivot_data.columns):
            fig.add_trace(
                go.Scatter(
                    x=[str(year) for year in investment_periods],
                    y=pivot_data[entity_id].tolist(),
                    name=entity_id,
                    mode="lines+markers",
                    hovertemplate=f"<b>{entity_id}</b><br>%{{y:.2f}} MW<extra></extra>",
                )
            )

    # Apply professional styling
    layout = create_plotly_professional_layout(
        title=f"{region} - {chart_type} Capacity Expansion"
    )
    layout["yaxis_title"] = {"text": "Cumulative Capacity (MW)", "font": {"size": 14}}
    layout["xaxis_title"] = {"text": "Build Year", "font": {"size": 14}}
    layout["xaxis"]["tickformat"] = None  # Years are categorical
    fig.update_layout(**layout)

    return {"plot": fig, "data": region_data}


def plot_regional_capacity_expansion(
    transmission_expansion: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
) -> dict:
    """Plot REZ and flow path capacity expansion for each NEM region.

    Creates two line charts for each region:
    - REZ capacity expansion (one line per REZ)
    - Flow path capacity expansion (one line per flow path)

    Args:
        transmission_expansion: DataFrame with transmission expansion data.
            Expected columns: isp_name, isp_type, investment_period, node_from, node_to, forward_capacity_mw
        regions_and_zones_mapping: DataFrame mapping nodes to NEM regions.
            Expected columns: nem_region_id, isp_sub_region_id, rez_id

    Returns:
        Dictionary with structure:
        {
            region_name: {
                "rez_capacity": {"plot": plotly.Figure, "data": DataFrame},
                "flow_path_capacity": {"plot": plotly.Figure, "data": DataFrame}
            }
        }
    """
    # Prepare data
    rez_data = prepare_transmission_capacity_by_region(
        transmission_expansion,
        regions_and_zones_mapping,
        isp_types=["rez"],
        aggregate=False,
    )
    flow_path_data = prepare_transmission_capacity_by_region(
        transmission_expansion,
        regions_and_zones_mapping,
        isp_types=["flow_path"],
        aggregate=False,
    )

    plots = {}

    # Get all regions from both datasets
    regions = set()
    if not rez_data.empty:
        regions.update(rez_data["nem_region_id"].unique())
    if not flow_path_data.empty:
        regions.update(flow_path_data["nem_region_id"].unique())

    for region in sorted(regions):
        # Filter data for this region
        region_rez_data = (
            rez_data[rez_data["nem_region_id"] == region]
            if not rez_data.empty
            else rez_data
        )
        region_flow_data = (
            flow_path_data[flow_path_data["nem_region_id"] == region]
            if not flow_path_data.empty
            else flow_path_data
        )

        plots[region] = {
            "rez_capacity": _create_entity_capacity_chart(
                region_rez_data, region, "REZ"
            ),
            "flow_path_capacity": _create_entity_capacity_chart(
                region_flow_data, region, "Flow Path"
            ),
        }

    return plots
