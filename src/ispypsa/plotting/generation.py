from typing import Literal

import pandas as pd
import plotly.graph_objects as go

from ispypsa.plotting.helpers import _calculate_week_starting
from ispypsa.plotting.style import (
    create_plotly_professional_layout,
    get_carrier_color,
)
from ispypsa.results.helpers import _build_node_to_geography_mapping


def prepare_dispatch_data(
    dispatch: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
    geography_level: Literal["nem_region_id", "isp_sub_region_id"],
) -> pd.DataFrame:
    """Prepare generator dispatch data for plotting.

    Aggregates dispatch by carrier type, calculates weekly groupings,
    and maps generators to geographic regions.

    Args:
        dispatch: DataFrame from extract_generator_dispatch() with columns:
            generator, node, carrier, investment_period, timestep, dispatch_mw
        regions_and_zones_mapping: DataFrame mapping buses to regions with columns:
            nem_region_id, isp_sub_region_id, (optionally rez_id)
        geography_level: Level of geography to map to (e.g. "nem_region_id", "isp_sub_region_id")

    Returns:
        DataFrame with columns: node, carrier, investment_period, week_starting,
            timestep, dispatch_mw,
    """
    # Convert timestep to datetime
    dispatch = dispatch.copy()
    dispatch["timestep"] = pd.to_datetime(dispatch["timestep"])

    # Map node to geography level
    node_to_region = _build_node_to_geography_mapping(
        regions_and_zones_mapping, geography_level
    )
    dispatch["node"] = dispatch["node"].map(node_to_region)

    dispatch = (
        dispatch.groupby(["node", "investment_period", "carrier", "timestep"])
        .agg(
            {
                "dispatch_mw": "sum",
            }
        )
        .reset_index()
    )

    # Calculate week_starting (Monday of each week)
    dispatch["week_starting"] = _calculate_week_starting(dispatch["timestep"])

    return dispatch


def prepare_demand_data(
    demand: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
    geography_level: Literal["nem_region_id", "isp_sub_region_id"],
) -> pd.DataFrame:
    """Prepare demand data for plotting.

    Aggregates demand by location, calculates weekly groupings,
    and maps loads to geographic regions.

    Args:
        demand: DataFrame from extract_demand() with columns:
            load, node, investment_period, timestep, demand_mw
        regions_and_zones_mapping: DataFrame mapping buses to regions
        geography_level: Level of geography to map to (e.g. "nem_region_id", "isp_sub_region_id")

    Returns:
        DataFrame with columns: node, investment_period, week_starting,
            timestep, demand_mw,
    """
    # Convert timestep to datetime
    demand = demand.copy()
    demand["timestep"] = pd.to_datetime(demand["timestep"])

    # Map node to geography level
    node_to_region = _build_node_to_geography_mapping(
        regions_and_zones_mapping, geography_level
    )
    demand["node"] = demand["node"].map(node_to_region)

    demand = (
        demand.groupby(["node", "investment_period", "timestep"])
        .agg(
            {
                "demand_mw": "sum",
            }
        )
        .reset_index()
    )

    # Calculate week_starting
    demand["week_starting"] = _calculate_week_starting(demand["timestep"])

    return demand


def _prepare_transmission_data(
    transmission_flows: pd.DataFrame,
    geography_level: Literal["nem_region_id", "isp_sub_region_id"],
) -> pd.DataFrame:
    """Prepare transmission data with week_starting column.

    Args:
        transmission_flows: Raw transmission flows
        geography_level: Name of geography column (e.g., 'isp_sub_region_id', 'nem_region_id')

    Returns:
        Prepared transmission flows with week_starting and node columns
    """
    transmission_prepared = transmission_flows.copy()
    transmission_prepared["timestep"] = pd.to_datetime(
        transmission_prepared["timestep"]
    )
    transmission_prepared["week_starting"] = _calculate_week_starting(
        transmission_prepared["timestep"]
    )
    transmission_prepared["node"] = transmission_prepared[geography_level]
    return transmission_prepared


def _create_generation_trace(
    carrier: str, timesteps: pd.DatetimeIndex, values: list
) -> go.Scatter:
    """Create a Plotly scatter trace for generation stacking."""
    return go.Scatter(
        x=timesteps,
        y=values,
        name=carrier,
        mode="lines",
        stackgroup="one",
        fillcolor=get_carrier_color(carrier),
        line=dict(width=0),
        legendgroup="Generation",
        showlegend=carrier != "Transmission Exports",
        legendgrouptitle_text="Generation",
        hovertemplate=f"<b>{carrier}</b><br>%{{y:.2f}} MW<extra></extra>",
    )


def _create_demand_trace(timesteps: pd.DatetimeIndex, values: list) -> go.Scatter:
    """Create a Plotly scatter trace for demand line."""
    return go.Scatter(
        x=timesteps,
        y=values,
        name="Demand",
        mode="lines",
        line=dict(color="black", width=2, dash="dash"),
        legendgroup="Demand",
        legendgrouptitle_text="Demand",
        hovertemplate="<b>Demand</b><br>%{y:.2f} MW<extra></extra>",
    )


def _create_export_trace(timesteps: pd.DatetimeIndex, values: list) -> go.Scatter:
    """Create a Plotly scatter trace for transmission exports (shown as negative)."""
    return go.Scatter(
        x=timesteps,
        y=values,  # Negative to show exports reduce net generation
        name="Transmission Exports",
        mode="lines",
        stackgroup="two",
        fillcolor=get_carrier_color("Transmission Exports"),
        line=dict(width=0),
        legendgroup="Load",  # Appears in Load legend group
        legendgrouptitle_text="Load",
        visible="legendonly",
        hovertemplate="<b>Transmission Exports</b><br>%{y:.2f} MW<extra></extra>",
    )


def _create_plotly_figure(
    dispatch: pd.DataFrame,
    transmission: pd.DataFrame,
    demand: pd.Series,
    title: str,
) -> go.Figure:
    """Create a Plotly figure with generation, transmission, and demand."""
    fig = go.Figure()

    # Add transmission exports to generation and load stacks
    fig.add_trace(
        _create_generation_trace(
            "Transmission Exports Hidden",
            transmission["timestep"],
            -1 * transmission["exports_mw"],
        )
    )

    fig.add_trace(
        _create_generation_trace(
            "Transmission Imports",
            transmission["timestep"],
            transmission["imports_mw"],
        )
    )

    # Add generation traces (sorted alphabetically)
    carriers = sorted(dispatch["carrier"].unique())
    for carrier in carriers:
        fig.add_trace(
            _create_generation_trace(
                carrier,
                dispatch["timestep"],
                dispatch[dispatch["carrier"] == carrier]["dispatch_mw"],
            )
        )

    # Add transmission imports to generation stack
    fig.add_trace(
        _create_export_trace(transmission["timestep"], -1 * transmission["exports_mw"])
    )

    fig.add_trace(_create_demand_trace(demand["timestep"], demand["demand_mw"]))

    # Apply professional styling
    layout = create_plotly_professional_layout(title=title)
    fig.update_layout(**layout)
    return fig


def plot_node_level_dispatch(
    dispatch: pd.DataFrame,
    demand: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
    geography_level: Literal["nem_region_id", "isp_sub_region_id"],
    transmission_flows: pd.DataFrame,
) -> dict:
    """Plot interactive dispatch charts by geography_level.

    Creates interactive stacked area charts showing dispatch by technology for each
    node and week, with demand overlaid as a line and transmission flows.

    Args:
        dispatch: Generator dispatch data from extract_generator_dispatch().
            Expected columns: generator, node, carrier, investment_period, timestep, dispatch_mw
        demand: Demand data from extract_demand().
            Expected columns: load, node, investment_period, timestep, demand_mw
        regions_and_zones_mapping: Geographic mapping from extract_regions_and_zones_mapping().
            Expected columns: nem_region_id, isp_sub_region_id, rez_id
        transmission_flows: Transmission flows from
            Expected columns: <geography_level>, investment_period, timestep, imports_mw, exports_mw, net_imports_mw
        geography_level: Level of geography to map to (e.g. "nem_region_id", "isp_sub_region_id")

    Returns:
        Dictionary with structure:
        {
            node: {"plot": plotly.Figure, "data": DataFrame}
        }
    """
    # Prepare data
    dispatch_prepared = prepare_dispatch_data(
        dispatch, regions_and_zones_mapping, geography_level
    )
    demand_prepared = prepare_demand_data(
        demand, regions_and_zones_mapping, geography_level
    )

    transmission_prepared = _prepare_transmission_data(
        transmission_flows, geography_level
    )

    plots = {}

    # Group by sub-region, period, and week
    for (
        node,
        investment_period,
        week_starting,
    ), dispatch_group in dispatch_prepared.groupby(
        ["node", "investment_period", "week_starting"]
    ):
        demand_group = demand_prepared[
            (demand_prepared["node"] == node)
            & (demand_prepared["investment_period"] == investment_period)
            & (demand_prepared["week_starting"] == week_starting)
        ]

        transmission_group = transmission_prepared[
            (transmission_prepared["node"] == node)
            & (transmission_prepared["investment_period"] == investment_period)
            & (transmission_prepared["week_starting"] == week_starting)
        ]

        # Create figure
        title = f"{node} - Week {week_starting} (Investment Period {investment_period})"
        fig = _create_plotly_figure(
            dispatch_group, transmission_group, demand_group, title
        )

        # Store in nested dict
        if node not in plots:
            plots[node] = {}
        if str(investment_period) not in plots[node]:
            plots[node][str(investment_period)] = {}

        # Prepare data for CSV
        plot_data = dispatch_group.copy()
        plot_data["data_type"] = "dispatch"

        plots[node][str(investment_period)][str(week_starting)] = {
            "plot": fig,
            "data": plot_data,
        }

    return plots
