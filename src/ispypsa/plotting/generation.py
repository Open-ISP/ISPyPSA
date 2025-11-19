"""Plotting functions for generator dispatch and demand using Plotly."""

from datetime import timedelta

import pandas as pd
import plotly.graph_objects as go

from ispypsa.plotting.style import (
    create_plotly_professional_layout,
    get_carrier_color,
)


def prepare_dispatch_data(
    dispatch: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Prepare generator dispatch data for plotting.

    Aggregates dispatch by carrier type, calculates weekly groupings,
    and maps generators to geographic regions.

    Args:
        dispatch: DataFrame from extract_generator_dispatch() with columns:
            generator, node, carrier, investment_period, timestep, dispatch_mw
        regions_and_zones_mapping: DataFrame mapping buses to regions with columns:
            nem_region_id, isp_sub_region_id, (optionally rez_id)

    Returns:
        DataFrame with columns: node, carrier, investment_period, week_starting,
            timestep, dispatch_mw, nem_region_id
    """
    # Convert timestep to datetime
    dispatch = dispatch.copy()
    dispatch["timestep"] = pd.to_datetime(dispatch["timestep"])

    # Calculate week_starting (Monday of each week)
    dispatch["week_starting"] = (
        (dispatch["timestep"] - timedelta(seconds=1))
        .dt.to_period("W")
        .dt.start_time.dt.date
    )

    # Map node (sub-region) to NEM region
    node_to_region = dict(
        zip(
            regions_and_zones_mapping["isp_sub_region_id"],
            regions_and_zones_mapping["nem_region_id"],
        )
    )

    # Also add REZ mappings if rez_id column exists
    if "rez_id" in regions_and_zones_mapping.columns:
        rez_to_region = dict(
            zip(
                regions_and_zones_mapping["rez_id"].dropna(),
                regions_and_zones_mapping.loc[
                    regions_and_zones_mapping["rez_id"].notna(), "nem_region_id"
                ],
            )
        )
        node_to_region.update(rez_to_region)

    dispatch["nem_region_id"] = dispatch["node"].map(node_to_region)

    return dispatch


def prepare_demand_data(
    demand: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Prepare demand data for plotting.

    Aggregates demand by location, calculates weekly groupings,
    and maps loads to geographic regions.

    Args:
        demand: DataFrame from extract_demand() with columns:
            load, node, investment_period, timestep, demand_mw
        regions_and_zones_mapping: DataFrame mapping buses to regions

    Returns:
        DataFrame with columns: node, investment_period, week_starting,
            timestep, demand_mw, nem_region_id
    """
    # Convert timestep to datetime
    demand = demand.copy()
    demand["timestep"] = pd.to_datetime(demand["timestep"])

    # Calculate week_starting
    demand["week_starting"] = (
        (demand["timestep"] - timedelta(seconds=1))
        .dt.to_period("W")
        .dt.start_time.dt.date
    )

    # Map node to NEM region
    node_to_region = dict(
        zip(
            regions_and_zones_mapping["isp_sub_region_id"],
            regions_and_zones_mapping["nem_region_id"],
        )
    )

    demand["nem_region_id"] = demand["node"].map(node_to_region)

    return demand


def _prepare_transmission_data(
    transmission_flows: pd.DataFrame,
    geography_column: str,
) -> pd.DataFrame:
    """Prepare transmission data with week_starting column.

    Args:
        transmission_flows: Raw transmission flows
        geography_column: Name of geography column (e.g., 'isp_sub_region_id', 'nem_region_id')

    Returns:
        Prepared transmission flows with week_starting and node columns
    """
    transmission_prepared = transmission_flows.copy()
    transmission_prepared["timestep"] = pd.to_datetime(
        transmission_prepared["timestep"]
    )
    transmission_prepared["week_starting"] = (
        (transmission_prepared["timestep"] - timedelta(seconds=1))
        .dt.to_period("W")
        .dt.start_time.dt.date
    )
    # Add node column for consistent filtering
    transmission_prepared["node"] = transmission_prepared[geography_column]
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


def _prepare_week_data(
    dispatch: pd.DataFrame,
    demand: pd.DataFrame,
    transmission_flows: pd.DataFrame,
    node: str,
    investment_period: int,
    week_starting,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.DataFrame]:
    """Prepare data for a single week's plot.

    Returns:
        Tuple of (dispatch_pivot, transmission_pivot, actual_demand, dispatch_by_carrier)
    """
    # Get week dispatch
    week_dispatch = dispatch[
        (dispatch["node"] == node)
        & (dispatch["investment_period"] == investment_period)
        & (dispatch["week_starting"] == week_starting)
    ]

    # Aggregate by carrier
    dispatch_by_carrier = (
        week_dispatch.groupby(["timestep", "carrier"])["dispatch_mw"]
        .sum()
        .reset_index()
    )

    # Pivot dispatch
    dispatch_pivot = dispatch_by_carrier.pivot(
        index="timestep", columns="carrier", values="dispatch_mw"
    ).fillna(0)

    # Get transmission
    week_transmission = transmission_flows[
        (transmission_flows["node"] == node)
        & (transmission_flows["investment_period"] == investment_period)
        & (transmission_flows["week_starting"] == week_starting)
    ]

    transmission_agg = week_transmission.groupby("timestep").agg(
        {"imports_mw": "sum", "exports_mw": "sum"}
    )
    transmission_pivot = pd.DataFrame(
        {
            "Transmission Imports": transmission_agg["imports_mw"],
            "Transmission Exports": transmission_agg["exports_mw"],
        }
    )

    # Get actual demand
    week_demand = demand[
        (demand["node"] == node)
        & (demand["investment_period"] == investment_period)
        & (demand["week_starting"] == week_starting)
    ]
    actual_demand = week_demand.groupby("timestep")["demand_mw"].sum()

    return dispatch_pivot, transmission_pivot, actual_demand, dispatch_by_carrier


def _create_plotly_figure(
    dispatch_pivot: pd.DataFrame,
    transmission_pivot: pd.DataFrame,
    actual_demand: pd.Series,
    title: str,
) -> go.Figure:
    """Create a Plotly figure with generation, transmission, and demand."""
    fig = go.Figure()

    # Add transmission exports to generation and load stacks
    fig.add_trace(
        _create_generation_trace(
            "Transmission Exports Hidden",
            transmission_pivot.index,
            -1 * transmission_pivot["Transmission Exports"],
        )
    )

    fig.add_trace(
        _create_generation_trace(
            "Transmission Imports",
            transmission_pivot.index,
            transmission_pivot["Transmission Imports"],
        )
    )

    # Add generation traces (sorted alphabetically)
    carriers = sorted(dispatch_pivot.columns)
    for carrier in carriers:
        fig.add_trace(
            _create_generation_trace(
                carrier, dispatch_pivot.index, dispatch_pivot[carrier]
            )
        )

    # Add transmission imports to generation stack
    fig.add_trace(
        _create_export_trace(
            transmission_pivot.index, -1 * transmission_pivot["Transmission Exports"]
        )
    )

    fig.add_trace(_create_demand_trace(actual_demand.index, actual_demand.values))

    # Calculate y-axis range
    total_generation = dispatch_pivot.sum(axis=1)
    total_generation = total_generation - transmission_pivot[
        "Transmission Imports"
    ].reindex(dispatch_pivot.index, fill_value=0)
    y_max = max(total_generation.max(), actual_demand.max())
    y_min = -1 * transmission_pivot["Transmission Imports"].min()

    # Apply professional styling
    layout = create_plotly_professional_layout(title=title)
    fig.update_layout(**layout)

    return fig


def plot_sub_regional_dispatch(
    dispatch: pd.DataFrame,
    demand: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
    transmission_flows: pd.DataFrame | None = None,
) -> dict:
    """Plot interactive dispatch charts by sub-region.

    Creates interactive stacked area charts showing dispatch by technology for each
    sub-region and week, with demand overlaid as a line and transmission flows.

    Args:
        dispatch: Generator dispatch data from extract_generator_dispatch().
            Expected columns: generator, node, carrier, investment_period, timestep, dispatch_mw
        demand: Demand data from extract_demand().
            Expected columns: load, node, investment_period, timestep, demand_mw
        regions_and_zones_mapping: Geographic mapping from extract_regions_and_zones_mapping().
            Expected columns: nem_region_id, isp_sub_region_id, rez_id
        transmission_flows: Sub-regional transmission flows from
            extract_isp_sub_region_transmission_flows() (optional).
            Expected columns: isp_sub_region_id, investment_period, timestep, imports_mw, exports_mw, net_imports_mw

    Returns:
        Nested dictionary with structure:
        {
            sub_region: {
                period: {
                    week_starting: {"plot": plotly.Figure, "data": DataFrame}
                }
            }
        }
    """
    # Prepare data
    dispatch_prepared = prepare_dispatch_data(dispatch, regions_and_zones_mapping)
    demand_prepared = prepare_demand_data(demand, regions_and_zones_mapping)

    # Prepare transmission if provided - use empty DataFrame with correct columns if not
    transmission_prepared = pd.DataFrame(
        columns=[
            "node",
            "investment_period",
            "timestep",
            "week_starting",
            "imports_mw",
            "exports_mw",
        ]
    )
    if transmission_flows is not None and not transmission_flows.empty:
        transmission_prepared = _prepare_transmission_data(
            transmission_flows, "isp_sub_region_id"
        )

    plots = {}

    # Group by sub-region, period, and week
    for (node, investment_period, week_starting), _ in dispatch_prepared.groupby(
        ["node", "investment_period", "week_starting"]
    ):
        dispatch_pivot, transmission_pivot, actual_demand, dispatch_by_carrier = (
            _prepare_week_data(
                dispatch_prepared,
                demand_prepared,
                transmission_prepared,
                node,
                investment_period,
                week_starting,
            )
        )

        # Create figure
        title = f"{node} - Week {week_starting} (Investment Period {investment_period})"
        fig = _create_plotly_figure(
            dispatch_pivot, transmission_pivot, actual_demand, title
        )

        # Store in nested dict
        if node not in plots:
            plots[node] = {}
        if str(investment_period) not in plots[node]:
            plots[node][str(investment_period)] = {}

        # Prepare data for CSV
        plot_data = dispatch_by_carrier.copy()
        plot_data["data_type"] = "dispatch"

        plots[node][str(investment_period)][str(week_starting)] = {
            "plot": fig,
            "data": plot_data,
        }

    return plots


def plot_regional_dispatch(
    dispatch: pd.DataFrame,
    demand: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
    transmission_flows: pd.DataFrame | None = None,
) -> dict:
    """Plot interactive dispatch charts by NEM region.

    Creates interactive stacked area charts showing dispatch by technology for each
    region and week, with aggregate demand overlaid as a line and transmission flows.

    Args:
        dispatch: Generator dispatch data from extract_generator_dispatch().
            Expected columns: generator, node, carrier, investment_period, timestep, dispatch_mw
        demand: Demand data from extract_demand().
            Expected columns: load, node, investment_period, timestep, demand_mw
        regions_and_zones_mapping: Geographic mapping from extract_regions_and_zones_mapping().
            Expected columns: nem_region_id, isp_sub_region_id, rez_id
        transmission_flows: Regional transmission flows from
            extract_nem_region_transmission_flows() (optional).
            Expected columns: nem_region_id, investment_period, timestep, imports_mw, exports_mw, net_imports_mw

    Returns:
        Nested dictionary with structure:
        {
            region: {
                period: {
                    week_starting: {"plot": plotly.Figure, "data": DataFrame}
                }
            }
        }
    """
    # Prepare data
    dispatch_prepared = prepare_dispatch_data(dispatch, regions_and_zones_mapping)
    demand_prepared = prepare_demand_data(demand, regions_and_zones_mapping)

    # Prepare transmission if provided - use empty DataFrame with correct columns if not
    transmission_prepared = pd.DataFrame(
        columns=[
            "node",
            "investment_period",
            "timestep",
            "week_starting",
            "imports_mw",
            "exports_mw",
        ]
    )
    if transmission_flows is not None and not transmission_flows.empty:
        transmission_prepared = _prepare_transmission_data(
            transmission_flows, "nem_region_id"
        )

    plots = {}

    # Group by region, period, and week
    for (region, investment_period, week_starting), _ in dispatch_prepared.groupby(
        ["nem_region_id", "investment_period", "week_starting"]
    ):
        if pd.isna(region):
            continue

        # Use nem_region_id as node for regional plots
        dispatch_pivot, transmission_pivot, actual_demand, dispatch_by_carrier = (
            _prepare_week_data(
                dispatch_prepared.assign(node=dispatch_prepared["nem_region_id"]),
                demand_prepared.assign(node=demand_prepared["nem_region_id"]),
                transmission_prepared,
                region,
                investment_period,
                week_starting,
            )
        )

        if dispatch_pivot.empty:
            continue

        # Create figure
        title = f"{region} - Week {week_starting} (Period {investment_period})"
        fig = _create_plotly_figure(
            dispatch_pivot, transmission_pivot, actual_demand, title
        )

        # Store in nested dict
        if region not in plots:
            plots[region] = {}
        if str(investment_period) not in plots[region]:
            plots[region][str(investment_period)] = {}

        # Prepare data for CSV
        plot_data = dispatch_by_carrier.copy()
        plot_data["data_type"] = "dispatch"

        plots[region][str(investment_period)][str(week_starting)] = {
            "plot": fig,
            "data": plot_data,
        }

    return plots
