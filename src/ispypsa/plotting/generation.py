from typing import Literal

import pandas as pd
import plotly.graph_objects as go

from ispypsa.plotting.helpers import _calculate_week_starting
from ispypsa.plotting.style import (
    create_plotly_professional_layout,
    get_fuel_type_color,
)
from ispypsa.results.helpers import _build_node_to_geography_mapping


def prepare_generation_capacity(
    generation_expansion: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame | None = None,
    geography_level: Literal["nem_region_id", "isp_sub_region_id", "rez_id"]
    | None = None,
) -> pd.DataFrame:
    """Prepare cumulative installed generation capacity by geography and fuel type.

    Accounts for generator closures based on closure_year. Generators with
    fuel_type "Unserved Energy" are excluded as these represent model slack
    variables rather than real generation capacity.

    Args:
        generation_expansion: DataFrame from extract_generation_expansion_results() with columns:
            generator, fuel_type, node, capacity_mw, investment_period, closure_year
        regions_and_zones_mapping: DataFrame mapping nodes to regions with columns:
            nem_region_id, isp_sub_region_id, (optionally rez_id). Required if geography_level is specified.
        geography_level: Geographic aggregation level. One of:
            - None: System-wide aggregation (no geographic breakdown)
            - "nem_region_id": Aggregate to NEM regions (e.g., "NSW", "VIC", "QLD")
            - "isp_sub_region_id": Aggregate to ISP sub-regions (e.g., "CNSW", "SNW")
            - "rez_id": Aggregate to Renewable Energy Zones (e.g., "N1", "N2")

    Returns:
        DataFrame with columns: fuel_type, year, capacity_mw (and {geography_level} if specified)
    """
    df = generation_expansion.copy()

    # Filter out Unserved Energy generators
    df = df[df["fuel_type"] != "Unserved Energy"]

    # Map node to geography level if specified
    if geography_level is not None:
        node_to_geo = _build_node_to_geography_mapping(
            regions_and_zones_mapping, geography_level
        )
        df[geography_level] = df["node"].map(node_to_geo)

    # Create year column for each investment period
    years = df["investment_period"].unique()

    # Cross join generators with years, filter to active generators, then aggregate
    df["key"] = 0
    years_df = pd.DataFrame({"year": years, "key": 0})
    expanded = df.merge(years_df, on="key").drop(columns="key")
    active = expanded[
        (expanded["investment_period"] <= expanded["year"])
        & (expanded["closure_year"] > expanded["year"])
    ]

    group_cols = (
        [geography_level, "fuel_type", "year"]
        if geography_level
        else ["fuel_type", "year"]
    )
    return active.groupby(group_cols)["capacity_mw"].sum().reset_index()


def _create_generation_capacity_chart(
    data: pd.DataFrame,
    title: str,
) -> dict:
    """Create a stacked bar chart showing generation capacity by fuel type over years.

    Args:
        data: DataFrame with columns: fuel_type, year, capacity_mw
        title: Chart title

    Returns:
        Dictionary with keys "plot" (Plotly Figure) and "data" (DataFrame)
    """
    fig = go.Figure()

    # Get sorted years for consistent x-axis ordering
    sorted_years = sorted(data["year"].unique())

    # Add a bar trace for each fuel type
    for fuel_type in sorted(data["fuel_type"].unique()):
        fuel_data = data[data["fuel_type"] == fuel_type]
        fig.add_trace(
            go.Bar(
                name=fuel_type,
                x=fuel_data["year"].astype(str),
                y=fuel_data["capacity_mw"],
                marker_color=get_fuel_type_color(fuel_type),
                hovertemplate=f"<b>{fuel_type}</b><br>%{{y:,.0f}} MW<extra></extra>",
            )
        )

    layout = create_plotly_professional_layout(title=title)
    layout["barmode"] = "stack"
    layout["yaxis_title"] = {"text": "Capacity (MW)", "font": {"size": 14}}
    layout["xaxis_title"] = {"text": "Year", "font": {"size": 14}}
    layout["xaxis"]["tickformat"] = None
    layout["xaxis"]["categoryorder"] = "array"
    layout["xaxis"]["categoryarray"] = [str(y) for y in sorted_years]
    fig.update_layout(**layout)

    return {"plot": fig, "data": data}


def plot_generation_capacity_expansion(
    generation_expansion: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
) -> dict:
    """Plot generation capacity expansion as stacked bar charts at all geography levels.

    Creates stacked bar charts showing cumulative installed capacity by fuel type
    for the system overall and for each node at regional, sub-regional, and REZ levels.

    Args:
        generation_expansion: DataFrame from extract_generation_expansion_results() with columns:
            generator, fuel_type, node, capacity_mw, investment_period, closure_year
        regions_and_zones_mapping: DataFrame mapping nodes to regions with columns:
            nem_region_id, isp_sub_region_id, (optionally rez_id)

    Returns:
        Dictionary with structure:
        {
            "aggregate_capacity": {"plot": go.Figure, "data": DataFrame},  # System-wide
            "regional": {<region>: {"plot": go.Figure, "data": DataFrame}, ...},
            "sub_regional": {<sub_region>: {"plot": go.Figure, "data": DataFrame}, ...},
            "rez": {<rez_id>: {"plot": go.Figure, "data": DataFrame}, ...}
        }
    """
    result = {}

    # System-wide plot
    system_data = prepare_generation_capacity(generation_expansion)
    result["aggregate_capacity"] = _create_generation_capacity_chart(
        system_data, "System Generation Capacity"
    )

    # Regional, sub-regional, and REZ plots
    geography_configs = [
        ("regional", "nem_region_id"),
        ("sub_regional", "isp_sub_region_id"),
        ("rez", "rez_id"),
    ]

    for key, geography_level in geography_configs:
        geo_data = prepare_generation_capacity(
            generation_expansion, regions_and_zones_mapping, geography_level
        )
        result[key] = {}
        for node in sorted(geo_data[geography_level].dropna().unique()):
            node_data = geo_data[geo_data[geography_level] == node].drop(
                columns=geography_level
            )
            result[key][node] = _create_generation_capacity_chart(
                node_data, f"{node} Generation Capacity"
            )

    return result


def prepare_dispatch_data(
    dispatch: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame | None = None,
    geography_level: Literal["nem_region_id", "isp_sub_region_id"] | None = None,
) -> pd.DataFrame:
    """Prepare generator dispatch data for plotting.

    Aggregates dispatch by fuel type, calculates weekly groupings,
    and optionally maps generators to geographic regions.

    Args:
        dispatch: DataFrame from extract_generator_dispatch() with columns:
            generator, node, fuel_type, investment_period, timestep, dispatch_mw
        regions_and_zones_mapping: DataFrame mapping buses to regions with columns:
            nem_region_id, isp_sub_region_id, (optionally rez_id). Required if geography_level is specified.
        geography_level: Level of geography to map to. One of:
            - None: System-wide aggregation (no geographic breakdown)
            - "nem_region_id": Aggregate to NEM regions
            - "isp_sub_region_id": Aggregate to ISP sub-regions

    Returns:
        DataFrame with columns: fuel_type, investment_period, week_starting,
            timestep, dispatch_mw (and node if geography_level is specified)
    """
    # Convert timestep to datetime
    dispatch = dispatch.copy()
    dispatch["timestep"] = pd.to_datetime(dispatch["timestep"])

    # Map node to geography level if specified
    if geography_level is not None:
        node_to_region = _build_node_to_geography_mapping(
            regions_and_zones_mapping, geography_level
        )
        dispatch["node"] = dispatch["node"].map(node_to_region)
        group_cols = ["node", "investment_period", "fuel_type", "timestep"]
    else:
        group_cols = ["investment_period", "fuel_type", "timestep"]

    dispatch = (
        dispatch.groupby(group_cols)
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
    regions_and_zones_mapping: pd.DataFrame | None = None,
    geography_level: Literal["nem_region_id", "isp_sub_region_id"] | None = None,
) -> pd.DataFrame:
    """Prepare demand data for plotting.

    Aggregates demand by location, calculates weekly groupings,
    and optionally maps loads to geographic regions.

    Args:
        demand: DataFrame from extract_demand() with columns:
            load, node, investment_period, timestep, demand_mw
        regions_and_zones_mapping: DataFrame mapping buses to regions.
            Required if geography_level is specified.
        geography_level: Level of geography to map to. One of:
            - None: System-wide aggregation (no geographic breakdown)
            - "nem_region_id": Aggregate to NEM regions
            - "isp_sub_region_id": Aggregate to ISP sub-regions

    Returns:
        DataFrame with columns: investment_period, week_starting,
            timestep, demand_mw (and node if geography_level is specified)
    """
    # Convert timestep to datetime
    demand = demand.copy()
    demand["timestep"] = pd.to_datetime(demand["timestep"])

    # Map node to geography level if specified
    if geography_level is not None:
        node_to_region = _build_node_to_geography_mapping(
            regions_and_zones_mapping, geography_level
        )
        demand["node"] = demand["node"].map(node_to_region)
        group_cols = ["node", "investment_period", "timestep"]
    else:
        group_cols = ["investment_period", "timestep"]

    demand = (
        demand.groupby(group_cols)
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
        fillcolor=get_fuel_type_color(carrier),
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
        fillcolor=get_fuel_type_color("Transmission Exports"),
        line=dict(width=0),
        legendgroup="Load",  # Appears in Load legend group
        legendgrouptitle_text="Load",
        visible="legendonly",
        hovertemplate="<b>Transmission Exports</b><br>%{y:.2f} MW<extra></extra>",
    )


def _create_plotly_figure(
    dispatch: pd.DataFrame,
    demand: pd.Series,
    title: str,
    transmission: pd.DataFrame | None = None,
) -> go.Figure:
    """Create a Plotly figure with generation, demand, and optionally transmission."""
    fig = go.Figure()

    # Add transmission traces if provided
    if transmission is not None and not transmission.empty:
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
    fuel_types = sorted(dispatch["fuel_type"].unique())
    for fuel_type in fuel_types:
        fig.add_trace(
            _create_generation_trace(
                fuel_type,
                dispatch["timestep"],
                dispatch[dispatch["fuel_type"] == fuel_type]["dispatch_mw"],
            )
        )

    # Add export trace if transmission provided
    if transmission is not None and not transmission.empty:
        fig.add_trace(
            _create_export_trace(
                transmission["timestep"], -1 * transmission["exports_mw"]
            )
        )

    fig.add_trace(_create_demand_trace(demand["timestep"], demand["demand_mw"]))

    # Apply professional styling
    layout = create_plotly_professional_layout(title=title)
    fig.update_layout(**layout)
    return fig


def plot_dispatch(
    dispatch: pd.DataFrame,
    demand: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame | None = None,
    geography_level: Literal["nem_region_id", "isp_sub_region_id"] | None = None,
    transmission_flows: pd.DataFrame | None = None,
) -> dict:
    """Plot interactive dispatch charts, optionally by geography level.

    Creates interactive stacked area charts showing dispatch by technology for each
    week, with demand overlaid as a line. When geography_level is specified,
    creates separate charts per node with transmission flows.

    Args:
        dispatch: Generator dispatch data from extract_generator_dispatch().
            Expected columns: generator, node, fuel_type, investment_period, timestep, dispatch_mw
        demand: Demand data from extract_demand().
            Expected columns: load, node, investment_period, timestep, demand_mw
        regions_and_zones_mapping: Geographic mapping from extract_regions_and_zones_mapping().
            Required if geography_level is specified.
        geography_level: Level of geography to map to. One of:
            - None: System-wide aggregation (no geographic breakdown)
            - "nem_region_id": Aggregate to NEM regions
            - "isp_sub_region_id": Aggregate to ISP sub-regions
        transmission_flows: Transmission flows. Required if geography_level is specified.
            Expected columns: <geography_level>, investment_period, timestep, imports_mw, exports_mw, net_imports_mw

    Returns:
        Dictionary with structure (when geography_level is None):
        {
            <investment_period>: {
                <week_starting>: {"plot": plotly.Figure, "data": DataFrame}
            }
        }
        Or when geography_level is specified:
        {
            <node>: {
                <investment_period>: {
                    <week_starting>: {"plot": plotly.Figure, "data": DataFrame}
                }
            }
        }
    """
    dispatch_prepared = prepare_dispatch_data(
        dispatch, regions_and_zones_mapping, geography_level
    )
    demand_prepared = prepare_demand_data(
        demand, regions_and_zones_mapping, geography_level
    )

    if geography_level is not None:
        transmission_prepared = _prepare_transmission_data(
            transmission_flows, geography_level
        )
        group_cols = ["node", "investment_period", "week_starting"]
    else:
        group_cols = ["investment_period", "week_starting"]

    plots = {}

    for group_key, dispatch_group in dispatch_prepared.groupby(group_cols):
        if geography_level is not None:
            node, investment_period, week_starting = group_key
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
            title = (
                f"{node} - Week {week_starting} (Investment Period {investment_period})"
            )
        else:
            investment_period, week_starting = group_key
            demand_group = demand_prepared[
                (demand_prepared["investment_period"] == investment_period)
                & (demand_prepared["week_starting"] == week_starting)
            ]
            transmission_group = None
            title = f"System Dispatch - Week {week_starting} (Investment Period {investment_period})"

        fig = _create_plotly_figure(
            dispatch_group, demand_group, title, transmission_group
        )

        plot_data = dispatch_group.copy()
        plot_data["data_type"] = "dispatch"

        if geography_level is not None:
            if node not in plots:
                plots[node] = {}
            if str(investment_period) not in plots[node]:
                plots[node][str(investment_period)] = {}
            plots[node][str(investment_period)][str(week_starting)] = {
                "plot": fig,
                "data": plot_data,
            }
        else:
            if str(investment_period) not in plots:
                plots[str(investment_period)] = {}
            plots[str(investment_period)][str(week_starting)] = {
                "plot": fig,
                "data": plot_data,
            }

    return plots
