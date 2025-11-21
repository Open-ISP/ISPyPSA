from datetime import timedelta

import pandas as pd
import plotly.graph_objects as go

from ispypsa.plotting.style import create_plotly_professional_layout


def prepare_transmission_capacity_by_region(
    transmission_expansion: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Prepare transmission capacity data aggregated by NEM region.

    This function processes transmission expansion data and allocates capacity to NEM regions.
    For transmission lines connecting two different regions, capacity is split 50/50 between them.

    Args:
        transmission_expansion: DataFrame with columns: isp_name, isp_type, build_year,
            node_from, node_to, forward_direction_nominal_capacity_mw
        regions_and_zones_mapping: DataFrame with columns: nem_region_id, isp_sub_region_id,
            (and optionally rez_id)

    Returns:
        DataFrame with columns: build_year, nem_region_id, capacity_gw
        Contains cumulative capacity by region and year in GW.
    """
    # Filter out rez_no_limit
    df = transmission_expansion[
        transmission_expansion["isp_type"] != "rez_no_limit"
    ].copy()

    if df.empty:
        return pd.DataFrame(columns=["build_year", "nem_region_id", "capacity_gw"])

    # Create mapping from node to NEM region
    # Use isp_sub_region_id as the node identifier
    node_to_region = dict(
        zip(
            regions_and_zones_mapping["isp_sub_region_id"],
            regions_and_zones_mapping["nem_region_id"],
        )
    )

    # Also add REZ mappings if rez_id column exists
    if "rez_id" in regions_and_zones_mapping.columns:
        rez_rows = regions_and_zones_mapping.dropna(subset=["rez_id"])
        node_to_region.update(dict(zip(rez_rows["rez_id"], rez_rows["nem_region_id"])))

    # Map nodes to regions
    region_from = df["node_from"].map(node_to_region)
    region_to = df["node_to"].map(node_to_region)

    # Vectorized allocation: Split every line into two halves.
    # - Inter-region (A->B): A gets 50%, B gets 50%.
    # - Intra-region (A->A): A gets 50% + 50% = 100%.
    half_capacity = df["forward_direction_nominal_capacity_mw"] / 2.0

    allocations = pd.concat(
        [
            pd.DataFrame(
                {"region": region_from, "year": df["build_year"], "cap": half_capacity}
            ),
            pd.DataFrame(
                {"region": region_to, "year": df["build_year"], "cap": half_capacity}
            ),
        ]
    )

    # Group by build_year and region, sum capacities
    capacity_by_region = (
        allocations.groupby(["year", "region"])["cap"]
        .sum()
        .reset_index()
        .sort_values(["region", "year"])
    )

    # Calculate cumulative sum (GW)
    capacity_by_region["capacity_gw"] = (
        capacity_by_region.groupby("region")["cap"].cumsum() / 1000
    )

    return capacity_by_region.rename(
        columns={"year": "build_year", "region": "nem_region_id"}
    )[["build_year", "nem_region_id", "capacity_gw"]]


def prepare_rez_capacity_by_region(
    transmission_expansion: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Prepare REZ capacity expansion data organized by NEM region.

    Args:
        transmission_expansion: DataFrame with columns: isp_name, isp_type, build_year,
            node_from, node_to, forward_direction_nominal_capacity_mw
        regions_and_zones_mapping: DataFrame with columns: nem_region_id, isp_sub_region_id, rez_id

    Returns:
        DataFrame with columns: nem_region_id, rez_id, build_year, capacity_mw
        Contains cumulative capacity by REZ and year.
    """
    # Filter for REZ connections only (exclude rez_no_limit)
    rez_expansion = transmission_expansion[
        (transmission_expansion["isp_type"] != "rez_no_limit")
    ].copy()

    if rez_expansion.empty:
        return pd.DataFrame(
            columns=["nem_region_id", "rez_id", "build_year", "capacity_mw"]
        )

    # Create mapping from REZ to NEM region
    if "rez_id" not in regions_and_zones_mapping.columns:
        return pd.DataFrame(
            columns=["nem_region_id", "rez_id", "build_year", "capacity_mw"]
        )

    rez_to_region = dict(
        zip(
            regions_and_zones_mapping["rez_id"].dropna(),
            regions_and_zones_mapping.loc[
                regions_and_zones_mapping["rez_id"].notna(), "nem_region_id"
            ],
        )
    )

    # Map REZs to regions - check both node_from and node_to
    rez_expansion["rez_id_from"] = rez_expansion["node_from"].map(rez_to_region).notna()
    rez_expansion["rez_id_to"] = rez_expansion["node_to"].map(rez_to_region).notna()

    # Identify which node is the REZ
    rez_expansion["rez_id"] = rez_expansion.apply(
        lambda row: row["node_from"]
        if row["node_from"] in rez_to_region
        else row["node_to"],
        axis=1,
    )
    rez_expansion["nem_region_id"] = rez_expansion["rez_id"].map(rez_to_region)

    # Select and rename columns
    rez_data = rez_expansion[
        [
            "nem_region_id",
            "rez_id",
            "build_year",
            "forward_direction_nominal_capacity_mw",
        ]
    ].copy()
    rez_data = rez_data.rename(
        columns={"forward_direction_nominal_capacity_mw": "capacity_mw"}
    )

    # Remove rows with NaN region (shouldn't happen but safety check)
    rez_data = rez_data.dropna(subset=["nem_region_id"])

    # Calculate cumulative sum for each REZ
    rez_data = rez_data.sort_values(["nem_region_id", "rez_id", "build_year"])
    rez_data["capacity_mw"] = rez_data.groupby(["nem_region_id", "rez_id"])[
        "capacity_mw"
    ].cumsum()

    return rez_data


def prepare_flow_path_capacity_by_region(
    transmission_expansion: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Prepare flow path capacity expansion data organized by NEM region.

    Flow paths connecting two regions appear in both regions' data.

    Args:
        transmission_expansion: DataFrame with columns: isp_name, isp_type, build_year,
            node_from, node_to, forward_direction_nominal_capacity_mw
        regions_and_zones_mapping: DataFrame with columns: nem_region_id, isp_sub_region_id

    Returns:
        DataFrame with columns: nem_region_id, flow_path, build_year, capacity_mw
        Contains cumulative capacity by flow path and year.
    """
    # Filter for flow paths only
    paths = transmission_expansion[
        transmission_expansion["isp_type"] == "flow_path"
    ].copy()

    if paths.empty:
        return pd.DataFrame(
            columns=["nem_region_id", "flow_path", "build_year", "capacity_mw"]
        )

    # Create mapping from node to NEM region
    node_to_region = dict(
        zip(
            regions_and_zones_mapping["isp_sub_region_id"],
            regions_and_zones_mapping["nem_region_id"],
        )
    )

    # Map nodes to regions
    region_from = paths["node_from"].map(node_to_region)
    region_to = paths["node_to"].map(node_to_region)

    # Create two views of the data: one for the 'from' side, one for the 'to' side.
    # We only want to keep 'to' if it's a different region.
    # This effectively "melts" the paths so each region gets a row for the paths connected to it.
    base_data = pd.DataFrame(
        {
            "flow_path": paths["isp_name"],
            "build_year": paths["build_year"],
            "capacity_mw": paths["forward_direction_nominal_capacity_mw"],
        }
    )

    from_df = base_data.copy()
    from_df["nem_region_id"] = region_from

    to_df = base_data.copy()
    to_df["nem_region_id"] = region_to

    # Filter:
    # 1. Keep valid regions (not NaN)
    # 2. For to_df, keep only if region_to != region_from
    #    (If region_to == region_from, it's an intra-region path and already covered by from_df)
    from_df = from_df.dropna(subset=["nem_region_id"])

    # We need to compare region_to vs region_from safely.
    # Since we just created them from the same df, indices align.
    to_df = to_df[
        to_df["nem_region_id"].notna() & (to_df["nem_region_id"] != region_from)
    ]

    flow_df = pd.concat([from_df, to_df])

    if flow_df.empty:
        return pd.DataFrame(
            columns=["nem_region_id", "flow_path", "build_year", "capacity_mw"]
        )

    # Calculate cumulative sum for each flow path within each region
    flow_df = flow_df.sort_values(["nem_region_id", "flow_path", "build_year"])
    flow_df["capacity_mw"] = flow_df.groupby(["nem_region_id", "flow_path"])[
        "capacity_mw"
    ].cumsum()

    return flow_df.loc[:, ["nem_region_id", "flow_path", "build_year", "capacity_mw"]]


def plot_aggregate_transmission_capacity(
    transmission_expansion: pd.DataFrame,
    regions_and_zones_mapping: pd.DataFrame,
) -> dict:
    """Plot aggregate transmission capacity by NEM region as a stacked bar chart.

    Args:
        transmission_expansion: DataFrame with transmission expansion data.
            Expected columns: isp_name, isp_type, build_year, node_from, node_to, forward_direction_nominal_capacity_mw
        regions_and_zones_mapping: DataFrame mapping nodes to NEM regions.
            Expected columns: nem_region_id, isp_sub_region_id, rez_id

    Returns:
        A dictionary with keys:
            - "plot": Plotly Figure with stacked bar chart showing cumulative capacity by region and year
            - "data": DataFrame with the underlying data (build_year, nem_region_id, capacity_gw)
    """
    # Prepare data aggregated by region
    capacity_by_region = prepare_transmission_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )

    fig = go.Figure()

    if capacity_by_region.empty:
        layout = create_plotly_professional_layout(
            title="Aggregate Transmission Capacity by NEM Region"
        )
        layout["yaxis_title"] = {
            "text": "Cumulative Capacity (GW)",
            "font": {"size": 14},
        }
        layout["xaxis_title"] = {"text": "Build Year", "font": {"size": 14}}
        fig.update_layout(**layout)
        return {"plot": fig, "data": capacity_by_region}

    # Get all build years and regions
    build_years = sorted(capacity_by_region["build_year"].unique())
    regions = sorted(capacity_by_region["nem_region_id"].unique())

    # Add bar for each region
    for region in regions:
        region_data = capacity_by_region[capacity_by_region["nem_region_id"] == region]

        # Create a list with values for each build year
        values = []
        for year in build_years:
            year_data = region_data[region_data["build_year"] == year]
            if not year_data.empty:
                values.append(float(year_data["capacity_gw"].iloc[0]))
            else:
                # Use previous cumulative value if no expansion in this year
                if values:
                    values.append(values[-1])
                else:
                    values.append(0)

        fig.add_trace(
            go.Bar(
                name=region,
                x=[str(year) for year in build_years],
                y=values,
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
        transmission_expansion: DataFrame with columns: isp_name, isp_type, build_year,
            forward_direction_nominal_capacity_mw, reverse_direction_nominal_capacity_mw

    Returns:
        DataFrame with columns: isp_type, isp_name, investment_period, week_starting,
            timestep, flow, forward_limit, reverse_limit
    """
    # Handle empty input
    if flows.empty:
        return flows.copy()

    flows = flows.copy()

    # Handle case when transmission_expansion is empty (e.g., operational models)
    if transmission_expansion.empty:
        # Set isp_type to "unknown" for all flows when no expansion data
        flows["isp_type"] = "unknown"
    else:
        # Get unique isp_name to isp_type mapping from transmission_expansion
        isp_type_mapping = (
            transmission_expansion[["isp_name", "isp_type"]]
            .drop_duplicates()
            .set_index("isp_name")["isp_type"]
            .to_dict()
        )

        # Merge flows with isp_type
        flows["isp_type"] = flows["isp_name"].map(isp_type_mapping)

    # Convert timestep to datetime if it's not already
    flows["timestep"] = pd.to_datetime(flows["timestep"])

    # Calculate week_starting as the Monday of each week
    flows["week_starting"] = (
        (flows["timestep"] - timedelta(seconds=1))
        .dt.to_period("W")
        .dt.start_time.dt.date
    )

    # Calculate capacity limits for each isp_name and investment_period
    # Skip if transmission_expansion is empty (e.g., operational models with no expansion data)
    if transmission_expansion.empty:
        # For operational models, set limits to NaN (will be handled in plotting)
        flows["forward_limit"] = pd.NA
        flows["reverse_limit"] = pd.NA
    else:
        capacity_limits = []

        for isp_name in flows["isp_name"].unique():
            for investment_period in flows["investment_period"].unique():
                # Get cumulative capacity for this line up to and including investment_period
                capacity_data = transmission_expansion[
                    (transmission_expansion["isp_name"] == isp_name)
                    & (transmission_expansion["build_year"] <= investment_period)
                ]

                if not capacity_data.empty:
                    forward_limit = capacity_data[
                        "forward_direction_nominal_capacity_mw"
                    ].sum()
                    reverse_limit = capacity_data[
                        "reverse_direction_nominal_capacity_mw"
                    ].sum()
                else:
                    forward_limit = 0
                    reverse_limit = 0

                capacity_limits.append(
                    {
                        "isp_name": isp_name,
                        "investment_period": investment_period,
                        "forward_limit": forward_limit,
                        "reverse_limit": reverse_limit,
                    }
                )

        capacity_limits_df = pd.DataFrame(capacity_limits)

        # Merge capacity limits with flows
        flows = flows.merge(
            capacity_limits_df, on=["isp_name", "investment_period"], how="left"
        )

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
            Expected columns: isp_name, isp_type, build_year, forward_direction_nominal_capacity_mw, reverse_direction_nominal_capacity_mw

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
    # Prepare the flow data with capacity limits
    flows = prepare_flow_data(flows, transmission_expansion)

    # Initialize the nested dictionary structure
    plots = {}

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
                y=week_data["flow"],
                name="Flow",
                mode="lines",
                line=dict(color="#1f77b4", width=2),
                hovertemplate="<b>Flow</b><br>%{y:.2f} MW<extra></extra>",
            )
        )

        # Add capacity limit lines (except for rez_no_limit and when limits are not available)
        if isp_type != "rez_no_limit":
            # Get limits from prepared data (same for all rows in this group)
            forward_limit = week_data["forward_limit"].iloc[0]
            reverse_limit = week_data["reverse_limit"].iloc[0]

            # Only add limit lines if limits are available (not NaN)
            if pd.notna(forward_limit) and pd.notna(reverse_limit):
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
        if isp_type not in plots:
            plots[isp_type] = {}
        if isp_name not in plots[isp_type]:
            plots[isp_type][isp_name] = {}
        if investment_period not in plots[isp_type][isp_name]:
            plots[isp_type][isp_name][str(investment_period)] = {}

        # Store the chart and data with week_starting as string for consistency
        # Keep only relevant columns for the CSV output
        plot_data = week_data[["timestep", "flow"]].copy()
        plots[isp_type][isp_name][str(investment_period)][str(week_starting)] = {
            "plot": fig,
            "data": plot_data,
        }

    return plots


def _create_entity_capacity_chart(
    region_data: pd.DataFrame,
    entity_column: str,
    region: str,
    chart_type: str,
) -> dict:
    """Create a line chart showing capacity expansion for entities in a region.

    Args:
        region_data: DataFrame filtered for a specific region with columns:
            build_year, capacity_mw, and entity_column
        entity_column: Name of the column containing entity IDs ("rez_id" or "flow_path")
        region: Region name for chart title
        chart_type: Type of entity for chart title ("REZ" or "Flow Path")

    Returns:
        Dictionary with keys "plot" (Plotly Figure) and "data" (DataFrame)
    """
    fig = go.Figure()

    if not region_data.empty:
        build_years = sorted(region_data["build_year"].unique())

        # Reshape data: years as rows, entities as columns
        pivot_data = region_data.pivot(
            index="build_year", columns=entity_column, values="capacity_mw"
        )

        # Add each entity as a line series
        for entity_id in sorted(pivot_data.columns):
            fig.add_trace(
                go.Scatter(
                    x=[str(year) for year in build_years],
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
            Expected columns: isp_name, isp_type, build_year, node_from, node_to, forward_direction_nominal_capacity_mw
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
    rez_data = prepare_rez_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
    )
    flow_path_data = prepare_flow_path_capacity_by_region(
        transmission_expansion, regions_and_zones_mapping
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
                region_rez_data, "rez_id", region, "REZ"
            ),
            "flow_path_capacity": _create_entity_capacity_chart(
                region_flow_data, "flow_path", region, "Flow Path"
            ),
        }

    return plots
