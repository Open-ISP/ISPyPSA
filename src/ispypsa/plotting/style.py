"""Shared styling and color definitions for plotting."""

# Fuel type color mapping
# Based on fuel types in ecaa_generators.csv and new_entrant_generators.csv
FUEL_TYPE_COLORS = {
    # Coal
    "Black Coal": "#121212",
    "Brown Coal": "#744A26",
    # Gas
    "Gas": "#F48E1B",
    # Liquid Fuel
    "Liquid Fuel": "#E15C34",
    # Hydro
    "Water": "#5EA0C0",
    # Solar
    "Solar": "#FED500",
    # Wind
    "Wind": "#2C7629",
    # Bioenergy
    "Biomass": "#1D7A7A",
    # Hydrogen
    "Hydrogen": "#DDA0DD",
    "Hyblend": "#DDA0DD",
    # Battery Storage
    "Battery": "#3245c9",
    "Battery Charging": "#577CFF",
    "Battery Discharging": "#3245c9",
    # Transmission (for plotting)
    "Transmission Exports": "#927BAD",
    "Transmission Imports": "#521986",
    # Other
    "Unserved Energy": "#FF0000",
}


def get_fuel_type_color(fuel_type: str) -> str:
    """Get the color for a fuel type.

    Args:
        fuel_type: Fuel/technology type

    Returns:
        Hex color code
    """
    return FUEL_TYPE_COLORS.get(fuel_type, "#999999")  # Default gray


# Backwards compatibility alias
CARRIER_COLORS = FUEL_TYPE_COLORS
get_carrier_color = get_fuel_type_color


def create_plotly_professional_layout(
    title: str,
    height: int = 600,
    width: int = 1200,
    timeseries: bool = False,
) -> dict:
    """Create professional/academic style layout for Plotly charts.

    Args:
        title: Chart title
        y_max: Maximum y-axis value
        y_min: Minimum y-axis value
        height: Chart height in pixels (used as minimum height)
        width: Chart width in pixels (ignored when autosize is True)
        timeseries: If True, applies timeseries-specific formatting (rotated x-axis labels)

    Returns:
        Plotly layout dictionary
    """
    xaxis_config = {
        "gridcolor": "#E0E0E0",
        "gridwidth": 0.5,
        "showgrid": True,
        "showline": True,
        "linewidth": 1,
        "linecolor": "#CCCCCC",
        "mirror": True,
        "ticks": "outside",
        "tickfont": {"size": 11},
    }

    if timeseries:
        xaxis_config["tickformat"] = "%Y-%m-%d %H:%M"
        xaxis_config["tickangle"] = 45

    return {
        "title": {
            "text": title,
            "font": {"size": 18, "family": "Arial, sans-serif", "color": "#2C3E50"},
            "x": 0.5,
            "xanchor": "center",
        },
        "xaxis_title": {
            "text": "Time",
            "font": {"size": 14, "family": "Arial, sans-serif", "color": "#2C3E50"},
        },
        "yaxis_title": {
            "text": "Power (MW)",
            "font": {"size": 14, "family": "Arial, sans-serif", "color": "#2C3E50"},
        },
        "hovermode": "x unified",
        "plot_bgcolor": "#FAFAFA",  # Very light gray background
        "paper_bgcolor": "white",
        "font": {"family": "Arial, sans-serif", "size": 12, "color": "#2C3E50"},
        "legend": {
            "orientation": "v",
            "yanchor": "top",
            "y": 0.98,
            "xanchor": "left",
            "x": 1.02,
            "bgcolor": "rgba(255, 255, 255, 0.9)",
            "bordercolor": "#CCCCCC",
            "borderwidth": 1,
            "font": {"size": 11},
        },
        "xaxis": xaxis_config,
        "yaxis": {
            "gridcolor": "#E0E0E0",
            "gridwidth": 0.5,
            "showgrid": True,
            "showline": True,
            "linewidth": 1,
            "linecolor": "#CCCCCC",
            "mirror": True,
            "ticks": "outside",
            "tickfont": {"size": 11},
            "rangemode": "tozero",
            "tickformat": ",",  # Comma separator
        },
        "autosize": True,
        "margin": {"l": 80, "r": 200, "t": 80, "b": 60},
    }
