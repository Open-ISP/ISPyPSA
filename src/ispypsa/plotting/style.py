"""Shared styling and color definitions for plotting."""

# Technology-specific color mapping for carrier types
CARRIER_COLORS = {
    "Solar": "#FFD700",  # Yellow (gold)
    "Wind": "#228B22",  # Green (forest green)
    "Hydro": "#4682B4",  # Blue (steel blue)
    "Water": "#4682B4",  # Same as hydro
    "Gas": "#FF8C00",  # Orange (dark orange)
    "Black Coal": "#2F4F4F",  # Dark gray (dark slate gray)
    "Brown Coal": "#8B4513",  # Brown (saddle brown)
    "Liquid Fuel": "#9370DB",  # Purple (medium purple)
    "Hyblend": "#DDA0DD",  # Light purple (plum)
    "Unserved Energy": "#FF0000",  # Red
    "Battery": "#90EE90",  # Light green
    "Pumped Hydro": "#20B2AA",  # Teal (light sea green)
    "Transmission Imports": "#00CED1",  # Dark turquoise
    "Transmission Exports": "#87CEEB",  # Sky blue
}


def get_carrier_color(carrier: str) -> str:
    """Get the color for a carrier type.

    Args:
        carrier: Carrier/technology type

    Returns:
        Hex color code
    """
    return CARRIER_COLORS.get(carrier, "#999999")  # Default gray


def create_plotly_professional_layout(
    title: str,
    height: int = 600,
    width: int = 1200,
) -> dict:
    """Create professional/academic style layout for Plotly charts.

    Args:
        title: Chart title
        y_max: Maximum y-axis value
        y_min: Minimum y-axis value
        height: Chart height in pixels
        width: Chart width in pixels

    Returns:
        Plotly layout dictionary
    """
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
        "xaxis": {
            "gridcolor": "#E0E0E0",
            "gridwidth": 0.5,
            "showgrid": True,
            "showline": True,
            "linewidth": 1,
            "linecolor": "#CCCCCC",
            "mirror": True,
            "ticks": "outside",
            "tickfont": {"size": 11},
            "tickformat": "%Y-%m-%d %H:%M",
        },
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
        "height": height,
        "width": width,
        "margin": {"l": 80, "r": 200, "t": 80, "b": 60},
    }
