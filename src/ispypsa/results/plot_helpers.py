import matplotlib.figure
from matplotlib.patches import Patch

from ispypsa.config.validators import ModelConfig

_DEFAULT_CARRIER_COLOUR_MAPPING = {
    # corresponds to distillate in OpenElectricity
    "Liquid Fuel": "#E46E56",
    "Black Coal": "#251C00",
    "Brown Coal": "#675B42",
    "Gas": "#E78114",
    "Water": "#ACE9FE",
    "Solar": "#FECE00",
    "Wind": "#2A7E3F",
    # corresponds to gas_hydrogen in OpenElectricity
    "Hyblend": "#C75338",
}
"""Colour mapping for carriers/fuel types. Same colour scheme as OpenElectricity"""

_DEFAULT_GEOMAP_COLOURS = dict(ocean="#dbdbdd", land="#fdfdfe")
"""Colour mapping for ocean and land in the map plot"""

_DEFAULT_FACECOLOR = "#faf9f6"
"""Facecolour to use in Figures (from OpenElectricity)"""


def _determine_title_year_range(config: ModelConfig) -> str:
    """
    Determines the year range string for use in plot titles based on
    ISPyPSA configuration options.
    """
    (start, end) = (config.traces.start_year, config.traces.end_year)
    year_type = config.traces.year_type
    if year_type == "fy" and start != end:
        year_range = f"FY{str(start)[-2:]}-{str(end)[-2:]}"
    elif start == end:
        year_range = f"{start}"
    else:
        year_range = f"{start}-{end}"
    return year_range


def _add_figure_fuel_type_legend(
    fig: matplotlib.figure.Figure,
    carrier_colour_mapping: dict[str, str],
    legend_kwargs=dict(),
) -> None:
    """Adds a legend that maps fuel types to their patch colours to a
    `matplotlib.figure.Figure`.

    Args:
        fig: `matplotlib.figure.Figure`
        carrier_colour_mapping: Dictionary that maps each carrier to a colour
        legend_kwargs (optional): Keyword arguments for
            `matplotlib.figure.Figure.legend()`. Anything specified in this dict will
            overwrite ISPyPSA defaults. Defaults to dict().
    """
    legend_patches = [
        Patch(color=color, label=carrier)
        for carrier, color in carrier_colour_mapping.items()
    ]
    fig.legend(
        handles=legend_patches,
        **_consolidate_plot_kwargs(
            dict(
                title="Fuel Type",
                loc="lower center",
                fontsize=8,
                title_fontsize=10,
                ncol=4,
                frameon=False,
            ),
            legend_kwargs,
        ),
    )


def _consolidate_plot_kwargs(
    predefined_kwargs: dict, user_specified_kwargs: dict
) -> dict:
    """Adds to or replaces ISPyPSA's keyword arguments for plot functions using those
    provided by the user in the function call.
    """
    kwargs = predefined_kwargs
    kwargs.update(user_specified_kwargs)
    return kwargs
