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
"""Colour mapping based on mapping from OpenElectricity"""

_DEFAULT_GEOMAP_COLOURS = dict(ocean="#dbdbdd", land="#fdfdfe")

_DEFAULT_FACECOLOR = "#faf9f6"


def _determine_title_year_range(config: ModelConfig) -> str:
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
    kwargs = predefined_kwargs
    kwargs.update(user_specified_kwargs)
    return kwargs
