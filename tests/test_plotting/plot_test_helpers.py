import numpy as np
import pandas as pd
import plotly.graph_objects as go


def drawn_areas_mwh(figure: go.Figure) -> pd.DataFrame:
    """Returns the area between each trace of the figure and zero, in MWh, as a
    table with the columns trace and area_mwh.

    The area is worked out from the trace's own x and y values and its line
    shape, as plotly draws them: with the "vh" shape a point's value is held
    back to the point before it, and otherwise points are joined by straight
    lines. A blank y value breaks the trace.
    """
    areas = [_drawn_area_mwh(trace) for trace in figure.data]
    names = [trace.name for trace in figure.data]
    return pd.DataFrame({"trace": names, "area_mwh": areas})


def _drawn_area_mwh(trace: go.Scatter) -> float:
    y = np.array(trace.y, dtype=float)
    hours = pd.Series(trace.x).diff().dt.total_seconds().to_numpy()[1:] / 3600
    if trace.line.shape == "vh":
        heights = y[1:]
    else:
        heights = (y[1:] + y[:-1]) / 2
    return np.nansum(hours * heights)
