from importlib.resources import files

import numpy as np
import pandas as pd

from ispypsa.model_years import _model_year_of_dates
from ispypsa.templater.helpers import _build_geo_region_lookup
from ispypsa.templater.transmission import (
    _add_flow_path_carrier,
    _aggregate_to_granularity,
    _parse_flow_path_topology,
)

# The steps in the PLEXOS limits that are committed or anticipated network
# projects, chosen by hand. The PLEXOS model also steps its limits for the
# builds AEMO's own modelling selected (VNI West, the QNI upgrade and so on),
# and nothing in the model tells the two apart, so every other step is left
# for ISPyPSA to decide through the expansion options.
#
# A step is identified by the PLEXOS object carrying the limit and the date it
# takes effect; path_id is the ISPyPSA sub-regional path it applies to. The
# Waratah Super Battery SIPS control uplifts are temporary: PLEXOS steps them
# back off on date_to. The Latrobe Valley reconfiguration, after Yallourn
# retires, is a reduction.
#
# expansion_prerequisite marks a project that the path's augmentation options
# assume is already built (the IASR lists HumeLink as the pre-requisite of
# every other SNSW-CNSW option), so the path can only be expanded once the
# project is in service.
_COMMITTED_LIMIT_STEPS = pd.DataFrame(
    [
        (
            "EnergyConnect",
            "2027-11-30T00:00:00",
            np.nan,
            "SNSW-CSA",
            "Project EnergyConnect Stage 2",
            False,
        ),
        (
            "V-SA",
            "2027-11-30T00:00:00",
            np.nan,
            "WNV-SESA",
            "Project EnergyConnect Stage 2",
            False,
        ),
        (
            "CNSW-SNW South",
            "2027-08-01T00:00:00",
            np.nan,
            "CNSW-SNW_STH",
            "Waratah Super Battery network augmentations",
            False,
        ),
        (
            "CNSW-SNW North",
            "2027-08-01T00:00:00",
            "2030-07-01T00:00:00",
            "CNSW-SNW_NTH",
            "Waratah Super Battery SIPS control",
            False,
        ),
        (
            "SNSW-CNSW",
            "2027-08-01T00:00:00",
            "2030-07-01T00:00:00",
            "SNSW-CNSW",
            "Waratah Super Battery SIPS control",
            False,
        ),
        (
            "CNSW-NNSW",
            "2027-08-01T00:00:00",
            "2030-07-01T00:00:00",
            "CNSW-NNSW",
            "Waratah Super Battery SIPS control",
            False,
        ),
        ("SNSW-CNSW", "2027-12-02T00:00:00", np.nan, "SNSW-CNSW", "HumeLink", True),
        (
            "MEL-WNV",
            "2029-11-30T00:00:00",
            np.nan,
            "MEL-WNV",
            "Western Renewables Link",
            False,
        ),
        (
            "SEV-MEL",
            "2028-07-01T00:00:00",
            np.nan,
            "SEV-MEL",
            "Latrobe Valley reconfiguration",
            False,
        ),
    ],
    columns=[
        "object_name",
        "date_from",
        "date_to",
        "path_id",
        "project",
        "expansion_prerequisite",
    ],
)

# A constraint's RHS caps flow in the path's forward direction, like Max Flow.
_PROPERTY_TO_DIRECTION = {
    "Max Flow": "forward",
    "RHS": "forward",
    "Min Flow": "reverse",
}

_CAPACITY_CHANGE_COLUMNS = [
    "path_id",
    "direction",
    "capacity_change",
    "date_from",
    "date_to",
    "project",
]


def load_plexos_line_limits(iasr_workbook_version: str) -> pd.DataFrame:
    """Loads the PLEXOS line limits shipped with ISPyPSA for the given IASR
    workbook version (from ``src/ispypsa/templater/plexos/``)."""
    extract_dir = files("ispypsa.templater") / "plexos" / iasr_workbook_version
    return pd.read_csv(extract_dir / "line_limits.csv")


def _template_path_capacity_changes(
    line_limits: pd.DataFrame,
    flow_path_transfer_capability: pd.DataFrame,
    renewable_energy_zones: pd.DataFrame,
    sub_regional_geography: pd.DataFrame,
    regional_granularity: str,
) -> pd.DataFrame:
    """Creates the network_transmission_path_capacity_changes table.

    The network_transmission_path_limits table holds each path's capacity as it
    stands today. Committed and anticipated network projects change that
    capacity on known dates: HumeLink adds 2,200 MW to SNSW-CNSW in both
    directions, and the reconfiguration that follows Yallourn's retirement
    takes 1,775 MW off SEV-MEL. The IASR workbook's own table of these uplifts
    doesn't parse cleanly, so the changes are read from AEMO's PLEXOS model,
    which holds each path's limit as a series of dated values.

    line_limits is that series, as extracted by
    scripts/extract_plexos_line_limits.py. The IASR tables and
    regional_granularity are only used to carry each change onto the path ids
    of the chosen granularity.

    Returns a table with one row per path, direction and project:

    - path_id: the path whose capacity changes.
    - direction: forward or reverse.
    - capacity_change: MW added to the path's limit in that direction, the same
      in every timeslice. Negative for a reduction.
    - date_from: the date the change takes effect.
    - date_to: the date a temporary change stops applying; blank if permanent.
    - project: the network project behind the change.

    Only the limit steps listed in _COMMITTED_LIMIT_STEPS are kept, and a
    direction a project leaves unchanged gets no row. A change on a path the
    chosen granularity doesn't model is dropped with the path.

    I/O Example:
        line_limits (abbreviated; date_to is blank throughout):
            object_class  object_name  property  value    date_from            tags
            Line          SNSW-CNSW    Max Flow  2700.0                        NSW Hot Day
            Line          SNSW-CNSW    Max Flow  2950.0   2027-08-01T00:00:00  NSW Hot Day
            Line          SNSW-CNSW    Max Flow  5150.0   2027-12-02T00:00:00  NSW Hot Day
            Line          SNSW-CNSW    Max Flow  5600.0   2029-07-01T00:00:00  NSW Hot Day   # not committed
            Line          SNSW-CNSW    Min Flow  -2320.0                       NSW Hot Day
            Line          SNSW-CNSW    Min Flow  -2320.0  2027-08-01T00:00:00  NSW Hot Day   # no change
            Line          SNSW-CNSW    Min Flow  -4520.0  2027-12-02T00:00:00  NSW Hot Day

        regional_granularity = "sub_regions" returns:
            path_id    direction  capacity_change  date_from            date_to              project
            SNSW-CNSW  forward    250.0            2027-08-01T00:00:00  2030-07-01T00:00:00  Waratah Super Battery SIPS control
            SNSW-CNSW  forward    2200.0           2027-12-02T00:00:00                       HumeLink
            SNSW-CNSW  reverse    2200.0           2027-12-02T00:00:00                       HumeLink

        regional_granularity = "nem_regions" returns no rows: SNSW-CNSW sits
        inside NSW, so the path isn't modelled.
    """
    steps = _calculate_limit_steps(line_limits)
    changes = steps.merge(_COMMITTED_LIMIT_STEPS, on=["object_name", "date_from"])
    changes = changes[changes["capacity_change"] != 0.0]
    # A step is the same size in every timeslice, so the timeslices collapse
    # to one row.
    changes = changes.loc[:, _CAPACITY_CHANGE_COLUMNS].drop_duplicates()
    changes = _rekey_to_granularity(
        changes,
        flow_path_transfer_capability,
        renewable_energy_zones,
        sub_regional_geography,
        regional_granularity,
    )
    return changes.sort_values(["path_id", "date_from", "direction"]).reset_index(
        drop=True
    )


def _calculate_limit_steps(line_limits: pd.DataFrame) -> pd.DataFrame:
    """Works out how much each dated limit differs from the one before it.

    Limits are compared as magnitudes, so a reverse limit moving from -2320 to
    -4520 is a step of +2200. The undated value is the starting point and has
    no step of its own.

    I/O Example:
        line_limits (abbreviated):
            object_name  property  value    date_from            tags
            SNSW-CNSW    Max Flow  2700.0                        NSW Hot Day
            SNSW-CNSW    Max Flow  2950.0   2027-08-01T00:00:00  NSW Hot Day
            SNSW-CNSW    Max Flow  5150.0   2027-12-02T00:00:00  NSW Hot Day
            SNSW-CNSW    Min Flow  -2320.0                       NSW Hot Day
            SNSW-CNSW    Min Flow  -4520.0  2027-12-02T00:00:00  NSW Hot Day

        returns:
            object_name  direction  tags         date_from            capacity_change
            SNSW-CNSW    forward    NSW Hot Day  2027-08-01T00:00:00  250.0
            SNSW-CNSW    forward    NSW Hot Day  2027-12-02T00:00:00  2200.0
            SNSW-CNSW    reverse    NSW Hot Day  2027-12-02T00:00:00  2200.0
    """
    limits = line_limits.sort_values("date_from", na_position="first")
    limits = limits.assign(
        direction=limits["property"].map(_PROPERTY_TO_DIRECTION),
        magnitude=limits["value"].abs(),
    )
    limits["capacity_change"] = limits.groupby(["object_name", "property", "tags"])[
        "magnitude"
    ].diff()
    steps = limits.dropna(subset="capacity_change")
    return steps.loc[
        :, ["object_name", "direction", "tags", "date_from", "capacity_change"]
    ]


def _rekey_to_granularity(
    changes: pd.DataFrame,
    flow_path_transfer_capability: pd.DataFrame,
    renewable_energy_zones: pd.DataFrame,
    sub_regional_geography: pd.DataFrame,
    regional_granularity: str,
) -> pd.DataFrame:
    """Carries the changes from sub-regional path ids onto the path ids of the
    chosen granularity, dropping changes on paths that granularity doesn't model.

    This is the re-keying network_transmission_path_limits goes through (see
    _aggregate_to_granularity), applied to the flow paths alone: no REZ
    connection has a committed change.

    I/O Example:
        changes:
            path_id    direction  capacity_change
            SNSW-CSA   forward    650.0
            SNSW-CNSW  forward    2200.0

        regional_granularity = "nem_regions" returns:
            path_id  direction  capacity_change
            NSW-SA   forward    650.0             # SNSW-CNSW is inside NSW: dropped
    """
    topology = _parse_flow_path_topology(flow_path_transfer_capability["Flow Paths"])
    flow_paths = _add_flow_path_carrier(topology)
    region_lookup = _build_geo_region_lookup(sub_regional_geography)
    _, changes = _aggregate_to_granularity(
        flow_paths, changes, regional_granularity, renewable_energy_zones, region_lookup
    )
    return changes


def _drop_expansion_costs_before_prerequisite_projects(
    expansion_costs: pd.DataFrame,
) -> pd.DataFrame:
    """Drops a path's expansion costs for the years before its prerequisite
    project is in service.

    The translator builds an expansion link only for the years a path has a
    cost, so this stops the model expanding the path before the committed
    project its augmentation options assume (see expansion_prerequisite in
    _COMMITTED_LIMIT_STEPS). Year labels are financial years, so HumeLink's
    December 2027 date makes 2028 the first year SNSW-CNSW can be expanded.

    I/O Example:
        expansion_costs:
            expansion_id  year  cost
            SNSW-CNSW     2027  480000   # before HumeLink: dropped
            SNSW-CNSW     2028  490000
            CQ-NQ         2027  410000   # no prerequisite: kept

        returns:
            expansion_id  year  cost
            SNSW-CNSW     2028  490000
            CQ-NQ         2027  410000
    """
    prerequisites = _COMMITTED_LIMIT_STEPS[
        _COMMITTED_LIMIT_STEPS["expansion_prerequisite"]
    ]
    in_service_years = _model_year_of_dates(
        "fy", pd.to_datetime(prerequisites["date_from"])
    )
    first_year_by_path = dict(zip(prerequisites["path_id"], in_service_years))
    first_year = expansion_costs["expansion_id"].map(first_year_by_path)
    return expansion_costs[~(expansion_costs["year"] < first_year)]
