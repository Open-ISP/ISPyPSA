"""Manually extract a rez-group custom constraint from the IASR workbook.

This is an independent reconstruction of one custom constraint, built directly
from the workbook tables, used as a cross-check on the PLEXOS-route templater
(``custom_constraints_from_plexos.py``).

For a given constraint id (e.g. ``NQ1``) it:

1. Looks the constraint up in ``rez_group_constraint_summary`` to get the list
   of Terms participating in it.
2. Parses each Term into ``(coefficient, body)``. Bodies can be REZ ids
   (``Q1``) or interconnector path ids (``CQ-NQ``); coefficients can be
   implicit (``Q1`` => 1.0), signed-prefixed (``- CQ-NQ`` => -1.0), or
   explicit (``0.78 * V7``, ``-0.5 * SQ-CQ``).
3. Expands each parsed Term to LHS unit rows:
    - REZ body -> every unit (generators + batteries + electrolysers) tagged
      with that REZ ID in the unit summary tables, coefficient applied
      uniformly.
    - Path-id body -> a single ``link_flow`` row carrying the coefficient.

CER units are not reachable via REZ-id expansion (REZ ID = ``Not Applicable``)
so they are absent from the workbook side -- see Open-ISP/ISPyPSA#104.

Term forms not yet handled (raise ``NotImplementedError``) -- deferred to
later tiers as more constraints are validated:

* Bare plant names (e.g. ``Loy Yang``, ``Limondale SF``)
* Power-station families that expand to multiple units (e.g. ``Murray``,
  ``Bayswater``, ``Mt Piper``)
* Sub-region demand/load terms (e.g. ``-0.1 * NSA demand``)
* Custom area aggregates / GPG groupings (e.g. ``WD``, ``CNSWGPGSO``)
* Constraints defined in ``rez_transmission_limit_summary`` or
  ``rez_secondary_transmission_limit_summary`` (e.g. SWQLD1, WNV1, SNW1,
  SWNSW1) -- these tables aren't loaded yet.

CLI usage:

    uv run python scripts/workbook_extract_constraint.py NQ1
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from ispypsa.data_fetch.csv_read_write import read_csvs

_CONSTRAINT_ID_COL = "Group constraint ID"
_TERM_COL = "Term"
_DESCRIPTION_COL = "Description"
_REZ_ID_COL = "REZ ID"
_IASR_ID_COL = "IASR ID / DLT names"
_TECH_COL = "Technology Type"
_FLOW_PATHS_COL = "Flow Paths"

# Summary tables that carry per-unit REZ IDs (so they're reachable via the
# REZ-id expansion). CER's "Not Applicable" REZ ID means it isn't.
_UNIT_TABLES = (
    "existing_committed_anticipated_additional_generator_summary",
    "new_entrants_summary",
    "new_entrant_electrolysers_summary",
)
_FLOW_PATH_TABLE = "flow_path_transfer_capability"

# Matches an optional "<coef> * " prefix followed by the body. coef may be
# negative, integer, or decimal. Examples that match (groups in parens):
#   "Q1"           -> (None,  "Q1")
#   "0.78 * V7"    -> ("0.78", "V7")
#   "-0.5 * SQ-CQ" -> ("-0.5", "SQ-CQ")
_COEF_BODY_RE = re.compile(r"^(?:(-?\d*\.?\d+)\s*\*\s*)?(.+)$")


def workbook_extract_constraint_lhs(
    constraint_id: str, iasr_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Build the LHS unit list for one constraint by expanding its Terms.

    I/O Example:
        constraint_id = "MN1"
        iasr_tables   = read_csvs(workbook cache)

        returns:
            constraint_id  term_type        variable_name   coefficient
            MN1            generator_output S3_SAT_Mid-North SA  1.0    # from REZ S3
            MN1            generator_output S4_SAT_Yorke Peninsula 1.0  # from REZ S4
            MN1            link_flow        CSA-NSA         -1.0        # from "- CSA-NSA"
            MN1            link_flow        SNSW-CSA         0.2        # from "0.2 * SNSW-CSA"
            ...
    """
    rgc = _load_constraint_definitions(iasr_tables)
    raw_terms = _terms_for_constraint(constraint_id, rgc)
    rez_ids = _all_known_rez_ids(iasr_tables)
    path_ids = _all_known_path_ids(iasr_tables)
    per_term_rows = [
        _term_to_lhs_rows(t, iasr_tables, rez_ids, path_ids) for t in raw_terms
    ]
    lhs = pd.concat(per_term_rows, ignore_index=True)
    lhs.insert(0, "constraint_id", constraint_id)
    return lhs


def _load_constraint_definitions(iasr_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Return ``rez_group_constraint_summary`` with constraint IDs forward-filled.

    Workbook quirk: ``Group constraint ID`` is only populated on the first
    row of each group. Continuation rows have the Description text duplicated
    into the ID column (an isp-workbook-parser merged-cell artefact). Mask
    those continuation rows out before forward-filling so we only propagate
    real ids.

    I/O Example:
        input rows (excerpt):
            Term  Description                 Group constraint ID
            Q1    Far North QLD               NQ1
            Q2    North QLD Clean Energy Hub  North QLD Clean Energy Hub    # continuation
            Q3    Northern QLD                Northern QLD                  # continuation

        returns same rows with extra column:
            constraint_id
            NQ1
            NQ1
            NQ1
    """
    rgc = iasr_tables["rez_group_constraint_summary"].copy()
    is_header = rgc[_CONSTRAINT_ID_COL] != rgc[_DESCRIPTION_COL]
    rgc["constraint_id"] = rgc[_CONSTRAINT_ID_COL].where(is_header).ffill()
    return rgc


def _terms_for_constraint(constraint_id: str, rgc: pd.DataFrame) -> list[str]:
    """Return the ``Term`` column values for one constraint, in workbook order.

    I/O Example:
        constraint_id = "MN1"
        returns: ["S3", "S4", "- CSA-NSA", "0.2 * SNSW-CSA"]
    """
    return rgc.loc[rgc["constraint_id"] == constraint_id, _TERM_COL].tolist()


def _all_known_rez_ids(iasr_tables: dict[str, pd.DataFrame]) -> set[str]:
    """Collect the set of REZ IDs that appear in any unit summary table."""
    rez_ids: set[str] = set()
    for tbl in _UNIT_TABLES:
        rez_ids |= set(iasr_tables[tbl][_REZ_ID_COL].dropna().astype(str))
    rez_ids.discard("Not Applicable")
    return rez_ids


def _all_known_path_ids(iasr_tables: dict[str, pd.DataFrame]) -> set[str]:
    """Collect the set of interconnector path ids from the flow-path table."""
    return set(iasr_tables[_FLOW_PATH_TABLE][_FLOW_PATHS_COL].dropna().astype(str))


def _parse_term(raw_term: str) -> tuple[float, str]:
    """Split a Term string into ``(coefficient, body)``.

    I/O Example:
        "Q1"               -> (1.0,  "Q1")
        "0.78 * V7"        -> (0.78, "V7")
        "-0.5 * SQ-CQ"     -> (-0.5, "SQ-CQ")
        "- CQ-NQ"          -> (-1.0, "CQ-NQ")     # bare leading minus
    """
    raw = raw_term.strip()
    if raw.startswith("- "):
        return -1.0, raw[2:].strip()
    coef_str, body = _COEF_BODY_RE.match(raw).groups()
    return (float(coef_str) if coef_str else 1.0), body.strip()


def _term_to_lhs_rows(
    raw_term: str,
    iasr_tables: dict[str, pd.DataFrame],
    rez_ids: set[str],
    path_ids: set[str],
) -> pd.DataFrame:
    """Dispatch one Term to its expansion strategy.

    I/O Example:
        "0.78 * V7"     -> all V7 units, coefficient 0.78
        "- CQ-NQ"       -> single link_flow row, coefficient -1.0
        "Loy Yang"      -> raises NotImplementedError (plant names deferred)
    """
    coefficient, body = _parse_term(raw_term)
    if body in rez_ids:
        return _rez_unit_rows(body, coefficient, iasr_tables)
    if body in path_ids:
        return _flow_term_row(body, coefficient)
    raise NotImplementedError(
        f"Unsupported Term body {body!r} in {raw_term!r} -- not a REZ id "
        "or interconnector path id. Plant names, demand, and area groupings "
        "are deferred to later tiers."
    )


def _rez_unit_rows(
    rez_id: str, coefficient: float, iasr_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Return all unit rows tagged with REZ ID, with coefficient applied.

    I/O Example:
        rez_id = "Q1", coefficient = 0.78
        returns:
            term_type        variable_name           coefficient
            generator_output Q1_SAT_Far North QLD    0.78
            storage_output   Q1 Battery - 2h         0.78
            ...
    """
    pieces = [
        iasr_tables[tbl].loc[
            iasr_tables[tbl][_REZ_ID_COL] == rez_id, [_IASR_ID_COL, _TECH_COL]
        ]
        for tbl in _UNIT_TABLES
    ]
    units = pd.concat(pieces, ignore_index=True)
    return pd.DataFrame(
        {
            "term_type": units[_TECH_COL].map(_term_type_for_tech),
            "variable_name": units[_IASR_ID_COL],
            "coefficient": coefficient,
        }
    )


def _flow_term_row(path_id: str, coefficient: float) -> pd.DataFrame:
    """Return a single ``link_flow`` LHS row for an interconnector Term.

    I/O Example:
        path_id = "SQ-CQ", coefficient = -0.5
        returns:
            term_type  variable_name  coefficient
            link_flow  SQ-CQ          -0.5
    """
    return pd.DataFrame(
        {
            "term_type": ["link_flow"],
            "variable_name": [path_id],
            "coefficient": [coefficient],
        }
    )


def _term_type_for_tech(tech: str) -> str:
    """Map a workbook Technology Type string to an ISPyPSA term_type.

    I/O Example:
        "Battery Storage (2hrs storage)" -> "storage_output"
        "Battery storage (2hrs storage)" -> "storage_output"   # case insensitive
        "Large scale Solar PV"           -> "generator_output"
        "Alkaline Electrolyser"          -> "load"
    """
    tech_l = tech.lower()
    if "battery" in tech_l:
        return "storage_output"
    if "electrolyser" in tech_l:
        return "load"
    return "generator_output"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("constraint_id", help="e.g. NQ1")
    parser.add_argument(
        "--cache",
        default="data/workbook_table_cache",
        help="Path to the parsed-workbook cache directory",
    )
    args = parser.parse_args()
    iasr_tables = read_csvs(Path(args.cache))
    lhs = workbook_extract_constraint_lhs(args.constraint_id, iasr_tables)
    print(lhs.to_string(index=False))


if __name__ == "__main__":
    main()
