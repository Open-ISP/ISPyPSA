"""Extract the dated flow limits of transmission lines from an AEMO PLEXOS XML model.

Run manually after AEMO publishes a new PLEXOS model. The output CSV lands in
``src/ispypsa/templater/plexos/<version>/`` and is consumed by the ISPyPSA
templater, which picks out the steps that are committed network projects.

Usage:
    uv run python scripts/extract_plexos_line_limits.py path/to/model.xml --version 7.5

The PLEXOS data model, and the five-step query pipeline this script borrows,
are described in ``extract_plexos_constraints.py``.


Where PLEXOS keeps a flow limit
===============================

Most limits are properties of a Line object: ``Max Flow`` is the forward
limit and ``Min Flow`` the reverse limit (zero or negative). A few paths are
limited by a Constraint instead, with the limit as the constraint's ``RHS``:
PLEXOS has one CNSW-SNW line, and the northern and southern limits on it are
the constraints "CNSW-SNW North" and "CNSW-SNW South".

Either way the value is a ``t_data`` row on the membership joining the System
object (parent) to the Line or Constraint (child). A limit that changes over
time has one row per effective date (``t_date_from``), and each row is tagged
with the demand-condition timeslice it applies in.

The model does not say why a limit changes. A step may be a committed
project, or a build that AEMO's own modelling chose; which is which is
recorded by hand in the templater.


Output schema
=============

``line_limits.csv``, long format, one row per limit value:
``object_class, object_name, property, value, date_from, date_to, tags``

PLEXOS object names, property names and tags are preserved verbatim from the
XML. The templater translates them into ISPyPSA path ids and directions.
"""

import argparse
from pathlib import Path

import pandas as pd
from extract_plexos_constraints import (
    _load_plexos_db,
    _placeholders,
    _query_data_points,
    _query_dates,
    _query_tags,
)
from plexosdb import PlexosDB

# The two constraints that limit the PLEXOS CNSW-SNW line. Every Line object
# is extracted; constraints are listed by name because only these two stand
# in for a line limit.
LIMIT_CONSTRAINT_NAMES = ["CNSW-SNW North", "CNSW-SNW South"]

_LIMIT_PROPERTIES = ["Max Flow", "Min Flow", "RHS"]

_OUTPUT_COLUMNS = [
    "object_class",
    "object_name",
    "property",
    "value",
    "date_from",
    "date_to",
    "tags",
]


def main():
    args = _parse_args()
    db = _load_plexos_db(args.xml_path)
    limits = _query_limit_rows(db, LIMIT_CONSTRAINT_NAMES)
    out_dir = args.out_dir / args.version
    out_dir.mkdir(parents=True, exist_ok=True)
    limits.to_csv(out_dir / "line_limits.csv", index=False)
    print(f"Wrote: {out_dir / 'line_limits.csv'} ({len(limits)} rows)")


def _parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("xml_path", help="Path to the PLEXOS XML model file.")
    p.add_argument(
        "--version",
        required=True,
        help="Workbook version label for the output subdirectory (e.g. 7.5).",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=Path("src/ispypsa/templater/plexos"),
        help="Parent output directory (default: src/ispypsa/templater/plexos).",
    )
    return p.parse_args()


def _query_limit_rows(db: PlexosDB, constraint_names: list[str]) -> pd.DataFrame:
    """Builds the long table of limit values for every line and the named constraints.

    I/O Example:
        constraint_names: ["CNSW-SNW North"]

        returns (abbreviated):
            object_class  object_name     property  value    date_from            date_to  tags
            Line          SNSW-CNSW       Max Flow  2700.0   NaN                  NaN      NSW Hot Day
            Line          SNSW-CNSW       Max Flow  5150.0   2027-12-02T00:00:00  NaN      NSW Hot Day
            Line          SNSW-CNSW       Min Flow  -2320.0  NaN                  NaN      NSW Hot Day
            Constraint    CNSW-SNW North  RHS       4490.0   NaN                  NaN      NSW Hot Day
    """
    memberships = _query_system_memberships(db, constraint_names)
    membership_ids = memberships["membership_id"]
    data_points = _query_data_points(db, membership_ids)
    data_points = data_points[data_points["property"].isin(_LIMIT_PROPERTIES)]
    dates = _query_dates(db, membership_ids)
    tags = _query_tags(db, membership_ids)
    rows = data_points.merge(memberships, on="membership_id")
    rows = rows.merge(dates, on="data_id", how="left")
    rows = rows.merge(tags, on="data_id", how="left")
    rows = rows.sort_values(
        ["object_class", "object_name", "property", "date_from", "tags"],
        na_position="first",
    )
    return rows.loc[:, _OUTPUT_COLUMNS].reset_index(drop=True)


def _query_system_memberships(
    db: PlexosDB, constraint_names: list[str]
) -> pd.DataFrame:
    """Lists the System memberships of every Line and of the named constraints.

    The limit values hang off the membership joining the System object
    (parent) to the Line or Constraint (child).

    Reads:
        t_membership  -- the System -> object links.
        t_object      -- joined twice: the child for its name, the parent to
                         require it is the System object.
        t_class       -- joined twice, to name the child's class and to
                         require the parent's class is 'System'.

    I/O Example:
        constraint_names: ["CNSW-SNW North"]

        returns:
            membership_id  object_class  object_name
            1523           Line          SNSW-CNSW
            1279           Constraint    CNSW-SNW North
    """
    sql = f"""
        SELECT mem.membership_id, child_cls.name AS object_class,
               child.name AS object_name
        FROM t_membership AS mem
        JOIN t_object AS child      ON mem.child_object_id = child.object_id
        JOIN t_class  AS child_cls  ON child.class_id = child_cls.class_id
        JOIN t_object AS parent     ON mem.parent_object_id = parent.object_id
        JOIN t_class  AS parent_cls ON parent.class_id = parent_cls.class_id
        WHERE parent_cls.name = 'System'
          AND (
            child_cls.name = 'Line'
            OR (
              child_cls.name = 'Constraint'
              AND child.name IN ({_placeholders(len(constraint_names))})
            )
          )
    """
    cols = ["membership_id", "object_class", "object_name"]
    return pd.DataFrame(db.query(sql, tuple(constraint_names)), columns=cols)


if __name__ == "__main__":
    main()
