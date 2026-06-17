"""Extract custom-constraint definitions from an AEMO PLEXOS XML model.

Run manually after AEMO publishes a new PLEXOS model. The output CSVs land
in ``src/ispypsa/templater/plexos/<version>/`` and are consumed by the
ISPyPSA templater at template time.

Usage:
    uv run python scripts/extract_plexos_constraints.py path/to/model.xml --version 7.5

The PLEXOS XML model is published by AEMO alongside each ISP and can be
downloaded from their website -- pass the path to that file as the first
argument.


The PLEXOS data model
=====================

A PLEXOS XML file is a dump of a relational database. Everything in the model
is an *object*; objects are wired together by *memberships*; and numeric
*data* values hang off those memberships. Seven tables matter here -- only
the columns this script uses are listed:

    t_class       The catalogue of object types -- Generator, Battery, Line,
                  Node, Constraint, System, ... One row per type.
                      class_id, name

    t_object      Every object in the model: each generator, each battery,
                  each constraint, the single System object, ...
                      object_id, class_id, name

    t_membership  A directed link joining a parent object to a child object.
                  This is how PLEXOS records "X participates in Y".
                      membership_id, parent_object_id, child_object_id

    t_property    The catalogue of value types -- "Generation Sent Out
                  Coefficient", "RHS", "Sense", "Penalty Price", ...
                      property_id, name

    t_data        A single numeric value: "on membership M, property P has
                  value V". One membership can carry many t_data rows.
                      data_id, membership_id, property_id, value

    t_date_from   Optional sidecars to t_data. Mark a value as effective only
    t_date_to     from / until a given date. Absent for always-on values.
                      data_id, date

    t_tag         Optional sidecar to t_data. Attaches a tag object to a
                  value. PLEXOS uses tags for scenario / timeslice scoping;
                  here the tags are the demand-condition timeslices.
                      data_id, object_id


How a constraint is represented
===============================

A constraint is just an object whose class is "Constraint". Each thing that
participates in it -- every LHS generator / battery / line, plus the System
object that carries the right-hand side and the sense -- is joined to the
constraint by a membership in which the constraint is the *child* and the
participant is the *parent*:

    t_membership.parent_object_id  ->  the participant (Generator, System, ...)
    t_membership.child_object_id   ->  the constraint

The coefficients, RHS and sense are t_data rows hanging off those memberships.

Worked example -- a constraint "ExportGroup_SWQLD1" that reads, in part,
``0.14 * KINGASF1  <=  3000``:

    t_class
      class_id  name
      2         Generator
      78        Constraint
      1         System

    t_object
      object_id  class_id  name
      10         2         KINGASF1
      20         78        ExportGroup_SWQLD1
      30         1         NEM

    t_membership
      membership_id  parent_object_id  child_object_id
      500            10                20
      501            30                20

    t_property
      property_id  name
      44           Generation Sent Out Coefficient
      12           RHS
      13           Sense

    t_data
      data_id  membership_id  property_id  value
      900      500            44           0.14
      901      501            12           3000
      902      501            13           -1

    -> Membership 500 wires generator KINGASF1 into the constraint; its
       t_data row 900 is the 0.14 coefficient. Membership 501 wires in the
       System object; its t_data rows carry the RHS (3000) and the Sense
       (-1, meaning "<="). Sense encoding: -1 "<=", 0 "=", +1 ">=".


The extraction pipeline
=======================

The script resolves the constraints by name, then walks outward one step at a
time -- each step scoped by the ids found in the step before:

    1. names           -> constraint objects     (_query_constraint_objects)
    2. constraint ids  -> their memberships       (_query_memberships)
    3. membership ids  -> the data values on them (_query_data_points)
    4. membership ids  -> effective dates         (_query_dates)
    5. membership ids  -> tags                    (_query_tags)

Steps 1-3 are the spine (one row per value); steps 4-5 enrich it. The five
result frames are stitched into one long table by ``_merge_into_long_table``,
then split into the three output CSVs by ``_split_into_tables``.


Validation
==========

Before the tables are written, the extraction asserts a battery of structural
invariants against the *real* model. Each is a small ``_assert_*`` /
``_check_*`` function whose docstring states the assumption it tests and how.
Together they cover name resolution, structural completeness (a Sense, an RHS
and LHS terms per constraint), reference integrity, value sanity, tag scoping,
and the absence of bands or duplicated data points. Most run on the merged
table, gathered by ``_validate_constraint_rows``; a few need the database and
run earlier, in ``_query_constraint_rows``.

These are deliberately not unit tests on synthetic data -- a synthetic
fixture can only encode the schema assumptions we are trying to test. The
invariants instead check properties that must hold *if* our model of the
PLEXOS schema is correct, so a violation means the model is wrong (or a new
AEMO model has broken an assumption), and the run fails loudly rather than
emitting wrong or incomplete CSVs.


Output schema
=============

Three output tables, all long format:

* ``constraints.csv`` -- the constraint-level properties: every
  System-membership property other than ``RHS`` -- ``Sense``,
  ``Penalty Price`` and ``Include in LT Plan``:
  ``constraint_name, property, value, date_from, date_to, tags``
  (``Sense`` is PLEXOS native: ``-1`` ``<=``, ``0`` ``=``, ``+1`` ``>=``.)

* ``lhs_terms.csv`` -- one row per LHS data point (the non-System
  memberships -- Generator / Battery / Line / Node coefficients):
  ``constraint_name, parent_class, parent_name, property, value, date_from, date_to, tags``

* ``rhs_values.csv`` -- one row per RHS data point. ``RHS`` is a
  System-membership property too, but gets its own table because it is the
  timeslice-varying bound (one tagged row per demand condition):
  ``constraint_name, value, date_from, date_to, tags``

PLEXOS object names, property names, sense encoding, and tags are preserved
verbatim from the XML. The templater is responsible for translating these
into ISPyPSA conventions (timeslice mapping, IASR ID lookup, term-type
classification, etc.).
"""

import argparse
from pathlib import Path

import pandas as pd
import plexosdb.xml_handler
from plexosdb import PlexosDB

# The 15 custom constraints we extract. Fourteen are REZ / sub-region
# export-limit constraints (PLEXOS names them "ExportGroup_<region>"); the
# last is a gas-powered-generation constraint. The list is hardcoded
# deliberately -- the set ISPyPSA needs is small and fixed, and is clearer
# stated explicitly than inferred from the model. Revisit if a new PLEXOS
# model changes the set.
CONSTRAINT_NAMES = [
    "ExportGroup_NQ1",
    "ExportGroup_CQ1",
    "ExportGroup_SQ1",
    "ExportGroup_SEVIC1",
    "ExportGroup_SWV1",
    "ExportGroup_MN1",
    "ExportGroup_NSA1",
    "ExportGroup_NET1",
    "ExportGroup_SWNSW2",
    "ExportGroup_WV1",
    "ExportGroup_CNSW1",
    "ExportGroup_SWQLD1",
    "ExportGroup_WNV1",
    "ExportGroup_SWNSW1",
    "CNSW-SNW South GPG",
]

# Structural invariants asserted against the real model -- see the module
# docstring's "Validation" section and ``_validate_constraint_rows``.
_KNOWN_PROPERTIES = {
    "Generation Sent Out Coefficient",  # Generator LHS coefficient
    "Generation Coefficient",  # Battery LHS coefficient
    "Load Coefficient",  # Battery / Node LHS coefficient
    "Flow Coefficient",  # Line LHS coefficient
    "Installed Capacity Coefficient",  # constraint relaxation variable
    "RHS",  # System: the constraint bound
    "Sense",  # System: the inequality direction
    "Penalty Price",  # System: solver setting
    "Include in LT Plan",  # System: solver setting
}
_KNOWN_PARENT_CLASSES = {"Generator", "Battery", "Line", "Node", "Purchaser", "System"}
_VALID_SENSE_VALUES = {-1, 0, 1}


def main():
    args = _parse_args()
    db = _load_plexos_db(args.xml_path)
    rows = _query_constraint_rows(db, CONSTRAINT_NAMES)
    constraints, lhs, rhs = _split_into_tables(rows)
    _write_csvs(args.out_dir / args.version, constraints, lhs, rhs)


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


def _load_plexos_db(xml_path: str) -> PlexosDB:
    """Load a PLEXOS XML model into an in-memory SQLite database via plexosdb.

    Installs a workaround for AEMO's 20-digit ``t_data.uid`` values, which
    exceed SQLite's 64-bit signed integer range. Without it,
    ``PlexosDB.from_xml`` raises ``OverflowError`` on AEMO files. Reported
    upstream as plexosdb issue #135.
    """
    _install_big_int_workaround()
    return PlexosDB.from_xml(xml_path)


def _install_big_int_workaround() -> None:
    """Patch plexosdb so ``t_data.uid`` values too large for SQLite become strings.

    plexosdb's ``validate_string`` coerces numeric-looking XML text to ``int``.
    AEMO's ``uid`` values have ~20 digits, overflowing SQLite's signed 64-bit
    integer column. We wrap ``validate_string`` to keep such values as strings.
    ``uid`` is not read by this script, so stringifying it is harmless.
    """
    original = plexosdb.xml_handler.validate_string

    def patched(value):
        result = original(value)
        if isinstance(result, int) and abs(result) >= 2**63:
            return str(result)
        return result

    plexosdb.xml_handler.validate_string = patched


def _query_constraint_rows(db: PlexosDB, names: list[str]) -> pd.DataFrame:
    """Build the long-format constraint table via the five-step query pipeline.

    See the module docstring ("The extraction pipeline") for the rationale.
    Each step is scoped by ids carried over from the previous step, so the
    SQL stays simple -- no query repeats the constraint-scoping joins. The
    result is validated throughout (see the module docstring's "Validation"
    section) before being returned.

    I/O Example:
        names: ["ExportGroup_SWQLD1"]

        returns (abbreviated to three of ~80 rows):
            constraint_name      parent_class  parent_name  property   value   date_from  date_to  tags
            ExportGroup_SWQLD1   Generator     KINGASF1     Gen...     0.14    NaN        NaN      NaN
            ExportGroup_SWQLD1   System        NEM          RHS        3000.0  NaN        NaN      QLD Hot Day
            ExportGroup_SWQLD1   System        NEM          Sense      -1.0    NaN        NaN      NaN
    """
    constraints = _query_constraint_objects(db, names)
    _check_constraints_resolved(constraints, names)
    _assert_constraints_are_never_parents(db, constraints["constraint_object_id"])
    memberships = _query_memberships(db, constraints["constraint_object_id"])
    membership_ids = memberships["membership_id"]
    _assert_no_banded_data_points(db, membership_ids)
    data_points = _query_data_points(db, membership_ids)
    dates = _query_dates(db, membership_ids)
    tags = _query_tags(db, membership_ids)
    rows = _merge_into_long_table(constraints, memberships, data_points, dates, tags)
    _assert_merge_is_one_to_one(data_points, rows)
    _validate_constraint_rows(rows)
    return rows


def _placeholders(n: int) -> str:
    """Return an SQL placeholder list ``"?,?,..."`` of length n for an IN clause."""
    return ",".join(["?"] * n)


def _query_constraint_objects(db: PlexosDB, names: list[str]) -> pd.DataFrame:
    """Step 1 -- resolve constraint names to their ``t_object`` rows.

    PLEXOS identifies objects internally by ``object_id``; every later step
    needs those ids, so we first look them up from the human-readable names.

    Reads:
        t_object  -- every object; we keep the ones whose name is in `names`.
        t_class   -- joined only to require the object's class is 'Constraint',
                     so a same-named object of another class can't slip in.

    I/O Example:
        names: ["ExportGroup_SWQLD1"]

        t_object:
            object_id  class_id  name
            20         78        ExportGroup_SWQLD1

        t_class:
            class_id  name
            78        Constraint

        returns:
            constraint_object_id  constraint_name
            20                    ExportGroup_SWQLD1
    """
    sql = f"""
        SELECT obj.object_id AS constraint_object_id, obj.name AS constraint_name
        FROM t_object AS obj
        JOIN t_class  AS cls ON obj.class_id = cls.class_id
        WHERE cls.name = 'Constraint'
          AND obj.name IN ({_placeholders(len(names))})
    """
    cols = ["constraint_object_id", "constraint_name"]
    return pd.DataFrame(db.query(sql, tuple(names)), columns=cols)


def _query_memberships(db: PlexosDB, constraint_object_ids) -> pd.DataFrame:
    """Step 2 -- list every membership wiring an entity into our constraints.

    A constraint's participants are exactly the memberships whose *child* is
    one of the constraint objects from step 1. The *parent* of each such
    membership is a participating entity: an LHS Generator / Battery / Line /
    Node, or the System object that carries the RHS and sense.

    Reads:
        t_membership  -- the links; kept where child_object_id is a constraint.
        t_object      -- LEFT-joined on the parent side for the parent's name.
        t_class       -- LEFT-joined on the parent side for the parent class.

    The object and class joins are LEFT joins on purpose: a broken reference
    then surfaces as a NaN (caught by ``_assert_no_unresolved_references``)
    instead of silently dropping the membership row. The returned
    ``membership_id`` column is the scope for steps 3-5.

    I/O Example:
        constraint_object_ids: [20]   # ExportGroup_SWQLD1

        t_membership:
            membership_id  parent_object_id  child_object_id
            500            10                20
            501            30                20

        t_object:
            object_id  class_id  name
            10         2         KINGASF1
            30         1         NEM

        t_class:
            class_id  name
            2         Generator
            1         System

        returns:
            membership_id  constraint_object_id  parent_class  parent_name
            500            20                    Generator     KINGASF1
            501            20                    System        NEM
    """
    ids = list(constraint_object_ids)
    sql = f"""
        SELECT mem.membership_id,
               mem.child_object_id AS constraint_object_id,
               parent_cls.name     AS parent_class,
               parent.name         AS parent_name
        FROM t_membership AS mem
        LEFT JOIN t_object AS parent     ON mem.parent_object_id = parent.object_id
        LEFT JOIN t_class  AS parent_cls ON parent.class_id = parent_cls.class_id
        WHERE mem.child_object_id IN ({_placeholders(len(ids))})
    """
    cols = ["membership_id", "constraint_object_id", "parent_class", "parent_name"]
    return pd.DataFrame(db.query(sql, tuple(ids)), columns=cols)


def _query_data_points(db: PlexosDB, membership_ids) -> pd.DataFrame:
    """Step 3 -- collect every numeric value attached to the given memberships.

    Each ``t_data`` row is one value -- a coefficient, an RHS, a sense, a
    penalty price. A membership can carry several: the System membership
    carries RHS + Sense + Penalty Price, and a time-varying coefficient adds
    one ``t_data`` row per effective date.

    Reads:
        t_data      -- the values; kept where membership_id is one of ours.
        t_property  -- LEFT-joined to turn property_id into a readable name;
                       LEFT so a dangling property_id surfaces as a NaN
                       (caught by ``_assert_no_unresolved_references``) rather
                       than dropping the data row.

    I/O Example:
        membership_ids: [500, 501]

        t_data:
            data_id  membership_id  property_id  value
            900      500            44           0.14
            901      501            12           3000
            902      501            13           -1

        t_property:
            property_id  name
            44           Generation Sent Out Coefficient
            12           RHS
            13           Sense

        returns:
            data_id  membership_id  property                          value
            900      500            Generation Sent Out Coefficient   0.14
            901      501            RHS                               3000
            902      501            Sense                             -1
    """
    ids = list(membership_ids)
    sql = f"""
        SELECT data.data_id, data.membership_id, prop.name AS property, data.value
        FROM t_data AS data
        LEFT JOIN t_property AS prop ON data.property_id = prop.property_id
        WHERE data.membership_id IN ({_placeholders(len(ids))})
    """
    cols = ["data_id", "membership_id", "property", "value"]
    return pd.DataFrame(db.query(sql, tuple(ids)), columns=cols)


def _query_dates(db: PlexosDB, membership_ids) -> pd.DataFrame:
    """Step 4 -- find effective-date overrides for values on the given memberships.

    Most values are always-effective and have no row in ``t_date_from`` /
    ``t_date_to``. A time-varying value -- e.g. a coefficient that changes
    when a project comes online -- gets a ``t_date_from`` row; ``t_date_to``
    is rarer. Values with neither are excluded here (the WHERE clause) and
    surface as NaN dates after the final merge.

    Reads:
        t_data       -- the spine, so the filter can be by membership_id.
        t_date_from  -- left-joined; supplies date_from where present.
        t_date_to    -- left-joined; supplies date_to where present.

    I/O Example:
        membership_ids: [501]

        t_data:
            data_id  membership_id
            901      501
            902      501

        t_date_from:
            data_id  date
            901      2037-07-01T00:00:00

        t_date_to:
            (no matching rows)

        returns:
            data_id  date_from            date_to
            901      2037-07-01T00:00:00  None

        Data point 902 has no row in either date table, so the WHERE clause
        drops it; it surfaces as NaN dates after the final merge. (In the
        real model ExportGroup_SWQLD1's values are all always-effective --
        this example is illustrative.)
    """
    ids = list(membership_ids)
    sql = f"""
        SELECT data.data_id, dfrom.date AS date_from, dto.date AS date_to
        FROM t_data AS data
        LEFT JOIN t_date_from AS dfrom ON data.data_id = dfrom.data_id
        LEFT JOIN t_date_to   AS dto   ON data.data_id = dto.data_id
        WHERE data.membership_id IN ({_placeholders(len(ids))})
          AND (dfrom.date IS NOT NULL OR dto.date IS NOT NULL)
    """
    cols = ["data_id", "date_from", "date_to"]
    return pd.DataFrame(db.query(sql, tuple(ids)), columns=cols)


def _query_tags(db: PlexosDB, membership_ids) -> pd.DataFrame:
    """Step 5 -- find the tags on the values attached to the given memberships.

    A tag links a ``t_data`` value to a tag *object* (a row in ``t_object``).
    PLEXOS uses tags for scenario / timeslice scoping; for these constraints
    the tags are the demand-condition timeslices -- "QLD Hot Day",
    "QLD Typical Summer", "QLD Winter" and the NSW / VIC / SA / TAS
    equivalents -- one on each region's RHS rows. A value can in principle
    carry more than one tag, so the tag names are ``GROUP_CONCAT``-ed into a
    single '|'-separated string. Values with no tags are excluded and surface
    as NaN after the final merge.

    Reads:
        t_tag     -- the value <-> tag-object links.
        t_data    -- joined as the spine, so the filter can be by membership_id.
        t_object  -- joined to turn the tag's object_id into its name.

    I/O Example:
        membership_ids: [501]

        t_tag:
            data_id  object_id
            901      700

        t_data:
            data_id  membership_id
            901      501

        t_object:
            object_id  name
            700        QLD Hot Day

        returns:
            data_id  tags
            901      QLD Hot Day
    """
    ids = list(membership_ids)
    sql = f"""
        SELECT data.data_id, GROUP_CONCAT(tag_obj.name, '|') AS tags
        FROM t_tag    AS tag
        JOIN t_data   AS data    ON tag.data_id = data.data_id
        JOIN t_object AS tag_obj ON tag.object_id = tag_obj.object_id
        WHERE data.membership_id IN ({_placeholders(len(ids))})
        GROUP BY data.data_id
    """
    return pd.DataFrame(db.query(sql, tuple(ids)), columns=["data_id", "tags"])


def _check_constraints_resolved(constraints: pd.DataFrame, names: list[str]) -> None:
    """Each requested name resolves to exactly one Constraint object.

    Two failure modes break this: a name matching no Constraint object (a typo,
    or a constraint renamed in a new model), and a name matching more than one
    -- a name collision -- which would silently double every row for it.
    Tested against the names ``_query_constraint_objects`` resolved.
    """
    resolved = constraints["constraint_name"]
    missing = sorted(set(names) - set(resolved))
    if missing:
        raise ValueError(f"Constraint names not found in the PLEXOS model: {missing}")
    collisions = sorted(resolved[resolved.duplicated()].unique())
    if collisions:
        raise ValueError(
            f"Constraint names matching more than one object: {collisions}"
        )


def _merge_into_long_table(
    constraints: pd.DataFrame,
    memberships: pd.DataFrame,
    data_points: pd.DataFrame,
    dates: pd.DataFrame,
    tags: pd.DataFrame,
) -> pd.DataFrame:
    """Stitch the five query slices into one row per ``t_data`` entry.

    ``data_points`` is the spine -- one row per value. Each value is joined
    out to its membership (parent entity), to its constraint (name), and then
    left-joined to its optional dates and tags.

    I/O Example:
        data_points:
            data_id  membership_id  property  value
            900      500            Gen...    0.14

        memberships:
            membership_id  constraint_object_id  parent_class  parent_name
            500            20                    Generator     KINGASF1

        constraints:
            constraint_object_id  constraint_name
            20                    ExportGroup_SWQLD1

        dates:
            (no matching rows -- value 900 is always-effective)

        tags:
            (no matching rows -- value 900 is untagged)

        returns:
            constraint_name      parent_class  parent_name  property  value  date_from  date_to  tags
            ExportGroup_SWQLD1   Generator     KINGASF1     Gen...    0.14   NaN        NaN      NaN
    """
    return (
        data_points.merge(memberships, on="membership_id", how="left")
        .merge(constraints, on="constraint_object_id", how="left")
        .merge(dates, on="data_id", how="left")
        .merge(tags, on="data_id", how="left")
        .drop(columns=["membership_id", "data_id", "constraint_object_id"])
        .loc[
            :,
            [
                "constraint_name",
                "parent_class",
                "parent_name",
                "property",
                "value",
                "date_from",
                "date_to",
                "tags",
            ],
        ]
        .sort_values(
            ["constraint_name", "parent_class", "parent_name", "property", "date_from"],
            na_position="first",
        )
        .reset_index(drop=True)
    )


# --- structural validation ---
#
# Each function below asserts one assumption the extraction makes about the
# PLEXOS schema, checked against the real model. Its docstring states the
# assumption and how it is tested; a failure halts the run.


def _validate_constraint_rows(rows: pd.DataFrame) -> None:
    """Run every invariant that can be checked on the assembled long table.

    Two further checks need the database and so run earlier, inside
    ``_query_constraint_rows``: ``_assert_constraints_are_never_parents`` and
    ``_assert_no_banded_data_points``. See the module docstring's "Validation"
    section for why these invariants are not circular.
    """
    # Nothing was structurally lost or left unresolved.
    _assert_no_unresolved_references(rows)
    # Every constraint is complete: a direction, a bound, and LHS terms.
    _assert_one_sense_per_constraint(rows)
    _assert_every_constraint_has_rhs(rows)
    _assert_every_constraint_has_lhs(rows)
    # Every property and participant is one the extraction knows.
    _assert_properties_are_known(rows)
    _assert_parent_classes_are_known(rows)
    # Every value makes sense for its kind.
    _assert_sense_values_are_valid(rows)
    _assert_values_are_numeric(rows)
    # Tags behave as the demand-condition timeslices we take them to be.
    _assert_tags_only_on_rhs(rows)
    _assert_one_tag_per_rhs(rows)
    # No two rows describe the same logical data point.
    _assert_no_duplicate_data_points(rows)


def _assert_no_unresolved_references(rows: pd.DataFrame) -> None:
    """Every membership resolves to a parent object and class, and every data
    point to a property.

    ``_query_memberships`` and ``_query_data_points`` LEFT-join those lookups,
    so a PLEXOS reference pointing at nothing surfaces here as a NaN instead
    of silently dropping the row. Tested by requiring no nulls in the four
    structural columns.
    """
    structural = ["constraint_name", "parent_class", "parent_name", "property"]
    unresolved = sorted(col for col in structural if rows[col].isna().any())
    if unresolved:
        raise ValueError(
            f"Rows with unresolved {unresolved} -- a PLEXOS reference is broken."
        )


def _assert_one_sense_per_constraint(rows: pd.DataFrame) -> None:
    """Each constraint carries exactly one Sense value -- its direction.

    Tested by counting Sense rows per constraint. A count of zero or more than
    one means the System membership was not found, or was found more than once.
    """
    counts = rows.loc[rows["property"] == "Sense", "constraint_name"].value_counts()
    bad = sorted(c for c in set(rows["constraint_name"]) if counts.get(c, 0) != 1)
    if bad:
        raise ValueError(f"Constraints without exactly one Sense row: {bad}")


def _assert_every_constraint_has_rhs(rows: pd.DataFrame) -> None:
    """Each constraint carries at least one RHS value -- its bound.

    Tested by set difference: every constraint name must appear among the
    names on RHS rows. A miss could mean the RHS is inherited from a category
    default, which this direct extraction does not follow.
    """
    with_rhs = set(rows.loc[rows["property"] == "RHS", "constraint_name"])
    missing = sorted(set(rows["constraint_name"]) - with_rhs)
    if missing:
        raise ValueError(f"Constraints with no RHS row: {missing}")


def _assert_every_constraint_has_lhs(rows: pd.DataFrame) -> None:
    """Each constraint carries at least one LHS term -- a non-System row.

    Tested by set difference against the names on non-System rows. A
    constraint with no LHS terms binds nothing, and almost certainly means
    memberships were missed.
    """
    with_lhs = set(rows.loc[rows["parent_class"] != "System", "constraint_name"])
    missing = sorted(set(rows["constraint_name"]) - with_lhs)
    if missing:
        raise ValueError(f"Constraints with no LHS terms: {missing}")


def _assert_properties_are_known(rows: pd.DataFrame) -> None:
    """Every property is one the extraction recognises and routes.

    Tested against ``_KNOWN_PROPERTIES``. An unrecognised property is a new
    kind of value the templater has no handling for -- better to stop than
    route it silently into the wrong table.
    """
    unknown = sorted(set(rows["property"]) - _KNOWN_PROPERTIES)
    if unknown:
        raise ValueError(
            f"Unrecognised PLEXOS properties on the constraints: {unknown}"
        )


def _assert_parent_classes_are_known(rows: pd.DataFrame) -> None:
    """Every participant belongs to a class the extraction handles.

    Tested against ``_KNOWN_PARENT_CLASSES``. An unrecognised class is a new
    kind of LHS participant the templater would not know how to translate.
    """
    unknown = sorted(set(rows["parent_class"]) - _KNOWN_PARENT_CLASSES)
    if unknown:
        raise ValueError(
            f"Unrecognised participant classes on the constraints: {unknown}"
        )


def _assert_sense_values_are_valid(rows: pd.DataFrame) -> None:
    """Sense encodes a direction: -1 (<=), 0 (=) or +1 (>=).

    Tested against ``_VALID_SENSE_VALUES``; any other value would not map to a
    constraint direction downstream.
    """
    seen = set(rows.loc[rows["property"] == "Sense", "value"])
    invalid = sorted(seen - _VALID_SENSE_VALUES)
    if invalid:
        raise ValueError(f"Unexpected Sense values (want -1, 0 or +1): {invalid}")


def _assert_values_are_numeric(rows: pd.DataFrame) -> None:
    """Every data point has a plain numeric value.

    PLEXOS can also store text- or expression-valued data, which would fail in
    the templater far from its cause. Tested by requiring no null values and
    that every value parses as a number.
    """
    if rows["value"].isna().any():
        raise ValueError("Some constraint data points have a null value.")
    non_numeric = pd.to_numeric(rows["value"], errors="coerce").isna()
    if non_numeric.any():
        bad = sorted(set(rows.loc[non_numeric, "property"]))
        raise ValueError(f"Non-numeric values found on properties: {bad}")


def _assert_tags_only_on_rhs(rows: pd.DataFrame) -> None:
    """Tags appear only on RHS rows.

    We read every tag as a demand-condition timeslice, and PLEXOS attaches
    those only to the RHS -- the bound varies by demand condition; the
    coefficients and Sense do not. Tested by requiring every tagged row to be
    an RHS row; a tag elsewhere would be a different kind of tag, e.g. a
    scenario.
    """
    tagged = rows[rows["tags"].notna()]
    non_rhs = sorted(set(tagged.loc[tagged["property"] != "RHS", "property"]))
    if non_rhs:
        raise ValueError(f"Tags found on non-RHS rows (scenario tags?): {non_rhs}")


def _assert_one_tag_per_rhs(rows: pd.DataFrame) -> None:
    """Each RHS value is scoped to exactly one timeslice.

    The templater maps an RHS row's ``tags`` straight to a timeslice, so a
    missing tag, or several joined by '|' (how ``_query_tags`` concatenates
    multiple tags), would break that mapping. Tested over every RHS row.
    """
    rhs_tags = rows.loc[rows["property"] == "RHS", "tags"]
    untagged = int(rhs_tags.isna().sum())
    multi = int(rhs_tags.fillna("").str.contains("|", regex=False).sum())
    if untagged or multi:
        raise ValueError(
            f"RHS rows not scoped to exactly one timeslice: "
            f"{untagged} untagged, {multi} with multiple tags."
        )


def _assert_no_duplicate_data_points(rows: pd.DataFrame) -> None:
    """No two rows describe the same logical data point.

    A duplicate of the natural key -- constraint, participant, property,
    effective date, tag -- means PLEXOS stored more than one value for it,
    which happens with bands or scenarios. This extraction handles neither, so
    a duplicate signals data being silently merged or dropped.
    """
    key = [
        "constraint_name",
        "parent_class",
        "parent_name",
        "property",
        "date_from",
        "date_to",
        "tags",
    ]
    dups = rows[rows.duplicated(subset=key, keep=False)]
    if not dups.empty:
        where = sorted(
            set(zip(dups["constraint_name"], dups["parent_name"], dups["property"]))
        )
        raise ValueError(f"Duplicate data points (bands or scenarios?): {where}")


def _assert_merge_is_one_to_one(data_points: pd.DataFrame, rows: pd.DataFrame) -> None:
    """The merge produces exactly one output row per t_data value.

    ``data_points`` is the merge spine, so a left merge cannot drop a row --
    but it *fans out* if a joined frame has a duplicated key (e.g. a data
    point with two ``t_date_from`` entries). Tested by comparing the row count
    before and after the merge.
    """
    if len(rows) != len(data_points):
        raise ValueError(
            f"Merge changed the row count: {len(data_points)} data points "
            f"became {len(rows)} rows -- a joined frame has a duplicated key."
        )


def _assert_constraints_are_never_parents(db: PlexosDB, constraint_object_ids) -> None:
    """No constraint object sits on the *parent* side of a membership.

    The extraction finds participants by taking the memberships where a
    constraint is the *child* (see ``_query_memberships``). If a constraint
    were ever a parent instead, that membership -- and any data on it --
    would be silently missed. Tested with a direct count over ``t_membership``.
    """
    ids = list(constraint_object_ids)
    sql = f"""
        SELECT COUNT(*) FROM t_membership
        WHERE parent_object_id IN ({_placeholders(len(ids))})
    """
    (count,) = db.query(sql, tuple(ids))[0]
    if count:
        raise ValueError(
            f"{count} membership(s) have a constraint as the parent object; "
            "the extraction assumes a constraint is always the membership child."
        )


def _assert_no_banded_data_points(db: PlexosDB, membership_ids) -> None:
    """None of our data points are split across bands.

    Each value is read as a single ``t_data`` row. PLEXOS can also split a
    value across bands (the ``t_band`` table); the model uses bands elsewhere
    but not on these constraints, and a banded point here would be silently
    reduced to one band. Tested by counting ``t_band`` rows that join to a
    ``t_data`` row on one of our memberships.
    """
    ids = list(membership_ids)
    sql = f"""
        SELECT COUNT(*)
        FROM t_band
        JOIN t_data ON t_band.data_id = t_data.data_id
        WHERE t_data.membership_id IN ({_placeholders(len(ids))})
    """
    (count,) = db.query(sql, tuple(ids))[0]
    if count:
        raise ValueError(
            f"{count} data point(s) on these constraints are banded (t_band); "
            "the extraction reads one value per point and does not handle bands."
        )


def _split_into_tables(rows: pd.DataFrame):
    """Route the long table's rows to the three output tables.

    Every non-System row is an LHS coefficient. System-parent rows hold the
    constraint-level properties: ``RHS`` -- the timeslice-varying bound --
    gets its own table; everything else on the System membership (``Sense``,
    ``Penalty Price``, ``Include in LT Plan``) describes the constraint
    itself and goes to the constraints table.

    Returns:
        A 3-tuple ``(constraints, lhs, rhs)`` -- one DataFrame per output
        CSV, in that order. See the module docstring's "Output schema"
        section for each table's columns.
    """
    is_system = rows["parent_class"] == "System"
    is_rhs = rows["property"] == "RHS"

    lhs = rows[~is_system].reset_index(drop=True)

    rhs = (
        rows[is_system & is_rhs]
        .drop(columns=["parent_class", "parent_name", "property"])
        .reset_index(drop=True)
    )

    constraints = (
        rows[is_system & ~is_rhs]
        .drop(columns=["parent_class", "parent_name"])
        .reset_index(drop=True)
    )
    return constraints, lhs, rhs


def _write_csvs(out_dir, constraints, lhs, rhs):
    out_dir.mkdir(parents=True, exist_ok=True)
    constraints.to_csv(out_dir / "constraints.csv", index=False)
    lhs.to_csv(out_dir / "lhs_terms.csv", index=False)
    rhs.to_csv(out_dir / "rhs_values.csv", index=False)
    print(f"Wrote: {out_dir / 'constraints.csv'} ({len(constraints)} rows)")
    print(f"Wrote: {out_dir / 'lhs_terms.csv'} ({len(lhs)} rows)")
    print(f"Wrote: {out_dir / 'rhs_values.csv'} ({len(rhs)} rows)")


if __name__ == "__main__":
    main()
