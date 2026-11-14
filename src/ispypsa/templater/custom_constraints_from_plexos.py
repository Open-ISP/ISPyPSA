"""Template the three ISPyPSA custom-constraint tables from the PLEXOS extract.

The PLEXOS extract under src/ispypsa/templater/plexos/<version>/ is the
opinionless intermediate produced by scripts/extract_plexos_constraints.py.
This module translates that extract into the custom-constraint shape the rest
of ISPyPSA consumes.

Inputs (CSVs in the PLEXOS extract dir):

* constraints.csv -- one row per (constraint, property); only the Sense
  property is translated.

      constraint_name     property  value
      ExportGroup_SWQLD1  Sense     -1.0

* lhs_terms.csv -- one row per left-hand-side term.

      constraint_name     parent_class  parent_name      property                         value  date_from
      ExportGroup_SWQLD1  Generator     KINGASF1         Generation Sent Out Coefficient  0.14
      ExportGroup_SWQLD1  Battery       Q8 Battery - 2h  Generation Coefficient           0.43

* rhs_values.csv -- one row per right-hand-side value.

      constraint_name     value   date_from  tags
      ExportGroup_SWQLD1  3000.0             QLD Hot Day

Inputs (templated tables): the four generator and storage tables, one row per
unit. They say which units ISPyPSA models, whether each is a generator or
storage, and where the new entrants are.

* generators_existing_planned and storage_existing_planned:

      name
      KINGASF1

* generators_new_entrant and storage_new_entrant (technology is only read
  from the storage table):

      name             technology                      geo_id
      Q8 Battery - 2h  Battery Storage (2hrs storage)  Q8

Outputs (keys of the returned dict): each constraint is a linear inequality,
sum(coefficient * variable) <relation> rhs, and the three tables hold its
three parts.

* custom_constraints -- one row per constraint, carrying the direction
  (the <= / = / >= relation):

      constraint_id  direction
      SWQLD1         <=

* custom_constraints_lhs -- one row per left-hand-side term:

      constraint_id  term_type         variable_name    coefficient  date_from
      SWQLD1         generator_output  KINGASF1         0.14
      SWQLD1         storage_output    Q8 Battery - 2h  0.43

* custom_constraints_rhs -- the right-hand-side limit, which can vary by
  timeslice and over time:

      constraint_id  timeslice        rhs     date_from
      SWQLD1         qld_peak_demand  3000.0

Pipeline:

The custom_constraints and custom_constraints_rhs tables are direct
translations -- strip the ExportGroup_ prefix from the constraint name, map
the PLEXOS sense to a direction symbol, and map each regional RHS tag to a
timeslice (all three mappings are tabulated in the reference sections below).

The custom_constraints_lhs table is the involved part, and is built in two
passes:

* Pass 1 translates the raw PLEXOS LHS rows (generators, batteries, lines,
  nodes) into the ISPyPSA term schema. It maps each row's
  (parent_class, property) to a term_type, resolves each parent_name
  to an ISPyPSA variable_name, and drops any row whose unit has no ISPyPSA
  counterpart. A term that keeps a match *survives* pass 1. Units are matched
  against the templated generator and storage tables (generators_existing_planned,
  generators_new_entrant, storage_existing_planned, storage_new_entrant), so a
  surviving term always names a unit the template contains. Where PLEXOS and
  ISPyPSA disagree about whether a unit is a generator or storage, the term
  takes the type of the table the unit is templated in: Shoalhaven's pumped
  hydro units are PLEXOS generators but ISPyPSA storage, so their terms become
  storage_output.

* Pass 2 gives each constraint a consistent set of new-entrant battery terms,
  in place of a PLEXOS battery formulation that can't be carried across.

  The PLEXOS file describes new-entrant battery participation in a more
  nuanced way than the other units. Some batteries are named for a location,
  as in ISPyPSA (Q8 Battery - 2h). Others are named for the constraint itself
  (SWQLD1 Battery - 2h and SWQLD1 Battery - 8h, and likewise for SQ1, MN1,
  NSA1, NET1, SEVIC1 and SWV1), which we call *constraint-scoped*. In most
  constraints the 4h batteries are left out, with only the 2h and 8h listed.
  A constraint-scoped battery has no counterpart among the ISPyPSA units, and
  the name doesn't say which location's batteries it stands for, so the PLEXOS
  formulation can't be reconstructed in ISPyPSA terms (Open-ISP/ISPyPSA#110).

  So the constraint-scoped batteries are dropped in pass 1, along with every
  other unit that matches no ISPyPSA name, and battery participation is
  rebuilt from the rest of the constraint. Wherever a new-entrant generator or
  storage unit survives pass 1, pass 2 adds every new-entrant battery at that
  unit's location, at the coefficients the batteries that survived there carry
  over time, or at 1.0 where none survived. When a surviving unit brings its
  location's batteries in like this we say it *triggers* that location, and a
  battery's coefficients over time are its *profile*.
  _inject_iasr_new_entrant_batteries carries the detail.

  The result is a set of constraints that treats batteries the same way
  everywhere, but that is known to differ from the PLEXOS implementation:

    - The constraint-scoped batteries are not carried across, and nothing
      replaces them in a constraint where no new-entrant unit survives pass 1.
    - The batteries added are chosen by where the constraint's other
      new-entrant units are, not by anything PLEXOS says about batteries.
    - They take a coefficient from the batteries that survived at their
      location, or 1.0, not the coefficient of a constraint-scoped battery
      (SWQLD1 Battery - 2h is 1.0 in PLEXOS; the batteries added at Q8 are
      0.43).
    - The 4h batteries are included wherever PLEXOS leaves them out.

Vocabulary (terms reused throughout the helpers below):

* *term*: one summand on a constraint's left-hand side -- a (term_type,
  variable_name, coefficient) row in custom_constraints_lhs. The
  variable is a generator, battery, link, or load, and term_type says which.
* *date_from*: the date a coefficient (or rhs value) takes effect, applying from
  then until the next date_from for the same term; a blank date_from
  applies from the start. A term with two date_from rows is therefore a
  single value that steps over time -- e.g. 0.78 until 2031-11-30 and then
  0.00 from 2031-11-30 -- not two competing values.
* *location*: a new-entrant unit's geo_id in the templated tables -- its REZ
  ID, or its sub-region when it has no REZ.
* *survive*: a pass-1 term survives when its PLEXOS unit matched a templated
  unit, so it was not dropped and carries through to the LHS output with its
  PLEXOS coefficient.
* *trigger*: a surviving new-entrant generator or storage unit triggers its
  location. For each triggered location, pass 2 injects a storage_output row
  for every new-entrant battery there, so battery durations PLEXOS omitted
  (e.g. the 4h battery) are re-added.
* *profile*: the set of (date_from, coefficient) rows a single battery
  carries -- its coefficient over time. Pass 2 copies a location's profile from
  the batteries that survived there onto the durations it re-adds, which is
  well-defined only when that location's surviving batteries share one profile.

PLEXOS -> ISPyPSA translations applied here:

* constraint_name -> constraint_id: strip the "ExportGroup_" prefix.
* sense -> direction: -1 => "<=", 0 => "=", +1 => ">=".
* (parent_class, property) -> term_type:
    - Generator + Generation Sent Out Coefficient => generator_output
    - Battery + Generation Coefficient => storage_output
    - Line + Flow Coefficient => link_flow
    - Node + Load Coefficient => load
  A generator or battery term is then given the term_type of the table its
  unit is templated in (generators_* => generator_output, storage_* =>
  storage_output), which overrides the PLEXOS class where the two differ.
* parent_name -> variable_name: PLEXOS units pass through as-is; only
  their names are translated to the ISPyPSA unit names. The name differences
  are all systematic, so matching is deterministic -- no fuzzy matching:
    - Generator / Battery: apply the systematic renames below, then
      match (case-insensitively) against the name column of the four
      templated generator and storage tables. Terms that still don't match
      are dropped.
    - Line: hardcoded line -> path_id table.
    - Node: PLEXOS node names are already ISPyPSA sub-region codes.
* RHS tags -> timeslice: the regional suffix is mapped to the
  ISPyPSA canonical timeslice name and the region prefix is preserved
  lowercased -- "QLD Hot Day" => "qld_peak_demand",
  "VIC Typical Summer" => "vic_summer_typical",
  "TAS Winter" => "tas_winter_reference". The regional weather
  conditions correspond to distinct time periods (see
  data/timeslice_RefYear4006.csv), so the prefix is preserved; the
  suffix uses the codebase-wide _CANONICAL_TIMESLICES vocabulary.

Systematic name renames (PLEXOS -> ISPyPSA). Custom constraints are only
templated at sub_regions granularity (see create_ispypsa_inputs_template),
where a templated unit's name is its IASR ID, so the renames below are stated
in terms of the IASR names:

* Case only (e.g. "North QLD" -> "North Qld") -- handled by the
  case-insensitive match, no rename rule needed.
* Prefix rename: DN1/DN3 (PLEXOS REZ ids) -> DREZ (IASR) for
  both generator and battery names -- a one-to-one naming-style mapping
  where DN1/DN3 are IASR REZ IDs and DREZ is just IASR's naming prefix
  for batteries inside them. The constraint-scoped batteries (prefixes
  SWQLD1, SQ1, MN1, NSA1, NET1, SEVIC1, SWV1) get no rename -- the name
  doesn't say which location's batteries they stand for, so they match no
  ISPyPSA unit and are dropped. Pass 2 does not restore them; it rebuilds
  battery participation from the constraint's other units (see Pipeline).
* Suffix strip: " Area<n>" -- a small set of PLEXOS unit names carry
  this suffix (only Area1 appears in the current PLEXOS extract); IASR
  doesn't carry it. The regex tolerates any \\d+ in case future extracts
  introduce other Area numbers.

Dropped rows:

* Installed Capacity Coefficient properties -- PLEXOS constraint
  relaxation terms (Linear Augmentation / Option generators), not
  operational-constraint LHS terms. [INFO]
* Purchaser parent_class -- hydrogen electrolysers, not modelled. [INFO]
* Battery Load Coefficient rows -- the negative pair of the
  Generation Coefficient row kept as the single storage_output. [INFO]
* Terms whose unit has no ISPyPSA counterpart -- e.g. CER units not yet
  modelled (Open-ISP/ISPyPSA#104), the PLEXOS constraint-scoped batteries
  (see Pipeline, pass 2), or plant absent from the templated generator and
  storage tables. Dropping loosens the constraint, so this is
  logged as a WARNING.

date_to is currently always empty in AEMO's PLEXOS files and is dropped
from the output. The templater raises if a future model starts using it.
date_from is retained so time-varying coefficients carry through to the
output.
"""

from __future__ import annotations

import logging
import re
from importlib.resources import files
from pathlib import Path

import pandas as pd

from .helpers import _is_battery_row
from .mappings import _CANONICAL_TIMESLICES

# The PLEXOS unit-name prefixes DN1 and DN3, which IASR (and so ISPyPSA) spells
# DREZ. DN1 and DN3 are the IASR REZ IDs and DREZ is IASR's naming prefix for
# the units inside those REZs, so this is a one-to-one difference in naming
# style for the same unit, and the renamed unit keeps its PLEXOS coefficients.
# The constraint-scoped batteries (SWQLD1 Battery - 2h etc.) have no such
# rename: their names don't say which location's batteries they stand for, so
# they are dropped (see pass 2 in the module docstring).
#
# Example names matched:
#     "DN1_SAT_Dubbo"             a generator: the prefix, then "_"
#     "DN3 Marulan Battery - 2h"  a battery: the prefix, then " "
# ^(DN1|DN3)  the prefix at the start of the name, replaced by "DREZ"
# (?=[_ ])    the "_" or " " that must follow it; it is left in place, and it
#             stops a longer first token such as "DN10" from matching
_DREZ_PREFIX_PATTERN = re.compile(r"^(DN1|DN3)(?=[_ ])")

# PLEXOS line names -> ISPyPSA path_ids. Lines are network elements that
# don't appear in Summary Mapping, so this table is the source of truth.
# Several PLEXOS lines can map to the same path_id when they represent
# distinct build options for the same conceptual interconnector (e.g.
# Marinus and T-V-MNSP1 are both Tasmania <-> Victoria links).
_LINE_TO_PATH_ID = {
    "CNSW-NNSW": "CNSW-NNSW",
    "CQ-NQ": "CQ-NQ",
    "CSA-NSA": "CSA-NSA",
    "SNSW-CNSW": "SNSW-CNSW",
    "SQ-CQ": "SQ-CQ",
    "NSW1-QLD1": "NSW-QLD",
    "VIC1-NSW1": "WNV-SNSW",
    "V-SA": "WNV-SESA",
    "EnergyConnect": "SNSW-CSA",
    "Marinus": "TAS-SEV",
    "T-V-MNSP1": "TAS-SEV",
}

_PROPERTY_TO_TERM_TYPE = {
    ("Generator", "Generation Sent Out Coefficient"): "generator_output",
    ("Battery", "Generation Coefficient"): "storage_output",
    ("Line", "Flow Coefficient"): "link_flow",
    ("Node", "Load Coefficient"): "load",
}

_SENSE_TO_DIRECTION = {-1: "<=", 0: "=", 1: ">="}

# PLEXOS RHS tag suffix -> ISPyPSA canonical timeslice. The region prefix
# in the tag (e.g. "QLD ") is preserved separately by _tag_to_timeslice.
# Values must remain a subset of _CANONICAL_TIMESLICES.
_REGIONAL_SUFFIX_TO_CANONICAL = {
    "Hot Day": "peak_demand",
    "Typical Summer": "summer_typical",
    "Winter": "winter_reference",
}
assert set(_REGIONAL_SUFFIX_TO_CANONICAL.values()) <= set(_CANONICAL_TIMESLICES)

_CONSTRAINT_NAME_PREFIX = "ExportGroup_"
_EXCLUDED_PARENT_CLASSES = ("Purchaser",)
# The " Area<n>" suffix a few PLEXOS unit names carry and IASR doesn't.
#
# Example names matched:
#     "PV CNSW Area1"
#     "CNSW SAT - Distributed Resources Area1"
# " Area"  the literal suffix, including its leading space
# \d+$     the area number, at the end of the name. Only Area1 appears in the
#          7.5 extract; any number is accepted so a later Area2 is stripped too
_AREA_SUFFIX_PATTERN = re.compile(r" Area\d+$")

# Output columns of the three custom-constraint template tables. Shared by the
# LHS/RHS builders below and empty_custom_constraint_tables() so the header-only
# and populated tables carry identical columns.
_CUSTOM_CONSTRAINTS_COLUMNS = ["constraint_id", "direction"]
_CUSTOM_CONSTRAINTS_LHS_COLUMNS = [
    "constraint_id",
    "term_type",
    "variable_name",
    "coefficient",
    "date_from",
]
_CUSTOM_CONSTRAINTS_RHS_COLUMNS = ["constraint_id", "timeslice", "rhs", "date_from"]


def empty_custom_constraint_tables() -> dict[str, pd.DataFrame]:
    """Header-only versions of the three custom-constraint template tables.

    Emitted at nem_regions / single_region granularity, where the sub-region
    nodes, sub-regional flow paths, and REZ-located units the constraints
    reference are collapsed, leaving nothing to constrain. Emitting "all
    columns, no rows" tables keeps the templater's output set
    granularity-invariant, so downstream consumers never need to check for
    missing tables.
    """
    return {
        "custom_constraints": pd.DataFrame(columns=_CUSTOM_CONSTRAINTS_COLUMNS),
        "custom_constraints_lhs": pd.DataFrame(columns=_CUSTOM_CONSTRAINTS_LHS_COLUMNS),
        "custom_constraints_rhs": pd.DataFrame(columns=_CUSTOM_CONSTRAINTS_RHS_COLUMNS),
    }


def template_custom_constraints_from_plexos(
    generators_existing_planned: pd.DataFrame,
    generators_new_entrant: pd.DataFrame,
    storage_existing_planned: pd.DataFrame,
    storage_new_entrant: pd.DataFrame,
    iasr_workbook_version: str,
    plexos_extract_dir: Path | None = None,
) -> dict[str, pd.DataFrame]:
    """Build the three custom-constraint template tables from the PLEXOS extract.

    Args:
        generators_existing_planned: templated table; its name column lists the
            existing and planned generators a constraint term can refer to.
        generators_new_entrant: templated table; name as above, and geo_id
            gives each new-entrant generator's location for pass 2.
        storage_existing_planned: templated table; its name column lists the
            existing and planned storage units a constraint term can refer to.
        storage_new_entrant: templated table; name as above, with technology
            and geo_id identifying the new-entrant batteries and their
            locations for pass 2.
        iasr_workbook_version: selects the plexos extract subdirectory under
            src/ispypsa/templater/plexos/ (e.g. "7.5"). Ignored when
            plexos_extract_dir is given.
        plexos_extract_dir: optional override for the directory holding the
            PLEXOS extract CSVs (constraints.csv, lhs_terms.csv,
            rhs_values.csv). Used by tests to inject a fixture; in
            production leave unset and the version-based path is used.

    Returns:
        dict with keys "custom_constraints", "custom_constraints_lhs",
        "custom_constraints_rhs", ready to splice into the templater's
        table dict.
    """
    logging.info("Creating a custom-constraints template from the PLEXOS extract")
    extract_dir = plexos_extract_dir or _plexos_extract_dir(iasr_workbook_version)
    constraints = _load_plexos_csv(extract_dir / "constraints.csv")
    lhs_terms = _load_plexos_csv(extract_dir / "lhs_terms.csv")
    rhs_values = _load_plexos_csv(extract_dir / "rhs_values.csv")
    _assert_no_date_to(constraints, "constraints")
    _assert_no_date_to(lhs_terms, "lhs_terms")
    _assert_no_date_to(rhs_values, "rhs_values")
    unit_term_types = _unit_term_types(
        generators_existing_planned,
        generators_new_entrant,
        storage_existing_planned,
        storage_new_entrant,
    )
    tables = {
        "custom_constraints": _build_custom_constraints(constraints),
        "custom_constraints_lhs": _build_custom_constraints_lhs(
            lhs_terms, unit_term_types, generators_new_entrant, storage_new_entrant
        ),
        "custom_constraints_rhs": _build_custom_constraints_rhs(rhs_values),
    }
    _warn_on_constraints_missing_lhs(
        tables["custom_constraints"], tables["custom_constraints_lhs"]
    )
    return tables


def _plexos_extract_dir(version: str) -> Path:
    return files("ispypsa.templater") / "plexos" / version


def _load_plexos_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _assert_no_date_to(df: pd.DataFrame, label: str) -> None:
    """Raise if any row has a non-empty date_to.

    date_to is dropped from the templater output. If AEMO starts using it,
    we want to fail loud rather than silently lose data.
    """
    if "date_to" in df.columns and df["date_to"].notna().any():
        raise ValueError(
            f"PLEXOS extract '{label}' has non-empty date_to values; "
            "the templater currently drops date_to and needs updating."
        )


def _unit_term_types(
    generators_existing_planned: pd.DataFrame,
    generators_new_entrant: pd.DataFrame,
    storage_existing_planned: pd.DataFrame,
    storage_new_entrant: pd.DataFrame,
) -> dict[str, str]:
    """Map every templated unit name to the term_type a constraint term on it
    takes: generator_output for a generator, storage_output for a storage unit.

    The keys are the unit names a PLEXOS term can resolve to; the values say
    which of the two kinds of unit table each one is templated in.

    I/O Example:
        generators_existing_planned:
            name
            BW01

        generators_new_entrant:
            name
            Q8_SAT_Brisbane

        storage_existing_planned:
            name
            SHGEN01             # pumped hydro, templated as storage

        storage_new_entrant:
            name
            Q8 Battery - 2h

        returns:
            {"BW01": "generator_output", "Q8_SAT_Brisbane": "generator_output",
             "SHGEN01": "storage_output", "Q8 Battery - 2h": "storage_output"}
    """
    generators = pd.concat(
        [generators_existing_planned["name"], generators_new_entrant["name"]]
    )
    storage = pd.concat([storage_existing_planned["name"], storage_new_entrant["name"]])
    return {
        **dict.fromkeys(generators, "generator_output"),
        **dict.fromkeys(storage, "storage_output"),
    }


# --- constraints table ---


def _build_custom_constraints(constraints: pd.DataFrame) -> pd.DataFrame:
    """Strip ExportGroup_ prefix from names and map sense to direction symbols.

    The PLEXOS constraints extract is long-format with one row per
    (constraint, property); only the Sense property is translated --
    Penalty Price and Include in LT Plan are PLEXOS solver hints we
    don't model.

    I/O Example:
        constraints:
            constraint_name      property            value
            ExportGroup_SWQLD1   Sense               -1.0
            ExportGroup_SWQLD1   Penalty Price       -1.0    # ignored
            ExportGroup_SWQLD1   Include in LT Plan  -1.0    # ignored
            CNSW-SNW South GPG   Sense               -1.0

        returns:
            constraint_id        direction
            SWQLD1               <=
            CNSW-SNW South GPG   <=
    """
    sense_rows = constraints[constraints["property"] == "Sense"]
    directions = sense_rows["value"].astype(int).map(_SENSE_TO_DIRECTION)
    _raise_on_unmapped_sense(sense_rows, directions)
    return pd.DataFrame(
        {
            "constraint_id": sense_rows["constraint_name"].map(
                _strip_constraint_prefix
            ),
            "direction": directions,
        }
    ).reset_index(drop=True)


def _raise_on_unmapped_sense(sense_rows: pd.DataFrame, directions: pd.Series) -> None:
    """Raise if any Sense value has no direction mapping -- like an unmapped
    (parent_class, property) pair, this signals a new PLEXOS encoding the
    templater needs to learn about, and must not pass through as a NaN
    direction silently.
    """
    unmapped = sense_rows[directions.isna()]
    if not unmapped.empty:
        pairs = sorted(set(zip(unmapped["constraint_name"], unmapped["value"])))
        raise ValueError(f"PLEXOS Sense values with no direction mapping: {pairs}")


def _strip_constraint_prefix(name: str) -> str:
    """Remove the ExportGroup_ namespace prefix if present.

    I/O Example:
        "ExportGroup_SWQLD1"   -> "SWQLD1"
        "CNSW-SNW South GPG"   -> "CNSW-SNW South GPG"  # passes through
    """
    if name.startswith(_CONSTRAINT_NAME_PREFIX):
        return name[len(_CONSTRAINT_NAME_PREFIX) :]
    return name


# --- LHS table ---


def _build_custom_constraints_lhs(
    lhs_terms: pd.DataFrame,
    unit_term_types: dict[str, str],
    generators_new_entrant: pd.DataFrame,
    storage_new_entrant: pd.DataFrame,
) -> pd.DataFrame:
    """Filter, collapse the battery Load/Generation pairs, and translate the
    raw LHS rows into ISPyPSA's term schema, then inject new-entrant
    batteries for triggered REZ/sub-regions.

    I/O Example:
        lhs_terms (abbreviated):
            constraint_name      parent_class  parent_name           property               value
            ExportGroup_SWQLD1   Generator     Q8_SAT_Brisbane       Generation Sent Out C   0.14   # new entrant in REZ Q8 -> triggers Q8
            ExportGroup_SWQLD1   Generator     SHGEN01               Generation Sent Out C   0.33   # templated as storage -> storage_output
            ExportGroup_SWQLD1   Line          NSW1-QLD1             Flow Coefficient        0.84
            ExportGroup_SWQLD1   Battery       Q8 Battery - 2h       Generation Coefficient  0.43   # matches a templated unit -> survives pass 1
            ExportGroup_SWQLD1   Battery       SWQLD1 Battery - 2h   Generation Coefficient  1.0    # dropped (constraint-scoped, no templated unit)
            ExportGroup_SWQLD1   Battery       SWQLD1 Battery - 2h   Load Coefficient       -1.0    # dropped (Load pair)

        unit_term_types:
            {"Q8_SAT_Brisbane": "generator_output", "SHGEN01": "storage_output",
             "Q8 Battery - 2h": "storage_output", "Q8 Battery - 4h": "storage_output"}

        generators_new_entrant (abbreviated):
            name             geo_id
            Q8_SAT_Brisbane  Q8

        storage_new_entrant (abbreviated):
            name             technology                      geo_id
            Q8 Battery - 2h  Battery Storage (2hrs storage)  Q8
            Q8 Battery - 4h  Battery Storage (4hrs storage)  Q8

        returns:
            constraint_id  term_type        variable_name    coefficient  date_from
            SWQLD1         generator_output Q8_SAT_Brisbane   0.14
            SWQLD1         storage_output   SHGEN01           0.33
            SWQLD1         link_flow        NSW-QLD           0.84
            SWQLD1         storage_output   Q8 Battery - 2h   0.43          # survived pass 1 (pass-2 re-injection deduped)
            SWQLD1         storage_output   Q8 Battery - 4h   0.43          # injected; copies the 2h sibling's coefficient
    """
    lhs = _drop_excluded_classes(lhs_terms)
    lhs = _drop_constraint_relaxation_terms(lhs)
    lhs = _drop_battery_load_coefficient_rows(lhs)
    lhs = _add_term_type_column(lhs)
    lhs = _add_variable_name_column(lhs, set(unit_term_types))
    lhs = _drop_unresolved_terms(lhs)
    lhs = _set_unit_term_types_from_templated_tables(lhs, unit_term_types)
    lhs = lhs.rename(columns={"value": "coefficient"})
    lhs["constraint_id"] = lhs["constraint_name"].map(_strip_constraint_prefix)
    lhs = lhs[_CUSTOM_CONSTRAINTS_LHS_COLUMNS]
    lhs = _inject_iasr_new_entrant_batteries(
        lhs, generators_new_entrant, storage_new_entrant
    )
    lhs = _dedupe_lhs_terms(lhs)
    return lhs.reset_index(drop=True)


def _drop_excluded_classes(lhs: pd.DataFrame) -> pd.DataFrame:
    """Drop rows whose parent_class is in the excluded set (e.g. Purchaser).

    I/O Example:
        lhs:
            parent_class  parent_name
            Generator     KINGASF1
            Purchaser     Some Hydrogen Electrolyser

        returns:
            parent_class  parent_name
            Generator     KINGASF1
    """
    excluded_mask = lhs["parent_class"].isin(_EXCLUDED_PARENT_CLASSES)
    if excluded_mask.any():
        dropped = sorted(set(lhs.loc[excluded_mask, "parent_name"]))
        logging.info(
            f"Dropped {excluded_mask.sum()} LHS rows for excluded parent classes "
            f"{sorted(_EXCLUDED_PARENT_CLASSES)}: parents={dropped}"
        )
    return lhs[~excluded_mask].copy()


def _drop_constraint_relaxation_terms(lhs: pd.DataFrame) -> pd.DataFrame:
    """Drop Installed Capacity Coefficient rows -- PLEXOS constraint
    relaxation terms (Linear Augmentation, named Option, etc.) attached to
    each export constraint, not LHS terms for the operational constraint.

    I/O Example:
        lhs:
            parent_class  parent_name                   property
            Generator     KINGASF1                      Generation Sent Out Coefficient
            Generator     SWQLD1_Linear Augmentation 1  Installed Capacity Coefficient
            Generator     MN1_CSA-NSA Option 1 Augmen   Installed Capacity Coefficient

        returns:
            parent_class  parent_name                   property
            Generator     KINGASF1                      Generation Sent Out Coefficient
    """
    drop_mask = lhs["property"] == "Installed Capacity Coefficient"
    if drop_mask.any():
        dropped = sorted(set(lhs.loc[drop_mask, "parent_name"]))
        logging.info(
            f"Dropped {drop_mask.sum()} constraint relaxation LHS rows "
            f"(PLEXOS Installed Capacity Coefficient): {dropped}"
        )
    return lhs[~drop_mask].copy()


def _drop_battery_load_coefficient_rows(lhs: pd.DataFrame) -> pd.DataFrame:
    """Drop battery Load Coefficient rows -- they are the negative-magnitude
    pair of Generation Coefficient rows that survive as the single
    storage_output term.

    Raises if a Load row has no Generation pair for the same (constraint,
    battery): dropping it would silently delete the battery's only LHS term
    rather than a redundant negative pair.

    I/O Example:
        lhs:
            parent_class  parent_name    property                value
            Battery       Tarong BESS    Generation Coefficient  0.14
            Battery       Tarong BESS    Load Coefficient       -0.14

        returns:
            parent_class  parent_name    property                value
            Battery       Tarong BESS    Generation Coefficient  0.14
    """
    drop_mask = (lhs["parent_class"] == "Battery") & (
        lhs["property"] == "Load Coefficient"
    )
    _raise_on_unpaired_battery_load_rows(lhs, drop_mask)
    if drop_mask.any():
        logging.info(
            f"Dropped {drop_mask.sum()} battery Load Coefficient LHS rows "
            "(negative pairs of the kept Generation Coefficient rows)"
        )
    return lhs[~drop_mask].copy()


def _raise_on_unpaired_battery_load_rows(
    lhs: pd.DataFrame, load_mask: pd.Series
) -> None:
    """Raise if a battery Load Coefficient row has no Generation Coefficient
    row for the same (constraint, battery) -- a load-only battery term that
    dropping would silently lose, loosening the constraint.
    """
    generation_mask = (lhs["parent_class"] == "Battery") & (
        lhs["property"] == "Generation Coefficient"
    )
    generation_keys = set(
        zip(
            lhs.loc[generation_mask, "constraint_name"],
            lhs.loc[generation_mask, "parent_name"],
        )
    )
    load_keys = set(
        zip(lhs.loc[load_mask, "constraint_name"], lhs.loc[load_mask, "parent_name"])
    )
    unpaired = sorted(
        f"{_strip_constraint_prefix(constraint)}: {name}"
        for constraint, name in load_keys - generation_keys
    )
    if unpaired:
        raise ValueError(
            "Battery Load Coefficient rows with no Generation Coefficient pair "
            f"(dropping them would lose the battery's only LHS term): {unpaired}"
        )


def _add_term_type_column(lhs: pd.DataFrame) -> pd.DataFrame:
    """Map each (parent_class, property) pair to an ISPyPSA term_type string.

    Raises ValueError if any pair in the input has no mapping defined --
    this signals a new PLEXOS property the templater needs to learn about.

    I/O Example:
        lhs:
            parent_class  property
            Generator     Generation Sent Out Coefficient
            Battery       Generation Coefficient
            Line          Flow Coefficient

        returns: same rows with extra column
            term_type
            generator_output
            storage_output
            link_flow
    """
    pairs = list(zip(lhs["parent_class"], lhs["property"]))
    lhs = lhs.copy()
    lhs["term_type"] = [_PROPERTY_TO_TERM_TYPE.get(p) for p in pairs]
    _raise_on_unmapped_term_type(lhs)
    return lhs


def _raise_on_unmapped_term_type(lhs: pd.DataFrame) -> None:
    unmapped = lhs[lhs["term_type"].isna()]
    if not unmapped.empty:
        pairs = sorted(set(zip(unmapped["parent_class"], unmapped["property"])))
        raise ValueError(
            f"PLEXOS (parent_class, property) pairs with no term_type mapping: {pairs}"
        )


def _add_variable_name_column(lhs: pd.DataFrame, unit_names: set[str]) -> pd.DataFrame:
    """Add variable_name: the ISPyPSA name of the component each LHS term is on.

    How a PLEXOS parent_name becomes a variable_name depends on its
    parent_class:

    - Generator and Battery: renamed to the ISPyPSA naming style, then matched
      to unit_names, the names of the templated generators and storage units.
      A unit with no match is left as NaN, and dropped downstream.
    - Line: looked up in the fixed line -> path_id table.
    - Node: kept as is; PLEXOS node names are already ISPyPSA sub-region codes.

    I/O Example:
        lhs (abbreviated):
            parent_class  parent_name
            Generator     DN1_SAT_Dubbo
            Generator     SA Hydrogen Turbine
            Battery       SWQLD1 Battery - 2h
            Line          NSW1-QLD1
            Node          CNSW

        unit_names: {"DREZ_SAT_Dubbo"}

        returns:
            parent_class  parent_name          variable_name
            Generator     DN1_SAT_Dubbo        DREZ_SAT_Dubbo
            Generator     SA Hydrogen Turbine  NaN             # no templated unit
            Battery       SWQLD1 Battery - 2h  NaN             # constraint-scoped
            Line          NSW1-QLD1            NSW-QLD
            Node          CNSW                 CNSW
    """
    _raise_on_unexpected_parent_class(lhs)
    is_unit = lhs["parent_class"].isin(["Generator", "Battery"])
    is_line = lhs["parent_class"] == "Line"
    is_node = lhs["parent_class"] == "Node"
    plexos_names = lhs["parent_name"]
    lhs = lhs.copy()
    lhs["variable_name"] = None
    lhs.loc[is_unit, "variable_name"] = _match_to_templated_unit_names(
        plexos_names[is_unit], unit_names
    )
    lhs.loc[is_line, "variable_name"] = _line_names_to_path_ids(plexos_names[is_line])
    lhs.loc[is_node, "variable_name"] = plexos_names[is_node]
    _log_name_renames(lhs)
    return lhs


def _raise_on_unexpected_parent_class(lhs: pd.DataFrame) -> None:
    """Raise if a term's parent_class is one this module has no naming rule for."""
    expected = {"Generator", "Battery", "Line", "Node"}
    unexpected = sorted(set(lhs["parent_class"]) - expected)
    if unexpected:
        raise ValueError(f"Unexpected LHS parent_class values: {unexpected}")


def _match_to_templated_unit_names(
    plexos_names: pd.Series, unit_names: set[str]
) -> pd.Series:
    """Translate PLEXOS generator and battery names to templated unit names,
    giving NaN where there is no such unit.

    The names are first rewritten in the ISPyPSA naming style: the DN1/DN3
    prefix becomes DREZ and the " Area<n>" suffix is removed. A rewritten name
    that is itself a templated unit is kept as it is. Otherwise it is matched
    ignoring case, because PLEXOS and IASR capitalise some names differently,
    and takes the templated spelling.

    I/O Example:
        plexos_names:
            parent_name
            DN1_SAT_Dubbo
            DN3 Marulan Battery - 8h
            CNSW SAT - Distributed Resources Area1
            Q2_CST_North QLD Clean Energy Hub
            BW01
            SWQLD1 Battery - 2h
            Cultana Solar Farm

        unit_names:
            {"DREZ_SAT_Dubbo", "DREZ Marulan Battery - 8h",
             "CNSW SAT - Distributed Resources",
             "Q2_CST_North Qld Clean Energy Hub", "BW01"}

        returns:
            parent_name
            DREZ_SAT_Dubbo                      # prefix renamed
            DREZ Marulan Battery - 8h           # prefix renamed
            CNSW SAT - Distributed Resources    # Area suffix removed
            Q2_CST_North Qld Clean Energy Hub   # matched ignoring case
            BW01                                # already a templated unit
            NaN                                 # constraint-scoped: no rename, no unit
            NaN                                 # no templated unit
    """
    names = plexos_names.str.replace(_DREZ_PREFIX_PATTERN, "DREZ", regex=True)
    names = names.str.replace(_AREA_SUFFIX_PATTERN, "", regex=True)
    templated_name_by_lowercase = {name.lower(): name for name in unit_names}
    case_insensitive_matches = names.str.lower().map(templated_name_by_lowercase)
    return names.where(names.isin(unit_names), case_insensitive_matches)


def _line_names_to_path_ids(line_names: pd.Series) -> pd.Series:
    """Look up the ISPyPSA path_id for each PLEXOS line name.

    Raises for a line with no entry in _LINE_TO_PATH_ID, which means the
    PLEXOS extract has a line this module hasn't been told about.

    I/O Example:
        "NSW1-QLD1"  -> "NSW-QLD"
        "V-SA"       -> "WNV-SESA"
        "SQ-CQ"      -> "SQ-CQ"
    """
    unmapped = sorted(set(line_names) - set(_LINE_TO_PATH_ID))
    if unmapped:
        raise ValueError(
            f"PLEXOS line names not in _LINE_TO_PATH_ID -- add a mapping: {unmapped}"
        )
    return line_names.map(_LINE_TO_PATH_ID)


def _log_name_renames(lhs: pd.DataFrame) -> None:
    """Log every PLEXOS -> ISPyPSA name rename applied, so they can be audited."""
    renamed = lhs[
        lhs["variable_name"].notna() & (lhs["variable_name"] != lhs["parent_name"])
    ]
    pairs = sorted(set(zip(renamed["parent_name"], renamed["variable_name"])))
    if pairs:
        renames = [f"{old} -> {new}" for old, new in pairs]
        logging.info(
            f"Applied {len(pairs)} PLEXOS->ISPyPSA LHS name renames: {renames}"
        )


def _drop_unresolved_terms(lhs: pd.DataFrame) -> pd.DataFrame:
    """Drop LHS terms whose parent_name resolved to no ISPyPSA unit.

    These are PLEXOS units with no ISPyPSA counterpart -- e.g. CER units not
    yet modelled (Open-ISP/ISPyPSA#104) or plant absent from the templated
    generator and storage tables.
    Dropping loosens the constraint, so it is logged as a WARNING.

    I/O Example:
        lhs:
            constraint_name   parent_name          variable_name
            ExportGroup_NSA1  SA Hydrogen Turbine  NaN
            ExportGroup_NSA1  KINGASF1             KINGASF1

        returns:
            constraint_name   parent_name          variable_name
            ExportGroup_NSA1  KINGASF1             KINGASF1
    """
    unresolved = lhs["variable_name"].isna()
    if unresolved.any():
        dropped = sorted(
            set(
                f"{_strip_constraint_prefix(constraint)}: {name}"
                for constraint, name in zip(
                    lhs.loc[unresolved, "constraint_name"],
                    lhs.loc[unresolved, "parent_name"],
                )
            )
        )
        logging.warning(
            f"Dropped {unresolved.sum()} LHS term rows ({len(dropped)} distinct "
            f"units) with no ISPyPSA unit match: {dropped}"
        )
    return lhs[~unresolved].copy()


def _set_unit_term_types_from_templated_tables(
    lhs: pd.DataFrame, unit_term_types: dict[str, str]
) -> pd.DataFrame:
    """Give each generator or battery term the term_type of the table its unit
    is templated in, overriding the one implied by its PLEXOS class.

    PLEXOS and ISPyPSA mostly agree on which units are generators and which are
    storage. Where they differ, the term has to follow ISPyPSA: a
    generator_output term on a unit the model builds as storage would name a
    generator that doesn't exist. Shoalhaven's pumped hydro units are the case
    in the 7.5 extract -- PLEXOS generators, templated as storage. A
    storage_output term is the unit's net dispatch (discharging minus
    charging), which is how PLEXOS's own battery-class terms are read here.
    Line and node terms are left as they are.

    I/O Example:
        lhs (abbreviated):
            constraint_name  term_type         variable_name
            X                generator_output  BW01
            X                generator_output  SHGEN01          # templated as storage
            X                storage_output    Q8 Battery - 2h
            X                load              CNSW             # not a unit term

        unit_term_types:
            {"BW01": "generator_output", "SHGEN01": "storage_output",
             "Q8 Battery - 2h": "storage_output"}

        returns:
            constraint_name  term_type         variable_name
            X                generator_output  BW01
            X                storage_output    SHGEN01
            X                storage_output    Q8 Battery - 2h
            X                load              CNSW
    """
    is_unit_term = lhs["term_type"].isin(["generator_output", "storage_output"])
    templated_types = lhs["variable_name"].map(unit_term_types).where(is_unit_term)
    _log_changed_unit_term_types(lhs, templated_types)
    lhs = lhs.copy()
    lhs["term_type"] = templated_types.fillna(lhs["term_type"])
    return lhs


def _log_changed_unit_term_types(lhs: pd.DataFrame, templated_types: pd.Series) -> None:
    """Log the units whose term_type was changed to match the templated tables."""
    changed = templated_types.notna() & (templated_types != lhs["term_type"])
    unit = lhs.loc[changed, "variable_name"]
    old_type = lhs.loc[changed, "term_type"]
    new_type = templated_types[changed]
    changes = sorted(set(unit + ": " + old_type + " -> " + new_type))
    if changes:
        logging.info(
            f"Changed the term_type of {changed.sum()} LHS term rows to match the "
            f"table their unit is templated in: {changes}"
        )


# --- pass 2: inject new-entrant batteries by triggered REZ/sub-region ---


def _inject_iasr_new_entrant_batteries(
    lhs: pd.DataFrame,
    generators_new_entrant: pd.DataFrame,
    storage_new_entrant: pd.DataFrame,
) -> pd.DataFrame:
    """Append a storage_output row for every new-entrant battery in
    each REZ/sub-region whose new-entrant units participate in the constraint.

    For each generator_output or storage_output term whose
    variable_name is a new-entrant unit, look up that unit's location -- its
    geo_id -- and add storage_output rows
    for every new-entrant battery at that same location. A surviving battery
    triggers its location just as a surviving generator does, so a location
    that PLEXOS represents through batteries alone still has its omitted
    durations (e.g. the 4h battery) re-added.
    Each injected battery copies
    the coefficient (and time-varying date_from rows) carried by the
    new-entrant batteries that survived pass 1 at that location: PLEXOS applies
    a per-unit export-loss coefficient and omits the 4h-duration batteries, but
    the new-entrant batteries at a location share one value (e.g. 0.43 at
    SWQLD1's Q8), so re-adding the omitted durations at the siblings'
    coefficient keeps the whole location consistent. Where no new-entrant
    battery survived pass 1 at the location, the coefficient defaults to 1.0
    with empty date_from.

    Only batteries are injected. The other new-entrant storage technologies
    (pumped hydro) can trigger a location but are not added to it.

    Every battery at a triggered location is appended, including any that
    already survived pass 1, so the result can contain a duplicate that
    _dedupe_lhs_terms removes downstream.

    See Open-ISP/ISPyPSA#110 for the PLEXOS battery layout that motivated
    dropping the pass-1 constraint-scoped variants.

    I/O Example:
        lhs (after pass 1, abbreviated):
            constraint_id  term_type        variable_name    coefficient
            SWQLD1         generator_output Q8_SAT_Brisbane  0.43
            SWQLD1         storage_output   Q8 Battery - 2h  0.43   # survived pass 1
            NQ1            generator_output Q1_WH_Cairns     1.0

        generators_new_entrant (abbreviated):
            name             geo_id
            Q8_SAT_Brisbane  Q8
            Q1_WH_Cairns     Q1

        storage_new_entrant (abbreviated):
            name                   technology                      geo_id
            Q8 Battery - 2h        Battery Storage (2hrs storage)  Q8
            Q8 Battery - 4h        Battery Storage (4hrs storage)  Q8
            Q1 Battery - 2h        Battery Storage (2hrs storage)  Q1
            Q1 Pumped Hydro - 10h  Pumped Hydro (10hrs storage)    Q1    # not a battery, never injected

        returns (these rows appended after the unchanged pass-1 lhs rows):
            constraint_id  term_type        variable_name    coefficient  date_from
            SWQLD1         storage_output   Q8 Battery - 2h  0.43          # re-injected duplicate of the survivor; _dedupe_lhs_terms drops it downstream
            SWQLD1         storage_output   Q8 Battery - 4h  0.43          # PLEXOS-omitted duration, copies the 2h sibling's coefficient
            NQ1            storage_output   Q1 Battery - 2h  1.0           # no surviving sibling -> default
    """
    unit_to_location = _new_entrant_to_location(
        generators_new_entrant, storage_new_entrant
    )
    batteries_by_location = _batteries_by_location(storage_new_entrant)
    triggered = _triggered_locations_per_constraint(lhs, unit_to_location)
    coefficients = _surviving_battery_coefficients(lhs, storage_new_entrant)
    injected = _battery_rows_for_triggers(
        triggered, batteries_by_location, coefficients
    )
    _log_injected_batteries(injected)
    _warn_on_default_battery_coefficients(
        triggered, coefficients, batteries_by_location
    )
    if injected.empty:
        return lhs
    return pd.concat([lhs, injected], ignore_index=True)


def _warn_on_default_battery_coefficients(
    triggered: pd.DataFrame,
    coefficients: pd.DataFrame,
    batteries_by_location: dict[str, list[str]],
) -> None:
    """Warn when injected batteries fall back to the default 1.0 coefficient.

    Fires for triggered locations that hold new-entrant batteries but have no
    surviving sibling in coefficients to copy from, so the injection
    defaults to 1.0. On a fractional-loss location that default is likely wrong
    (cf. Open-ISP/ISPyPSA#110), and -- unlike an adopted coefficient -- it isn't
    distinguishable from a correct 1.0 in the output, so the pairs are surfaced
    for audit as one sorted line.
    """
    with_profile = set(map(tuple, coefficients[["constraint_id", "location"]].values))
    injecting = triggered[triggered["location"].isin(batteries_by_location)]
    pairs = set(map(tuple, injecting[["constraint_id", "location"]].values))
    defaulted = sorted(f"{cid}: {loc}" for cid, loc in pairs - with_profile)
    if defaulted:
        logging.warning(
            "New-entrant batteries injected with default coefficient 1.0 "
            f"(no surviving sibling to copy from): {defaulted}"
        )


def _new_entrant_to_location(
    generators_new_entrant: pd.DataFrame, storage_new_entrant: pd.DataFrame
) -> dict[str, str]:
    """Build a {unit name: geo_id} lookup covering every new-entrant generator
    and storage unit.

    I/O Example:
        generators_new_entrant (abbreviated):
            name             geo_id
            Q8_SAT_Brisbane  Q8
            CSA CCGT         CSA       # no REZ, so its geo_id is the sub-region

        storage_new_entrant (abbreviated):
            name                   geo_id
            Q8 Battery - 2h        Q8
            Q8 Pumped Hydro - 10h  Q8

        returns:
            {"Q8_SAT_Brisbane": "Q8", "CSA CCGT": "CSA",
             "Q8 Battery - 2h": "Q8", "Q8 Pumped Hydro - 10h": "Q8"}
    """
    new_entrants = pd.concat(
        [
            generators_new_entrant[["name", "geo_id"]],
            storage_new_entrant[["name", "geo_id"]],
        ]
    )
    return dict(zip(new_entrants["name"], new_entrants["geo_id"]))


def _batteries_by_location(storage_new_entrant: pd.DataFrame) -> dict[str, list[str]]:
    """Build a {geo_id: [battery names]} lookup for new-entrant batteries.

    I/O Example:
        storage_new_entrant (abbreviated):
            name                   technology                      geo_id
            Q8 Battery - 2h        Battery Storage (2hrs storage)  Q8
            Q8 Battery - 4h        Battery Storage (4hrs storage)  Q8
            CSA Battery - 2h       Battery Storage (2hrs storage)  CSA
            Q8 Pumped Hydro - 10h  Pumped Hydro (10hrs storage)    Q8     # not a battery

        returns:
            {"Q8":  ["Q8 Battery - 2h", "Q8 Battery - 4h"],
             "CSA": ["CSA Battery - 2h"]}
    """
    batteries = storage_new_entrant[_is_battery_row(storage_new_entrant, "technology")]
    return (
        batteries.groupby("geo_id")["name"]
        .apply(lambda s: sorted(s.unique()))
        .to_dict()
    )


def _battery_to_location(storage_new_entrant: pd.DataFrame) -> dict[str, str]:
    """Build a {battery name: geo_id} lookup for new-entrant batteries -- the
    inverse of _batteries_by_location.

    I/O Example:
        storage_new_entrant (abbreviated):
            name                   technology                      geo_id
            Q8 Battery - 2h        Battery Storage (2hrs storage)  Q8
            CSA Battery - 2h       Battery Storage (2hrs storage)  CSA
            Q8 Pumped Hydro - 10h  Pumped Hydro (10hrs storage)    Q8     # not a battery

        returns:
            {"Q8 Battery - 2h": "Q8", "CSA Battery - 2h": "CSA"}
    """
    batteries = storage_new_entrant[_is_battery_row(storage_new_entrant, "technology")]
    return dict(zip(batteries["name"], batteries["geo_id"]))


def _triggered_locations_per_constraint(
    lhs: pd.DataFrame, unit_to_location: dict[str, str]
) -> pd.DataFrame:
    """Find the distinct {constraint_id, location} pairs implied by the
    surviving new-entrant generator and storage terms.

    Units whose name isn't in unit_to_location (existing and planned plant)
    don't trigger; only new-entrant units drive the injection. A surviving battery triggers its location just as a surviving
    generator does, so a location whose generators were all dropped (or that
    PLEXOS represents through batteries alone) still has its omitted battery
    durations re-added.

    I/O Example:
        lhs (abbreviated):
            constraint_id  term_type        variable_name
            SWQLD1         generator_output Q8_SAT_Brisbane     # triggers Q8
            SWQLD1         generator_output Mt Piper            # existing, no trigger
            SWQLD1         link_flow        NSW-QLD             # not a unit term
            NQ1            storage_output   Q1 Battery - 2h     # battery triggers Q1

        unit_to_location: {"Q8_SAT_Brisbane": "Q8", "Q1 Battery - 2h": "Q1"}

        returns:
            constraint_id  location
            SWQLD1         Q8
            NQ1            Q1
    """
    units = lhs[lhs["term_type"].isin(["generator_output", "storage_output"])].copy()
    units["location"] = units["variable_name"].map(unit_to_location)
    return (
        units.dropna(subset=["location"])[["constraint_id", "location"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )


def _surviving_battery_coefficients(
    lhs: pd.DataFrame, storage_new_entrant: pd.DataFrame
) -> pd.DataFrame:
    """The coefficient *profile* -- the (date_from, coefficient) rows -- that
    the pass-1 new-entrant batteries surviving in each (constraint_id, location)
    carry, returned once per location for pass 2 to copy.

    PLEXOS applies a per-unit export-loss coefficient and drops the 4h-duration
    batteries, so pass 2 re-adds those dropped durations by copying a surviving
    battery's profile rather than defaulting to 1.0. That copy needs each
    (constraint, location) to expose a single profile, which holds because the
    surviving batteries at a location share one (e.g. 0.43 at SWQLD1's Q8, even
    though the co-located generators may each differ -- 0.14/0.43/1.0 there). A
    profile can still span several dates if the coefficient steps over time; the
    requirement is only that the location's surviving batteries share the same
    profile as each other, which _raise_on_inconsistent_battery_profiles
    enforces (generators are deliberately not checked: they keep their own
    per-unit coefficients). Existing-plant batteries carry no new-entrant
    location and are ignored, as are storage_output terms on new-entrant
    storage that isn't a battery (pumped hydro).

    I/O Example:
        lhs (storage_output rows, abbreviated):
            constraint_id  term_type       variable_name    coefficient  date_from
            SWQLD1         storage_output  Q8 Battery - 2h  0.43
            SWQLD1         storage_output  Q8 Battery - 8h  0.43
            WNV1           storage_output  V7 Battery - 2h  0.78                     # 0.78 until 2031-11-30 ...
            WNV1           storage_output  V7 Battery - 2h  0.00         2031-11-30  # ... then 0.00 -- one battery's stepped profile, not a conflict
            SWQLD1         storage_output  Tarong BESS      0.14         # existing -> no location, ignored

        storage_new_entrant (abbreviated):
            name             technology                      geo_id
            Q8 Battery - 2h  Battery Storage (2hrs storage)  Q8
            Q8 Battery - 8h  Battery Storage (8hrs storage)  Q8
            V7 Battery - 2h  Battery Storage (2hrs storage)  V7

        returns:
            constraint_id  location  coefficient  date_from
            SWQLD1         Q8        0.43
            WNV1           V7        0.78
            WNV1           V7        0.00         2031-11-30              # V7's two-date profile passes through intact
    """
    battery_location = _battery_to_location(storage_new_entrant)
    surviving = lhs[lhs["term_type"] == "storage_output"].copy()
    surviving["location"] = surviving["variable_name"].map(battery_location)
    surviving = surviving.dropna(subset=["location"])
    _raise_on_inconsistent_battery_profiles(surviving)
    return surviving[
        ["constraint_id", "location", "coefficient", "date_from"]
    ].drop_duplicates()


def _raise_on_inconsistent_battery_profiles(surviving: pd.DataFrame) -> None:
    """Raise unless, within each (constraint_id, location), every surviving
    new-entrant battery carries the same profile as the others.

    A battery's profile is its whole set of (date_from, coefficient) rows, so
    this allows a battery to be time-varying (several dates) as long as the
    other batteries at the location vary the same way -- what is disallowed is
    the batteries disagreeing, not time-variation itself. Pass 2 copies one of
    these profiles onto the PLEXOS-omitted durations (e.g. the 4h battery), so
    they must all agree for the copied profile to be unambiguous. Comparing
    whole profiles catches both ways they could disagree: a coefficient that
    differs at a shared date, and one battery carrying an extra date the others
    lack (its extra-date rows would otherwise be grafted onto the others
    silently, since _dedupe_lhs_terms keeps them -- nothing matches their
    date_from). The batteries do agree across the current 7.5 extract; this
    guards future extracts.
    """
    if surviving.empty:
        return
    per_battery = surviving.groupby(["constraint_id", "location", "variable_name"])[
        ["coefficient", "date_from"]
    ].apply(lambda g: frozenset(zip(g["date_from"].fillna(""), g["coefficient"])))
    per_location = per_battery.groupby(level=["constraint_id", "location"]).nunique()
    conflicting = per_location[per_location > 1]
    if not conflicting.empty:
        offenders = sorted(f"{cid}: {loc}" for cid, loc in conflicting.index)
        raise ValueError(
            "Surviving new-entrant batteries at a (constraint, location) do not "
            f"share one (date_from, coefficient) profile: {offenders}"
        )


def _location_battery_pairs(
    batteries_by_location: dict[str, list[str]],
) -> pd.DataFrame:
    """Flatten {location: [batteries]} into (location, variable_name) rows.

    I/O Example:
        {"Q8": ["Q8 Battery - 2h", "Q8 Battery - 4h"], "Q1": ["Q1 Battery - 2h"]}

        returns:
            location  variable_name
            Q8        Q8 Battery - 2h
            Q8        Q8 Battery - 4h
            Q1        Q1 Battery - 2h
    """
    pairs = [
        (location, battery)
        for location, batteries in batteries_by_location.items()
        for battery in batteries
    ]
    return pd.DataFrame(pairs, columns=["location", "variable_name"])


def _battery_rows_for_triggers(
    triggered: pd.DataFrame,
    batteries_by_location: dict[str, list[str]],
    coefficients: pd.DataFrame,
) -> pd.DataFrame:
    """Build the storage_output rows to inject for each (constraint_id,
    location) pair, weighted by the location's surviving-battery coefficients.

    Each triggered location's batteries are emitted once per (coefficient,
    date_from) row in coefficients for that location; locations with no
    surviving new-entrant battery fall back to a single row at coefficient 1.0
    with empty date_from. The left merge keeps the np.nan null flavour the
    pass-1 rows carry (read from CSV), avoiding a mixed-null date_from column
    after the pass-1/pass-2 concat.

    I/O Example:
        triggered:
            constraint_id  location
            SWQLD1         Q8
            NQ1            Q1

        batteries_by_location:
            {"Q8": ["Q8 Battery - 2h", "Q8 Battery - 4h"],
             "Q1": ["Q1 Battery - 2h"]}

        coefficients:
            constraint_id  location  coefficient  date_from
            SWQLD1         Q8        0.43

        returns:
            constraint_id  term_type       variable_name    coefficient  date_from
            SWQLD1         storage_output  Q8 Battery - 2h  0.43          # 0.43 from Q8 profile
            SWQLD1         storage_output  Q8 Battery - 4h  0.43
            NQ1            storage_output  Q1 Battery - 2h  1.0           # Q1 has no profile -> default
    """
    # Short-circuit the empty case: an empty triggered carries a float64
    # location column, which would raise on the object-keyed merge below.
    if triggered.empty:
        return pd.DataFrame(columns=_CUSTOM_CONSTRAINTS_LHS_COLUMNS)
    # Inner merge: expand each triggered (constraint_id, location) into one row
    # per new-entrant battery at that location. Triggered locations that hold no
    # batteries (location absent from batteries_by_location) drop out here.
    location_batteries = _location_battery_pairs(batteries_by_location)
    triggered_batteries = triggered.merge(location_batteries, on="location")
    # Left merge: attach the location's surviving-sibling coefficient profile. A
    # time-varying profile (multiple date_from rows) fans each battery out into
    # one row per profile entry; a location with no profile leaves coefficient
    # NaN, which fillna turns into the 1.0 default.
    rows = triggered_batteries.merge(
        coefficients, on=["constraint_id", "location"], how="left"
    )
    rows["coefficient"] = rows["coefficient"].fillna(1.0)
    rows["term_type"] = "storage_output"
    return rows[_CUSTOM_CONSTRAINTS_LHS_COLUMNS].reset_index(drop=True)


def _log_injected_batteries(injected: pd.DataFrame) -> None:
    """Summarise the pass-2 injection at INFO."""
    if injected.empty:
        logging.info("Injected no new-entrant batteries (no triggers fired)")
        return
    n_constraints = injected["constraint_id"].nunique()
    n_batteries = injected["variable_name"].nunique()
    logging.info(
        f"Injected {len(injected)} new-entrant battery storage_output rows "
        f"across {n_constraints} constraints "
        f"({n_batteries} distinct batteries)"
    )


def _dedupe_lhs_terms(lhs: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate LHS rows on (constraint_id, term_type, variable_name,
    date_from).

    Safety net for the pass-1/pass-2 overlap: a battery that survives pass 1
    (it matched a templated unit) and is also injected by pass 2 would otherwise
    appear twice at the same date_from. The first occurrence is kept --
    pass-1 rows come first and carry the coefficient from PLEXOS.

    date_from is part of the key because PLEXOS carries time-varying
    coefficients as multiple rows for the same (constraint, term, unit) with
    different date_from -- those legitimately co-exist and must not be
    collapsed.
    """
    before = len(lhs)
    deduped = lhs.drop_duplicates(
        subset=["constraint_id", "term_type", "variable_name", "date_from"],
        keep="first",
    )
    dropped = before - len(deduped)
    if dropped:
        logging.info(
            f"Deduped {dropped} overlapping LHS rows (pass-1/pass-2 collisions)"
        )
    return deduped


def _warn_on_constraints_missing_lhs(
    constraints: pd.DataFrame, lhs: pd.DataFrame
) -> None:
    """Warn about constraints left with no LHS terms after translation.

    A constraint with no LHS terms is degenerate -- it can't bind anything --
    so the caller almost certainly wants to know.
    """
    with_terms = set(lhs["constraint_id"])
    missing = sorted(set(constraints["constraint_id"]) - with_terms)
    if missing:
        logging.warning(f"Custom constraints left with no LHS terms: {missing}")


# --- RHS table ---


def _build_custom_constraints_rhs(rhs_values: pd.DataFrame) -> pd.DataFrame:
    """Reshape RHS to long format with region-prefixed canonical timeslices.

    I/O Example:
        rhs_values:
            constraint_name      value   date_from  tags
            ExportGroup_SWQLD1   3000.0             QLD Hot Day
            ExportGroup_SWQLD1   3000.0             QLD Winter
            ExportGroup_NQ1      2650.0             QLD Winter

        returns:
            constraint_id  timeslice            rhs     date_from
            SWQLD1         qld_peak_demand      3000.0
            SWQLD1         qld_winter_reference 3000.0
            NQ1            qld_winter_reference 2650.0
    """
    rhs = rhs_values.copy()
    rhs["constraint_id"] = rhs["constraint_name"].map(_strip_constraint_prefix)
    rhs["timeslice"] = rhs["tags"].map(_tag_to_timeslice)
    rhs = rhs.rename(columns={"value": "rhs"})
    return rhs[_CUSTOM_CONSTRAINTS_RHS_COLUMNS].reset_index(drop=True)


def _tag_to_timeslice(tag: str) -> str:
    """Map a PLEXOS regional RHS tag to its ISPyPSA region-prefixed timeslice.

    The region prefix is preserved lowercased (regional weather conditions
    are physically distinct time periods); the suffix is mapped to the
    codebase-wide canonical timeslice name.

    I/O Example:
        "QLD Hot Day"        -> "qld_peak_demand"
        "VIC Typical Summer" -> "vic_summer_typical"
        "TAS Winter"         -> "tas_winter_reference"
    """
    region, _, suffix = tag.partition(" ")
    return f"{region.lower()}_{_REGIONAL_SUFFIX_TO_CANONICAL[suffix]}"
