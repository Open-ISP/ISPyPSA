"""Template the three ISPyPSA custom-constraint tables from the PLEXOS extract.

The PLEXOS extract under ``src/ispypsa/templater/plexos/<version>/`` is the
opinionless intermediate produced by ``scripts/extract_plexos_constraints.py``.
This module translates that into the ISPyPSA custom-constraint shape.

LHS construction is two-pass:

1. **PLEXOS pass**: translate the raw PLEXOS LHS rows -- generators, batteries,
   lines, nodes -- into the ISPyPSA term schema. Unit names that don't match
   an IASR ID are dropped (see "Dropped rows" below).
2. **IASR new-entrant battery injection**: PLEXOS' battery entries in the
   export constraints are scoped to per-constraint variants (e.g.
   ``SWQLD1 Battery - 2h``) that don't match any IASR battery and that, on
   inspection, encode a non-obvious LP variable layout we don't want to
   mirror (see Open-ISP/ISPyPSA#110 for the underlying PLEXOS layout). The
   pass-1 dropped constraint-scoped variants are replaced by a "common
   sense" injection: for each constraint, find the REZ/sub-region locations
   of its surviving new-entrant generator terms, then add a
   ``storage_output`` row for every IASR new-entrant battery at those same
   locations.

PLEXOS -> ISPyPSA translations applied here:

* ``constraint_name`` -> ``constraint_id``: strip the ``"ExportGroup_"`` prefix.
* ``sense`` -> ``direction``: ``-1`` => ``"<="``, ``0`` => ``"="``, ``+1`` => ``">="``.
* ``(parent_class, property)`` -> ``term_type``:
    - ``Generator`` + ``Generation Sent Out Coefficient`` => ``generator_output``
    - ``Battery`` + ``Generation Coefficient`` => ``storage_output``
    - ``Line`` + ``Flow Coefficient`` => ``link_flow``
    - ``Node`` + ``Load Coefficient`` => ``load``
* ``parent_name`` -> ``variable_name``: PLEXOS units pass through as-is; only
  their names are translated to the ISPyPSA unit names. The name differences
  are all systematic, so matching is deterministic -- no fuzzy matching:
    - ``Generator`` / ``Battery``: apply the systematic renames below, then
      match (case-insensitively) against ``IASR ID / DLT names`` in the
      generator summary tables. Terms that still don't match are dropped.
    - ``Line``: hardcoded line -> path_id table.
    - ``Node``: PLEXOS node names are already ISPyPSA sub-region codes.
* RHS ``tags`` -> ``timeslice``: the regional suffix is mapped to the
  ISPyPSA canonical timeslice name and the region prefix is preserved
  lowercased -- ``"QLD Hot Day"`` => ``"qld_peak_demand"``,
  ``"VIC Typical Summer"`` => ``"vic_summer_typical"``,
  ``"TAS Winter"`` => ``"tas_winter_reference"``. The regional weather
  conditions correspond to distinct time periods (see
  ``data/timeslice_RefYear4006.csv``), so the prefix is preserved; the
  suffix uses the codebase-wide ``_CANONICAL_TIMESLICES`` vocabulary.

Systematic name renames (PLEXOS -> IASR):

* Case only (e.g. ``"North QLD"`` -> ``"North Qld"``) -- handled by the
  case-insensitive match, no rename rule needed.
* Prefix rename: ``DN1``/``DN3`` (PLEXOS REZ ids) -> ``DREZ`` (IASR) for
  both generator and battery names -- a one-to-one naming-style mapping
  where DN1/DN3 are IASR REZ IDs and ``DREZ`` is just IASR's naming prefix
  for batteries inside them. The constraint-scoped battery prefixes
  (``SWQLD1``, ``SQ1``, ``MN1``, ``NSA1``, ``NET1``, ``SEVIC1``, ``SWV1``)
  get no rename -- they're many-to-one mappings onto sub-region batteries
  with unclear semantics, handled by pass 2 instead.
* Suffix strip: ``" Area<n>"`` -- a small set of PLEXOS unit names carry
  this suffix (only ``Area1`` appears in the current PLEXOS extract); IASR
  doesn't carry it. The regex tolerates any ``\\d+`` in case future extracts
  introduce other Area numbers.

Dropped rows:

* ``Installed Capacity Coefficient`` properties -- PLEXOS constraint
  relaxation terms (Linear Augmentation / Option generators), not
  operational-constraint LHS terms. [INFO]
* ``Purchaser`` parent_class -- hydrogen electrolysers, not modelled. [INFO]
* Battery ``Load Coefficient`` rows -- the negative pair of the
  ``Generation Coefficient`` row kept as the single ``storage_output``. [INFO]
* Terms whose unit has no ISPyPSA counterpart -- e.g. CER units not yet
  modelled (Open-ISP/ISPyPSA#104), PLEXOS constraint-scoped batteries
  replaced by the pass-2 injection, or plant absent from the IASR tables.
  Dropping loosens the constraint, so this is logged as a WARNING.

``date_to`` is currently always empty in AEMO's PLEXOS files and is dropped
from the output. The templater raises if a future model starts using it.
``date_from`` is retained so time-varying coefficients survive.

TODO: switch the IASR ID lookup to a templated generator-summary table once
one exists -- matching against the raw table currently also matches DER/CER
rows that ISPyPSA may not template.

TODO: emitted timeslices are region-prefixed (``qld_peak_demand`` etc.) while
the rest of the templater still emits the bare canonical names
(``peak_demand`` etc.). Downstream consumers (``filter_template``,
``translator``, ``pypsa_build``) need extending to handle the region-prefixed
timeslice space before custom constraints flow through end-to-end.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

from .mappings import _CANONICAL_TIMESLICES

# PLEXOS REZ-id prefixes that IASR renamed to ``DREZ``. Applied to the first
# token of generator names (``DN1_SAT_Dubbo`` -> ``DREZ_SAT_Dubbo``) and to
# battery names of the form ``"DN1 <location> Battery - <duration>"``
# (``DN1 Dubbo Battery - 2h`` -> ``DREZ Dubbo Battery - 2h``).
_GENERATOR_PREFIX_RENAME = {"DN1": "DREZ", "DN3": "DREZ"}

# Battery-name rename. Unlike the (deliberately removed) constraint-scoped
# battery variants (``SWQLD1 Battery - 2h`` etc., which were many-to-one
# mappings onto sub-region batteries and are now handled by the pass-2
# injection), the DN1/DN3 -> DREZ rename is one-to-one: it's a naming-style
# difference for the same physical battery, since DN1/DN3 are the literal
# IASR REZ IDs and ``DREZ`` is just IASR's naming prefix for batteries inside
# them. Keeping it lets pass-1 preserve PLEXOS' time-varying coefficients.
_BATTERY_PREFIX_RENAME = {"DN1": "DREZ", "DN3": "DREZ"}

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
# in the tag (e.g. ``"QLD "``) is preserved separately by ``_tag_to_timeslice``.
# Values must remain a subset of ``_CANONICAL_TIMESLICES``.
_REGIONAL_SUFFIX_TO_CANONICAL = {
    "Hot Day": "peak_demand",
    "Typical Summer": "summer_typical",
    "Winter": "winter_reference",
}
assert set(_REGIONAL_SUFFIX_TO_CANONICAL.values()) <= set(_CANONICAL_TIMESLICES)

_CONSTRAINT_NAME_PREFIX = "ExportGroup_"
_EXCLUDED_PARENT_CLASSES = ("Purchaser",)
_AREA_SUFFIX_PATTERN = re.compile(r" Area\d+$")


def template_custom_constraints_from_plexos(
    iasr_tables: dict[str, pd.DataFrame],
    iasr_workbook_version: str,
    plexos_extract_dir: Path | None = None,
) -> dict[str, pd.DataFrame]:
    """Build the three custom-constraint template tables from the PLEXOS extract.

    Args:
        iasr_tables: parsed IASR tables -- must contain
            ``existing_committed_anticipated_additional_generator_summary``
            and ``new_entrants_summary``.
        iasr_workbook_version: selects the plexos extract subdirectory under
            ``src/ispypsa/templater/plexos/`` (e.g. ``"7.5"``). Ignored when
            ``plexos_extract_dir`` is given.
        plexos_extract_dir: optional override for the directory holding the
            PLEXOS extract CSVs (``constraints.csv``, ``lhs_terms.csv``,
            ``rhs_values.csv``). Used by tests to inject a fixture; in
            production leave unset and the version-based path is used.

    Returns:
        dict with keys ``"custom_constraints"``, ``"custom_constraints_lhs"``,
        ``"custom_constraints_rhs"``, ready to splice into the templater's
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
    iasr_ids = _iasr_id_choices(iasr_tables)
    new_entrants = iasr_tables["new_entrants_summary"]
    tables = {
        "custom_constraints": _build_custom_constraints(constraints),
        "custom_constraints_lhs": _build_custom_constraints_lhs(
            lhs_terms, iasr_ids, new_entrants
        ),
        "custom_constraints_rhs": _build_custom_constraints_rhs(rhs_values),
    }
    _warn_on_constraints_missing_lhs(
        tables["custom_constraints"], tables["custom_constraints_lhs"]
    )
    return tables


def _plexos_extract_dir(version: str) -> Path:
    return Path(__file__).parent / "plexos" / version


def _load_plexos_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _assert_no_date_to(df: pd.DataFrame, label: str) -> None:
    """Raise if any row has a non-empty date_to.

    ``date_to`` is dropped from the templater output. If AEMO starts using it,
    we want to fail loud rather than silently lose data.
    """
    if "date_to" in df.columns and df["date_to"].notna().any():
        raise ValueError(
            f"PLEXOS extract '{label}' has non-empty date_to values; "
            "the templater currently drops date_to and needs updating."
        )


def _iasr_id_choices(iasr_tables: dict[str, pd.DataFrame]) -> set[str]:
    """Collect the set of valid IASR IDs from the generator summary tables.

    I/O Example:
        iasr_tables: {
            "existing_committed_anticipated_additional_generator_summary":
                df with "IASR ID / DLT names" column = ["BW01", "BW02", ...],
            "new_entrants_summary":
                df with column = ["NQ SAT - Distributed Resources", ...],
        }

        returns: {"BW01", "BW02", "NQ SAT - Distributed Resources", ...}
    """
    column = "IASR ID / DLT names"
    existing = iasr_tables[
        "existing_committed_anticipated_additional_generator_summary"
    ][column]
    new_entrants = iasr_tables["new_entrants_summary"][column]
    return set(existing.dropna()) | set(new_entrants.dropna())


# --- constraints table ---


def _build_custom_constraints(constraints: pd.DataFrame) -> pd.DataFrame:
    """Strip ExportGroup_ prefix from names and map sense to direction symbols.

    The PLEXOS constraints extract is long-format with one row per
    (constraint, property); only the ``Sense`` property is translated --
    ``Penalty Price`` and ``Include in LT Plan`` are PLEXOS solver hints we
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
    return pd.DataFrame(
        {
            "constraint_id": sense_rows["constraint_name"].map(
                _strip_constraint_prefix
            ),
            "direction": sense_rows["value"].astype(int).map(_SENSE_TO_DIRECTION),
        }
    ).reset_index(drop=True)


def _strip_constraint_prefix(name: str) -> str:
    """Remove the ``ExportGroup_`` namespace prefix if present.

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
    iasr_ids: set[str],
    new_entrants: pd.DataFrame,
) -> pd.DataFrame:
    """Filter, fold, and translate the raw LHS rows into ISPyPSA's term schema,
    then inject IASR new-entrant batteries for triggered REZ/sub-regions.

    I/O Example:
        lhs_terms (abbreviated):
            constraint_name      parent_class  parent_name           property               value
            ExportGroup_SWQLD1   Battery       SWQLD1 Battery - 2h   Generation Coefficient  1.0    # dropped (constraint-scoped)
            ExportGroup_SWQLD1   Battery       SWQLD1 Battery - 2h   Load Coefficient       -1.0    # dropped (Load pair)
            ExportGroup_SWQLD1   Generator     KINGASF1              Generation Sent Out C   0.14
            ExportGroup_SWQLD1   Line          NSW1-QLD1             Flow Coefficient        0.84

        new_entrants (abbreviated):
            IASR ID / DLT names  Sub-region  REZ ID           Technology Type
            KINGASF1             SQ          Q8               Pumped Hydro ...
            Q8 Battery - 2h      SQ          Q8               Battery Storage (2hrs storage)
            Q8 Battery - 4h      SQ          Q8               Battery Storage (4hrs storage)

        returns:
            constraint_id  term_type        variable_name    coefficient  date_from
            SWQLD1         generator_output KINGASF1          0.14
            SWQLD1         link_flow        NSW-QLD           0.84
            SWQLD1         storage_output   Q8 Battery - 2h   1.0           # injected
            SWQLD1         storage_output   Q8 Battery - 4h   1.0           # injected
    """
    lhs = _drop_excluded_classes(lhs_terms)
    lhs = _drop_constraint_relaxation_terms(lhs)
    lhs = _drop_battery_load_coefficient_rows(lhs)
    lhs = _add_term_type_column(lhs)
    lhs = _add_variable_name_column(lhs, iasr_ids)
    lhs = _drop_unresolved_terms(lhs)
    lhs = lhs.rename(columns={"value": "coefficient"})
    lhs["constraint_id"] = lhs["constraint_name"].map(_strip_constraint_prefix)
    lhs = lhs[
        ["constraint_id", "term_type", "variable_name", "coefficient", "date_from"]
    ]
    lhs = _inject_iasr_new_entrant_batteries(lhs, new_entrants)
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
    """Drop ``Installed Capacity Coefficient`` rows -- PLEXOS constraint
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
    """Drop battery ``Load Coefficient`` rows -- they are the negative-magnitude
    pair of ``Generation Coefficient`` rows that survive as the single
    ``storage_output`` term.

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
    return lhs[~drop_mask].copy()


def _add_term_type_column(lhs: pd.DataFrame) -> pd.DataFrame:
    """Map each (parent_class, property) pair to an ISPyPSA term_type string.

    Raises ``ValueError`` if any pair in the input has no mapping defined --
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


def _add_variable_name_column(lhs: pd.DataFrame, iasr_ids: set[str]) -> pd.DataFrame:
    """Resolve each LHS term's parent_name to an ISPyPSA variable_name.

    Generators and batteries are matched against IASR unit names (left as NaN
    when unmatched, then dropped downstream); lines use a fixed path_id table;
    node names are already ISPyPSA sub-region codes and pass through.
    """
    lhs = lhs.copy()
    lower_index = {name.lower(): name for name in iasr_ids}
    lhs["variable_name"] = [
        _resolve_variable_name(parent_class, parent_name, iasr_ids, lower_index)
        for parent_class, parent_name in zip(lhs["parent_class"], lhs["parent_name"])
    ]
    _log_name_renames(lhs)
    return lhs


def _resolve_variable_name(
    parent_class: str, parent_name: str, iasr_ids: set[str], lower_index: dict[str, str]
) -> str | None:
    """Dispatch one LHS term to its per-class name-resolution strategy.

    I/O Example:
        ("Generator", "DN1_SAT_Dubbo", ...)        -> "DREZ_SAT_Dubbo"
        ("Battery",   "SWQLD1 Battery - 2h", ...)   -> "SQ Battery - 2h"
        ("Line",      "NSW1-QLD1", ...)             -> "NSW-QLD"
        ("Node",      "CNSW", ...)                  -> "CNSW"
        ("Generator", "SA Hydrogen Turbine", ...)   -> None   # no IASR match
    """
    if parent_class == "Generator":
        return _match_unit_name(
            _rename_generator_name(parent_name), iasr_ids, lower_index
        )
    if parent_class == "Battery":
        return _match_unit_name(
            _rename_battery_name(parent_name), iasr_ids, lower_index
        )
    if parent_class == "Line":
        return _line_variable_name(parent_name)
    if parent_class == "Node":
        return parent_name  # PLEXOS node names are already ISPyPSA sub-region codes
    raise ValueError(f"Unexpected LHS parent_class: {parent_class!r}")


def _match_unit_name(
    name: str, iasr_ids: set[str], lower_index: dict[str, str]
) -> str | None:
    """Match a renamed unit name to an IASR unit name, case-insensitively.

    I/O Example:
        "BW01"                              -> "BW01"        # exact
        "Q2_CST_North QLD Clean Energy Hub" -> "Q2_CST_North Qld Clean Energy Hub"
        "Cultana Solar Farm"                -> None          # no IASR unit
    """
    if name in iasr_ids:
        return name
    return lower_index.get(name.lower())


def _rename_generator_name(name: str) -> str:
    """Apply the systematic PLEXOS -> IASR renames to a generator name.

    I/O Example:
        "DN1_SAT_Dubbo"                          -> "DREZ_SAT_Dubbo"
        "CNSW SAT - Distributed Resources Area1" -> "CNSW SAT - Distributed Resources"
        "BW01"                                   -> "BW01"
    """
    name = _rename_first_token(name, "_", _GENERATOR_PREFIX_RENAME)
    return _strip_area_suffix(name)


def _rename_battery_name(name: str) -> str:
    """Apply the DN1/DN3 -> DREZ prefix rename and strip the ``" Area<n>"``
    distributed-resource subdivision suffix.

    Constraint-scoped batteries (``SWQLD1 Battery - 2h`` etc.) intentionally
    have NO rename rule -- they are designed not to match any IASR battery,
    fall through ``_drop_unresolved_terms``, and are re-supplied by
    ``_inject_iasr_new_entrant_batteries``.

    I/O Example:
        "DN1 Dubbo Battery - 2h"               -> "DREZ Dubbo Battery - 2h"
        "DN3 Marulan Battery - 8h"             -> "DREZ Marulan Battery - 8h"
        "CNSW Battery - Distributed Res. Area1"-> "CNSW Battery - Distributed Res."
        "Tarong BESS"                          -> "Tarong BESS"
        "SWQLD1 Battery - 2h"                  -> "SWQLD1 Battery - 2h"  # no IASR match
    """
    name = _rename_first_token(name, " ", _BATTERY_PREFIX_RENAME)
    return _strip_area_suffix(name)


def _rename_first_token(name: str, delimiter: str, rename_map: dict[str, str]) -> str:
    """Replace the first delimiter-delimited token of name via rename_map.

    I/O Example (delimiter="_", rename_map={"DN1": "DREZ"}):
        "DN1_SAT_Dubbo"  -> "DREZ_SAT_Dubbo"
        "BW01"           -> "BW01"   # no delimiter, or token not in map
    """
    head, sep, tail = name.partition(delimiter)
    if sep and head in rename_map:
        return rename_map[head] + sep + tail
    return name


def _strip_area_suffix(name: str) -> str:
    """Strip the ``" Area<n>"`` suffix from a PLEXOS unit name.

    A small set of PLEXOS unit names (e.g. ``"PV CNSW Area1"``) carry an
    " Area<n>" suffix that IASR doesn't carry; only ``Area1`` is seen in
    the current PLEXOS extract. The regex tolerates any digit suffix so
    future Area2/3/... values won't silently slip through unstripped.

    I/O Example:
        "PV CNSW Area1"  -> "PV CNSW"
        "BW01"           -> "BW01"
    """
    return _AREA_SUFFIX_PATTERN.sub("", name)


def _line_variable_name(parent_name: str) -> str:
    """Look up the ISPyPSA path_id for a PLEXOS line name.

    I/O Example:
        "NSW1-QLD1"  -> "NSW-QLD"
        "V-SA"       -> "WNV-SESA"
        "SQ-CQ"      -> "SQ-CQ"
    """
    if parent_name not in _LINE_TO_PATH_ID:
        raise ValueError(
            f"PLEXOS line name {parent_name!r} not in _LINE_TO_PATH_ID -- add a mapping."
        )
    return _LINE_TO_PATH_ID[parent_name]


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
    yet modelled (Open-ISP/ISPyPSA#104) or plant absent from the IASR tables.
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


# --- pass 2: inject IASR new-entrant batteries by triggered REZ/sub-region ---


def _inject_iasr_new_entrant_batteries(
    lhs: pd.DataFrame, new_entrants: pd.DataFrame
) -> pd.DataFrame:
    """Append a ``storage_output`` row for every IASR new-entrant battery in
    each REZ/sub-region whose new-entrant generators participate in the
    constraint.

    For each ``generator_output`` term whose variable_name is a new-entrant
    generator IASR ID, look up that generator's location -- REZ ID if
    populated, else Sub-region -- and add ``storage_output`` rows for every
    new-entrant battery at that same location. Coefficient is fixed at 1.0
    (matches PLEXOS' Generation Coefficient on battery export terms) and
    ``date_from`` is empty.

    See module docstring for the rationale; see Open-ISP/ISPyPSA#110
    for the PLEXOS battery layout that motivated dropping the pass-1
    constraint-scoped variants.

    I/O Example:
        lhs (after pass 1, abbreviated):
            constraint_id  term_type        variable_name
            SWQLD1         generator_output Q8_SAT_Brisbane
            NQ1            generator_output Q1_WH_Cairns

        new_entrants (abbreviated):
            IASR ID / DLT names  Sub-region  REZ ID  Technology Type
            Q8_SAT_Brisbane      SQ          Q8      Solar
            Q1_WH_Cairns         NQ          Q1      Wind
            Q8 Battery - 2h      SQ          Q8      Battery Storage ...
            Q1 Battery - 2h      NQ          Q1      Battery Storage ...

        returns (appended rows only):
            constraint_id  term_type        variable_name    coefficient  date_from
            SWQLD1         storage_output   Q8 Battery - 2h   1.0
            NQ1            storage_output   Q1 Battery - 2h   1.0
    """
    gen_to_location = _generator_to_location(new_entrants)
    batteries_by_location = _batteries_by_location(new_entrants)
    triggered = _triggered_locations_per_constraint(lhs, gen_to_location)
    injected = _battery_rows_for_triggers(triggered, batteries_by_location)
    _log_injected_batteries(injected)
    if injected.empty:
        return lhs
    return pd.concat([lhs, injected], ignore_index=True)


def _generator_to_location(new_entrants: pd.DataFrame) -> dict[str, str]:
    """Build a {generator IASR ID: REZ ID or Sub-region} lookup for non-battery
    new entrants.

    The location is the REZ ID when populated, otherwise the Sub-region.

    I/O Example:
        new_entrants (abbreviated):
            IASR ID / DLT names  Sub-region  REZ ID           Technology Type
            Q8_SAT_Brisbane      SQ          Q8               Solar Photovoltaic
            CSA Coal             CSA         Not Applicable   Black Coal
            CSA Battery - 2h     CSA         Not Applicable   Battery Storage (2hrs storage)

        returns:
            {"Q8_SAT_Brisbane": "Q8", "CSA Coal": "CSA"}   # battery excluded
    """
    non_battery = new_entrants[~_is_battery_row(new_entrants)]
    locations = non_battery.apply(_pick_location, axis=1)
    return dict(zip(non_battery["IASR ID / DLT names"], locations))


def _batteries_by_location(new_entrants: pd.DataFrame) -> dict[str, list[str]]:
    """Build a {location: [battery IASR IDs]} lookup for new-entrant batteries.

    I/O Example:
        new_entrants (abbreviated):
            IASR ID / DLT names  Sub-region  REZ ID  Technology Type
            Q8 Battery - 2h      SQ          Q8      Battery Storage (2hrs storage)
            Q8 Battery - 4h      SQ          Q8      Battery Storage (4hrs storage)
            CSA Battery - 2h     CSA         Not Applicable  Battery Storage (2hrs storage)

        returns:
            {"Q8":  ["Q8 Battery - 2h", "Q8 Battery - 4h"],
             "CSA": ["CSA Battery - 2h"]}
    """
    batteries = new_entrants[_is_battery_row(new_entrants)].copy()
    batteries["location"] = batteries.apply(_pick_location, axis=1)
    return (
        batteries.groupby("location")["IASR ID / DLT names"]
        .apply(lambda s: sorted(s.unique()))
        .to_dict()
    )


def _is_battery_row(new_entrants: pd.DataFrame) -> pd.Series:
    """Boolean mask selecting battery rows in ``new_entrants_summary``.

    Matches any Technology Type that contains the literal substring
    ``"Batter"`` -- covers both ``"Battery Storage (Xhrs storage)"`` (singular)
    and ``"Distributed Resources Batteries"`` (plural). Other storage
    technologies (pumped hydro, solar thermal) intentionally do not match.
    """
    return new_entrants["Technology Type"].str.contains("Batter", na=False)


def _pick_location(row: pd.Series) -> str:
    """Return ``REZ ID`` when populated, otherwise ``Sub-region``.

    I/O Example:
        {"REZ ID": "Q8",             "Sub-region": "SQ"}  -> "Q8"
        {"REZ ID": "Not Applicable", "Sub-region": "SQ"}  -> "SQ"
    """
    rez_id = row["REZ ID"]
    if pd.notna(rez_id) and rez_id != "Not Applicable":
        return rez_id
    return row["Sub-region"]


def _triggered_locations_per_constraint(
    lhs: pd.DataFrame, gen_to_location: dict[str, str]
) -> pd.DataFrame:
    """Find the distinct {constraint_id, location} pairs implied by the
    surviving new-entrant generator terms.

    Generators whose IASR ID isn't in ``gen_to_location`` (existing /
    committed / anticipated plant) don't trigger; only new-entrant generators
    drive the injection.

    I/O Example:
        lhs (abbreviated):
            constraint_id  term_type        variable_name
            SWQLD1         generator_output Q8_SAT_Brisbane     # triggers Q8
            SWQLD1         generator_output Tarong BESS         # existing, no trigger
            SWQLD1         link_flow        NSW-QLD             # not generator

        gen_to_location: {"Q8_SAT_Brisbane": "Q8"}

        returns:
            constraint_id  location
            SWQLD1         Q8
    """
    generators = lhs[lhs["term_type"] == "generator_output"].copy()
    generators["location"] = generators["variable_name"].map(gen_to_location)
    return (
        generators.dropna(subset=["location"])[["constraint_id", "location"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )


def _battery_rows_for_triggers(
    triggered: pd.DataFrame, batteries_by_location: dict[str, list[str]]
) -> pd.DataFrame:
    """Build the ``storage_output`` rows to inject for each (constraint_id,
    location) pair.

    I/O Example:
        triggered:
            constraint_id  location
            SWQLD1         Q8
            NQ1            Q1

        batteries_by_location:
            {"Q8": ["Q8 Battery - 2h", "Q8 Battery - 4h"],
             "Q1": ["Q1 Battery - 2h"]}

        returns:
            constraint_id  term_type       variable_name    coefficient  date_from
            SWQLD1         storage_output  Q8 Battery - 2h   1.0
            SWQLD1         storage_output  Q8 Battery - 4h   1.0
            NQ1            storage_output  Q1 Battery - 2h   1.0
    """
    # date_from uses float("nan"), not pd.NA, to match the null representation
    # of the pass-1 rows (read from CSV via pandas, which yields np.nan for
    # empty cells). Keeping a single null flavour avoids a mixed-null date_from
    # column after the pass-1/pass-2 concat.
    rows = [
        (constraint_id, "storage_output", battery, 1.0, float("nan"))
        for constraint_id, location in triggered.itertuples(index=False)
        for battery in batteries_by_location.get(location, [])
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "constraint_id",
            "term_type",
            "variable_name",
            "coefficient",
            "date_from",
        ],
    )


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
    (exact IASR name match) and is also injected by pass 2 would otherwise
    appear twice at the same date_from. The first occurrence is kept --
    pass-1 rows come first and carry the coefficient from PLEXOS.

    ``date_from`` is part of the key because PLEXOS carries time-varying
    coefficients as multiple rows for the same (constraint, term, unit) with
    different ``date_from`` -- those legitimately co-exist and must not be
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
    return rhs[["constraint_id", "timeslice", "rhs", "date_from"]].reset_index(
        drop=True
    )


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
