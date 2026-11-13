"""Applies known corrections to the parsed IASR tables before any templating happens.

A few IASR workbook tables contain known mistakes or inconsistencies: a typo'd ID, a
misspelt column header, or a name that one table spells differently from the others.
Rather than each templater step working around the ones it happens to meet, they are
listed once per workbook version in iasr_table_edits_detail/<version>.yaml and applied
here, at the start of templating. Every later step then reads the same corrected
tables, and each edit is logged once.

The input is the dict of parsed IASR tables, keyed by table name. The output is the
same dict with only the listed tables changed. For example, the 7.5 file corrects the
case of one IASR ID:

    maximum_capacity_existing_committed_anticipated_additional_generators (input):
        IASR ID   Installed capacity (MW)
        KiataWF1  31.05

    maximum_capacity_existing_committed_anticipated_additional_generators (output):
        IASR ID   Installed capacity (MW)
        KIATAWF1  31.05

Each entry in the YAML file names a table, gives a short reason, and makes one type of
edit. A 'replace' edit changes values in one column, either on every row or only on the
rows picked by the optional 'where'. A 'rename_columns' edit renames column headers.

    - table: new_entrants_summary
      column: Regional build cost zone
      where: {IASR ID / DLT names: [NSA Pumped Hydro - 10h, ...]}
      replace: {CSA: NSA}
      reason: ...

    - table: flow_path_transfer_capability
      rename_columns: {old header: new header}
      reason: ...

The steps are:
    1. Load the entries for the workbook version.
    2. Check every entry before applying any. Each must have exactly one type of edit,
       plus the keys that type requires and no others, so that a misspelt key (say,
       'whre') raises instead of being silently ignored. Every table the entries name
       must also be in the input.
    3. Apply each edit to its table as left by any earlier edits, so that several
       edits to the same table all take effect. A replace edit's columns are checked
       at this point, so that it can use a column renamed by an earlier edit -
       the order of edits defined in the YAML matters!
    4. Log one line per edit, tagged 'IASR TABLE EDIT' and including its reason. An
       edit that matches nothing logs a warning tagged 'IASR TABLE EDIT (not applied)'
       instead. The run is still correct in that case, but the entry may no longer be
       needed, e.g. because AEMO has fixed the typo in a later workbook.
    5. Warn about a replace edit that was applied to some cells but not everywhere it
       says, tagged 'IASR TABLE EDIT (partly applied)'. Every 'where' value and every
       'replace' key an entry lists is expected to be used at least once, so one that
       isn't (say, a misspelt IASR ID in a 'where' list) is named in the warning.

What counts as an edit: a mistake or inconsistency we would report to AEMO. Modelling
choices stay in the templater, even when they change table values. For example, the
Q8a/b/c sub-areas are mapped to Q8 in helpers._set_geo_id, and the N0/V0 non-REZ rows
are added in geography.py.
"""

import logging
from importlib.resources import files

import pandas as pd
import yaml

_LOG_TAG = "IASR TABLE EDIT"

# The keys each type of edit must have, and the optional keys it may also have. A type
# is named by its own key: a 'replace' edit has a 'replace' key. Any other key (e.g. a
# misspelt 'whre') raises rather than being silently ignored - a dropped 'where' would
# edit every row of the column.
_REQUIRED_EDIT_KEYS = {
    "replace": {"table", "column", "replace", "reason"},
    "rename_columns": {"table", "rename_columns", "reason"},
}
_OPTIONAL_EDIT_KEYS = {"replace": {"where"}, "rename_columns": set()}

_EDIT_TYPES = set(_REQUIRED_EDIT_KEYS)

# Every key some type of edit allows, for checking an edit whose type is unclear.
_ALL_EDIT_KEYS = set().union(
    *_REQUIRED_EDIT_KEYS.values(), *_OPTIONAL_EDIT_KEYS.values()
)


def _apply_edits_to_iasr_tables(
    iasr_tables: dict[str, pd.DataFrame], iasr_workbook_version: str
) -> dict[str, pd.DataFrame]:
    """Returns ``iasr_tables`` with the edits listed for ``iasr_workbook_version`` applied.

    I/O Example:
        iasr_tables["maximum_capacity_..."]:
            IASR ID   Installed capacity (MW)
            KiataWF1  31.05
            BW01      660.0

        iasr_workbook_version: "7.5"   # lists KiataWF1 -> KIATAWF1, among others

        returns iasr_tables, with:
            iasr_tables["maximum_capacity_..."]:
                IASR ID   Installed capacity (MW)
                KIATAWF1  31.05
                BW01      660.0
    """
    edits = _load_edit_details(iasr_workbook_version)
    return _apply_edits(iasr_tables, edits)


def _load_edit_details(iasr_workbook_version: str) -> list[dict]:
    """Loads the IASR table edits listed for ``iasr_workbook_version``.

    Each workbook version has its own file in iasr_table_edits_detail/, named by
    version (e.g. 7.5.yaml). A version with no file raises FileNotFoundError.
    """
    edits_file = (
        files("ispypsa.templater")
        / "iasr_table_edits_detail"
        / f"{iasr_workbook_version}.yaml"
    )
    return yaml.safe_load(edits_file.read_text())


def _apply_edits(
    iasr_tables: dict[str, pd.DataFrame], edits: list[dict]
) -> dict[str, pd.DataFrame]:
    """Checks all of ``edits``, then applies each in turn to its table as left by any
    earlier edits.

    The checks all run first, so a bad entry anywhere in ``edits`` raises before any
    edit is applied or logged. Reading each table from ``edited_tables`` (not
    ``iasr_tables``) is what lets several edits to the same table all take effect.
    Tables with no edits pass through as they are, and ``iasr_tables`` itself isn't
    modified.

    I/O Example:
        iasr_tables["t"]:
            name  zone
            A     X
            B     Y

        edits:
            {"table": "t", "column": "name", "replace": {"A": "A1"}, ...}
            {"table": "t", "column": "zone", "replace": {"Y": "Z"}, ...}

        returns, with:
            iasr_tables["t"]:
                name  zone
                A1    X       # first edit
                B     Z       # second edit, applied on top of the first
    """
    # Numbered from 1, so the number matches the entry's position in the YAML file.
    for number, edit in enumerate(edits, start=1):
        _assert_valid_edit(edit, number)
    _assert_edited_tables_present(iasr_tables, edits)
    edited_tables = dict(iasr_tables)
    for edit in edits:
        table_name = edit["table"]
        edited_tables[table_name] = _apply_edit(edited_tables[table_name], edit)
    return edited_tables


def _assert_valid_edit(details: dict, number: int) -> None:
    """Raises if ``details`` doesn't have exactly one type of edit, or has the wrong
    keys for its type.

    ``number`` names the edit in the error message, which lists only the problems
    found. Once the type is clear, the message names it too.

    I/O Example (number: 7):
        {table, column, replace, reason}  -> passes
        {table, column, replce, reason}   -> raises:
            Invalid IASR table edit 7: no 'rename_columns' or 'replace' key (exactly
            one required); unexpected keys ['replce'].
        {table, column, replace, whre}    -> raises:
            Invalid IASR table edit 7 (replace): missing keys ['reason']; unexpected
            keys ['whre'].
    """
    edit_types = sorted(_EDIT_TYPES & details.keys())
    if len(edit_types) == 1:
        label = f"{number} ({edit_types[0]})"
        problems = _find_key_problems(details, edit_types[0])
    else:
        label = f"{number}"
        problems = _find_edit_type_problems(details, edit_types)
    if problems:
        raise ValueError(f"Invalid IASR table edit {label}: {'; '.join(problems)}.")


def _find_edit_type_problems(details: dict, edit_types: list[str]) -> list[str]:
    """Describes an edit with no type or both types, plus any keys that no type of
    edit allows.

    I/O Example:
        edit_types [],  keys {table, column, replce, reason}  ->
            ["no 'rename_columns' or 'replace' key (exactly one required)",
             "unexpected keys ['replce']"]
        edit_types ['rename_columns', 'replace'],  no other keys  ->
            ["both 'rename_columns' and 'replace' keys (exactly one required)"]
    """
    problems = []
    if not edit_types:
        problems.append("no 'rename_columns' or 'replace' key (exactly one required)")
    if len(edit_types) > 1:
        problems.append(
            "both 'rename_columns' and 'replace' keys (exactly one required)"
        )
    unexpected_keys = sorted(details.keys() - _ALL_EDIT_KEYS)
    if unexpected_keys:
        problems.append(f"unexpected keys {unexpected_keys}")
    return problems


def _find_key_problems(details: dict, edit_type: str) -> list[str]:
    """Describes the keys ``details`` is missing for its ``edit_type``, and any it has
    that the type doesn't use.

    I/O Example:
        edit_type 'replace',  keys {table, column, replace, whre}  ->
            ["missing keys ['reason']", "unexpected keys ['whre']"]
        edit_type 'rename_columns',  keys {table, rename_columns, where, reason}  ->
            ["unexpected keys ['where']"]
    """
    required_keys = _REQUIRED_EDIT_KEYS[edit_type]
    allowed_keys = required_keys | _OPTIONAL_EDIT_KEYS[edit_type]
    missing_keys = sorted(required_keys - details.keys())
    unexpected_keys = sorted(details.keys() - allowed_keys)
    problems = []
    if missing_keys:
        problems.append(f"missing keys {missing_keys}")
    if unexpected_keys:
        problems.append(f"unexpected keys {unexpected_keys}")
    return problems


def _assert_edited_tables_present(
    iasr_tables: dict[str, pd.DataFrame], edits: list[dict]
) -> None:
    """Raises if any of ``edits`` names a table that isn't in ``iasr_tables``, listing
    every such table.

    I/O Example:
        iasr_tables keys:  new_entrants_summary
        edits' tables:     new_entrants_summary, technology_specific_lcf
        raises:
            IASR table edits defined for missing tables: ['technology_specific_lcf'].
    """
    missing_tables = sorted({edit["table"] for edit in edits} - iasr_tables.keys())
    if missing_tables:
        raise ValueError(
            f"IASR table edits defined for missing tables: {missing_tables}."
        )


def _apply_edit(table: pd.DataFrame, details: dict) -> pd.DataFrame:
    """Applies ``details`` to ``table`` with the helper for its type of edit."""
    if "replace" in details:
        return _apply_edit_replace(table, details)
    return _apply_edit_rename_columns(table, details)


def _apply_edit_replace(table: pd.DataFrame, details: dict) -> pd.DataFrame:
    """Replaces values in one column of ``table``, only on the rows picked by
    ``where`` (every row if there's no ``where``).

    Raises if ``column`` or a ``where`` column isn't in ``table``. Warns if the edit
    matches nothing, or if it leaves a listed ``where`` value or ``replace`` key
    unused (see ``_log_edit_replacement_partly_applied``).

    I/O Example:
        table:
            IASR ID / DLT names     Regional build cost zone
            NSA Pumped Hydro - 24h  CSA
            CSA Pumped Hydro - 24h  CSA

        details:
            column: Regional build cost zone
            where: {IASR ID / DLT names: [NSA Pumped Hydro - 24h]}
            replace: {CSA: NSA}

        returns:
            IASR ID / DLT names     Regional build cost zone
            NSA Pumped Hydro - 24h  NSA
            CSA Pumped Hydro - 24h  CSA     # not named in ``where``, unchanged
    """
    _assert_edit_columns_present(table, details)
    column = details["column"]
    replacements = details["replace"]
    rows = _select_rows_to_edit(table, details.get("where", {}))
    num_replacements = table.loc[rows, column].isin(replacements.keys()).sum()
    edited = table.copy()
    edited.loc[rows, column] = edited.loc[rows, column].replace(replacements)
    _log_edit_replacement(details, num_replacements)
    _log_edit_replacement_partly_applied(table[rows], details)
    return edited


def _assert_edit_columns_present(table: pd.DataFrame, details: dict) -> None:
    """Raises if a replace edit's ``column`` or any of its ``where`` columns isn't in
    ``table``, listing every such column.

    I/O Example:
        table.columns:  IASR ID / DLT names, Sub-region
        details:
            table: new_entrants_summary
            column: Sub region
            where: {REZ ID: [N13]}
        raises:
            IASR table edit on 'new_entrants_summary' defined for missing columns:
            ['REZ ID', 'Sub region'].
    """
    edit_columns = {details["column"], *details.get("where", {})}
    missing_columns = sorted(edit_columns - set(table.columns))
    if missing_columns:
        raise ValueError(
            f"IASR table edit on '{details['table']}' defined for missing columns: "
            f"{missing_columns}."
        )


def _select_rows_to_edit(table: pd.DataFrame, where: dict[str, list]) -> pd.Series:
    """Every row when ``where`` is empty. Otherwise, the rows whose value in each
    ``where`` column is one of those listed: a row must match every column (AND,
    not OR)."""
    rows = pd.Series(True, index=table.index)
    for column, values in where.items():
        rows &= table[column].isin(values)
    return rows


def _find_unused_where_values(
    selected_rows: pd.DataFrame, where: dict[str, list]
) -> dict[str, list]:
    """Finds the ``where`` values that none of ``selected_rows`` holds, by column.

    ``selected_rows`` are the rows the whole ``where`` picked, so with several
    ``where`` columns a value counts as unused when no row holds it alongside a
    listed value in every other column. Columns whose values are all used are left
    out.

    I/O Example:
        selected_rows:
            IASR ID / DLT names  Sub-region
            CSPVPS1              CQ

        where: {IASR ID / DLT names: [CSPVPS1, DAYDSF2]}

        returns: {IASR ID / DLT names: [DAYDSF2]}
    """
    unused_values = {
        column: sorted(set(values) - set(selected_rows[column]))
        for column, values in where.items()
    }
    return {column: values for column, values in unused_values.items() if values}


def _find_unused_replace_keys(selected_cells: pd.Series, replacements: dict) -> list:
    """Finds the ``replacements`` keys (current values) that no cell in
    ``selected_cells`` holds.

    I/O Example:
        selected_cells:  CQ, CQ, NQ
        replacements:    {CQ: NQ, SQ: NQ}

        returns: [SQ]
    """
    return sorted(set(replacements) - set(selected_cells))


def _apply_edit_rename_columns(table: pd.DataFrame, details: dict) -> pd.DataFrame:
    """Renames the column headers listed in ``rename_columns`` (old -> new).

    A listed header that isn't in ``table`` is skipped. Only the renames that are
    applied get logged, alongside how many of the listed headers were found.

    I/O Example:
        table:
            Flow Paths  ..._Winter refernce
            CQ-NQ       1400

        details:
            rename_columns: {..._Winter refernce: ..._Winter reference}

        returns:
            Flow Paths  ..._Winter reference
            CQ-NQ       1400
    """
    renames = details["rename_columns"]
    found_renames = {old: new for old, new in renames.items() if old in table.columns}
    _log_edit_rename_columns(details, found_renames)
    return table.rename(columns=found_renames)


def _log_edit_replacement(details: dict, num_replacements: int) -> None:
    """Logs a replace edit at INFO, or warns if it matched no cells.

    I/O Example:
        details:
            table: maximum_capacity_...
            column: IASR ID
            replace: {KiataWF1: KIATAWF1}
            reason: Case differs from the IASR ID in the ECAA summary table.

        num_replacements: 1

        logs (INFO):
            IASR TABLE EDIT: 'maximum_capacity_...' column 'IASR ID':
            {'KiataWF1': 'KIATAWF1'} (cells changed: 1). Case differs from the
            IASR ID in the ECAA summary table.

        num_replacements: 0     # e.g. a later workbook has fixed the typo

        logs (WARNING, without the reason):
            IASR TABLE EDIT (not applied): 'maximum_capacity_...' column 'IASR ID':
            {'KiataWF1': 'KIATAWF1'} matched nothing.
    """
    edit = f"'{details['table']}' column '{details['column']}': {details['replace']}"
    if num_replacements == 0:
        logging.warning(f"{_LOG_TAG} (not applied): {edit} matched nothing.")
    else:
        logging.info(
            f"{_LOG_TAG}: {edit} (cells changed: {num_replacements}). "
            f"{details['reason']}"
        )


def _log_edit_replacement_partly_applied(
    selected_rows: pd.DataFrame, details: dict
) -> None:
    """Warns if a replace edit changed some cells but left a listed ``where`` value
    or ``replace`` key unused.

    ``selected_rows`` are the rows of the unedited table that the edit's ``where``
    picked (every row if there's no ``where``). An edit that matched nothing at all
    is skipped, since ``_log_edit_replacement`` has already warned about it.

    I/O Example (every case has table: existing_..._summary, column: Sub-region):
        Case 1 - a 'where' value and a 'replace' key both unused, reported in one
        warning.

        selected_rows:      # DAYDSF2 is a misspelt DAYDSF1, so its row isn't picked
            IASR ID / DLT names  Sub-region
            CSPVPS1              CQ

        details:
            where: {IASR ID / DLT names: [CSPVPS1, DAYDSF2]}
            replace: {CQ: NQ, SQ: NQ}      # no selected row is SQ

        logs (WARNING):
            IASR TABLE EDIT (partly applied): 'existing_..._summary' column
            'Sub-region': 'where' values not found:
            {'IASR ID / DLT names': ['DAYDSF2']}; 'replace' keys not found:
            ['SQ'].

        Case 2 - nothing matched, so no warning here.

        selected_rows:      # e.g. a later workbook already says NQ
            IASR ID / DLT names  Sub-region
            CSPVPS1              NQ

        details:
            where: {IASR ID / DLT names: [CSPVPS1, DAYDSF2]}
            replace: {CQ: NQ}

        logs nothing: no 'replace' key was used at all, which
        ``_log_edit_replacement`` has already reported as 'not applied'.

        Case 3 - every 'where' value and 'replace' key used.

        selected_rows:
            IASR ID / DLT names  Sub-region
            CSPVPS1              CQ
            DAYDSF1              CQ

        details:
            where: {IASR ID / DLT names: [CSPVPS1, DAYDSF1]}
            replace: {CQ: NQ}

        logs nothing.
    """
    replacements = details["replace"]
    unused_values = _find_unused_where_values(selected_rows, details.get("where", {}))
    unused_keys = _find_unused_replace_keys(
        selected_rows[details["column"]], replacements
    )
    nothing_matched = len(unused_keys) == len(replacements)
    problems = _describe_unused_edit_details(unused_values, unused_keys)
    if problems and not nothing_matched:
        logging.warning(
            f"{_LOG_TAG} (partly applied): '{details['table']}' column "
            f"'{details['column']}': {'; '.join(problems)}."
        )


def _describe_unused_edit_details(
    unused_values: dict[str, list], unused_keys: list
) -> list[str]:
    """Describes the ``where`` values and ``replace`` keys a replace edit didn't use,
    leaving out whichever of the two is empty.

    I/O Example:
        unused_values:      # DAYDSF2 is a misspelt DAYDSF1, so it's in no row
            {IASR ID / DLT names: [DAYDSF2]}

        unused_keys:        # no selected row's Sub-region is SQ
            [SQ]

        returns:
            ["'where' values not found: {'IASR ID / DLT names': ['DAYDSF2']}",
             "'replace' keys not found: ['SQ']"]
    """
    problems = []
    if unused_values:
        problems.append(f"'where' values not found: {unused_values}")
    if unused_keys:
        problems.append(f"'replace' keys not found: {unused_keys}")
    return problems


def _log_edit_rename_columns(details: dict, found_renames: dict[str, str]) -> None:
    """Logs the renames that were applied at INFO, or warns if none of the listed
    old headers were found.

    I/O Example:
        details:
            table: flow_path_transfer_capability
            rename_columns:
                Forward ..._Winter refernce: Forward ..._Winter reference
                Reverse ..._Winter refernce: Reverse ..._Winter reference
            reason: Typo in column headers ('refernce').

        found_renames:      # only the Forward header was in the table
            {Forward ..._Winter refernce: Forward ..._Winter reference}

        logs (INFO):
            IASR TABLE EDIT: 'flow_path_transfer_capability' columns renamed:
            {'Forward ..._Winter refernce': 'Forward ..._Winter reference'}
            (headers found: 1 of 2). Typo in column headers ('refernce').

        found_renames: {}   # neither header was in the table

        logs (WARNING, without the reason):
            IASR TABLE EDIT (not applied): 'flow_path_transfer_capability' column
            renames {'Forward ..._Winter refernce': 'Forward ..._Winter reference',
            'Reverse ..._Winter refernce': 'Reverse ..._Winter reference'} matched
            nothing.
    """
    listed_renames = details["rename_columns"]
    if not found_renames:
        logging.warning(
            f"{_LOG_TAG} (not applied): '{details['table']}' column renames "
            f"{listed_renames} matched nothing."
        )
    else:
        logging.info(
            f"{_LOG_TAG}: '{details['table']}' columns renamed: {found_renames} "
            f"(headers found: {len(found_renames)} of {len(listed_renames)}). "
            f"{details['reason']}"
        )
