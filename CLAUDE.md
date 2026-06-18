# Claude Coding Preferences for ISPyPSA

## Code Style

### Guiding principle

**Readability first.** Favour clarity over DRY or efficiency. A little repetition is fine if it
makes the code easier to follow. Optimise for a reader who hasn't seen the codebase before.

### Orchestrator + helper pattern

High-level functions should read like a narrative — a sequence of descriptive verb-phrase calls
that tell the story of what the function does. Push all data manipulation into private (`_`)
helper functions.

```python
def template_network_transmission_paths(iasr_tables, scenario):
    paths = _extract_flow_paths(iasr_tables["flow_path_transfer_capability"])
    paths = _add_transfer_limits(paths, iasr_tables["interconnector_transfer_capability"])
    paths = _filter_to_scenario(paths, scenario)
    return paths
```

### Helper function guidelines

- **≤ 10 lines** of actual logic (excluding docstrings/blank lines) for any function that
  touches or transforms data.
- **Private by default** — prefix with `_` unless the function is part of the public API.
- **Descriptive names** — the name should make the orchestrator readable without needing to
  look at the helper's body. Prefer verb phrases: `_extract_*`, `_add_*`, `_filter_*`,
  `_map_*`, `_merge_*`.
- **Single responsibility** — each helper does one thing. If a helper needs an internal
  comment explaining a second step, it should probably be two helpers.
- **No hidden preconditions** — if a helper's correctness depends on a column
  value or invariant set by the caller, set it explicitly inside the helper
  instead. Preconditions that don't appear in the signature or name are easy
  to break during refactoring.

### Clarity over cleverness

- **Avoid positional access** like `iloc[:, 0]` — use named column access (e.g.
  `df["Flow Paths"]`) so the code states what it means.
- **Comment non-obvious regex** — add a concrete example of the input being matched and
  annotate each capture group.
- **Prefer explicit data over clever detection.** If the set of special cases is small and
  stable, declare them as data rather than building logic to infer them from surrounding
  context.

### Control flow

- **Keep it flat.** Prefer simple, linear control flow even if it means some repetition.
- **One level of nesting max** for `if` and `for` statements. If you find yourself writing
  nested loops or nested conditionals, extract the inner block into a helper function.
  The exception is when nesting genuinely is the simplest way to express the logic — but
  that should be rare.

### Non-defensive code

Write non-defensive code by default. Trust design decisions and caller contracts. Don't add
`None` checks, fallback logic, or `try/except` blocks unless explicitly needed or revealed
through testing. Let the code fail clearly when preconditions aren't met.

No backwards compatibility unless explicitly requested — update all call sites directly.

### Docstrings: I/O Example

Every non-trivial function should include an `I/O Example:` section in its docstring
showing a concrete input → output mapping. The goal is that a reviewer can understand
the function's behaviour from the docstring alone, without reading the body.

Conventions:

- Use a plain CSV-like table format for DataFrame inputs and outputs — no need to
  wrap in runnable `csv_str_to_df` calls, since this is illustrative, not a doctest.
- Abbreviate long column names when they would otherwise overflow the line; point at
  the relevant constants for the real names.
- Cover representative edge cases in the same example, with trailing `# comment`
  notes on the rows that demonstrate each case.
- For trivial utility functions, one-line input → output cases are enough.

```python
def _duplicate_for_both_directions(limits: pd.DataFrame) -> pd.DataFrame:
    """Mirrors each row into a forward and a reverse entry.

    I/O Example:
        limits:
            path_id  timeslice    capacity
            Q1-NQ    peak_demand  750

        returns:
            path_id  direction  timeslice    capacity
            Q1-NQ    forward    peak_demand  750
            Q1-NQ    reverse    peak_demand  750
    """
```

### Module docstrings

A module docstring orients a reader who has never seen the module before. By
the time they finish it they should know what the module turns into what, the
path the data takes through it, and the handful of terms the rest of the file
leans on. Write it in full sentences — not a condensed, telegraphic style — so
an unfamiliar reader can parse it on the first pass. Keep inline-code markup
light: these docstrings are read in source, not rendered, so lean on snake_case
names and quotes rather than wrapping every identifier in backticks.

Use these sections, in order:

1. **Intro (one or two sentences).** What the module does and where it sits in
   the wider pipeline.
2. **I/O framing.** The shape of the input and the output, each with a small
   block of literal example data. This is the whole-module version of the
   per-function `I/O Example`: the reader sees what goes in and what comes out
   before reading how. Let the example's header row introduce the column names
   rather than also listing them in prose; keep only the prose the example
   can't carry, such as each table's grain ("one row per …").
3. **Pipeline story.** A high-level, step-by-step narrative of what the module
   does, in execution order. Keep most steps to a sentence; spend extra words
   only on the conceptually tricky or opinionated steps, where the "why it's
   done this way" wouldn't be obvious from the code. Where the story first
   reaches a term it will reuse, state what happens in plain words and then
   attach the name ("…, which we call *triggering*"), rather than dropping the
   bare term in as though it were already defined.
4. **Keystone vocabulary (only when the module needs it).** Prefer wording
   clear enough that no term needs a glossary — a reader should rarely have to
   learn special vocabulary to follow well-written docs, and the best outcome
   is that this section is empty or absent. Add it only when a term genuinely
   recurs across the file and its helpers *and* carries a context-specific
   meaning that inline wording keeps having to re-explain. When that bar is
   met, pin the term down once here, grounded in the domain model (say what the
   thing actually is, not just how it's used), so helpers reuse it without
   redefining it. This section's presence is a deliberate call that the jargon
   earns its keep — never a slot to fill, and never a reason to coin a term
   where plain words would do.
5. **Reference detail.** The hardcoded decisions — translations/mappings,
   systematic name renames, and the rows that get dropped and why. These are
   lookups the reader returns to rather than narrative, so they come last.

See `src/ispypsa/templater/custom_constraints_from_plexos.py` for a worked
example.

### External references

Comments, docstrings, and other in-repo docs must not refer to files that
aren't checked in — anything under `notes/` is gitignored and any path that
only lives on one developer's machine is a dead link to everyone else.

When external reasoning needs to be referenced, link to a **GitHub issue**
on the project instead (e.g. `Open-ISP/ISPyPSA#123`). If the issue doesn't
exist yet, file it first and use the real number; if that's blocked, use a
clearly grep-able placeholder like `Open-ISP/ISPyPSA#TBD` and add a task to
come back and file it.

In-repo paths (`docs/...`, `src/...`, `tests/...`) are fine to reference.

## Logging

Logging surfaces things a user or operator wants to know during a template/translation
run that aren't visible from the returned DataFrames. Errors that should halt the run
are `raise`d, not logged.

### Levels

- **INFO** — used for:
  - The top of a public template/translator orchestrator
    (`logging.info("Creating a template for X")`). Gives a progress trace for long runs.
  - Start and completion of long-running CLI operations (downloads, deletions, file
    generation).
  - Silently dropped or filtered data — rows that appear in the input but not in the
    output (e.g. unmatched options dropped by an inner merge).

- **WARNING** — for data integrity issues the run will tolerate but the caller might
  want to act on:
  - Per-row computations that fail and produce NaN in the output, including paths/REZs
    that were missing from the IASR tables and will receive a default downstream.
  - Empty templated tables that mean a class of components won't appear in the model.
  - User-supplied filter inputs that match nothing in the data.
  - Missing entire input IASR tables.
    *(Note: this category will be deprecated once table-schema-based validation lands —
    that layer should surface missing tables instead.)*

- **DEBUG and ERROR are not used.** Errors are raised as exceptions.

### What not to log

- The successful happy path inside a helper.
- Repeated firings of the same event — when one logical decision (a drop, a fallback, a
  fuzzy match) would fire many times because of redundant rows (e.g. once per year per
  option), aggregate into a `sorted(...)` list and log once. When each firing is a
  *distinct* decision the user may want to audit (one log line per dropped option, one
  per fuzzy match), per-row is fine. The fuzzy-match log in `helpers.py` is the
  canonical example of the per-row case.
- Anything readily inspected from the returned DataFrame.
- The same condition at multiple call sites — log once at the source where the cause
  is visible.

### Style

- Use f-strings. Wrap collections with `sorted(...)` so messages are stable across runs
  and tests can rely on them.
- Name the specific input/table/region:
  `f"Missing augmentation tables: {missing}"` beats `"some tables are missing"`.
- One summary line over many per-row lines (except the fuzzy-match exception above).

### Tests

Log lines that surface non-obvious data behaviour should be covered with `caplog`. Assert the
**full emitted log message** in one substring check, not a marker plus per-name positive/negative
checks. Because logging style wraps collections in `sorted(...)`, the message is deterministic, so
asserting it whole pins the marker, the listed names, and the absence of any others all at once —
and it doesn't break when an unrelated log line later mentions one of the names you were
negatively checking for.

```python
def test_logs_paths_with_no_capacity(caplog):
    with caplog.at_level("WARNING"):
        my_function(inputs_with_missing_data)
    assert (
        "Flow paths with no capacity data in IASR table "
        "(default will be applied downstream): ['MN-SA']"
    ) in caplog.text
```

Cover both the firing case and a negative case (no log when data is complete).

## Testing

### Test structure

Tests follow a strict ordering: **inputs → function call → expected → assertion.**

Use the `csv_str_to_df` fixture to create readable DataFrame inputs and expected outputs. Place
each `assert_frame_equal` immediately after its expected DataFrame definition. Only include
columns in test inputs that the code actually accesses.

**Always assert with `pd.testing.assert_frame_equal` against a full expected DataFrame.** Don't
substitute row-count checks, set membership on a column, per-cell `iloc[...]` lookups, or
`pd.isna(...)` probes — they hide off-by-one, ordering, and stray-column bugs and read worse than
a single side-by-side comparison. The same applies to empty results: build an empty expected
DataFrame with a header-only `csv_str_to_df("path_id,  geo_from, ...")` call and compare, rather
than asserting `result.empty` plus `list(result.columns) == [...]` separately.

```python
def test_my_function(csv_str_to_df):
    input_data = csv_str_to_df("""
        name,   value
        item1,  100
        item2,  200
    """)

    result = my_function(input_data)

    expected = csv_str_to_df("""
        name,   processed_value
        item1,  150
        item2,  250
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("name").reset_index(drop=True),
        expected.sort_values("name").reset_index(drop=True),
    )
```

### Empty DataFrame handling

Missing data is represented as a DataFrame with all expected columns but no rows ("all columns,
no rows"). This is enforced by schema validation at module boundaries, so internal functions can
rely on receiving complete DataFrames and never need to check for `None` or missing tables.

Functions must handle empty DataFrames gracefully — pandas operations like filtering, groupby,
and concat naturally handle this without special-case code.

### Combinatorial edge cases

Functions with multiple input DataFrames must be tested with:

- Table A empty, Table B populated
- Table A populated, Table B empty
- Both tables empty

```python
def test_both_empty(csv_str_to_df):
    table_a = pd.DataFrame(columns=["id", "value"])
    table_b = pd.DataFrame(columns=["id", "name"])

    result = my_function(table_a, table_b)

    expected = csv_str_to_df("""
        id,  value,  name
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
```

Use `pd.DataFrame(columns=[...])` for empty *inputs* (where shared module-level column
constants like `_FLOW_PATH_COLUMNS` make the construction tighter), and header-only
`csv_str_to_df` for empty *expected outputs* (so the assertion's expected shape lives in the
same readable form as every other expected DataFrame in the test).

### Comparing DataFrames

- Sort both sides before comparison when row order doesn't matter
- Use `check_exact=False` or `rtol=1e-5` for floating point comparisons
- Represent NaN cells in `csv_str_to_df` as a blank field after the comma — e.g.
  `A-B,    ,   ,` produces a row with `path_id="A-B"` and NaN for the remaining columns.
  Use this for collapsed/missing-data rows instead of `iloc` + `pd.isna` probes.
- Use `check_dtype=False` when type precision isn't critical (e.g. NaN columns)

### Integration tests

When a public orchestrator calls into per-module helpers that have their own
thorough tests, the orchestrator's integration test should verify wiring only —
not duplicate content checks. Assert:

- the expected output keys are present
- each output has the expected column set
- each output has the expected row count

Skip `assert_frame_equal`. Add a one-line comment stating that the detailed
content is covered by the per-module tests.

## Development Environment

Use `uv` for package management. Key commands:

```bash
uv sync                  # Install dependencies from lock file
uv run pytest tests/     # Run tests
uv run pytest tests/test_foo.py::test_bar -v  # Run a specific test
uv run pre-commit run --all-files             # Run linters
```

## Version Control

- Only commit when explicitly requested
- Commit messages should focus on the "why" rather than the "what"
- Before staging, list the files you intend to commit and confirm with the user.
  Local-only edits (dev configs, feature flag flips, experiment artefacts) often
  sit alongside the real change and should not be swept into the commit.

### Pull request descriptions

The description orients the reviewer. After reading the opening, and before opening any
file, they should know what the change is and what to expect from the diff.

- **Lead with the story, not the mechanics.** Open with why the change exists and what it
  does conceptually — the problem, the decision, the shape of the fix. Leave the
  helper-by-helper detail to the code.
- **Include a directory-tree diagram** of where the key changes live, with a short arrow
  note against each path, so the reviewer sees the lay of the land before diving in.
- **Flag the design choices a reviewer would otherwise have to reverse-engineer** — the
  non-obvious "why it's done this way", not a changelog of every edit.
- **Drop sections with nothing distinctive to say.** A "Testing" section that only says
  "ran the tests" is noise; omit it. Include what's specific to this change.
- **Cut detail that doesn't earn its place.** Test-fixture tweaks, incidental renames and
  the like belong in the diff, not the orientation.
- **State relationships accurately, without overstating.** Describe what the change actually
  does; don't reach for a stronger claim than is true.

Tone: concise, direct, conversational; British/Australian English; understated rather than
effusive — no hyperbole. Don't anthropomorphise the PR: "this change adds X" / "X now does
Y", not "this PR teaches X".
