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
- **Say "investment period", never bare "period".** On its own, "period" reads as a
  snapshot or time window. In prose (docstrings, comments, schema descriptions, I/O
  Example notes) write "investment period", or name the `investment_period` column
  ("a row with a blank investment_period", not "a blank-period row"). Bare `period` is
  fine only where it is a literal name, such as PyPSA's snapshot index level.

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

### Docstrings

A docstring tells the reader how to use a function and what it does or returns. It does not
walk through the body step by step: a reader who needs the steps is going to read the code
anyway. The goal is that a reviewer can understand the function's behaviour from the
docstring alone, without reading the body.

This guidance applies to new docstrings, and to an existing docstring when it needs a
significant rewrite anyway. Don't go through the codebase updating docstrings to match it:
existing module docstrings, and `Args` and `Returns` sections on private helpers, stay as
they are.

How much a docstring says depends on the function's role:

- **Ordinary helpers** get a short summary and an I/O Example. This is most functions: a
  helper that names its own columns and serves one caller needs little more. Add a short
  second paragraph only when the summary leaves out something a caller needs, such as how
  a derived quantity is defined.
- **Entry points and generic helpers** get an orienting docstring, described below. These
  are the two places where the docstring is the only place the full story exists.
- **Helpers that implement a method** (a numerical routine, a derived formula) also
  describe how the result is built. This is rare; see below.
- **Modules** get no docstring. The entry point's docstring orients the reader instead.

#### I/O Example

Every non-trivial function should include an `I/O Example:` section showing a concrete
input → output mapping.

- Use a plain CSV-like table format for DataFrame inputs and outputs. There is no need to
  wrap them in runnable `csv_str_to_df` calls, since this is illustrative, not a doctest.
- Always lay DataFrame examples out as full tables: a header row plus example rows, each
  table in its own block, even when a single row would do. Never condense a table into
  `column=value` prose. Abbreviated column sets, row sets and column names are fine when
  the full table would otherwise be large; point at the relevant constants for the real
  names.
- Cover representative edge cases in the same example, with trailing `# comment` notes on
  the rows that demonstrate each case.
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

#### Which functions get an orienting docstring

**Entry points.** A function called from outside its module: the `_template_*` or
`_translate_*` function a pipeline orchestrator calls. (Public API functions follow the
same order, with the extra sections described further down.) Someone
arriving at the module reads this docstring first, and it has to make sense to a reader who
has not seen the table formats or the rest of the pipeline. Where a module has several
entry points, each gets an orienting docstring covering its own output. Define a shared
concept in the first and point to it from the others.

**Generic helpers.** The signals are that

- column or key names arrive as parameters instead of appearing literally in the body, so
  the body alone can't tell the reader what the data means;
- it has (or is written to invite) several call sites with different domain meanings; and
- it enforces a rule its callers depend on but don't restate: a precedence order, a
  validation contract, a guarantee about the output's shape.

The first signal is the gate; the other two raise the stakes.

#### The orienting docstring

An orienting docstring is a short narrative, one idea per paragraph, in this order. Not
every function needs every part: leave out a part that has nothing to say.

1. **Summary line.** The summary rules under "Wording rules" apply. Who uses the result
   and the shape of the output have their own paragraphs below, so keep them out of it.
   For a generic helper, name the parameters it connects.

2. **The concept and the problem.** Before any behaviour, say what the data is and why the
   function exists. For an entry point this is the domain idea and the gap the function
   closes ("A carbon budget is a cap, in Mt, on the NEM's total emissions over a window of
   years. The model may cover only some of those years, so the cap has to be adjusted to
   match."). For a generic helper it is the input's story: why the data looks the way it
   does, especially the oddity the function exists to handle ("Rather than write out a row
   for every key, the user may leave a key cell blank…"). This paragraph is what makes the
   later ones land, and it comes before any mention of rows or columns.

3. **What the parameters represent,** where the signature doesn't already say. Use domain
   terms rather than shape ("every value the model needs an entry for, e.g. the enabled
   expansion elements"). For an entry point this is usually a mention of the config
   settings it reads, and can be folded into the paragraphs that use them.

4. **What is returned.** Say it is a table and state its grain ("a table with one row per
   budget"). Then:

   - **Six columns or fewer:** list each column and what it holds, including what the
     example can't show: units, whether a bound is inclusive, which input column it came
     from.
   - **More than six:** describe only the columns the function adds or changes, and name
     the schema file under `validation/schemas/` that documents the rest.

   Where it helps the reader see what the output is for, add a sentence on what the next
   stage does with it.

5. **The behaviour.** How the function connects its inputs to its output, at the level a
   caller needs. This is not a walk through the body.

   - **When the behaviour splits into cases, say so and list them.** State the one thing
     the cases turn on ("How a budget is adjusted depends on how much of its window is
     modelled:") and give each case its own line. Don't thread the cases through a
     paragraph of "when… otherwise…", and don't leave one to be inferred from what is
     missing from the output.
   - **Keep options separate from cases.** If a config setting only matters in one case,
     list its options in their own list after the cases.
   - **Be precise enough to cover the compound case** (see the wording rules).

6. **The failure contract.** What happens when the work can't be completed for some of the
   data (raise, fallback, or log-and-continue), and why that response is the right one. It
   comes last, unless raising is itself one of the listed cases or options, in which case
   it stays in the list.

7. **The I/O Example.**

An orienting docstring is long, and that is only justified by the function's role. Don't
give an ordinary helper this treatment, and once the entry point has defined a term, the
helpers below it use the term without redefining it.

#### Public API functions

The functions listed in `docs/api.md` are rendered on the documentation site by
mkdocstrings, and are read there by someone calling the function from their own script.
They use Google-style sections so the page renders properly:

- **Summary and orienting prose**, in the order above, kept short: a user needs the concept
  and what comes back, not the internals.
- **Examples:** a short snippet showing a typical call, including where its arguments come
  from (the config, the previous pipeline step). This takes the place of the I/O Example: a
  public function usually returns a dictionary of tables, too large to show as one.
- **Args:** one entry per parameter, in domain terms.
- **Returns:** what is returned. For a dictionary of tables, name the keys or point to
  where they are documented.
- **Raises:** only the errors a user can cause and act on.

Private functions don't use `Args` and `Returns` sections. The type hints and the I/O
Example already say what goes in and what comes out, and a parameter that needs explaining
gets a sentence in the prose.

#### Helpers that implement a method

A few helpers implement a numerical or algorithmic method: an interpolation, a recursion, a
formula that had to be derived. Well-written code can show what such a method does at each
line, but not why it gives the right answer. The test: could a reader check the body is
correct by reading it alone? If they would first need the derivation, the docstring
carries it.

This is the one place a docstring describes the method step by step. After the summary,
give:

1. **What the result looks like.** A small chart or picture often says this better than
   prose.
2. **How the result is built.** Numbered steps giving the reasoning behind the method, with
   a worked example on real numbers. Describe the method, not the lines of code.
3. **The preconditions** the method relies on, and what is raised when they don't hold.

Hard-to-read data manipulation doesn't qualify. If pandas code needs its steps explained,
simplify it or split it into named helpers. Expect this kind of docstring to be rare.

Worked example: `_build_emissions_trajectory` in
`src/ispypsa/translator/carbon_budgets.py`.

#### Wording rules

These apply to every docstring, and matter most in the orienting ones.

- **The summary says one thing: the function's effect.** "Adjusts each carbon budget to
  the years the model covers." Don't hang a definition or a second idea off it with a
  colon: give that its own paragraph.
- **The summary earns the function's name.** Use the name's own verb and say what it acts
  on. A `_resolve_*` function says what gets resolved. `_weight_snapshots_by_model_year`
  says what the weight is ("Weights each snapshot by the hours of its model year that it
  represents"), not "Gives each snapshot the hours it stands for", which leaves the reader
  to work out that those hours are the weight.
- **Say what the result is for when the name doesn't.** A helper that works out a derived
  quantity should say how it is used: "so that output in MW times the weight gives energy
  in MWh".
- **Define a term where it first appears.** A reader shouldn't meet "window", "rhs" or "key
  column" before being told what it is. For vocabulary derived from the signature, pin it
  down inline: "a key column: any column of `table` not in `value_columns`". A term defined
  only by the body's code is a term the docstring hasn't defined.
- **One word per thing, one thing per word.** Don't call the same value a "limit" in one
  sentence and the "rhs" in the next: the reader will look for the difference. And don't
  let one word do double duty: "a key column *listed* in `allowed_values`" and "among the
  *listed* values" use one word for two different lookups.
- **Say the thing; don't point at it.** "Named for where it goes" tells the reader an
  explanation exists without giving it. Write the explanation ("the emissions of the
  modelled years in the window must be no more than rhs") or cut the phrase.
- **Use the plain, exact phrase.** "The sum of the shares of the modelled years", not "the
  modelled years' shares added up".
- **State behaviour for the compound case, not just the simple one.** "Each blank is
  expanded to one row per value" is vague about a row with two blanks; "one row per
  combination of `allowed_values` across its blank columns" is not.
- **Phrase rules to cover every case the code handles.** A tie-break described as "an
  expanded row vs one that named the key directly" misdescribes two expanded rows
  colliding; "where several rows land on the same key, the one that started with fewer
  blanks wins" covers both.
- **Borrow concrete names from the I/O Example in the prose** (direction, path_id). A
  function documented only in generic nouns is hard to follow; the example's columns give
  the prose something to point at.
- **Prefer a concrete failing input to set-membership phrasing.** "A direction of, say,
  'sideways' raises" beats "non-blank values must be among the listed values".
- **Verify claims about callers and upstream schemas against the code.** A docstring that
  states where a parameter's values come from is making a testable claim. Check the call
  sites first, and describe what callers actually pass rather than the provenance you
  assume.
- **Keep inline-code markup light.** Docstrings are read in source, not rendered, so lean
  on snake_case names and quotes rather than wrapping every identifier in backticks.

Worked examples: `_translate_carbon_budgets` in `src/ispypsa/translator/carbon_budgets.py`
(an entry point), `_resolve_wildcards` in `src/ispypsa/translator/helpers.py` (a generic
helper with a raising failure contract) and `_fuzzy_match_names` in
`src/ispypsa/templater/helpers.py` (a generic helper with a fallback-and-log contract).

#### No module docstrings

New modules don't get a module docstring. The entry point's orienting docstring does the
job of orienting a reader who has never seen the module, and it sits next to the code it
describes, so it is less likely to go stale.

The entry point's docstring doesn't have to carry everything a module docstring would
have. It orients; the reader then gets the full story from the source and the helpers'
docstrings. In particular:

- **The step-by-step story is the entry point's body.** Written as an orchestrator, it
  already reads as the sequence of steps.
- **Hardcoded decisions go beside the code that makes them:** a comment on the mapping
  constant, the docstring of the helper that drops the rows.

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

When a test's expected output follows from several inputs combining, or from rows that live
only in a shared baseline helper (e.g. `_constraint_tables`), add a brief comment above the
relevant table that names the specific rows and values and the output they produce. Prefer
`# Q1-NQ has no limit row, so it takes the default (500)` over `# default applies`. Use
Python comments, since `csv_str_to_df` doesn't strip `#` inside the CSV. Skip comments the
test name or docstring already covers, and don't call a baseline helper whose tables the
test fully overwrites.

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
