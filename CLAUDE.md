# Claude Coding Preferences for ISPyPSA

## Code Style

### Guiding principle

**Readability first.** Favour clarity over DRY or efficiency. A little repetition is fine if it
makes the code easier to follow. Optimise for a reader who hasn't seen the codebase before.

### Orchestrator + helper pattern

High-level functions coordinate; private (`_`) helpers do the work on the data. How to divide
the work between them is covered by the principle and worked examples below. Two basics:

- **Private by default** — prefix a helper with `_` unless it is part of the public API.
- **Descriptive names** — prefer verb phrases: `_extract_*`, `_add_*`, `_filter_*`, `_map_*`,
  `_merge_*`. The name should say what the helper does without the reader opening it.

### Code shaped like its explanation

**Code should have the same shape as the explanation you would give in words.**

Before writing a function, say what it does as you would to a colleague. If that takes three
sentences, the body should be three lines or three helpers. If the explanation lists cases, the
code lists them together in the same place. If the explanation never mentions something (a
lower-cased index, a lookup dict), it shouldn't appear in a signature.

The quickest check: write the docstring first. If the docstring and the body are structured
differently, change the body.

The worked examples below are this principle applied to situations that come up often.

### How big a helper should be

A helper is worth having when it holds a chunk of work: typically three to five lines.

- **One line: almost never.** A line of pandas that already reads as a sentence (`merge`,
  `concat`, a filter) stays where it is. If it needs explaining, give it a comment.
- **Two lines: rarely.**
- **Three to five lines: the normal size.**
- **Longer than five: typically not.** Look for a chunk inside it that can be named.

This counts statements, not physical lines: one DataFrame constructor formatted over eight
lines is one statement. Orchestrators, which are sequences of calls, and functions that lay
out a set of cases side by side with masks can run longer.

**This rule comes first.** Where it conflicts with anything else in this guide, the size rule
wins. Don't create a one- or two-line helper to merge twins (example 6), to give a step a name
(example 1) or to hold a mask: write the line where it is used. The only standing exception is
a function that has to exist with its signature, such as a public API function.

### Worked examples

#### 1. Name the chunks, not the lines

A function that coordinates work is an orchestrator, at any level, not only the public ones.
Where it has a block of several lines doing one job, that block wants a name: make it a helper.
Where it has a single line that already says what it does, leave it inline.

**Bad:** three jobs, each announced by a comment, and the reader has to read every line to see
where one ends and the next begins.

```python
def _template_rez_limits(limits, rez_paths):
    # Keep the REZs that have a connection path
    limits = limits.rename(columns={"REZ ID": "rez_id"})
    has_path = limits["rez_id"].isin(rez_paths["geo_from"])
    limits = limits[has_path]
    # One row per REZ and timeslice
    limits = limits.melt(id_vars="rez_id", var_name="timeslice", value_name="capacity")
    limits["timeslice"] = limits["timeslice"].map(_TIMESLICE_NAMES)
    limits["capacity"] = pd.to_numeric(limits["capacity"], errors="coerce")
    limits = limits.dropna(subset="capacity")
    # Name each row by its path
    limits = limits.merge(rez_paths, left_on="rez_id", right_on="geo_from")
    return limits[["path_id", "timeslice", "capacity"]]
```

**Good:** the two multi-line jobs are named. The merge is one line and stays.

```python
def _template_rez_limits(limits, rez_paths):
    limits = _keep_rezs_with_a_path(limits, rez_paths)
    limits = _melt_to_one_row_per_timeslice(limits)
    limits = limits.merge(rez_paths, left_on="rez_id", right_on="geo_from")
    return limits[["path_id", "timeslice", "capacity"]]
```

**Too far:** a helper per line. Nothing is easier to read, and there are now two more functions
to find and open.

```python
    limits = _keep_rezs_with_a_path(limits, rez_paths)
    limits = _melt_to_one_row_per_timeslice(limits)
    limits = _add_path_id(limits, rez_paths)
    return _select_output_columns(limits)
```

Modelled on: `_select_least_cost_option_per_expansion` in `templater/network_expansion.py`.

#### 2. If there is only one step, do the work

An orchestrator that makes a single call whose name restates its own hasn't told a story. It has
moved the work one level down and wrapped it in a loop. If there is one thing to say, say it
directly.

**Bad:** `_add_cost_column` calls `_get_cost`. The second name adds nothing to the first.

```python
def _add_cost_column(options, costs):
    options = options.copy()
    options["cost"] = [
        _get_cost(option, year, costs)
        for option, year in zip(options["option"], options["year"])
    ]
    return options


def _get_cost(option, year, costs):
    match = costs[(costs["option"] == option) & (costs["year"] == year)]
    return match["cost"].iloc[0]
```

**Good:** one line, written where it is needed. No helper at all.

```python
options = options.merge(costs[["option", "year", "cost"]], on=["option", "year"])
```

The opposite mistake is inventing steps so the orchestrator looks like a story: a "translate"
pass followed by a "match" pass, with a half-finished column in between. Only split where the
explanation has a real "and then".

#### 3. Put row logic in a row function, and apply it

When a value is worked out one row at a time, write the rule as a small function of the row and
apply it. The function reads like the rule ("a line is looked up, a node keeps its name"), and
`apply` says plainly that the rule is used on every row. A comprehension over zipped columns
says the same thing, but the reader has to match up the zip, the unpacking and the arguments.

**Bad:** the rule is fine; the way it is applied is the hard part to read.

```python
def _add_variable_name_column(terms):
    terms = terms.copy()
    terms["variable_name"] = [
        _variable_name(kind, name) for kind, name in zip(terms["kind"], terms["name"])
    ]
    return terms


def _variable_name(kind, name):
    if kind == "unit":
        return _AREA_SUFFIX_PATTERN.sub("", name)
    if kind == "line":
        return _LINE_TO_PATH_ID[name]
    return name
```

**Good:** the same rule, taking the row, defined straight after the function that applies it.

```python
def _add_variable_name_column(terms):
    terms = terms.copy()
    terms["variable_name"] = terms.apply(_variable_name, axis=1)
    return terms


def _variable_name(term):
    if term["kind"] == "unit":
        return _AREA_SUFFIX_PATTERN.sub("", term["name"])
    if term["kind"] == "line":
        return _LINE_TO_PATH_ID[term["name"]]
    return term["name"]
```

The row function is an ordinary module-level helper, placed directly below the function that
uses it, so the file reads top down: what is done, then how. Don't define it inside the
function that applies it.

This works while the row function needs only the row's own values and module constants. Once a
case needs something from outside the row (another table, or a lookup built from one), a row
function has to have it passed in, and example 5 applies. Handle that kind of row as a column
instead: select it with a mask and use `map` or `merge` on it. `_build_options_table` in
`templater/network_expansion.py` is a worked example: whether an option is a path or a
constraint relaxation depends on another table, so each kind is selected with a mask and the
two cases sit side by side.

One cost to know about: `apply(..., axis=1)` on a table with no rows returns an empty DataFrame,
not a Series, so a function that must handle empty input may need a column operation anyway.

#### 4. Write each column once, with its final meaning

A column should mean one thing for its whole life. If a value is only a stepping stone, give it
its own name (a local variable or a differently named column) so no reader ever sees the column
holding something its name doesn't describe.

**Bad:** `build_year` is a placeholder string, then a list, then an integer.

```python
def _add_build_years(generators, investment_periods):
    generators["build_year"] = "investment_periods"
    generators["build_year"] = generators["build_year"].map(
        {"investment_periods": investment_periods}
    )
    generators = generators.explode("build_year").reset_index(drop=True)
    generators["build_year"] = generators["build_year"].astype("int64")
    return generators
```

**Good:** the column appears once, already meaning what it says: every generator paired with
every investment period.

```python
def _add_build_years(generators, investment_periods):
    build_years = pd.DataFrame({"build_year": investment_periods})
    return generators.merge(build_years, how="cross")
```

The same applies to "fill for every row, then zero some out": compute the mask first and write
the column once.

Modelled on: `_derive_phes_symmetric_efficiency` in `templater/helpers.py`.

#### 5. Pass data, not preparations

Give a helper the data it works on and let it build what it needs. A lookup built at the top and
passed down through two signatures makes every signature on the way harder to read, and the
reader has to go looking for where it came from.

**Bad:** `rez_to_path_id` is built two levels above the one line that uses it.

```python
def _extract_rez_options(option_tables, rez_paths):
    rez_to_path_id = dict(zip(rez_paths["geo_from"], rez_paths["path_id"]))
    frames = [_normalise_options(table, rez_to_path_id) for table in option_tables]
    return pd.concat(frames, ignore_index=True)


def _normalise_options(table, rez_to_path_id):
    return pd.DataFrame(
        {
            "expansion_id": _to_expansion_id(table["REZ ID"], rez_to_path_id),
            "forward_mw": table["Additional network capacity (MW)"],
        }
    )


def _to_expansion_id(rez_ids, rez_to_path_id):
    return rez_ids.replace(rez_to_path_id)
```

**Good:** the helper that needs the lookup is given the table and builds it.

```python
def _extract_rez_options(option_tables, rez_paths):
    options = pd.concat(option_tables, ignore_index=True)
    options = _add_expansion_id(options, rez_paths)
    return options[["expansion_id", "Additional network capacity (MW)"]].rename(
        columns={"Additional network capacity (MW)": "forward_mw"}
    )


def _add_expansion_id(options, rez_paths):
    rez_to_path_id = dict(zip(rez_paths["geo_from"], rez_paths["path_id"]))
    options = options.copy()
    options["expansion_id"] = options["REZ ID"].replace(rez_to_path_id)
    return options
```

Two notes:

- If several helpers need the same derived thing, build it once in the orchestrator with a
  named helper, so it is a step in the story.
- A lookup passed down "to avoid rebuilding it for every row" is a sign the helper should take
  the column, not the row.

Modelled on: `_relaxation_coefficients` in `translator/constraints.py`, `_expand_column` in
`translator/helpers.py`.

#### 6. Merge twins, keep siblings

Two helpers whose bodies differ by one literal or one column are twins: a reader has to diff
them to find out whether the difference matters. Make one helper and show the difference at the
call site.

**Bad:** two copies of five lines, differing in one word.

```python
def _link_expansion_terms(links):
    terms = links[["isp_name", "name"]]
    terms = terms.rename(columns={"isp_name": "constraint_id", "name": "variable_name"})
    terms["component"] = "Link"
    terms["attribute"] = "p_nom"
    terms["coefficient"] = 1.0
    return terms


def _generator_expansion_terms(generators):
    terms = generators[["isp_name", "name"]]
    terms = terms.rename(columns={"isp_name": "constraint_id", "name": "variable_name"})
    terms["component"] = "Generator"
    terms["attribute"] = "p_nom"
    terms["coefficient"] = 1.0
    return terms
```

**Good:** one helper, and the difference is an argument the reader sees at the call site.

```python
def _expansion_terms(components, component_type):
    terms = components[["isp_name", "name"]]
    terms = terms.rename(columns={"isp_name": "constraint_id", "name": "variable_name"})
    terms["component"] = component_type
    terms["attribute"] = "p_nom"
    terms["coefficient"] = 1.0
    return terms


link_terms = _expansion_terms(links, "Link")
generator_terms = _expansion_terms(generators, "Generator")
```

This is for twins that are real chunks of work. If each twin is only a line or two, the size
rule wins: don't merge them into a shared one-line helper. Remove the helpers and write the
line at each place it is used.

"Readability over DRY" still holds. Two short helpers that look alike but state different rules
are siblings, not twins, and should both stay: `_drop_terms_before_build_year` and
`_drop_terms_from_retirement_year` in `translator/constraints.py` are near-identical, but one
bound is inclusive and the other exclusive, and the orchestrator reads better with both named.

Modelled on: `_expansion_limit_lhs` in `translator/constraints.py`, called once for links and
once for generators.

#### 7. Every helper earns its place

A helper should either do work on the data or set out a sequence or choice the reader needs to
see. One that only forwards to another function is a layer to read through. The test: would the
call site read worse with the helper's body written in place?

**Bad:** neither helper says anything the call it wraps doesn't.

```python
def _load_plexos_csv(path):
    return pd.read_csv(path)


def _strip_area_suffix(name):
    return _AREA_SUFFIX_PATTERN.sub("", name)


constraints = _load_plexos_csv(extract_dir / "constraints.csv")
```

**Good:**

```python
constraints = pd.read_csv(extract_dir / "constraints.csv")
```

The rare one-line helper that does earn its place has a name carrying meaning the line
doesn't, and is used in many places: `_is_battery_row(df)` for
`df["Technology Type"].str.contains("Batter")`, called across several modules.

#### 8. Declare the cases as data

When the cases are a small, stable mapping from one thing to another, write the mapping down as
a table or dict and apply it with `map` or `merge`. An `if` ladder makes the reader execute the
code in their head to recover the table.

**Bad:** the mapping from term to model variable is spread across branches, with `None` for
"anything else".

```python
def _get_variable(model, component, attribute, name):
    var = None
    if component == "Generator" and attribute == "p_nom":
        var = model.variables.Generator_p_nom.at[name]
    elif component == "Link" and attribute == "p":
        var = model.variables.Link_p.loc[:, name]
    elif component == "Generator" and attribute == "p":
        var = model.variables.Generator_p.loc[:, name]
    return var
```

**Good:** the table is the documentation.

```python
_TERM_TO_VARIABLE_TYPE = pd.DataFrame(
    [
        ("Generator", "p_nom", "Generator-p_nom"),
        ("Generator", "p", "Generator-p"),
        ("Link", "p", "Link-p"),
    ],
    columns=["component", "attribute", "variable_type"],
)

terms = terms.merge(_TERM_TO_VARIABLE_TYPE, on=["component", "attribute"])
```

Modelled on: `_TERM_TO_VARIABLE_TYPES` in `pypsa_build/custom_constraints.py`,
`_CONSTRAINT_TYPE_TO_RELAXATION_COEFFICIENT` in `translator/constraints.py`.

#### 9. Let empty tables flow through

Missing data is a table with all its columns and no rows. Filters, merges, group-bys and concats
all handle that without help. A helper that returns `None` or a column-less frame for the empty
case forces a guard on every caller, and the guards crowd out the story.

**Bad:** half the code is about emptiness.

```python
def _storage_dispatch(network):
    if network.storage_units.empty:
        return pd.DataFrame()
    return _dispatch_in_long_format(network.storage_units_t.p)


storage = _storage_dispatch(network)
if not storage.empty:
    dispatch = pd.concat([generators, storage[columns]], ignore_index=True)
else:
    dispatch = generators
```

**Good:** the helper always returns the same columns, so the caller has nothing to check.

```python
storage = _dispatch_in_long_format(network.storage_units_t.p)
dispatch = pd.concat([generators, storage], ignore_index=True)
```

The same goes for a sentinel set and then immediately tested (`trace = None` ... `if trace is
not None:`): select the rows that have the thing with a mask and work on those.

Modelled on: `_expand_terms_to_model_components` in `translator/constraints.py`.

#### 10. No unstated agreements between caller and helper

A helper's correctness shouldn't depend on something the caller did that isn't in the signature
or the name: a column created beforehand, or two tables that happen to be in the same row
order. Those agreements survive until the next refactor.

**Bad:** `path_ids` is attached by position, so this only works while both tables are in the
same order. The caller also had to compute it first.

```python
topology = _parse_flow_path_topology(capability["Flow Paths"])
limits = _extract_flow_path_limits(capability, topology["path_id"])


def _extract_flow_path_limits(capability, path_ids):
    limits = capability.copy()
    limits["path_id"] = path_ids.values
    return _melt_capacity_columns(limits)
```

**Good:** the helper works out the path id from its own rows.

```python
limits = _extract_flow_path_limits(capability)


def _extract_flow_path_limits(capability):
    limits = capability.copy()
    limits["path_id"] = _path_ids_from_names(limits["Flow Paths"])
    return _melt_capacity_columns(limits)
```

A second form of the same problem: a helper that fills in part of a column the caller had to
create first. Have the helper return the values it found, and let the orchestrator combine them
(`coal_minimum_load.combine_first(gas_minimum_load)`), which also puts the cases side by side.

#### 11. A comprehension over DataFrame columns is a column operation in disguise

A list or dict comprehension asks the reader to hold several things at once: what is being
looped over, how it is unpacked, and what is built from it. Over one plain list that is easy.
Over two or three zipped columns, with a function call inside, it is where the eyes glaze over.
When the things being zipped are columns of the same table, there is almost always a column
operation that says the same thing directly.

**Bad:** three columns zipped, unpacked and formatted, to build one string per row.

```python
changes = sorted(
    set(
        f"{name}: {old} -> {new}"
        for name, old, new in zip(
            lhs["variable_name"], lhs["term_type"], lhs["templated_type"]
        )
    )
)
```

**Good:** the same strings, built from the columns.

```python
changes = lhs["variable_name"] + ": " + lhs["term_type"] + " -> " + lhs["templated_type"]
changes = sorted(set(changes))
```

When the comprehension calls a function for each row, look at what that function does, because
the cure depends on it:

- It works from the row's own values, with or without a branch: keep the function, make it
  take the row, and use `apply` (example 3).
- It looks something up in another table: use `map` or `merge` (example 2).
- It needs a lookup passed in: the helper should take the column (example 5).

A comprehension is fine when it has one loop variable and a plain expression and fits on a
line, and it is not walking the rows of a table:

```python
frames = [_normalise_options(table) for table in option_tables]
templated_name_by_lowercase = {name.lower(): name for name in unit_names}
```

If it needs a condition, a second loop variable or a nested loop and no column operation fits,
write a plain `for` loop.


### When the rules pull against each other

These are the places where judgement is needed.

- **Helper size versus visible cases.** Helpers are typically three to five lines. A function
  that lays out three or four cases with masks can run to about ten. Prefer keeping
  the cases together over splitting to meet the count.
- **Naming chunks versus wrapping lines.** Example 1 says to name a multi-line job; the size
  rule says not to wrap one or two lines. The size rule wins. A comment-led function whose
  comments each label one line is already readable and should be left alone.
- **Story versus invented steps.** Example 1 says to name the chunks; example 2 says not to
  make steps up. The test is whether the plain-words explanation has that step in it.
- **Cases as data versus explicit calls.** Example 8 says to turn a small mapping into a table.
  That doesn't extend to dispatching function calls from a dict in a loop with special cases for
  some keys: nine explicit `results["x"] = extract_x(...)` lines are clearer than a loop that
  skips two names and switches call signature on a third list.
- **Readability over DRY versus twins.** Repetition is fine when each copy says something
  different (siblings). It isn't when the reader has to diff two functions to learn they are the
  same (twins), unless the twins are a line or two each, in which case the size rule wins and
  the line is written where it is used.
- **Row function versus column operations.** A row function with `apply` is the default for
  per-row rules, including a simple choice between cases (example 3). Switch to masks and
  column operations when a case needs data from outside the row (example 5), or when the
  function has to cope with an empty table. No rule here about call depth: it is too hard to
  say where to start counting.

### Clarity over cleverness

- **Avoid positional access** like `iloc[:, 0]` — use named column access (e.g.
  `df["Flow Paths"]`) so the code states what it means.
- **Comment non-obvious regex** — add a concrete example of the input being matched and
  annotate each capture group.
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

### Docstrings: I/O Example

Every non-trivial function should include an `I/O Example:` section in its docstring
showing a concrete input → output mapping. The goal is that a reviewer can understand
the function's behaviour from the docstring alone, without reading the body.

Conventions:

- Use a plain CSV-like table format for DataFrame inputs and outputs — no need to
  wrap in runnable `csv_str_to_df` calls, since this is illustrative, not a doctest.
- Always lay DataFrame examples out as full tables — a header row plus example rows,
  each table in its own block — even when a single row would do. Never condense a
  table into `column=value` prose. Abbreviated column sets, row sets and column
  names are fine when the full table would otherwise be large; point at the
  relevant constants for the real names.
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

### Docstrings: generic helpers

Most helpers don't need this section — a one-line summary and an I/O Example
is right for a helper that names its own columns and serves one caller.
Apply this guide when a helper is *generic*: the signals are that

- column or key names arrive as parameters instead of appearing literally in
  the body, so the body alone can't tell the reader what the data means;
- it has (or is written to invite) several call sites with different domain
  meanings; and
- it enforces a rule its callers depend on but don't restate — a precedence
  order, a validation contract, a guarantee about the output's shape.

The first signal is the gate; the other two raise the stakes. For these
functions the docstring is the only place the full story exists, so it earns
a longer, structured treatment — a short narrative, one idea per paragraph,
in this order:

1. **Summary line: the function's effect, naming the parameters it
   connects.** "Resolve which row of ``table`` applies to each key
   combination in ``allowed_values``, returning one explicit row per
   combination." The summary should earn the function's name — if it is
   called _resolve_*, the summary should say what gets resolved.

2. **The input's story.** Before any mechanism, explain what the input data
   is and why it looks the way it does — especially the oddity the function
   exists to handle ("Rather than write out a row for every key, the user
   may leave a key cell blank…"). Motivating the weirdness is what makes the
   mechanism paragraph land.

3. **What each remaining parameter represents**, in domain terms rather than
   shape ("every value the model needs an entry for — e.g. the enabled
   expansion elements").

4. **The mechanism** — how the function connects the inputs, precise enough
   to cover the compound cases (see below).

5. **The failure contract last** — what happens when the work can't be
   completed for some of the data (raise, fallback, or log-and-continue),
   and why that response is the right one.

Wording rules, beyond the general terminology guidance:

- **Define derived vocabulary from the signature at first mention.** If the
  prose says "key columns", pin it down inline: "a key column — any column
  of ``table`` not in ``value_columns``". A term defined only by the body's
  code is a term the docstring hasn't defined.
- **State behaviour for the compound case, not just the simple one.** "Each
  blank is expanded to one row per value" is vague about a row with two
  blanks; "one row per combination of ``allowed_values`` across its blank
  columns" is not.
- **Phrase rules to cover every case the code handles.** A tie-break
  described as "an expanded row vs one that named the key directly"
  misdescribes two expanded rows colliding; "where several rows land on the
  same key, the one that started with fewer blanks wins" covers both.
- **Borrow concrete names from the I/O Example in the prose** (direction,
  path_id). A generic function documented only in generic nouns is hard to
  follow; the example's columns give the prose something to point at.
- **Prefer a concrete failing input to set-membership phrasing.** "A
  direction of, say, 'sideways' raises" beats "non-blank values must be
  among the listed values".
- **Don't let one word do double duty.** "A key column *listed* in
  ``allowed_values``" and "among the *listed* values" use one word for two
  different lookups.
- **Verify claims about callers and upstream schemas against the code.**
  A docstring that states where a parameter's values come from is making a
  testable claim — check the call sites first, and describe what callers
  actually pass rather than the provenance you assume.

Worked examples: `_resolve_wildcards` in `src/ispypsa/translator/helpers.py`
(a raising failure contract) and `_fuzzy_match_names` in
`src/ispypsa/templater/helpers.py` (a fallback-and-log contract).

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
