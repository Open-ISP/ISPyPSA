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

## Testing

### Test structure

Tests follow a strict ordering: **inputs → function call → expected → assertion.**

Use the `csv_str_to_df` fixture to create readable DataFrame inputs and expected outputs. Place
each `assert_frame_equal` immediately after its expected DataFrame definition. Only include
columns in test inputs that the code actually accesses.

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
def test_both_empty():
    table_a = pd.DataFrame(columns=["id", "value"])
    table_b = pd.DataFrame(columns=["id", "name"])

    result = my_function(table_a, table_b)

    expected = pd.DataFrame(columns=["id", "value", "name"])
    pd.testing.assert_frame_equal(result, expected)
```

### Comparing DataFrames

- Sort both sides before comparison when row order doesn't matter
- Use `check_exact=False` or `rtol=1e-5` for floating point comparisons
- Use `check_dtype=False` when type precision isn't critical (e.g. NaN columns)

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
