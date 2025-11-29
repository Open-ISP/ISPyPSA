# Claude Coding Preferences for ISPyPSA

This document captures coding preferences and patterns learned from working on the ISPyPSA project.

## Testing Preferences

### Test Structure
- Prefer comparing results to hardcoded DataFrames rather than using assert statements to check general properties
- Use the `csv_str_to_df` fixture to create test data in a readable format
- Sort DataFrames before comparison to ensure consistent ordering
- Test one thing at a time - implement and run tests individually before moving to the next

### Using csv_str_to_df Fixture

The `csv_str_to_df` fixture is a pytest fixture that converts CSV-formatted strings into pandas DataFrames. This makes tests more readable and maintainable.

#### Basic Usage
```python
def test_my_function(csv_str_to_df):
    # Create input data
    input_data_csv = """
    name,     value,  active
    item1,    100,    True
    item2,    200,    False
    """
    input_data = csv_str_to_df(input_data_csv)

    # Create expected output
    expected_output_csv = """
    name,     processed_value
    item1,    150
    item2,    250
    """
    expected_output = csv_str_to_df(expected_output_csv)

    # Call function and compare
    result = my_function(input_data)
    pd.testing.assert_frame_equal(result, expected_output)
```

#### Important Notes on CSV Formatting
- **Whitespace**: The fixture handles whitespace around commas, making the CSV more readable
- **Data Types**: The fixture infers data types (integers, floats, booleans, strings)
- **Special Values**: Use `NaN` for missing values, `inf` for infinity
- **Column Alignment**: Align columns for better readability (optional but recommended)

#### Complete Test Example
```python
def test_translate_custom_constraints_with_tables_no_rez_expansion(csv_str_to_df):
    """Test translation of custom constraints when tables are present but REZ transmission expansion is disabled."""

    # Input: REZ group constraints RHS
    rez_group_constraints_rhs_csv = """
    constraint_id,  summer_typical
    REZ_NSW,        5000
    REZ_VIC,        3000
    """

    # Input: REZ group constraints LHS
    rez_group_constraints_lhs_csv = """
    constraint_id,  term_type,           variable_name,  coefficient
    REZ_NSW,        generator_capacity,  GEN1,           1.0
    REZ_NSW,        generator_capacity,  GEN2,           1.0
    REZ_VIC,        generator_capacity,  GEN3,           1.0
    """

    # Input: Links DataFrame
    links_csv = """
    isp_name,    name,                 carrier,  bus0,    bus1,    p_nom,  p_nom_extendable
    PathA-PathB, PathA-PathB_existing, AC,       NodeA,   NodeB,   1000,   False
    """

    # Convert CSV strings to DataFrames
    ispypsa_tables = {
        "rez_group_constraints_rhs": csv_str_to_df(rez_group_constraints_rhs_csv),
        "rez_group_constraints_lhs": csv_str_to_df(rez_group_constraints_lhs_csv),
    }
    links = csv_str_to_df(links_csv)

    # Mock configuration
    class MockNetworkConfig:
        rez_transmission_expansion = False

    class MockConfig:
        network = MockNetworkConfig()

    config = MockConfig()

    # Call the function under test
    result = _translate_custom_constraints(config, ispypsa_tables, links)

    # Expected RHS result
    expected_rhs_csv = """
    constraint_name,  rhs
    REZ_NSW,          5000
    REZ_VIC,          3000
    """
    expected_rhs = csv_str_to_df(expected_rhs_csv)

    # Expected LHS result - note the column order matches the actual output
    expected_lhs_csv = """
    constraint_name,  variable_name,  coefficient,  component,  attribute
    REZ_NSW,          GEN1,           1.0,          Generator,  p_nom
    REZ_NSW,          GEN2,           1.0,          Generator,  p_nom
    REZ_VIC,          GEN3,           1.0,          Generator,  p_nom
    """
    expected_lhs = csv_str_to_df(expected_lhs_csv)

    # Assert results are as expected
    assert "custom_constraints_rhs" in result
    assert "custom_constraints_lhs" in result

    # Compare DataFrames with sorting to handle row order differences
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"]
        .sort_values("constraint_name")
        .reset_index(drop=True),
        expected_rhs.sort_values("constraint_name").reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"]
        .sort_values(["constraint_name", "variable_name"])
        .reset_index(drop=True),
        expected_lhs.sort_values(["constraint_name", "variable_name"])
        .reset_index(drop=True)
    )
```

### Test Writing Best Practices

1. **Column Order Matters**: Pay attention to the actual column order in the output. Run the test first to see the actual order, then adjust the expected CSV to match.

2. **Sorting for Comparison**: When row order doesn't matter, sort both DataFrames before comparison:
   ```python
   pd.testing.assert_frame_equal(
       actual.sort_values(["col1", "col2"]).reset_index(drop=True),
       expected.sort_values(["col1", "col2"]).reset_index(drop=True)
   )
   ```

3. **Handling Special Cases**:
   - For DataFrames with NaN values, use `check_dtype=False` if type precision isn't critical
   - For floating point comparisons, consider using `check_exact=False` or `rtol=1e-5`
   - For columns that are calculated (like capital_cost), exclude them from comparison:
     ```python
     actual_to_compare = actual.drop(columns=["capital_cost"])
     ```

4. **Empty DataFrame Testing**:
   ```python
   # Test empty input returns empty output
   result = my_function(pd.DataFrame())
   assert result.empty
   ```

## Code Organization

### Function Design
- Prefer small, focused functions with single responsibilities
- Extract complex workflows into independent subfunctions that can be tested separately
- Functions should return data (e.g., DataFrames) rather than modifying state
- **DEFAULT: Write non-defensive code**
  - Do NOT add defensive programming features unless explicitly needed or revealed through testing
  - Trust the design decisions and caller contracts
  - If the architecture dictates a parameter will always be provided, don't add `None` checks
  - Don't add fallback logic "just in case" when the system is designed to prevent that case
  - Don't add try/except blocks unless error handling is explicitly required
  - Defensive checks add noise, obscure the actual logic, and hide bugs that should fail fast
  - Let the code fail clearly when preconditions aren't met rather than silently handling edge cases
- Don't maintain backward compatibility unless explicitly requested
  - When refactoring function signatures, update all call sites directly
  - Don't create deprecated wrapper functions or aliases
  - Breaking changes to internal APIs are acceptable

### Refactoring Patterns
- When a function has multiple independent workflows, break it into:
  1. Separate functions for each workflow
  2. A main orchestration function that calls the subfunctions
- Move validation logic (like empty checks) into the lowest appropriate level

### Post-Drafting Review Questions

After drafting a function, consider these refactoring opportunities:

1. **Could vectorized pandas operations improve code clarity?**
   - Look for nested loops iterating over DataFrame rows
   - Consider using `.pivot()`, `.reindex()`, `.ffill()`, `.groupby()`, `.merge()` instead
   - Vectorized operations are typically faster and more readable

   Example:
   ```python
   # Before: Nested loops with manual state tracking
   for entity in entities:
       entity_data = data[data["entity_id"] == entity]
       values = []
       for year in years:
           year_data = entity_data[entity_data["year"] == year]
           if not year_data.empty:
               values.append(year_data["value"].iloc[0])
           else:
               values.append(values[-1] if values else 0)

   # After: Vectorized with pivot + reindex + ffill
   pivot_data = data.pivot(index="year", columns="entity_id", values="value")
   pivot_data = pivot_data.reindex(years).ffill().fillna(0)
   ```

2. **Could helper functions improve code clarity/conciseness?**
   - Look for duplicated code blocks (especially with only minor variations)
   - Consider extracting repeated logic into a parameterized helper function
   - Helper functions should be private (start with `_`) if not used outside the module

   Signs you need a helper function:
   - Same logic repeated with different variable names
   - Copy-pasted code blocks with minor differences
   - Function longer than ~50 lines with distinct sections

3. **Should defensive code be removed?**
   - **DEFAULT: Remove defensive checks unless explicitly needed**
   - Don't add defensive checks when the architecture prevents the case from occurring
   - Trust design decisions - if a parameter is always provided by design, don't check for None
   - Remove try/except blocks that silently handle errors without clear justification
   - Consider whether edge cases are realistic given the system's design

   Example of unnecessary defensiveness:
   ```python
   # Before: Overly defensive when link_flows is always provided by caller
   def extract_flows(link_flows: pd.DataFrame | None = None, network: pypsa.Network = None):
       if link_flows is None:
           link_flows = _extract_raw_link_flows(network)
       # ... process link_flows

   # After: Trust the design - link_flows is always provided
   def extract_flows(link_flows: pd.DataFrame):
       # ... process link_flows
   ```

   Example of pandas operations that don't need defensive checks:
   ```python
   # Filtering empty DataFrame returns empty DataFrame anyway - no check needed
   region_data = data[data["region"] == region]  # Works even if data is empty

   # Many pandas operations handle empty DataFrames gracefully
   result = data.groupby("category").sum()  # Returns empty if data is empty
   ```

   Only keep defensive code when:
   - Explicitly requested by the user or revealed necessary through testing
   - At module boundaries (public API) where you don't control the caller AND edge cases are documented
   - The edge case would cause cryptic errors or silent data corruption AND is explicitly tested
   - The defensive check is part of a tested, real-world usage pattern

### Example Refactoring Pattern
```python
# Before: Monolithic function with multiple responsibilities
def complex_function(inputs):
    # Workflow 1
    # ... lots of code ...

    # Workflow 2
    # ... lots of code ...

    return results

# After: Separated concerns
def _process_workflow_1(inputs):
    # Handle edge cases
    if inputs is None:
        return pd.DataFrame()
    # ... focused code ...
    return result

def _process_workflow_2(inputs):
    # ... focused code ...
    return result

def complex_function(inputs):
    result1 = _process_workflow_1(inputs)
    result2 = _process_workflow_2(inputs)
    return combine_results(result1, result2)
```

## Development Workflow

### Version Control
- Be cautious about committing changes - only commit when explicitly requested
- Use descriptive git messages that focus on the "why" rather than the "what"

### Environment Setup
- Use `uv` for Python package management
- Prefer `uv sync` over `uv pip install -e .` when a lock file exists
- Create separate virtual environments for different platforms (e.g., `.venv-wsl` for WSL)

### Using uv for Development

#### Initial Setup (WSL/Linux)
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Source the uv environment
source $HOME/.local/bin/env

# Create a virtual environment (use different names for different platforms)
uv venv .venv-wsl  # For WSL
# or
uv venv .venv      # For native Linux/Mac

# Install dependencies from lock file
uv sync

# Or if you need to specify the venv location
UV_PROJECT_ENVIRONMENT=.venv-wsl uv sync
```

#### Running Tests with uv
```bash
# Basic test execution (RECOMMENDED)
uv run pytest tests/

# Run a specific test file
uv run pytest tests/test_translator/test_translate_custom_constraints.py

# Run a specific test function with verbose output
uv run pytest tests/test_translator/test_translate_custom_constraints.py::test_translate_custom_constraints_no_tables_no_links -v

# Run tests matching a pattern
uv run pytest tests/ -k "custom_constraint" -v
```

#### Running Python Scripts with uv
```bash
# Run a Python script (RECOMMENDED)
uv run example_workflow.py

# Run a module
uv run python -m ispypsa.model.build

# Interactive Python shell with project dependencies
uv run python
```

#### Common Workflow Commands
```bash
# Check which packages are installed
uv pip list

# Add a new dependency (this updates pyproject.toml and uv.lock)
uv add pandas

# Add a development dependency
uv add --dev pytest-mock

# Update dependencies
uv sync --upgrade

# Run pre-commit hooks
uv run pre-commit run --all-files
```

#### Troubleshooting
```bash
# If you get "Project virtual environment directory cannot be used" error
rm -rf .venv
uv sync

# To explicitly set UV_LINK_MODE if you see hardlink warnings
export UV_LINK_MODE=copy
uv sync
```

#### Best Practices
1. Use `uv run` for all Python scripts and test execution
2. Use `uv sync` after pulling changes that might have updated dependencies
3. The `uv` command automatically detects and uses the project's virtual environment
4. For WSL/Linux environments where uv is installed via the shell script, you may need to source the environment first: `source $HOME/.local/bin/env`

### Testing Workflow
1. Implement the test with hardcoded expected results
2. Run the test to see if it passes
3. Fix any issues (like column ordering) based on actual results
4. Verify the test passes before moving to the next one

## Code Style

### DataFrame Operations
- Be explicit about column ordering in tests
- Use pandas testing utilities for DataFrame comparisons:
```python
pd.testing.assert_frame_equal(
    actual.sort_values("key").reset_index(drop=True),
    expected.sort_values("key").reset_index(drop=True)
)
```

### Function Naming
- Use descriptive names that indicate the function's purpose
- Private functions should start with underscore
- Use consistent naming patterns (e.g., `_process_*`, `_create_*`, `_translate_*`)

## Communication Preferences

### Progress Updates
- Work on one task at a time and show results before moving to the next
- Explain the reasoning behind refactoring suggestions
- Provide clear summaries of what was accomplished

### Problem Solving
- When tests fail, show the error and fix it step by step
- Consider alternative approaches (like refactoring) to simplify complex testing scenarios
- Ask for clarification when there are multiple possible approaches
