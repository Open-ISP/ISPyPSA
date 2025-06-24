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
- Each function should handle its own edge cases (None inputs, empty DataFrames)

### Refactoring Patterns
- When a function has multiple independent workflows, break it into:
  1. Separate functions for each workflow
  2. A main orchestration function that calls the subfunctions
- Move validation logic (like empty checks) into the lowest appropriate level

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
# Basic test execution
source $HOME/.local/bin/env && uv run pytest tests/

# Run a specific test file
source $HOME/.local/bin/env && uv run pytest tests/test_translator/test_translate_custom_constraints.py

# Run a specific test function with verbose output
source $HOME/.local/bin/env && uv run pytest tests/test_translator/test_translate_custom_constraints.py::test_translate_custom_constraints_no_tables_no_links -v

# Run tests matching a pattern
source $HOME/.local/bin/env && uv run pytest tests/ -k "custom_constraint" -v

# With a specific virtual environment
source $HOME/.local/bin/env && UV_PROJECT_ENVIRONMENT=.venv-wsl uv run pytest tests/test_translator/test_translate_custom_constraints.py -v
```

#### Running Python Scripts with uv
```bash
# Run a Python script
source $HOME/.local/bin/env && uv run python example_workflow.py

# Run a module
source $HOME/.local/bin/env && uv run python -m ispypsa.model.build

# Interactive Python shell with project dependencies
source $HOME/.local/bin/env && uv run python

# Run with specific virtual environment
source $HOME/.local/bin/env && UV_PROJECT_ENVIRONMENT=.venv-wsl uv run python example_workflow.py
```

#### Common Workflow Commands
```bash
# Check which packages are installed
source $HOME/.local/bin/env && uv pip list

# Add a new dependency (this updates pyproject.toml and uv.lock)
source $HOME/.local/bin/env && uv add pandas

# Add a development dependency
source $HOME/.local/bin/env && uv add --dev pytest-mock

# Update dependencies
source $HOME/.local/bin/env && uv sync --upgrade

# Run pre-commit hooks
source $HOME/.local/bin/env && uv run pre-commit run --all-files
```

#### Troubleshooting
```bash
# If you get "Project virtual environment directory cannot be used" error
rm -rf .venv
source $HOME/.local/bin/env && uv sync

# To explicitly set UV_LINK_MODE if you see hardlink warnings
export UV_LINK_MODE=copy
source $HOME/.local/bin/env && uv sync
```

#### Best Practices
1. Always source the uv environment before running commands: `source $HOME/.local/bin/env`
2. Use `UV_PROJECT_ENVIRONMENT` when you have multiple virtual environments
3. Run `uv sync` after pulling changes that might have updated dependencies
4. Use `uv run` prefix for all Python-related commands to ensure the correct environment is used

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