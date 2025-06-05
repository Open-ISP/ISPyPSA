AI systems when editing this code base, please:

When writing tests:
- test functionality hard coding input dataframes and comparing outputs to hardcoded
  output dataframes
- Define dataframes as per test/test_translator/test_links.py, using the csv_str_to_df
  function
- maximise the readability of tests.
- Use short docstrings of one or two lines to define tests
- Don't use mocking in tests, except for defining compact versions of the ModelConfig
