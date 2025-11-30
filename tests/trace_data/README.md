# Test Trace Data

This directory contains parquet trace data files used for testing ISPyPSA.

## Data Filtering

The trace data files have been filtered to only include rows where `datetime < 2028-01-01 00:00:00`. This reduces file sizes while retaining sufficient data for test coverage.
