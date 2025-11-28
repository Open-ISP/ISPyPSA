# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

## Types of Contributions

### Report Bugs

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug, preferably with a simple code example that reproduces the bug.

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

### Write Documentation

You can never have enough documentation! Please feel free to contribute to any
part of the documentation, such as the official docs, docstrings, or even
on the web in blog posts, articles, and such.

### Submit Feedback

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

## Get Started!

Ready to contribute? Here's how to set up `ispypsa` for local development.

1. Download a copy of `ispypsa` locally.
2. Install [`uv`](https://github.com/astral-sh/uv).
3. Install `ispypsa` using `uv` by running `uv sync` in the project directory.
4. Install the `pre-commit` git hook scripts that `ispypsa` uses by running the following code using `uv`:

      ```console
      $ uv run pre-commit install
      ```

5. Use `git` (or similar) to create a branch for local development and make your changes:

    ```console
    $ git checkout -b name-of-your-bugfix-or-feature
    ```

6. When you're done making changes, check that your changes conform to any code formatting requirements (we use [`ruff`](https://github.com/astral-sh/ruff)) and pass any tests.
    - `pre-commit` should run `ruff`, but if you wish to do so manually, run the following code to use `ruff` as a `uv` [tool](https://docs.astral.sh/uv/concepts/tools/):

      ```bash
      uvx ruff check --fix
      uvx ruff format
      ```

    - Run tests by running `uv run --frozen pytest`

    - To run extensive CLI tests (slower, includes dependency modification and missing file triggers), set the `ISPYPSA_RUN_EXTENSIVE` environment variable:

      ```bash
      # Unix/Linux/Mac/WSL
      ISPYPSA_RUN_EXTENSIVE=1 uv run pytest tests/test_cli/

      # Windows PowerShell
      $env:ISPYPSA_RUN_EXTENSIVE="1"; uv run pytest tests/test_cli/

      # Windows CMD
      set ISPYPSA_RUN_EXTENSIVE=1 && uv run pytest tests/test_cli/
      ```

7. Commit your changes and open a pull request.

## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include additional tests if appropriate.
2. If the pull request adds functionality, the docstrings/README/docs should be updated.
3. The pull request should work for all currently supported operating systems and versions of Python.

## Code of Conduct

Please note that the `ispypsa` project is released with a
[Code of Conduct](CONDUCT.md). By contributing to this project you agree to abide by its terms.
