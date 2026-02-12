import nox

nox.options.default_venv_backend = "uv"

PYTHON_VERSIONS = [
    "3.11",
    "3.12",
    "3.13",
    "3.14"
]


@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.Session) -> None:
    """Run the test suite."""
    session.install("-e", ".")
    session.install(
        "pytest",
        "pytest-cov",
        "pytest-mock",
        "requests-mock",
    )
    session.run("pytest")
