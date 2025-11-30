"""Tests for data_fetch download functionality."""

import pytest
import requests

from ispypsa.data_fetch import fetch_workbook


def test_fetch_workbook_success(tmp_path, requests_mock):
    """Test successful workbook download."""
    # Mock the HTTP request for the actual workbook manifest
    requests_mock.get(
        "https://data.openisp.au/archive/workbooks/6.0.xlsx",
        content=b"fake workbook content",
    )

    save_path = tmp_path / "my_workbook.xlsx"
    fetch_workbook("6.0", save_path)

    assert save_path.exists()
    assert save_path.read_bytes() == b"fake workbook content"


def test_fetch_workbook_creates_parent_directories(tmp_path, requests_mock):
    """Test that fetch_workbook creates parent directories."""
    # Mock the HTTP request
    requests_mock.get(
        "https://data.openisp.au/archive/workbooks/6.0.xlsx",
        content=b"fake workbook content",
    )

    save_path = tmp_path / "deep" / "nested" / "path" / "workbook.xlsx"
    fetch_workbook("6.0", save_path)

    assert save_path.exists()
    assert save_path.parent.exists()


def test_fetch_workbook_nonexistent_version(tmp_path):
    """Test that FileNotFoundError is raised for nonexistent version."""
    with pytest.raises(FileNotFoundError, match="Manifest file not found"):
        fetch_workbook("99.9", tmp_path / "workbook.xlsx")


def test_fetch_workbook_http_error(tmp_path, requests_mock):
    """Test that HTTP errors are raised properly."""
    # Mock a 404 response
    requests_mock.get(
        "https://data.openisp.au/archive/workbooks/6.0.xlsx", status_code=404
    )

    with pytest.raises(requests.HTTPError):
        fetch_workbook("6.0", tmp_path / "workbook.xlsx")
