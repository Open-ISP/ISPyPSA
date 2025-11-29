"""Tests for data_fetch download functionality."""

from pathlib import Path
from unittest.mock import patch

import pytest
import requests

from ispypsa.data_fetch import download_from_manifest, fetch_trace_data, fetch_workbook


class TestDownloadFromManifest:
    """Test the download_from_manifest function."""

    def test_download_from_manifest_nonexistent_manifest(self, tmp_path):
        """Test that FileNotFoundError is raised for nonexistent manifest."""
        with pytest.raises(FileNotFoundError, match="Manifest file not found"):
            download_from_manifest("nonexistent/manifest", tmp_path, strip_levels=0)

    def test_download_from_manifest_empty_manifest(self, tmp_path, monkeypatch):
        """Test that ValueError is raised for empty manifest."""
        # Create an empty manifest file
        manifest_dir = (
            Path(__file__).parent.parent.parent
            / "src"
            / "ispypsa"
            / "data_fetch"
            / "manifests"
            / "test"
        )
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = manifest_dir / "empty.txt"
        manifest_file.write_text("")

        try:
            with pytest.raises(ValueError, match="No URLs found in manifest"):
                download_from_manifest("test/empty", tmp_path, strip_levels=0)
        finally:
            # Clean up
            manifest_file.unlink()
            manifest_dir.rmdir()

    def test_download_from_manifest_strip_levels_validation(
        self, tmp_path, requests_mock
    ):
        """Test that ValueError is raised when strip_levels exceeds path depth."""
        # Create a test manifest
        manifest_dir = (
            Path(__file__).parent.parent.parent
            / "src"
            / "ispypsa"
            / "data_fetch"
            / "manifests"
            / "test"
        )
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = manifest_dir / "strip_test.txt"
        manifest_file.write_text("https://example.com/a/b/file.txt\n")

        # Mock the HTTP request
        requests_mock.get("https://example.com/a/b/file.txt", text="test content")

        try:
            # This should fail because we can't strip 3 levels from "a/b/file.txt" (only 3 parts)
            with pytest.raises(ValueError, match="Cannot strip"):
                download_from_manifest("test/strip_test", tmp_path, strip_levels=3)
        finally:
            # Clean up
            manifest_file.unlink()
            manifest_dir.rmdir()

    def test_download_from_manifest_creates_directories(self, tmp_path, requests_mock):
        """Test that download creates necessary directories."""
        # Create a test manifest
        manifest_dir = (
            Path(__file__).parent.parent.parent
            / "src"
            / "ispypsa"
            / "data_fetch"
            / "manifests"
            / "test"
        )
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = manifest_dir / "dirs_test.txt"
        manifest_file.write_text("https://example.com/path/to/deep/file.txt\n")

        # Mock the HTTP request
        requests_mock.get(
            "https://example.com/path/to/deep/file.txt", text="test content"
        )

        try:
            download_from_manifest("test/dirs_test", tmp_path, strip_levels=0)

            # Check that the file was created with correct directory structure
            expected_file = tmp_path / "path" / "to" / "deep" / "file.txt"
            assert expected_file.exists()
            assert expected_file.read_text() == "test content"
        finally:
            # Clean up
            manifest_file.unlink()
            manifest_dir.rmdir()

    def test_download_from_manifest_with_strip_levels(self, tmp_path, requests_mock):
        """Test that strip_levels correctly modifies the save path."""
        # Create a test manifest
        manifest_dir = (
            Path(__file__).parent.parent.parent
            / "src"
            / "ispypsa"
            / "data_fetch"
            / "manifests"
            / "test"
        )
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = manifest_dir / "strip_levels.txt"
        manifest_file.write_text("https://example.com/archive/workbooks/6.0.xlsx\n")

        # Mock the HTTP request
        requests_mock.get(
            "https://example.com/archive/workbooks/6.0.xlsx",
            content=b"fake xlsx content",
        )

        try:
            # Strip 2 levels: removes "archive/workbooks/"
            download_from_manifest("test/strip_levels", tmp_path, strip_levels=2)

            # File should be at tmp_path/6.0.xlsx (not tmp_path/archive/workbooks/6.0.xlsx)
            expected_file = tmp_path / "6.0.xlsx"
            assert expected_file.exists()
            assert expected_file.read_bytes() == b"fake xlsx content"
        finally:
            # Clean up
            manifest_file.unlink()
            manifest_dir.rmdir()

    def test_download_from_manifest_multiple_files(self, tmp_path, requests_mock):
        """Test downloading multiple files from manifest."""
        # Create a test manifest with multiple URLs
        manifest_dir = (
            Path(__file__).parent.parent.parent
            / "src"
            / "ispypsa"
            / "data_fetch"
            / "manifests"
            / "test"
        )
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = manifest_dir / "multi.txt"
        manifest_file.write_text(
            "https://example.com/data/file1.parquet\n"
            "https://example.com/data/file2.parquet\n"
            "https://example.com/data/file3.parquet\n"
        )

        # Mock the HTTP requests
        requests_mock.get("https://example.com/data/file1.parquet", content=b"data1")
        requests_mock.get("https://example.com/data/file2.parquet", content=b"data2")
        requests_mock.get("https://example.com/data/file3.parquet", content=b"data3")

        try:
            download_from_manifest("test/multi", tmp_path, strip_levels=0)

            # All files should exist
            assert (tmp_path / "data" / "file1.parquet").read_bytes() == b"data1"
            assert (tmp_path / "data" / "file2.parquet").read_bytes() == b"data2"
            assert (tmp_path / "data" / "file3.parquet").read_bytes() == b"data3"
        finally:
            # Clean up
            manifest_file.unlink()
            manifest_dir.rmdir()

    def test_download_from_manifest_http_error(self, tmp_path, requests_mock):
        """Test that HTTP errors are raised properly."""
        # Create a test manifest
        manifest_dir = (
            Path(__file__).parent.parent.parent
            / "src"
            / "ispypsa"
            / "data_fetch"
            / "manifests"
            / "test"
        )
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = manifest_dir / "error_test.txt"
        manifest_file.write_text("https://example.com/notfound.txt\n")

        # Mock a 404 response
        requests_mock.get("https://example.com/notfound.txt", status_code=404)

        try:
            with pytest.raises(requests.HTTPError):
                download_from_manifest("test/error_test", tmp_path, strip_levels=0)
        finally:
            # Clean up
            manifest_file.unlink()
            manifest_dir.rmdir()


class TestFetchWorkbook:
    """Test the fetch_workbook function."""

    def test_fetch_workbook_success(self, tmp_path, requests_mock):
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

    def test_fetch_workbook_creates_parent_directories(self, tmp_path, requests_mock):
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

    def test_fetch_workbook_nonexistent_version(self, tmp_path):
        """Test that FileNotFoundError is raised for nonexistent version."""
        with pytest.raises(FileNotFoundError, match="Manifest file not found"):
            fetch_workbook("99.9", tmp_path / "workbook.xlsx")

    def test_fetch_workbook_http_error(self, tmp_path, requests_mock):
        """Test that HTTP errors are raised properly."""
        # Mock a 404 response
        requests_mock.get(
            "https://data.openisp.au/archive/workbooks/6.0.xlsx", status_code=404
        )

        with pytest.raises(requests.HTTPError):
            fetch_workbook("6.0", tmp_path / "workbook.xlsx")


class TestFetchTraceData:
    """Test the fetch_trace_data function."""

    def test_fetch_trace_data_example_2024_success(self, tmp_path, requests_mock):
        """Test successful download of example trace data."""
        # Mock all the HTTP requests from the example_2024 manifest
        urls = [
            "https://data.openisp.au/processed/isp_2024/project/RefYear=2018/data_0.parquet",
            "https://data.openisp.au/processed/isp_2024/project/RefYear=2018/data_1.parquet",
            "https://data.openisp.au/processed/isp_2024/project/RefYear=2018/data_2.parquet",
            "https://data.openisp.au/processed/isp_2024/zone/RefYear=2018/data_0.parquet",
            "https://data.openisp.au/processed/isp_2024/zone/RefYear=2018/data_1.parquet",
            "https://data.openisp.au/processed/isp_2024/zone/RefYear=2018/data_2.parquet",
            "https://data.openisp.au/processed/isp_2024/demand/Scenario=Step_Change/RefYear=2018/data_0.parquet",
        ]

        for url in urls:
            requests_mock.get(url, content=b"fake parquet data")

        fetch_trace_data("example", 2024, tmp_path)

        # Check that files were created with correct structure (strip_levels=2)
        # Original: processed/isp_2024/project/RefYear=2018/data_0.parquet
        # After strip: project/RefYear=2018/data_0.parquet
        assert (tmp_path / "project" / "RefYear=2018" / "data_0.parquet").exists()
        assert (tmp_path / "zone" / "RefYear=2018" / "data_0.parquet").exists()
        assert (
            tmp_path
            / "demand"
            / "Scenario=Step_Change"
            / "RefYear=2018"
            / "data_0.parquet"
        ).exists()

    def test_fetch_trace_data_invalid_dataset_type(self, tmp_path):
        """Test that ValueError is raised for invalid dataset_type."""
        with pytest.raises(
            ValueError, match="dataset_type must be 'full' or 'example'"
        ):
            fetch_trace_data("invalid", 2024, tmp_path)

    def test_fetch_trace_data_invalid_year(self, tmp_path):
        """Test that ValueError is raised for unsupported year."""
        with pytest.raises(
            ValueError, match="Only dataset_year=2024 is currently supported"
        ):
            fetch_trace_data("example", 2023, tmp_path)

    def test_fetch_trace_data_full_manifest_not_ready(self, tmp_path):
        """Test that empty full manifest raises appropriate error."""
        # The full_2024.txt manifest is currently empty, so this should fail
        with pytest.raises(ValueError, match="No URLs found in manifest"):
            fetch_trace_data("full", 2024, tmp_path)
