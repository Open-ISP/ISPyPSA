"""Download data files from manifests."""

from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import requests
from tqdm import tqdm


def download_from_manifest(
    manifest_name: str,
    save_directory: Path | str,
    strip_levels: int = 0,
) -> None:
    """Download files from a manifest file.

    Reads URLs from a manifest file and downloads each file to the specified
    directory, preserving the URL path structure (with optional stripping of
    leading directory levels).

    Parameters
    ----------
    manifest_name : str
        Name of manifest file relative to manifests directory
        (e.g., "workbooks/6.0" or "trace_data/example_2024")
    save_directory : Path | str
        Root directory where files should be saved
    strip_levels : int, optional
        Number of directory levels to remove from URL path when creating
        local file structure (default: 0)

    Raises
    ------
    FileNotFoundError
        If the manifest file does not exist
    requests.HTTPError
        If any download fails
    OSError
        If there are filesystem errors (permissions, disk space, etc.)

    Examples
    --------
    >>> download_from_manifest("workbooks/6.0", "data", strip_levels=3)
    # Downloads to: data/6.0.xlsx

    >>> download_from_manifest("trace_data/example_2024", "data/traces", strip_levels=2)
    # Downloads to: data/traces/project/RefYear=2018/data_0.parquet
    """
    # Construct manifest path
    manifest_path = Path(__file__).parent / "manifests" / f"{manifest_name}.txt"

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest file not found: {manifest_path}")

    # Read URLs from manifest
    with open(manifest_path) as f:
        urls = [line.strip() for line in f if line.strip()]

    if not urls:
        raise ValueError(f"No URLs found in manifest: {manifest_path}")

    save_directory = Path(save_directory)

    # Download each file with progress bar
    for url in tqdm(urls, desc="Downloading files", unit="file"):
        _download_file(url, save_directory, strip_levels)


def _download_file(url: str, save_directory: Path, strip_levels: int) -> None:
    """Download a single file from URL to destination.

    Parameters
    ----------
    url : str
        URL of the file to download
    save_directory : Path
        Root directory where file should be saved
    strip_levels : int
        Number of directory levels to strip from URL path

    Raises
    ------
    requests.HTTPError
        If the download fails
    OSError
        If there are filesystem errors
    """
    # Parse URL to extract path
    parsed_url = urlparse(url)
    url_path = parsed_url.path.lstrip("/")

    # Strip specified number of directory levels
    path_parts = url_path.split("/")
    if strip_levels >= len(path_parts):
        raise ValueError(
            f"Cannot strip {strip_levels} levels from path with only "
            f"{len(path_parts)} parts: {url_path}"
        )

    stripped_path = "/".join(path_parts[strip_levels:])
    destination = save_directory / stripped_path

    # Create parent directories
    destination.parent.mkdir(parents=True, exist_ok=True)

    # Download file with progress bar for individual file
    response = requests.get(url, stream=True)
    response.raise_for_status()

    # Get file size if available for progress bar
    total_size = int(response.headers.get("content-length", 0))

    # Write file with progress bar
    with (
        open(destination, "wb") as f,
        tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=destination.name,
            leave=False,
        ) as pbar,
    ):
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            pbar.update(len(chunk))


def fetch_workbook(
    workbook_version: str,
    save_path: Path | str,
) -> None:
    """Download ISP workbook file.

    Downloads the ISP workbook for the specified version from the manifest
    to the specified file path.

    Parameters
    ----------
    workbook_version : str
        Version string (e.g., "6.0")
    save_path : Path | str
        Full path where the workbook file should be saved
        (e.g., "data/workbooks/6.0.xlsx")

    Raises
    ------
    FileNotFoundError
        If the manifest file does not exist
    requests.HTTPError
        If the download fails
    OSError
        If there are filesystem errors

    Examples
    --------
    >>> fetch_workbook("6.0", "data/workbooks/isp_6.0.xlsx")
    # Downloads ISP 6.0 workbook to data/workbooks/isp_6.0.xlsx
    """
    # Construct manifest path
    manifest_path = (
        Path(__file__).parent / "manifests" / "workbooks" / f"{workbook_version}.txt"
    )

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest file not found: {manifest_path}")

    # Read URL from manifest (should be single URL)
    with open(manifest_path) as f:
        urls = [line.strip() for line in f if line.strip()]

    if not urls:
        raise ValueError(f"No URLs found in manifest: {manifest_path}")

    if len(urls) > 1:
        raise ValueError(f"Expected single URL in workbook manifest, found {len(urls)}")

    url = urls[0]
    save_path = Path(save_path)

    # Create parent directories
    save_path.parent.mkdir(parents=True, exist_ok=True)

    # Download file
    print(f"Downloading workbook {workbook_version} from {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    # Get file size if available
    total_size = int(response.headers.get("content-length", 0))

    # Write file with progress bar
    with (
        open(save_path, "wb") as f,
        tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Downloading {save_path.name}",
        ) as pbar,
    ):
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            pbar.update(len(chunk))

    print(f"Workbook saved to: {save_path}")


def fetch_trace_data(
    dataset_type: Literal["full", "example"],
    dataset_year: int,
    save_directory: Path | str,
) -> None:
    """Download ISP trace data.

    Downloads the ISP trace data for the specified type and year from the
    manifest to the specified directory.

    Parameters
    ----------
    dataset_type : {"full", "example"}
        Type of dataset to download
    dataset_year : int
        Year of dataset (currently only 2024 is supported)
    save_directory : Path | str
        Directory where trace data should be saved. Files will be organized
        in subdirectories preserving the structure from the manifest.

    Raises
    ------
    ValueError
        If dataset_type or dataset_year are invalid
    FileNotFoundError
        If the manifest file does not exist
    requests.HTTPError
        If any download fails
    OSError
        If there are filesystem errors

    Examples
    --------
    >>> fetch_trace_data("example", 2024, "data/traces")
    # Downloads to: data/traces/isp_2024/project/RefYear=2018/data_0.parquet
    #               data/traces/isp_2024/zone/RefYear=2018/data_0.parquet
    #               data/traces/isp_2024/demand/Scenario=Step_Change/RefYear=2018/data_0.parquet
    """
    # Validate inputs
    if dataset_type not in ["full", "example"]:
        raise ValueError(
            f"dataset_type must be 'full' or 'example', got: {dataset_type}"
        )

    if dataset_year != 2024:
        raise ValueError(
            f"Only dataset_year=2024 is currently supported, got: {dataset_year}"
        )

    # Construct manifest name and download
    manifest_name = f"trace_data/{dataset_type}_{dataset_year}"

    print(f"Downloading {dataset_type} trace data for {dataset_year}")
    download_from_manifest(manifest_name, save_directory, strip_levels=2)
    print(f"Trace data saved to: {save_directory}")
