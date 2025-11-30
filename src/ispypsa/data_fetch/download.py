"""Download data files from manifests."""

from pathlib import Path
from urllib.parse import urlparse

import requests
from tqdm import tqdm


def fetch_workbook(
    workbook_version: str,
    save_path: Path | str,
) -> None:
    """Download ISP workbook file.

    Downloads the ISP workbook for the specified version from the manifest
    to the specified file path.

    Examples:
        >>> fetch_workbook("6.0", "data/workbooks/isp_6.0.xlsx")
        # Downloads ISP 6.0 workbook to data/workbooks/isp_6.0.xlsx

    Args:
        workbook_version : str
            Version string (e.g., "6.0")
        save_path : Path | str
            Full path where the workbook file should be saved
            (e.g., "data/workbooks/6.0.xlsx")

    Returns:
        None

    Raises:
        FileNotFoundError: If the manifest file does not exist
        requests.HTTPError: If the download fails
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
