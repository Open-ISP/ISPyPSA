"""Generate a static website for navigating ISPyPSA plots."""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# Words that should always be capitalized in the website navigation
ALWAYS_CAPITALIZED_WORDS = ["NEM", "ISP"]


def _is_year_folder(folder_name: str) -> bool:
    """Check if a folder name represents a year (4-digit number)."""
    return folder_name.isdigit() and len(folder_name) == 4


def _build_plot_tree(plot_paths: list[Path]) -> dict:
    """Build a tree structure from plot file paths, excluding year folders.

    Args:
        plot_paths: List of plot file paths

    Returns:
        Nested dictionary representing the plot tree structure
    """
    tree = {}

    for plot_path in sorted(plot_paths):
        parts = plot_path.parts
        current = tree

        # Filter out year folders from the path
        filtered_parts = [p for p in parts[:-1] if not _is_year_folder(p)]

        # Navigate/create nested structure for directories (excluding years)
        for i, part in enumerate(filtered_parts):
            if part not in current:
                current[part] = {}
            current = current[part]

        # Add the final file with forward slashes for browser compatibility
        file_name = parts[-1]
        current[file_name] = plot_path.as_posix()

    return tree


def _format_display_name(name: str, known_ids: Optional[List[str]] = None) -> str:
    """Format a name for display, preserving uppercase abbreviations.

    Args:
        name: The name to format
        known_ids: List of known IDs that should always be fully capitalized

    Returns:
        Formatted name with proper capitalization
    """
    # Replace underscores with spaces
    name = name.replace("_", " ")

    # Base formatting
    # If the name is all uppercase or looks like an abbreviation (2-5 chars, all caps),
    # keep it as-is
    if name.isupper() and len(name) <= 5:
        formatted_name = name
    else:
        # For mixed case or longer names, use title case
        formatted_name = name.title()

    # Apply known IDs capitalization if provided
    if known_ids:
        # Create a regex pattern to match any of the known IDs (case-insensitive)
        # use word boundaries to ensure we match full words/IDs
        # specific sorting by length happens in the caller
        pattern = re.compile(
            r"\b(" + "|".join(re.escape(kid) for kid in known_ids) + r")\b",
            re.IGNORECASE,
        )

        def replace_func(match):
            return match.group(0).upper()

        formatted_name = pattern.sub(replace_func, formatted_name)

    return formatted_name


def _tree_to_html(
    tree: dict, indent: int = 0, known_ids: Optional[List[str]] = None
) -> str:
    """Convert tree structure to HTML list elements.

    Args:
        tree: Nested dictionary representing the plot tree
        indent: Current indentation level
        known_ids: List of known IDs that should always be fully capitalized

    Returns:
        HTML string
    """
    html_parts = []

    for key, value in sorted(tree.items()):
        if isinstance(value, dict):
            # This is a folder
            folder_name = _format_display_name(key, known_ids)
            html_parts.append(f'<li class="folder">')
            html_parts.append(
                f'<div class="folder-header" onclick="toggleFolder(this)">'
                f'<span class="folder-icon">▶</span> {folder_name}'
                f"</div>"
            )
            html_parts.append('<ul class="folder-content">')
            html_parts.append(_tree_to_html(value, indent + 1, known_ids))
            html_parts.append("</ul>")
            html_parts.append("</li>")
        else:
            # This is a file
            file_name = key.replace(".html", "")
            file_name = _format_display_name(file_name, known_ids)
            html_parts.append(
                f'<li class="file" onclick="loadPlot(\'{value}\')">{file_name}</li>'
            )

    return "\n".join(html_parts)


def _generate_html_template(
    plot_tree_html: str,
    plot_dir_name: str,
    site_name: str,
    subtitle: str = "Capacity Expansion Analysis",
) -> str:
    """Generate the complete HTML template.

    Args:
        plot_tree_html: HTML string for the plot tree navigation
        plot_dir_name: Name of the directory containing plots
        subtitle: Subtitle to display in the header

    Returns:
        Complete HTML string
    """
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{site_name}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            height: 100vh;
            overflow: hidden;
            background: #f5f5f5;
            color: #333;
        }}

        .container {{
            display: flex;
            height: 100vh;
        }}

        .plot-viewer {{
            flex: 1;
            display: flex;
            flex-direction: column;
            background: white;
        }}

        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1.5rem 2rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}

        .header h1 {{
            font-size: 1.75rem;
            font-weight: 600;
            margin-bottom: 0.25rem;
        }}

        .header p {{
            font-size: 0.9rem;
            opacity: 0.95;
        }}

        .plot-container {{
            flex: 1;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 2rem;
            overflow: auto;
        }}

        .plot-frame {{
            width: 100%;
            height: 100%;
            border: none;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            background: white;
        }}

        .placeholder {{
            text-align: center;
            color: #999;
        }}

        .placeholder-icon {{
            font-size: 4rem;
            margin-bottom: 1rem;
            opacity: 0.3;
        }}

        .placeholder h2 {{
            font-size: 1.5rem;
            font-weight: 400;
            margin-bottom: 0.5rem;
        }}

        .placeholder p {{
            font-size: 1rem;
        }}

        .navigation {{
            width: 320px;
            background: white;
            border-right: 1px solid #e0e0e0;
            display: flex;
            flex-direction: column;
            box-shadow: 2px 0 8px rgba(0,0,0,0.05);
        }}

        .nav-header {{
            padding: 1.5rem;
            background: #f8f9fa;
            border-bottom: 1px solid #e0e0e0;
        }}

        .nav-header h2 {{
            font-size: 1.1rem;
            font-weight: 600;
            color: #555;
        }}

        .nav-content {{
            flex: 1;
            overflow-y: auto;
            padding: 1rem;
        }}

        ul {{
            list-style: none;
        }}

        .folder {{
            margin-bottom: 0.5rem;
        }}

        .folder-header {{
            padding: 0.6rem 0.8rem;
            background: #f8f9fa;
            border-radius: 6px;
            cursor: pointer;
            user-select: none;
            transition: all 0.2s ease;
            font-weight: 500;
            color: #555;
        }}

        .folder-header:hover {{
            background: #e9ecef;
            color: #333;
        }}

        .folder-icon {{
            display: inline-block;
            transition: transform 0.2s ease;
            font-size: 0.7rem;
            margin-right: 0.5rem;
        }}

        .folder.open > .folder-header .folder-icon {{
            transform: rotate(90deg);
        }}

        .folder-content {{
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease;
            margin-left: 1rem;
            margin-top: 0.5rem;
        }}

        .folder.open > .folder-content {{
            max-height: 2000px;
        }}

        .file {{
            padding: 0.6rem 0.8rem;
            margin: 0.25rem 0;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s ease;
            color: #666;
        }}

        .file:hover {{
            background: #667eea;
            color: white;
            transform: translateX(-2px);
        }}

        .file.active {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            font-weight: 500;
        }}

        /* Scrollbar styling */
        .nav-content::-webkit-scrollbar {{
            width: 8px;
        }}

        .nav-content::-webkit-scrollbar-track {{
            background: #f1f1f1;
            border-radius: 4px;
        }}

        .nav-content::-webkit-scrollbar-thumb {{
            background: #ccc;
            border-radius: 4px;
        }}

        .nav-content::-webkit-scrollbar-thumb:hover {{
            background: #999;
        }}

        @media (max-width: 768px) {{
            .navigation {{
                width: 100%;
                max-width: 280px;
                position: absolute;
                right: -280px;
                z-index: 100;
                transition: right 0.3s ease;
            }}

            .navigation.mobile-open {{
                right: 0;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="navigation">
            <div class="nav-header">
                <h2>Available Plots</h2>
            </div>
            <div class="nav-content">
                <ul id="plotTree">
{plot_tree_html}
                </ul>
            </div>
        </div>

        <div class="plot-viewer">
            <div class="header">
                <h1>Run name: {site_name}</h1>
                <p>{subtitle}</p>
            </div>
            <div class="plot-container" id="plotContainer">
                <div class="placeholder">
                    <div class="placeholder-icon">📊</div>
                    <h2>Select a plot to view</h2>
                    <p>Choose a plot from the navigation menu on the left</p>
                </div>
            </div>
        </div>
    </div>

    <script>
        const plotDirName = '{plot_dir_name}';

        function toggleFolder(element) {{
            const folder = element.parentElement;
            folder.classList.toggle('open');
        }}

        function loadPlot(plotPath) {{
            // Remove active class from all files
            document.querySelectorAll('.file').forEach(file => {{
                file.classList.remove('active');
            }});

            // Add active class to clicked file
            event.currentTarget.classList.add('active');

            // Load the plot
            const container = document.getElementById('plotContainer');
            const fullPath = plotDirName + '/' + plotPath;

            container.innerHTML = `<iframe class="plot-frame" src="${{fullPath}}"></iframe>`;
        }}

        // Auto-expand first folder on load
        window.addEventListener('load', function() {{
            const firstFolder = document.querySelector('.folder');
            if (firstFolder) {{
                firstFolder.classList.add('open');
            }}
        }});
    </script>
</body>
</html>"""


def generate_results_website(
    plots: Dict[Path, dict],
    plots_dir: Path,
    output_dir: Path,
    site_name: str = "ISPyPSA Results",
    output_filename: str = "results_viewer.html",
    subtitle: str = "Capacity Expansion Analysis",
    regions_and_zones_mapping: Optional[pd.DataFrame] = None,
) -> None:
    """Generate a static website for navigating ISPyPSA plot results.

    Creates a single HTML file with a navigation pane that mirrors
    the directory structure of the plots.

    Args:
        plots: Dictionary with Path keys (plot file paths) and dict values
               containing "plot" and "data" keys (output from create_*_plot_suite)
        plots_dir: Directory where plots are saved (e.g., outputs/capacity_expansion_plots)
        output_dir: Directory where the website HTML should be saved (e.g., outputs directory)
        site_name: Name of the website (default: "ISPyPSA Results")
        output_filename: Name of the output HTML file (default: "results_viewer.html")
        subtitle: Subtitle to display in the header (default: "Capacity Expansion Analysis")
        regions_and_zones_mapping: Optional mapping table to ensure correct capitalization
            of region and zone IDs

    Returns:
        None. The website is saved as output_filename in output_dir
    """
    logging.info(f"Generating results website: {output_filename}...")

    # Get list of plot paths
    plot_paths = list(plots.keys())

    if not plot_paths:
        logging.warning("No plots found to generate website")
        return

    # Derive plot directory name from the plots_dir path
    plot_dir_name = plots_dir.name

    # Build plot tree structure
    plot_tree = _build_plot_tree(plot_paths)

    # Build known IDs list if mapping is provided
    known_ids_list = []
    if regions_and_zones_mapping is not None:
        known_ids = set()
        for col in ["nem_region_id", "isp_sub_region_id", "rez_id"]:
            if col in regions_and_zones_mapping.columns:
                ids = regions_and_zones_mapping[col].dropna().astype(str).unique()
                for id_val in ids:
                    # Standard version with spaces (matches text where underscores were replaced)
                    known_ids.add(id_val.replace("_", " ").upper())
                    # Hyphenated version (matches text with hyphens, e.g. SEQ-1)
                    known_ids.add(id_val.replace("_", "-").upper())

        # Add always capitalized words
        known_ids.update(ALWAYS_CAPITALIZED_WORDS)

        # Convert to sorted list (by length descending to handle overlapping IDs correctly)
        known_ids_list = sorted(known_ids, key=len, reverse=True)
    else:
        # If no mapping provided, at least use the hardcoded list
        known_ids_list = sorted(ALWAYS_CAPITALIZED_WORDS, key=len, reverse=True)

    # Convert tree to HTML
    plot_tree_html = _tree_to_html(plot_tree, known_ids=known_ids_list)

    # Generate complete HTML
    html_content = _generate_html_template(
        plot_tree_html, plot_dir_name, site_name, subtitle
    )

    # Write HTML file
    output_file = output_dir / output_filename
    output_file.write_text(html_content, encoding="utf-8")

    logging.info(f"Website generated successfully: {output_file}")
    logging.info(f"Open {output_file.name} in a browser to view the results")
