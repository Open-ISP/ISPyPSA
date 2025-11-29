"""Tests for website generation functionality."""

import tempfile
from pathlib import Path

import plotly.graph_objects as go
import pytest

from ispypsa.plotting.website import (
    _build_plot_tree,
    _format_display_name,
    _tree_to_html,
    generate_results_website,
)


def test_format_display_name_short_uppercase():
    """Test that short uppercase names are preserved as-is."""
    # Short uppercase abbreviations (<=5 chars) should be kept as-is
    assert _format_display_name("NSW") == "NSW"
    assert _format_display_name("REZ") == "REZ"
    assert _format_display_name("VIC") == "VIC"
    assert _format_display_name("QLD1") == "QLD1"


def test_format_display_name_mixed_case():
    """Test that mixed case and longer names get title case."""
    # Mixed case or longer names should get title case
    assert _format_display_name("regional_dispatch") == "Regional Dispatch"
    assert _format_display_name("transmission_flows") == "Transmission Flows"


def test_format_display_name_with_known_ids():
    """Test that known IDs are properly capitalized."""
    known_ids = ["NSW1", "QLD1", "SEQ"]
    # Known IDs should be uppercased even within longer strings
    result = _format_display_name("nsw1_generation", known_ids)
    assert "NSW1" in result


def test_build_plot_tree():
    """Test building tree structure from plot paths."""
    plot_paths = [
        Path("transmission/aggregate_transmission_capacity.html"),
        Path("transmission/flows.html"),
        Path("transmission/regional_expansion.html"),
        Path("dispatch/regional.html"),
        Path("dispatch/sub_regional.html"),
    ]

    tree = _build_plot_tree(plot_paths)

    # Check structure
    assert "transmission" in tree
    assert "dispatch" in tree
    assert isinstance(tree["transmission"], dict)
    assert isinstance(tree["dispatch"], dict)

    # Check files
    assert "aggregate_transmission_capacity.html" in tree["transmission"]
    assert "flows.html" in tree["transmission"]
    assert "regional_expansion.html" in tree["transmission"]
    assert "regional.html" in tree["dispatch"]
    assert "sub_regional.html" in tree["dispatch"]


def test_tree_to_html():
    """Test HTML generation from tree structure."""
    tree = {
        "transmission": {
            "flows.html": "transmission/flows.html",
            "capacity.html": "transmission/capacity.html",
        },
        "dispatch": {
            "regional.html": "dispatch/regional.html",
        },
    }

    html = _tree_to_html(tree)

    # Check folder structure
    assert "Transmission" in html
    assert "Dispatch" in html
    assert 'class="folder"' in html
    assert 'class="folder-header"' in html

    # Check file structure
    assert "Flows" in html
    assert "Capacity" in html
    assert "Regional" in html
    assert 'class="file"' in html


def test_generate_results_website():
    """Test full website generation."""
    # Create temporary directory for test
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "outputs"
        plots_dir = output_dir / "capacity_expansion_plots"
        plots_dir.mkdir(parents=True, exist_ok=True)

        # Create some dummy HTML plot files
        transmission_dir = plots_dir / "transmission"
        dispatch_dir = plots_dir / "dispatch"
        transmission_dir.mkdir(parents=True, exist_ok=True)
        dispatch_dir.mkdir(parents=True, exist_ok=True)

        # Create dummy plot files
        (transmission_dir / "flows.html").write_text(
            "<html><body>Flows Plot</body></html>"
        )
        (dispatch_dir / "regional.html").write_text(
            "<html><body>Regional Plot</body></html>"
        )

        # Create dummy plotly figures for the plots dict
        dummy_fig = go.Figure()

        # Create plots dictionary
        plots = {
            Path("transmission/flows.html"): {
                "plot": dummy_fig,
                "data": None,
            },
            Path("dispatch/regional.html"): {
                "plot": dummy_fig,
                "data": None,
            },
        }

        # Generate website
        generate_results_website(
            plots, plots_dir, output_dir, site_name="Test ISPyPSA Results"
        )

        # Check that website was created
        html_file = output_dir / "results_viewer.html"
        assert html_file.exists()

        # Read and check HTML content
        html_content = html_file.read_text(encoding="utf-8")

        # Check basic structure
        assert "<!DOCTYPE html>" in html_content
        assert "ISPyPSA Results" in html_content
        assert "Transmission" in html_content
        assert "Dispatch" in html_content
        assert "Flows" in html_content
        assert "Regional" in html_content

        # Check for navigation elements
        assert 'class="navigation"' in html_content
        assert 'class="plot-viewer"' in html_content
        assert 'class="folder"' in html_content
        assert 'class="file"' in html_content

        # Check for JavaScript functions
        assert "toggleFolder" in html_content
        assert "loadPlot" in html_content


def test_generate_results_website_empty_plots():
    """Test website generation with no plots."""
    with tempfile.TemporaryDirectory() as tmpdir:
        plots_dir = Path(tmpdir) / "outputs"
        plots_dir.mkdir(parents=True, exist_ok=True)

        output_dir = Path(tmpdir) / "outputs"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Empty plots dictionary
        plots = {}

        # Generate website (should handle gracefully)
        generate_results_website(plots, plots_dir, output_dir)

        # HTML file should not be created if no plots
        html_file = output_dir / "results_viewer.html"
        assert not html_file.exists()


def test_generate_results_website_with_regions_and_zones_mapping(csv_str_to_df):
    """Test website generation with regions_and_zones_mapping provided."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "outputs"
        plots_dir = output_dir / "capacity_expansion_plots"
        plots_dir.mkdir(parents=True, exist_ok=True)

        # Create plot directories with names that include region/zone IDs
        dispatch_dir = plots_dir / "dispatch"
        dispatch_dir.mkdir(parents=True, exist_ok=True)

        # Create dummy plot files with region/zone names
        (dispatch_dir / "nsw1_generation.html").write_text(
            "<html><body>NSW1 Generation</body></html>"
        )
        (dispatch_dir / "seq_1_generation.html").write_text(
            "<html><body>SEQ-1 Generation</body></html>"
        )

        # Create dummy plotly figures for the plots dict
        dummy_fig = go.Figure()

        # Create plots dictionary with region/zone names in paths
        plots = {
            Path("dispatch/nsw1_generation.html"): {
                "plot": dummy_fig,
                "data": None,
            },
            Path("dispatch/seq_1_generation.html"): {
                "plot": dummy_fig,
                "data": None,
            },
        }

        # Create regions_and_zones_mapping DataFrame
        regions_and_zones_mapping_csv = """
        nem_region_id,  isp_sub_region_id,  rez_id
        NSW1,           CNSW,               N1
        QLD1,           SEQ,                Q1
        VIC1,           CVIC,               V1
        """
        regions_and_zones_mapping = csv_str_to_df(regions_and_zones_mapping_csv)

        # Generate website with regions_and_zones_mapping
        generate_results_website(
            plots,
            plots_dir,
            output_dir,
            site_name="Test ISPyPSA Results",
            regions_and_zones_mapping=regions_and_zones_mapping,
        )

        # Check that website was created
        html_file = output_dir / "results_viewer.html"
        assert html_file.exists()

        # Read and check HTML content
        html_content = html_file.read_text(encoding="utf-8")

        # Check that region IDs are properly capitalized
        # NSW1 should be capitalized (from nem_region_id)
        assert "NSW1" in html_content
        # SEQ should be capitalized (from isp_sub_region_id)
        assert "SEQ" in html_content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
