"""
Demo script to test the flow path cost templating function.
"""
from pathlib import Path

from ispypsa.data_fetch import read_csvs
from ispypsa.templater.flow_paths import _template_sub_regional_flow_path_costs
from ispypsa.logging import configure_logging


configure_logging()

def main():
    """Run the demo."""
    # Define root folder for data
    root_folder = Path("ispypsa_runs")
    workbook_cache_dir = root_folder / "workbook_table_cache"
    
    print("Loading test data...")
    iasr_tables = read_csvs(workbook_cache_dir)
    print(f"Loaded {len(iasr_tables)} tables")
    
    # Process each scenario
    scenarios = ["Step Change", "Progressive Change", "Green Energy Exports"]
    
    for scenario in scenarios:
        results = _template_sub_regional_flow_path_costs(iasr_tables, scenario)
        print(f"Found {len(results['flow_path'].unique())} flow paths")
        print("\nSample results:")
        print(results)
            
        # Save results to CSV
        scenario_name = scenario.lower().replace(" ", "_")
        output_file = Path(f"flow_path_costs_{scenario_name}.csv")
        results.to_csv(output_file, index=False)
        print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
