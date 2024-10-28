from pathlib import Path

from ispypsa.templater.generators import _template_ecaa_generators
from ispypsa.templater.mappings import _ECAA_GENERATOR_STATIC_PROPERTY_TABLE_MAP


def test_ecaa_generator_templater(workbook_table_cache_test_path: Path):
    df = _template_ecaa_generators(workbook_table_cache_test_path)
    for static_property_col in _ECAA_GENERATOR_STATIC_PROPERTY_TABLE_MAP.keys():
        if (
            "new_col_name"
            in _ECAA_GENERATOR_STATIC_PROPERTY_TABLE_MAP[static_property_col].keys()
        ):
            static_property_col = _ECAA_GENERATOR_STATIC_PROPERTY_TABLE_MAP[
                static_property_col
            ]["new_col_name"]
        assert all(df[static_property_col].notna())
        assert df[static_property_col].dtype != "object"
    assert set(df["status"]) == set(
        ("Existing", "Committed", "Anticipated", "Additional projects")
    )
    where_wind, where_solar = (
        df["technology_type"].str.contains("solar", case=False),
        df["technology_type"].str.contains("wind", case=False),
    )
    for where_tech in (where_solar, where_wind):
        tech_df = df.loc[where_tech, :]
        assert all(tech_df["minimum_load_mw"] == 0.0)
        assert all(tech_df["heat_rate_gj/mwh"] == 0.0)
        assert all(tech_df["partial_outage_derating_factor_%"] == 0.0)