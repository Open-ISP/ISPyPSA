import pandas as pd
import pytest

from ispypsa.templater.existing_planned import (
    _is_existing_planned_storage_row,
    _template_generators_existing_planned,
    _template_storage_existing_planned,
    _validate_phes_routing,
)

# --- _is_existing_planned_storage_row ---


def test_is_existing_planned_storage_row(csv_str_to_df):
    summary = csv_str_to_df("""
        Power Station,  Technology Type
        Wivenhoe,       Hydro
        Tarong,         Steam Sub Critical
        Q8 Battery,     Battery Storage (2hrs storage)
        QEJP - Borumba, Pumped Hydro (24hrs storage)
    """)
    phes_properties = csv_str_to_df("""
        Power Station
        Wivenhoe
        Borumba
    """)

    result = _is_existing_planned_storage_row(summary, phes_properties)

    # Wivenhoe routes to storage by PHES-table presence despite being labelled plain
    # "Hydro"; Q8 Battery routes by technology; Tarong is neither; QEJP - Borumba
    # routes by technology AND is matched to Borumba in phes_properties.
    assert list(result) == [True, False, True, True]


def test_is_existing_planned_storage_row_empty_summary():
    summary = pd.DataFrame(columns=["Power Station", "Technology Type"])
    phes_properties = pd.DataFrame(columns=["Power Station"])

    result = _is_existing_planned_storage_row(summary, phes_properties)

    assert list(result) == []


def test_is_existing_planned_storage_row_empty_phes_properties(csv_str_to_df):
    # Battery technologies still get correctly identified
    summary = csv_str_to_df("""
        Power Station,  Technology Type
        Tarong,         Steam Sub Critical
        Q8 Battery,     Battery Storage (2hrs storage)
    """)
    phes_properties = pd.DataFrame(columns=["Power Station"])

    result = _is_existing_planned_storage_row(summary, phes_properties)

    assert list(result) == [False, True]


# --- _validate_phes_routing ---


def test_validate_phes_routing_tolerates_lower_tumut(csv_str_to_df):
    # Tumut 3's pump/non-pump unit split means phes_properties' "Lower Tumut" never
    # matches any Power Station name in the summary ("Tumut 3" covers all 6 units)
    # -> known, no raise.
    # Borumba<->QEJP - Borumba name difference gets mapped correctly -> no raise.
    summary = csv_str_to_df("""
        Power Station,  Technology Type
        Tumut 3,        Hydro
        QEJP - Borumba, Pumped Hydro (24hrs storage)
        Tarong,         Steam Sub Critical
    """)
    phes_properties = csv_str_to_df("""
        Power Station
        Lower Tumut
        Borumba
    """)

    _validate_phes_routing(summary, phes_properties)  # no error


def test_validate_phes_routing_raises_on_unrecognised_station(csv_str_to_df):
    summary = csv_str_to_df("""
        Power Station,  Technology Type
        Tumut 3,        Hydro
        QEJP - Borumba, Pumped Hydro (24hrs storage)
        Tarong,         Steam Sub Critical
    """)
    phes_properties = csv_str_to_df("""
        Power Station
        Some New Station
    """)

    with pytest.raises(ValueError, match=r"\['Some New Station'\]"):
        _validate_phes_routing(summary, phes_properties)


# --- _template_generators_existing_planned / _template_storage_existing_planned ---


def test_template_generators_and_storage_existing_planned(csv_str_to_df):
    # Wiring check: the split routes each row to exactly one table, and the spine
    # columns are renamed. Every other summary column is expected to pass through
    # unchanged — not asserted row-by-row here since none are added/removed yet.
    summary = csv_str_to_df("""
        IASR ID / DLT names,  Power Station,  Technology Type
        BW01,                 Bayswater,      Steam Sub Critical
        WHOE1,                Wivenhoe,       Hydro
        Q8_BATT_2H,           Q8 Battery,     Battery Storage (2hrs storage)
    """)
    phes_properties = csv_str_to_df("""
        Power Station
        Wivenhoe
    """)
    iasr_tables = {
        "existing_committed_anticipated_additional_generator_summary": summary,
        "pumped_hydro_existing_committed_anticipated_additional_properties": phes_properties,
    }

    generators = _template_generators_existing_planned(iasr_tables)
    storage = _template_storage_existing_planned(iasr_tables)

    expected_generators = csv_str_to_df("""
        name,  power_station,  technology
        BW01,  Bayswater,      Steam Sub Critical
    """)
    expected_storage = csv_str_to_df("""
        name,        power_station,  technology
        WHOE1,       Wivenhoe,       Hydro
        Q8_BATT_2H,  Q8 Battery,     Battery Storage (2hrs storage)
    """)
    pd.testing.assert_frame_equal(
        generators.reset_index(drop=True), expected_generators
    )
    pd.testing.assert_frame_equal(storage.reset_index(drop=True), expected_storage)


def test_template_generators_and_storage_existing_planned_empty(csv_str_to_df):
    summary = pd.DataFrame(
        columns=["IASR ID / DLT names", "Power Station", "Technology Type"]
    )
    phes_properties = pd.DataFrame(columns=["Power Station"])
    iasr_tables = {
        "existing_committed_anticipated_additional_generator_summary": summary,
        "pumped_hydro_existing_committed_anticipated_additional_properties": phes_properties,
    }

    generators = _template_generators_existing_planned(iasr_tables)
    storage = _template_storage_existing_planned(iasr_tables)

    expected = csv_str_to_df("""
        name,  power_station,  technology
    """)
    pd.testing.assert_frame_equal(
        generators.reset_index(drop=True), expected, check_dtype=False
    )
    pd.testing.assert_frame_equal(
        storage.reset_index(drop=True), expected, check_dtype=False
    )
