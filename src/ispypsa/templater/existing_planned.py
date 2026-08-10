"""Splits the existing, committed, anticipated and additional (ECAA) technologies
summary into generator and storage tables.

Both target tables — see schemas/generators_existing_planned.yaml and
schemas/storage_existing_planned.yaml — are built from the single IASR
existing_committed_anticipated_additional_generator_summary table, which already lists
one row per real generating/storage unit (DUID-level). This module currently
covers only the first step of building those two tables: splitting the summary into
its generator and storage rows and renaming the three carried-over spine columns
(name, power_station, technology) to their schema names.

    existing_committed_anticipated_additional_generator_summary:
        IASR ID / DLT names  Power Station  Technology Type
        BW01                 Bayswater      Steam Sub Critical
        Q8 Battery - 2h      Q8 Battery     Battery Storage (2hrs storage)

    generators_existing_planned (partial):
        name  power_station  technology
        BW01  Bayswater      Steam Sub Critical

    storage_existing_planned (partial):
        name             power_station  technology
        Q8 Battery - 2h  Q8 Battery     Battery Storage (2hrs storage)

There are two independent public orchestrators, one per output table, each:
    1. Splits the summary's rows into generators and storage — see
       _is_existing_planned_storage_row. Storage is identified first by Technology
       Type, then by Power Station presence in phes_properties* table. A kind of
       backwards validation makes sure that no PHES stations are incorrectly treated
       as generators going forward.
    2. Renames the spine columns (_SUMMARY_COLUMN_RENAMES) to their schema names.
       Every other summary column passes through unchanged at this early draft.

Note: using 'phes_properties' as a short name for the IASR table with full name
'pumped_hydro_existing_committed_anticipated_additional_properties' here.
"""

import logging

import pandas as pd

from ispypsa.templater.helpers import _is_storage_row

# Source (IASR existing_committed_anticipated_additional_generator_summary) column
# names → schema output column names.
_SUMMARY_COLUMN_RENAMES = {
    "IASR ID / DLT names": "name",
    "Power Station": "power_station",
    "Technology Type": "technology",
}

# The PHES properties table keys Borumba by its short project name; the summary lists
# it under its full project name.
_BORUMBA_FULL_NAME_MAP = {"Borumba": "QEJP - Borumba"}

# Tumut 3 has a real pump/non-pump unit split that the summary and phes_properties
# tables can't currently be reconciled on by name. _validate_phes_routing tolerates
# this as known unmatched as part of an interim simplification to treat all Tumut 3
# units as generators. See Open-ISP/ISPyPSA#131 comment thread.
_KNOWN_UNMATCHED_PHES_STATIONS = {"Lower Tumut"}


# --- public orchestrators ---


def _template_generators_existing_planned(
    iasr_tables: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Templates the existing and planned (ECAA) generators table from the IASR summary.

    Currently just the generator/storage split and spine rename - skeleton/draft.

    Args:
        iasr_tables: IASR tables; uses
            existing_committed_anticipated_additional_generator_summary and
            pumped_hydro_existing_committed_anticipated_additional_properties.

    I/O Example (spine columns shown; every other summary column passes through
    unchanged):
        existing_committed_anticipated_additional_generator_summary:
            IASR ID / DLT names  Power Station  Technology Type
            BW01                 Bayswater      Steam Sub Critical
            Q8 Battery - 2h      Q8 Battery     Battery Storage (2hrs storage)  # storage, dropped

        returns:
            name  power_station  technology
            BW01  Bayswater      Steam Sub Critical
    """
    logging.info("Creating a template for existing and planned generators")
    summary = iasr_tables["existing_committed_anticipated_additional_generator_summary"]
    phes_properties = iasr_tables[
        "pumped_hydro_existing_committed_anticipated_additional_properties"
    ]
    is_storage = _is_existing_planned_storage_row(summary, phes_properties)
    generators = summary[~is_storage].copy()
    return generators.rename(columns=_SUMMARY_COLUMN_RENAMES)


def _template_storage_existing_planned(
    iasr_tables: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Templates the existing and planned (ECAA) storage table from the IASR summary.

    Currently just the generator/storage split and spine rename — skeleton/draft.

    Args:
        iasr_tables: IASR tables; uses
            existing_committed_anticipated_additional_generator_summary and
            pumped_hydro_existing_committed_anticipated_additional_properties.

    I/O Example (spine columns shown; every other summary column passes through
    unchanged):
        existing_committed_anticipated_additional_generator_summary:
            IASR ID / DLT names  Power Station  Technology Type
            BW01                 Bayswater      Steam Sub Critical   # generator, dropped
            Q8 Battery - 2h      Q8 Battery     Battery Storage (2hrs storage)

        returns:
            name             power_station  technology
            Q8 Battery - 2h  Q8 Battery     Battery Storage (2hrs storage)
    """
    logging.info("Creating a template for existing and planned storage")
    summary = iasr_tables["existing_committed_anticipated_additional_generator_summary"]
    phes_properties = iasr_tables[
        "pumped_hydro_existing_committed_anticipated_additional_properties"
    ]
    is_storage = _is_existing_planned_storage_row(summary, phes_properties)
    storage = summary[is_storage].copy()
    return storage.rename(columns=_SUMMARY_COLUMN_RENAMES)


# --- helpers ---


def _is_existing_planned_storage_row(
    summary: pd.DataFrame, phes_properties: pd.DataFrame
) -> pd.Series:
    """Boolean mask selecting storage rows: batteries plus stations with real PHES properties.

    Batteries and technology-labelled PHES (Borumba, Kidston, Snowy 2.0, Phoenix) are
    matched by ``_is_storage_row`` (Technology Type). Two more existing PHES stations
    — Wivenhoe, Shoalhaven — are labelled plain "Hydro" in the summary instead, so a
    station also counts as PHES storage if it appears by name in ``phes_properties``.

    ``_validate_phes_routing`` raises if a ``phes_properties`` station's name
    doesn't exist anywhere in the summary at all, with some known exceptions.

    I/O Example:
        summary:
            Power Station   Technology Type
            Wivenhoe        Hydro                            # PHES by table presence
            QEJP - Borumba  Pumped Hydro (24hrs storage)     # PHES by technology
            Tarong          Steam Sub Critical
            Q8 Battery      Battery Storage (2hrs storage)   # Battery by technology

        phes_properties:
            Power Station
            Wivenhoe
            Borumba

        returns: pd.Series([True, True, False, True])
    """
    is_storage = _is_storage_row(summary) | summary["Power Station"].isin(
        phes_properties["Power Station"]
    )
    _validate_phes_routing(summary, phes_properties)
    return is_storage


def _validate_phes_routing(
    summary: pd.DataFrame, phes_properties: pd.DataFrame
) -> None:
    """Raises if a phes_properties station's name doesn't exist anywhere in the summary.

    Checked after correcting the known Borumba name mismatch
    (``_BORUMBA_FULL_NAME_MAP``) and excusing the one known, documented gap
    (``_KNOWN_UNMATCHED_PHES_STATIONS`` — Tumut 3's "Lower Tumut"). Any other
    unmatched name means a real PHES station would otherwise be silently
    misclassified as a generator.

    I/O Example:
        summary: pd.DataFrame({"Power Station": ["Wivenhoe"]})
        phes_properties: pd.DataFrame({"Power Station": ["Wivenhoe"]})
        -> no error

        phes_properties: pd.DataFrame({"Power Station": ["Some New Station"]})
        -> raises ValueError listing ["Some New Station"]
    """
    phes_stations = set(
        phes_properties["Power Station"].replace(_BORUMBA_FULL_NAME_MAP)
    )
    known_stations = set(summary["Power Station"]) | _KNOWN_UNMATCHED_PHES_STATIONS
    unmatched = phes_stations - known_stations
    if unmatched:
        raise ValueError(
            f"PHES properties station(s) not found in the summary: {sorted(unmatched)}"
        )
