from pathlib import Path

import pandas as pd

from ispypsa.translator.mappings import _GENERATOR_ATTRIBUTES


def _translate_ecaa_generators(
    template_path: Path | str,  granularity: str = "sub_regional"
) -> pd.DataFrame:
    """Process data on existing, committed, anticipated, and additional (ECAA) generators
    into a format aligned with PyPSA inputs.

    Args:
        template_path: Path to directory containing modelling input template CSVs.

    Returns:
        `pd.DataFrame`: PyPSA style generator attributes in tabular format.
    """
    ecaa_generators_template = (
        pd.read_csv(template_path / Path("ecaa_generators_template.csv")))

    if granularity == "sub_regional":
        _GENERATOR_ATTRIBUTES['sub_region_id'] = "bus"
    elif granularity == "regional":
        _GENERATOR_ATTRIBUTES['region_id'] = "bus"

    ecaa_generators_pypsa_format = ecaa_generators_template.loc[:, _GENERATOR_ATTRIBUTES.keys()]
    ecaa_generators_pypsa_format = ecaa_generators_pypsa_format.rename(columns=_GENERATOR_ATTRIBUTES)

    if granularity == "single_region":
        ecaa_generators_pypsa_format['bus'] = "NEM"

    return ecaa_generators_pypsa_format
