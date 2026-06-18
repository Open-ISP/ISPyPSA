"""End-to-end validation of ``custom_constraints_from_plexos``.

For each validated constraint this module assembles its LHS two independent
ways and asserts that the two agree, modulo a small explicit set of known
differences:

1. **PLEXOS route** -- the production templater
   ``template_custom_constraints_from_plexos`` reads the PLEXOS extract.
2. **Workbook route** -- ``workbook_extract_constraint_lhs`` (in
   ``scripts/workbook_extract_constraint.py``) independently expands each
   constraint's REZ Terms straight from ``rez_group_constraint_summary``
   using the IASR unit summary tables.

If the two drift, the test fails and names the unexpected terms.


Why are there *any* differences?
=================================

1. **The two routes use different geographic lookups for existing plants.**
   Every IASR unit carries two geographic tags: a fine-grained ``REZ ID``
   (``Q1``, ``Q2``, ..., ``S3``, ``S4``, ...) and a coarser ``Sub-region``
   (``NQ``, ``CQ``, ``NSA``, ``CSA``, ...). Usually a REZ sits cleanly
   inside one sub-region, but a handful of plants are exceptions -- their
   REZ points one place and their sub-region points somewhere adjacent.

   The constraint ``NQ1`` is really "the NQ sub-region's export limit".
   Two ways to assemble its unit list:

       * **Workbook recipe (used here):** the workbook lists REZes
         (``Q1``, ``Q2``, ``Q3``) for ``NQ1`` -- expand each REZ to its
         units.
       * **PLEXOS recipe (for existing plants):** include units whose
         **sub-region** is ``NQ``.

   For 95%+ of units the two tags agree, so both recipes pick the same
   plants. They only diverge on boundary plants:

       * ``EMERASF1`` -- REZ ``Q4`` but sub-region ``NQ``. The workbook
         recipe puts it in ``CQ1`` (because ``Q4 -> CQ1``); PLEXOS puts
         it in ``NQ1`` (because sub-region ``NQ``). The same single fact
         explains "PLEXOS-only in ``NQ1``" *and* "workbook-only in
         ``CQ1``".
       * ``WPWF`` -- REZ ``S4`` but sub-region ``NSA``. Workbook puts it
         in ``MN1`` (``S4 -> MN1``); PLEXOS skips it (sub-region ``NSA``
         doesn't match ``MN1``'s ``CSA`` scope).

2. **Systematic include/exclude differences in what each side carries:**

       * PLEXOS routes electrolyser load via the ``Purchaser`` class,
         which the templater drops by design (hydrogen demand isn't
         modelled). The workbook lists them as REZ-tagged ``load`` units.

   (Batteries don't surface as a diff for these constraints either. PLEXOS
   carries two battery patterns a literal translation would expose --
   constraint-scoped variants (e.g. ``SWQLD1 Battery - 2h``) that map
   ambiguously to IASR units, and a quiet exclusion of the 4h-duration
   batteries. Pass 1 keeps the named PLEXOS batteries that match an IASR unit
   at their real coefficients and drops the constraint-scoped variants; pass 2
   then re-adds the omitted durations at their surviving siblings' coefficient
   (the second LHS pass). The five validated constraints are all
   unit-coefficient, so both routes land the same batteries at 1.0 and neither
   pattern shows up here -- the coefficient-copy itself only bites on the
   fractional constraints this cross-check can't reach. See Open-ISP/ISPyPSA#110
   for the underlying PLEXOS layout and ``custom_constraints_from_plexos.py``
   for the injection logic.)

The ``EXPECTED_DELTAS`` table below names every known mismatch with an
inline comment giving the reason. Any new unexpected mismatch in either
direction fails the test.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Make ``scripts/`` importable -- the workbook extraction lives there as a
# stand-alone validation tool. Same pattern as tests/test_scripts/conftest.py.
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from workbook_extract_constraint import workbook_extract_constraint_lhs  # noqa: E402

from ispypsa.data_fetch.csv_read_write import read_csvs  # noqa: E402
from ispypsa.templater.custom_constraints_from_plexos import (  # noqa: E402
    template_custom_constraints_from_plexos,
)

_FIXTURE_DIR = Path(__file__).parent / "data" / "custom_constraints_validation"


# Each entry lists the (term_type, variable_name, coefficient) tuples that
# we expect to be PLEXOS-only or workbook-only after running both routes on
# the fixture. New unexpected differences in either direction fail the test;
# expected differences gone missing also fail (meaning behaviour changed and
# the known-diff list should be updated).
EXPECTED_DELTAS: dict[str, dict[str, set[tuple[str, str, float]]]] = {
    "NQ1": {
        "plexos_only": {
            # EMERASF1 (Emerald SF): REZ Q4, sub-region NQ -- PLEXOS includes
            # by sub-region, workbook expands by REZ. Mirrored side of this
            # is the EMERASF1 entry in CQ1.workbook_only below.
            ("generator_output", "EMERASF1", 1.0),
        },
        "workbook_only": {
            # Electrolysers -- workbook lists them as REZ Q1 load units;
            # PLEXOS routes them via Purchaser class, which the templater
            # drops by design.
            ("load", "Q1 to NQ Flexible Electrolyser Piped for Domestic", 1.0),
            ("load", "Q1 to GG Flexible Electrolyser Piped for Green Commodities", 1.0),
        },
    },
    "NET1": {
        "plexos_only": set(),
        "workbook_only": {
            # Electrolysers -- routed via PLEXOS Purchaser, templater drops.
            ("load", "T1 to TAS Flexible Electrolyser Piped for Domestic", 1.0),
            (
                "load",
                "T1 to TAS Flexible Electrolyser Piped for Green Commodities",
                1.0,
            ),
        },
    },
    "WV1": {
        "plexos_only": set(),
        "workbook_only": set(),
    },
    "CQ1": {
        "plexos_only": {
            # CSPVPS1 (Collinsville SF), DAYDSF1 (Daydream SF): REZ Q10
            # but sub-region CQ -- PLEXOS includes by sub-region; workbook
            # only expands Q4 (the REZ the workbook names for CQ1).
            ("generator_output", "CSPVPS1", 1.0),
            ("generator_output", "DAYDSF1", 1.0),
        },
        "workbook_only": {
            # EMERASF1 -- mirrored side of the NQ1.plexos_only entry above
            # (REZ Q4 but sub-region NQ; PLEXOS routes to NQ1, workbook
            # places it in CQ1's REZ expansion).
            ("generator_output", "EMERASF1", 1.0),
            # Electrolysers.
            ("load", "Q4 to CQ Flexible Electrolyser Piped for Domestic", 1.0),
            ("load", "Q4 to GG Flexible Electrolyser Piped for Domestic", 1.0),
            ("load", "Q4 to GG Flexible Electrolyser Piped for Green Commodities", 1.0),
        },
    },
    "MN1": {
        "plexos_only": set(),
        "workbook_only": {
            # WPWF (Wattle Point WF): REZ S4 but sub-region NSA -- workbook
            # places it in MN1 by REZ (S4 -> MN1); PLEXOS skips it because
            # its sub-region NSA isn't MN1's CSA scope.
            ("generator_output", "WPWF", 1.0),
            # Electrolysers.
            ("load", "S3 to CSA Flexible Electrolyser Piped for Domestic", 1.0),
            (
                "load",
                "S3 to NSA Flexible Electrolyser Piped for Green Commodities",
                1.0,
            ),
        },
    },
}


@pytest.fixture(scope="module")
def iasr_tables():
    return read_csvs(_FIXTURE_DIR / "workbook")


@pytest.fixture(scope="module")
def plexos_lhs(iasr_tables):
    out = template_custom_constraints_from_plexos(
        iasr_tables,
        iasr_workbook_version="7.5",
        plexos_extract_dir=_FIXTURE_DIR / "plexos",
    )
    return out["custom_constraints_lhs"]


@pytest.mark.parametrize("constraint_id", list(EXPECTED_DELTAS))
def test_workbook_matches_plexos_modulo_known_diffs(
    constraint_id, iasr_tables, plexos_lhs
):
    plexos_terms = _term_set(plexos_lhs[plexos_lhs["constraint_id"] == constraint_id])
    workbook_terms = _term_set(
        workbook_extract_constraint_lhs(constraint_id, iasr_tables)
    )

    expected = EXPECTED_DELTAS[constraint_id]
    assert (plexos_terms - workbook_terms) == expected["plexos_only"], (
        f"{constraint_id}: PLEXOS-only LHS terms diverged from EXPECTED_DELTAS"
    )
    assert (workbook_terms - plexos_terms) == expected["workbook_only"], (
        f"{constraint_id}: workbook-only LHS terms diverged from EXPECTED_DELTAS"
    )


def _term_set(lhs: pd.DataFrame) -> set[tuple[str, str, float]]:
    """Return ``(term_type, variable_name, coefficient)`` tuples, deduped.

    PLEXOS rows can repeat the same logical term across multiple ``date_from``
    rows (time-varying coefficients); collapsing to a set discards that detail
    so the validation focuses on which units participate at what coefficient.
    """
    return set(
        zip(
            lhs["term_type"],
            lhs["variable_name"],
            lhs["coefficient"].round(5),
        )
    )
