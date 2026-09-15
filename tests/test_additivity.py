"""Which measures may be summed, declared on the column and held to the models.

A Parquet file carries column names and types and nothing else. Nothing in it
says that a price may not be summed. So the warehouse states it:
`meta: {additivity: …}` on the column, in the same ymls that carry the contract,
and `publish/export_warehouse.py` carries the labels into the release manifest
so a consumer who cannot be paged has them too.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from gold_warehouse.paths import dbt_manifest_path
from publish.export_warehouse import EXTRA_ADDITIVITY, additivity

# ci.yml runs pytest before `dbt parse`, so the manifest is missing there; it
# re-runs this file after the parse, which `tests/test_workflows.py` enforces.
manifest_path = dbt_manifest_path()
pytestmark = pytest.mark.skipif(
    not Path(manifest_path).exists(),
    reason="needs dbt/target/manifest.json — run `just dbt-deps` and `dbt parse` first",
)

LABELS = {"additive", "semi_additive", "non_additive", "not_a_measure"}

# The contract gives every mart column a `data_type`, so "is this a measure-
# shaped column" is answerable without opening the warehouse.
#
# **A pattern rather than a list, because a list fails in the wrong direction**:
# a numeric type missing from it (say `DECIMAL`, the natural type for money)
# exempts its columns from the coverage test, and labelling one anyway fails
# `test_only_numeric_columns_are_labelled`. The contracts hold only a few types
# today, so such a gap would stay invisible until the first column arrived.
#
# The `\b` matters twice: `INTERVAL` begins `INT` and is not a measure, and
# `INTEGER[]` is a list rather than something to sum, which the lookahead
# excludes. `test_the_numeric_pattern_knows_a_measure_from_a_timestamp` pins
# both, and `DECIMAL`.
NUMERIC = re.compile(
    r"^(?:"
    r"U?(?:TINY|SMALL|BIG|HUGE)INT"  # TINYINT … HUGEINT, signed and unsigned
    r"|U?INT(?:EGER|\d+)?"  # INT, INT4, INT128, INTEGER, UINTEGER
    r"|DECIMAL|NUMERIC"  # fixed point, both spellings
    r"|DOUBLE|REAL|FLOAT\d*"  # floating point
    r"|SIGNED|SHORT|LONG"  # DuckDB's own aliases for the integer widths
    r")\b(?!\[)"
)

# Names that promise a ratio. Deliberately a *name* rule and not a type rule:
# it is the only thing here that can catch a label which is present and wrong.
RATIO_SHAPED = re.compile(r"(_pct$|_per_|_share|share_|_rate$|rate_|intensity|median_|avg_|price)")


def mart_columns() -> dict[tuple[str, str], dict]:
    """Every column of every marts model, keyed by (relation, column).

    Keyed on the *published* relation (`schema.alias`) rather than the model
    name, because the versioned model is two relations and they are labelled
    independently — v1 inherits its 36 through `include: all` and declares
    `co2_per_gdp` itself.
    """
    manifest = json.loads(Path(manifest_path).read_text())
    out = {}
    for node in manifest["nodes"].values():
        if node.get("resource_type") != "model" or not node.get("path", "").startswith("marts/"):
            continue
        relation = f"{node['schema']}.{node.get('alias') or node['name']}"
        for column, spec in (node.get("columns") or {}).items():
            out[(relation, column)] = spec
    return out


def labelled() -> dict[tuple[str, str], str]:
    out: dict[tuple[str, str], str] = {}
    for key, spec in mart_columns().items():
        label = (spec.get("meta") or {}).get("additivity")
        if label:
            out[key] = label
    return out


def extras() -> dict[tuple[str, str], str]:
    """`EXTRA_ADDITIVITY`, rekeyed to match `labelled()`."""
    return {
        (f"{schema}.{table}", column): label
        for (schema, table, column), label in EXTRA_ADDITIVITY.items()
    }


def everything() -> dict[tuple[str, str], str]:
    """Every label the release publishes — dbt's and the Polars layer's."""
    return labelled() | extras()


def numeric() -> dict[tuple[str, str], dict]:
    return {
        key: spec
        for key, spec in mart_columns().items()
        if NUMERIC.match((spec.get("data_type") or "").upper())
    }


def test_the_numeric_pattern_knows_a_measure_from_a_timestamp():
    """The pin for `NUMERIC`, which decides what the two coverage tests below
    even look at.

    A type absent from the contracts is invisible to every other assertion in
    this file, so the rule has to be exercised against types the warehouse does
    not hold *yet*.

    The near-misses are the point of the negative list: `INTERVAL` starts `INT`,
    `INTEGER[]` is a list of measures rather than a measure, and `STRUCT(…)`
    can contain one without being one.
    """
    measures = [
        "BIGINT",
        "INTEGER",
        "DOUBLE",
        "HUGEINT",
        "FLOAT",
        "REAL",
        "SMALLINT",
        "TINYINT",
        "UBIGINT",
        "UINTEGER",
        "UTINYINT",
        "INT",
        "INT4",
        "INT128",
        "DECIMAL(18,2)",
        "NUMERIC(10,4)",
        "FLOAT8",
    ]
    not_measures = [
        "VARCHAR",
        "BOOLEAN",
        "DATE",
        "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE",
        "INTERVAL",
        "BLOB",
        "UUID",
        "JSON",
        "INTEGER[]",
        "STRUCT(a INTEGER)",
    ]

    assert [t for t in measures if not NUMERIC.match(t)] == [], "measures the pattern missed"
    assert [t for t in not_measures if NUMERIC.match(t)] == [], "non-measures the pattern claimed"


def test_every_numeric_mart_column_carries_an_additivity_label():
    """The exhaustive half. A new measure with no label is the failure mode.

    This one is scoped to `marts`, which is what dbt can describe. The
    `analytics` tables are written by Polars and invisible to dbt, so they are
    declared in `EXTRA_ADDITIVITY` and held by the two tests below — the same
    split `EXTRA_CLASSIFICATIONS` makes for `pii`. `staging` is outside both on
    purpose: it is a cleaning copy of a source whose measures are declared one
    layer up.
    """
    missing = sorted(key for key in numeric() if key not in labelled())
    assert not missing, (
        f"{len(missing)} numeric mart columns carry no `meta: {{additivity: …}}`: {missing}"
    )


def test_the_label_vocabulary_is_closed():
    """Four labels, and a typo is a fifth. Same rule as the `pii` vocabulary:
    a label nobody chose deliberately is how a classification becomes
    decoration."""
    # `<=`, not `==`: a fifth label is the typo this exists for, but a project
    # need not use all four.
    assert set(everything().values()) <= LABELS


def test_only_numeric_columns_are_labelled():
    """The vacuity guard. Every assertion above is over the numeric columns, so
    a rule that labelled everything — or that drifted onto the varchars — would
    satisfy them all while meaning nothing."""
    stray = sorted(key for key in labelled() if key not in numeric())
    assert not stray, f"labelled but not a numeric column: {stray}"


def test_a_ratio_shaped_name_is_never_summable():
    """The half that can catch a label which is present and *wrong*.

    Coverage alone cannot: a column labelled `additive` that is really a
    percentage is as labelled as one that isn't. A name carrying `_pct`,
    `_per_`, `share`, `rate`, `intensity`, `median_`, `avg_` or `price` is a
    ratio by construction, so `additive` or `semi_additive` on one is a
    contradiction between the name and the label — and it holds across the whole
    tree today with **no exceptions**, which is what makes it worth asserting
    rather than documenting.

    One-directional on purpose. Plenty of non-additive columns are not
    ratio-named (`temp_mean_c`, `longest_gap_days`, `n_customers`), and
    requiring the converse would be asserting that this pattern is a complete
    theory of measures, which it isn't.
    """
    summable = sorted(
        (relation, column, label)
        for (relation, column), label in everything().items()
        if RATIO_SHAPED.search(column) and label in {"additive", "semi_additive"}
    )
    assert not summable, f"named like a ratio but declared summable: {summable}"


def test_a_semi_additive_column_says_which_direction_fails():
    """`semi_additive` is the only label that is useless on its own.

    "Summable in some directions" without saying which leaves a reader exactly
    where they started. `cohort_size` adds across cohorts and multiplies down
    one; `population` adds across countries and gives person-years across years;
    `original_quantity` belongs to the purchase, and 16,398 matched returns point
    at 15,312 distinct purchases, so summing it counts 1,086 of them twice. None
    of that is recoverable from the label.
    """
    silent = sorted(
        key
        for key, label in labelled().items()
        if label == "semi_additive" and not (mart_columns()[key].get("description") or "").strip()
    )
    assert not silent, f"semi_additive with no description saying which direction fails: {silent}"


def test_the_labels_reach_the_release_manifest():
    """A label with no consequence is decoration — the same argument that makes
    `direct_identifier` real is what puts these in `manifest.json`.

    The release is the audience that cannot ask: a Parquet consumer has the
    types and nothing else. Asserted against the ymls rather than against a
    frozen number, so the two cannot drift apart.
    """
    shipped = additivity()
    assert shipped is not None, "the dbt manifest exists, so the export must have read it"
    flat = {
        (relation, column): label
        for relation, columns in shipped.items()
        for column, label in columns.items()
    }
    assert flat == everything()
