"""Layer 2 — dbt staging and marts, as one `dbt build`."""

# No `from __future__ import annotations`: Dagster refuses a string `context` annotation.

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets, get_asset_key_for_model

from modern_data_stack.paths import dbt_target_path
from orchestration.freshness import MODELLED_FRESHNESS
from orchestration.resources import dbt_project


class FolderGroupDbtTranslator(DagsterDbtTranslator):
    """Group dbt assets by top-level folder, key versioned models under their
    schema, and give every model a freshness policy."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        # The default keys an unversioned model `[schema, name]` but a versioned
        # one `[alias]` alone (`fct_thing`, `fct_thing_v1`). Prefixing the schema
        # keeps `marts/...` keys, the `key:"marts/*"` selection and the
        # materialisation history unchanged by versioning.
        key = super().get_asset_key(dbt_resource_props)
        if not dbt_resource_props.get("version"):
            return key
        schema = dbt_resource_props.get("config", {}).get("schema")
        return key.with_prefix(schema) if schema else key

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        # Snapshots live directly in `snapshots/`, so there's no folder to take —
        # and the default would name the group after the snapshot itself.
        if dbt_resource_props.get("resource_type") == "snapshot":
            return dbt_resource_props.get("schema")
        fqn = dbt_resource_props.get("fqn") or []
        # fqn is [project, <subfolders...>, name]
        return fqn[1] if len(fqn) > 2 else super().get_group_name(dbt_resource_props)

    def get_asset_spec(self, manifest, unique_id, project) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        return spec.replace_attributes(freshness_policy=MODELLED_FRESHNESS)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=FolderGroupDbtTranslator(),
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    # `build` runs the tests too, so they surface as asset checks on their models.
    # `target_path` is required: by default dagster-dbt writes each invocation's
    # artifacts to a unique subdirectory, and `pipeline_status` reads
    # `run_results.json` from this fixed path — without it `analytics.pipeline_runs`
    # gets no rows. `run_history_records_this_build` guards it.
    yield from dbt.cli(["build"], context=context, target_path=Path(dbt_target_path())).stream()


# The marts other layers read, keyed as the translator above keys them.
FCT_GOLD_PRICE_MONTH = get_asset_key_for_model([dbt_models], "fct_gold_price_month")
