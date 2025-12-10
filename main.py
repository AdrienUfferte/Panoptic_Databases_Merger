from typing import List

import logging

from pydantic import BaseModel

from panoptic.core.plugin.plugin import APlugin
from panoptic.core.plugin.plugin_project_interface import PluginProjectInterface
from panoptic.models import ActionContext, Instance

from .utils import (
    MergeMapping,
    ensure_merge_source_present,
    get_instances_from_context,
    mark_cluster_validated,
    merge_metadata_for_instances,
)

# logger for the plugin (Panoptic will normally configure logging handlers)
logger = logging.getLogger("PanopticDatabasesMerger")


class PluginParams(BaseModel):
    """
    @merge_source_field: metadata field carrying the source database label (e.g., "merge-source")
    @merge_validated_flag: metadata field marking a cluster as validated for merge
    @merge_mappings_raw: JSON string containing the mappings to apply at merge time.
    """

    merge_source_field: str = "merge-source"
    merge_validated_flag: str = "merge-validated"
    merge_mappings_raw: str = "[]"
    # Optional per-slot fields to make editing mappings easier in the UI.
    # Each `merge_map_X_sources` should be a comma-separated list of source
    # field names; `merge_map_X_destination` is the destination field name.
    # Empty slots are ignored. We provide 25 slots by default.
    merge_map_1_sources: str = ""
    merge_map_1_destination: str = ""
    merge_map_2_sources: str = ""
    merge_map_2_destination: str = ""
    merge_map_3_sources: str = ""
    merge_map_3_destination: str = ""
    merge_map_4_sources: str = ""
    merge_map_4_destination: str = ""
    merge_map_5_sources: str = ""
    merge_map_5_destination: str = ""
    merge_map_6_sources: str = ""
    merge_map_6_destination: str = ""
    merge_map_7_sources: str = ""
    merge_map_7_destination: str = ""
    merge_map_8_sources: str = ""
    merge_map_8_destination: str = ""
    merge_map_9_sources: str = ""
    merge_map_9_destination: str = ""
    merge_map_10_sources: str = ""
    merge_map_10_destination: str = ""
    merge_map_11_sources: str = ""
    merge_map_11_destination: str = ""
    merge_map_12_sources: str = ""
    merge_map_12_destination: str = ""
    merge_map_13_sources: str = ""
    merge_map_13_destination: str = ""
    merge_map_14_sources: str = ""
    merge_map_14_destination: str = ""
    merge_map_15_sources: str = ""
    merge_map_15_destination: str = ""
    merge_map_16_sources: str = ""
    merge_map_16_destination: str = ""
    merge_map_17_sources: str = ""
    merge_map_17_destination: str = ""
    merge_map_18_sources: str = ""
    merge_map_18_destination: str = ""
    merge_map_19_sources: str = ""
    merge_map_19_destination: str = ""
    merge_map_20_sources: str = ""
    merge_map_20_destination: str = ""
    merge_map_21_sources: str = ""
    merge_map_21_destination: str = ""
    merge_map_22_sources: str = ""
    merge_map_22_destination: str = ""
    merge_map_23_sources: str = ""
    merge_map_23_destination: str = ""
    merge_map_24_sources: str = ""
    merge_map_24_destination: str = ""
    merge_map_25_sources: str = ""
    merge_map_25_destination: str = ""
    merge_source_missing_label: str = "[merge-source not provided]"

class PanopticDatabasesMerger(APlugin):
    """
    Plugin to validate similar-image clusters and merge metadata across databases.
    """

    def __init__(self, project: PluginProjectInterface, plugin_path: str, name: str):
        super().__init__(name=name, project=project, plugin_path=plugin_path)
        self.params = PluginParams()
        # runtime-only list of MergeMapping objects (not part of the BaseModel
        # to avoid core introspection issues). Plugins can set this via the
        # UI as a list of dicts that conform to MergeMapping at runtime.
        self.merge_mappings: list[MergeMapping] = []
        logger.info("Initializing PanopticDatabasesMerger plugin")
        # try to initialise merge_mappings from raw param if present
        try:
            import json
            logger.debug("Parsing merge_mappings_raw: %s", self.params.merge_mappings_raw)
            parsed = json.loads(self.params.merge_mappings_raw or "[]")
            self.merge_mappings = []
            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        try:
                            mm = MergeMapping(**item)
                            self.merge_mappings.append(mm)
                        except Exception:
                            logger.exception("Invalid mapping entry in merge_mappings_raw, skipping: %s", item)
            logger.info("Parsed %d merge mappings from JSON.", len(self.merge_mappings))
        except Exception as e:
            logger.exception("Failed to parse merge_mappings_raw: %s", e)
            self.merge_mappings = []

        # Also allow per-slot mappings from individual fields in the plugin UI.
        try:
            added = 0
            for i in range(1, 26):
                src_field = getattr(self.params, f"merge_map_{i}_sources", "")
                dst_field = getattr(self.params, f"merge_map_{i}_destination", "")
                if src_field and dst_field:
                    try:
                        sources = [s.strip() for s in src_field.split(',') if s.strip()]
                        if sources:
                            mm = MergeMapping(sources=sources, destination=dst_field)
                            self.merge_mappings.append(mm)
                            added += 1
                    except Exception:
                        logger.exception("Invalid per-slot mapping at slot %d, skipping (sources=%s destination=%s)", i, src_field, dst_field)
            if added:
                logger.info("Added %d merge mappings from per-slot fields.", added)
        except Exception as e:
            logger.exception("Failed to parse per-slot merge mappings: %s", e)
        


        # Ensure every imported instance has a merge-source tag (or the default placeholder).
        self.project.on_instance_import(self._on_instance_import)
        logger.debug("Registered instance import hook")

        # Actions shown in the UI.
        # Make the actions visible for several common selection contexts so they
        # appear in the UI whether the user selects a group/cluster or a set
        # of images. This helps in Panoptic installs where the action context
        # string may differ between views.
        self.add_action_easy(self.validate_cluster, ["group", "selection", "images"])  # mark selected cluster as mergeable
        self.add_action_easy(self.execute_metadata_merge, ["execute", "selection", "images"])  # perform merge on selection
        logger.info("Registered actions: validate_cluster, execute_metadata_merge")

    async def _on_instance_import(self, instance: Instance):
        logger.debug("on_instance_import: Ensuring merge source for instance %s", getattr(instance, 'id', None))
        try:
            ensure_merge_source_present(
                instance,
                merge_source_field=self.params.merge_source_field,
                missing_label=self.params.merge_source_missing_label,
            )
        except Exception:
            logger.exception("Error while ensuring merge source for instance %s", getattr(instance, 'id', None))

    async def validate_cluster(self, context: ActionContext):
        """
        Marks the selected cluster as validated for metadata merge.
        """
        logger.info("validate_cluster called")
        try:
            instances = get_instances_from_context(context)
        except Exception:
            logger.exception("Failed to retrieve instances from context in validate_cluster")
            return

        if not instances:
            logger.warning("No instances found in context for validation.")
            return

        for inst in instances:
            try:
                logger.info("Marking instance %s as validated.", getattr(inst, 'id', None))
                mark_cluster_validated(inst, flag_field=self.params.merge_validated_flag, flag_value=True)
            except Exception:
                logger.exception("Failed to mark instance %s as validated", getattr(inst, 'id', None))

    async def execute_metadata_merge(self, context: ActionContext):
        """
        Executes metadata merge for the currently selected cluster (similar images).
        Requires the cluster to be validated first.
        """
        logger.info("execute_metadata_merge called")
        try:
            instances = get_instances_from_context(context)
        except Exception:
            logger.exception("Failed to retrieve instances from context in execute_metadata_merge")
            return

        if not instances:
            logger.warning("No instances found in context for merge.")
            return

        logger.info("Merging metadata for %d instances with %d mappings.", len(instances), len(self.merge_mappings))
        try:
            # Validate mappings quickly before calling merge
            valid_mappings = []
            for mm in self.merge_mappings:
                try:
                    if not mm.sources or not mm.destination:
                        logger.warning("Skipping mapping with empty sources or destination: %s", mm)
                        continue
                    valid_mappings.append(mm)
                except Exception:
                    logger.exception("Invalid mapping object encountered: %s", mm)

            merge_metadata_for_instances(
                instances,
                mappings=valid_mappings,
                merge_source_field=self.params.merge_source_field,
                merge_validated_flag=self.params.merge_validated_flag,
                missing_label=self.params.merge_source_missing_label,
            )
            logger.info("Metadata merge complete.")
        except Exception:
            logger.exception("Error during metadata merge")

    async def update_params(self, params: any):
        """
        Called when plugin parameters are updated from the UI.
        We override to parse `merge_mappings_raw` (JSON) into `self.merge_mappings`.
        """
        logger.info("update_params called with: %s", params)

        # Build a merged params dict: start from current params, overlay incoming values
        try:
            current = self.params.dict() if self.params is not None else {}
        except Exception:
            # fallback if self.params isn't a BaseModel yet
            current = {}

        incoming = {}
        if isinstance(params, dict):
            incoming = params
        else:
            try:
                incoming = params.dict()
            except Exception:
                incoming = {}

        merged = dict(current)
        merged.update(incoming)
        logger.debug("Built merged params: current_keys=%d incoming_keys=%d total_keys=%d", len(current), len(incoming), len(merged))
        try:
            changed_keys = [k for k in merged.keys() if merged.get(k) != current.get(k)]
            logger.debug("Changed keys after merge (sample): %s", changed_keys[:20])
        except Exception:
            logger.debug("Unable to compute changed keys")

        # Parse and canonicalize mappings from both JSON and per-slot fields
        try:
            import json

            mappings: list[MergeMapping] = []

            # parse JSON raw if present in merged
            raw = merged.get('merge_mappings_raw', '[]') or '[]'
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    for item in parsed:
                        if isinstance(item, dict):
                            try:
                                mappings.append(MergeMapping(**item))
                            except Exception:
                                logger.exception("Skipping invalid mapping from JSON: %s", item)
            except Exception:
                logger.exception("Invalid JSON in merge_mappings_raw: %s", raw)

            # parse per-slot fields from merged
            for i in range(1, 26):
                src_field = merged.get(f"merge_map_{i}_sources", "")
                dst_field = merged.get(f"merge_map_{i}_destination", "")
                if src_field and dst_field:
                    sources = [s.strip() for s in src_field.split(',') if s.strip()]
                    if sources:
                        mappings.append(MergeMapping(sources=sources, destination=dst_field))

            # deduplicate while preserving order
            seen = set()
            canonical: list[MergeMapping] = []
            for m in mappings:
                key = (tuple(m.sources), m.destination)
                if key not in seen:
                    seen.add(key)
                    canonical.append(m)

            self.merge_mappings = canonical

            # update merged dict: canonical JSON
            canonical_raw = json.dumps([
                {"sources": mm.sources, "destination": mm.destination} for mm in self.merge_mappings
            ])
            merged['merge_mappings_raw'] = canonical_raw

            # populate per-slot fields in merged
            for i in range(1, 26):
                if i <= len(self.merge_mappings):
                    mm = self.merge_mappings[i - 1]
                    merged[f"merge_map_{i}_sources"] = ','.join(mm.sources)
                    merged[f"merge_map_{i}_destination"] = mm.destination
                else:
                    merged[f"merge_map_{i}_sources"] = ''
                    merged[f"merge_map_{i}_destination"] = ''

        except Exception as e:
            logger.exception("Unexpected error in update_params canonicalization: %s", e)

        # Persist the canonical merged params once
        try:
            await super().update_params(merged)
            logger.info("update_params persisted canonical params to core. total_mappings=%d", len(self.merge_mappings))
        except Exception:
            logger.exception("Failed to persist canonical params to core")

