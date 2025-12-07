from typing import List

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
        print("[PanopticDatabasesMerger] Initializing plugin...")
        # try to initialise merge_mappings from raw param if present
        try:
            import json
            print(f"[PanopticDatabasesMerger] Parsing merge_mappings_raw: {self.params.merge_mappings_raw}")
            parsed = json.loads(self.params.merge_mappings_raw or "[]")
            self.merge_mappings = [MergeMapping(**m) for m in parsed if isinstance(m, dict)]
            print(f"[PanopticDatabasesMerger] Parsed {len(self.merge_mappings)} merge mappings from JSON.")
        except Exception as e:
            print(f"[PanopticDatabasesMerger] Failed to parse merge_mappings_raw: {e}")
            self.merge_mappings = []

        # Also allow per-slot mappings from individual fields in the plugin UI.
        try:
            added = 0
            for i in range(1, 26):
                src_field = getattr(self.params, f"merge_map_{i}_sources", "")
                dst_field = getattr(self.params, f"merge_map_{i}_destination", "")
                if src_field and dst_field:
                    sources = [s.strip() for s in src_field.split(',') if s.strip()]
                    if sources:
                        self.merge_mappings.append(MergeMapping(sources=sources, destination=dst_field))
                        added += 1
            if added:
                print(f"[PanopticDatabasesMerger] Added {added} merge mappings from per-slot fields.")
        except Exception as e:
            print(f"[PanopticDatabasesMerger] Failed to parse per-slot merge mappings: {e}")
        


        # Ensure every imported instance has a merge-source tag (or the default placeholder).
        self.project.on_instance_import(self._on_instance_import)

        # Actions shown in the UI.
        # Make the actions visible for several common selection contexts so they
        # appear in the UI whether the user selects a group/cluster or a set
        # of images. This helps in Panoptic installs where the action context
        # string may differ between views.
        self.add_action_easy(self.validate_cluster, ["group", "selection", "images"])  # mark selected cluster as mergeable
        self.add_action_easy(self.execute_metadata_merge, ["execute", "selection", "images"])  # perform merge on selection

    async def _on_instance_import(self, instance: Instance):
        print(f"[PanopticDatabasesMerger] on_instance_import: Ensuring merge source for instance {getattr(instance, 'id', None)}")
        ensure_merge_source_present(
            instance,
            merge_source_field=self.params.merge_source_field,
            missing_label=self.params.merge_source_missing_label,
        )

    async def validate_cluster(self, context: ActionContext):
        """
        Marks the selected cluster as validated for metadata merge.
        """
        print("[PanopticDatabasesMerger] validate_cluster called.")
        instances = get_instances_from_context(context)
        if not instances:
            print("[PanopticDatabasesMerger] No instances found in context for validation.")
            return

        for inst in instances:
            print(f"[PanopticDatabasesMerger] Marking instance {getattr(inst, 'id', None)} as validated.")
            mark_cluster_validated(inst, flag_field=self.params.merge_validated_flag, flag_value=True)

    async def execute_metadata_merge(self, context: ActionContext):
        """
        Executes metadata merge for the currently selected cluster (similar images).
        Requires the cluster to be validated first.
        """
        print("[PanopticDatabasesMerger] execute_metadata_merge called.")
        instances = get_instances_from_context(context)
        if not instances:
            print("[PanopticDatabasesMerger] No instances found in context for merge.")
            return

        print(f"[PanopticDatabasesMerger] Merging metadata for {len(instances)} instances with {len(self.merge_mappings)} mappings.")
        merge_metadata_for_instances(
            instances,
            mappings=self.merge_mappings,
            merge_source_field=self.params.merge_source_field,
            merge_validated_flag=self.params.merge_validated_flag,
            missing_label=self.params.merge_source_missing_label,
        )
        print("[PanopticDatabasesMerger] Metadata merge complete.")

    async def update_params(self, params: any):
        """
        Called when plugin parameters are updated from the UI.
        We override to parse `merge_mappings_raw` (JSON) into `self.merge_mappings`.
        """
        print(f"[PanopticDatabasesMerger] update_params called with: {params}")
        await super().update_params(params)
        # parse mappings if provided
        try:
            import json
            raw = getattr(self.params, 'merge_mappings_raw', '[]') or '[]'
            print(f"[PanopticDatabasesMerger] Parsing merge_mappings_raw in update_params: {raw}")
            parsed = json.loads(raw)
            self.merge_mappings = [MergeMapping(**m) for m in parsed if isinstance(m, dict)]
            print(f"[PanopticDatabasesMerger] Parsed {len(self.merge_mappings)} merge mappings in update_params from JSON.")
        except Exception as e:
            print(f"[PanopticDatabasesMerger] Failed to parse merge_mappings_raw in update_params: {e}")
            # keep previous value on error

        # Parse any per-slot mapping fields as well and append
        try:
            added = 0
            for i in range(1, 26):
                src_field = getattr(self.params, f"merge_map_{i}_sources", "")
                dst_field = getattr(self.params, f"merge_map_{i}_destination", "")
                if src_field and dst_field:
                    sources = [s.strip() for s in src_field.split(',') if s.strip()]
                    if sources:
                        self.merge_mappings.append(MergeMapping(sources=sources, destination=dst_field))
                        added += 1
            if added:
                print(f"[PanopticDatabasesMerger] Added {added} merge mappings from per-slot fields in update_params.")
        except Exception as e:
            print(f"[PanopticDatabasesMerger] Failed to parse per-slot merge mappings in update_params: {e}")

