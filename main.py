from typing import List

from pydantic import BaseModel

from panoptic.core.plugin.plugin import APlugin
from panoptic.core.plugin.plugin_project_interface import PluginProjectInterface
from panoptic.models import ActionContext, Instance

from utils import (
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
    @merge_mappings: list of mappings from source fields to the merged destination field(s)
    """

    merge_source_field: str = "merge-source"
    merge_validated_flag: str = "merge-validated"
    merge_mappings: List[MergeMapping] = []
    merge_source_missing_label: str = "[merge-source not provided]"


class PanopticDatabasesMerger(APlugin):
    """
    Plugin to validate similar-image clusters and merge metadata across databases.
    """

    def __init__(self, project: PluginProjectInterface, plugin_path: str, name: str):
        super().__init__(name=name, project=project, plugin_path=plugin_path)
        self.params = PluginParams()

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
        ensure_merge_source_present(
            instance,
            merge_source_field=self.params.merge_source_field,
            missing_label=self.params.merge_source_missing_label,
        )

    async def validate_cluster(self, context: ActionContext):
        """
        Marks the selected cluster as validated for metadata merge.
        """
        instances = get_instances_from_context(context)
        if not instances:
            return

        for inst in instances:
            mark_cluster_validated(inst, flag_field=self.params.merge_validated_flag, flag_value=True)

    async def execute_metadata_merge(self, context: ActionContext):
        """
        Executes metadata merge for the currently selected cluster (similar images).
        Requires the cluster to be validated first.
        """
        instances = get_instances_from_context(context)
        if not instances:
            return

        merge_metadata_for_instances(
            instances,
            mappings=self.params.merge_mappings,
            merge_source_field=self.params.merge_source_field,
            merge_validated_flag=self.params.merge_validated_flag,
            missing_label=self.params.merge_source_missing_label,
        )

