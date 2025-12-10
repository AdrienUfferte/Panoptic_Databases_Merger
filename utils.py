import json
import logging
import os
from typing import Iterable, List

from pydantic import BaseModel

from panoptic.models import ActionContext, Instance
from panoptic.utils import get_datadir

logger = logging.getLogger("PanopticDatabasesMerger.utils")


class MergeMapping(BaseModel):
    """
    Describes how to merge multiple source metadata fields into a destination field.
    """

    sources: List[str]
    destination: str


def after_install():
    """
    Adds plugin to the list of registered Panoptic plugins.
    """
    project_path = get_datadir() / "panoptic" / "projects.json"
    with open(project_path, "r", encoding="utf-8") as f:
        projects = json.load(f)
        projects["plugins"].append(os.path.dirname(os.path.abspath(__file__)))
    with open(project_path, "w", encoding="utf-8") as f:
        json.dump(projects, f)


def get_properties(instance: Instance) -> dict:
    """
    Tries common attributes used by Panoptic instances to store metadata and returns a dict.
    """
    for attr in ("properties", "props", "metadata"):
        if hasattr(instance, attr):
            value = getattr(instance, attr)
            if value is None:
                value = {}
                setattr(instance, attr, value)
            return value

    # Fallback if the instance does not expose a known metadata attribute.
    logger.debug("Instance has no standard metadata attributes; creating 'properties' on instance %s", getattr(instance, 'id', None))
    instance.properties = {}
    return instance.properties


def ensure_merge_source_present(instance: Instance, merge_source_field: str, missing_label: str) -> str:
    """
    Ensures the merge-source field exists; if missing, sets the placeholder label.
    """
    try:
        props = get_properties(instance)
        if not props.get(merge_source_field):
            logger.debug("Setting missing merge_source_field '%s' to placeholder for instance %s", merge_source_field, getattr(instance, 'id', None))
            props[merge_source_field] = missing_label
        return props[merge_source_field]
    except Exception:
        logger.exception("Error ensuring merge source present for instance %s", getattr(instance, 'id', None))
        return missing_label


def mark_cluster_validated(instance: Instance, flag_field: str, flag_value: bool = True):
    """
    Marks an instance as part of a validated cluster for merging.
    """
    try:
        props = get_properties(instance)
        props[flag_field] = flag_value
        logger.debug("Marked instance %s with validation flag %s=%s", getattr(instance, 'id', None), flag_field, flag_value)
    except Exception:
        logger.exception("Failed to mark instance %s as validated", getattr(instance, 'id', None))


def merge_metadata_for_instances(
    instances: Iterable[Instance],
    mappings: Iterable[MergeMapping],
    merge_source_field: str,
    merge_validated_flag: str,
    missing_label: str,
):
    """
    Merges metadata across instances according to provided mappings.
    All merged values are suffixed with the merge-source (or the missing placeholder).
    Only runs if the cluster has been marked as validated.
    """
    instances = list(instances)
    if not instances:
        return
    try:
        mappings = list(mappings)
        logger.info("merge_metadata_for_instances called: instances=%d mappings=%d", len(instances), len(mappings))

        # Require cluster validation before merging.
        if not _is_validated(instances, merge_validated_flag):
            logger.info("Cluster not validated (flag=%s); skipping merge", merge_validated_flag)
            return

        if not mappings:
            logger.info("No mappings provided; skipping merge")
            return

        # Ensure all instances have a merge-source value.
        for inst in instances:
            try:
                ensure_merge_source_present(inst, merge_source_field=merge_source_field, missing_label=missing_label)
            except Exception:
                logger.exception("Failed to ensure merge source for instance %s", getattr(inst, 'id', None))

        for idx, mapping in enumerate(mappings, start=1):
            try:
                logger.debug("Processing mapping %d -> destination=%s sources=%s", idx, mapping.destination, mapping.sources)
                merged_values = []
                for inst in instances:
                    try:
                        props = get_properties(inst)
                        source_label = props.get(merge_source_field, missing_label) or missing_label
                        for source_field in mapping.sources:
                            value = props.get(source_field)
                            if value:
                                merged_values.append(f"{value} [{source_label}]")
                    except Exception:
                        logger.exception("Error reading properties for instance %s while processing mapping %s", getattr(inst, 'id', None), mapping)

                merged_value = ";".join(merged_values)
                logger.debug("Mapping %d produced merged_value length=%d", idx, len(merged_value))
                for inst in instances:
                    try:
                        props = get_properties(inst)
                        props[mapping.destination] = merged_value
                    except Exception:
                        logger.exception("Failed to write merged value to instance %s for destination %s", getattr(inst, 'id', None), mapping.destination)
            except Exception:
                logger.exception("Unhandled error while applying mapping %s", mapping)
    except Exception:
        logger.exception("Unhandled error in merge_metadata_for_instances")


def _is_validated(instances: Iterable[Instance], flag_field: str) -> bool:
    """
    Checks if the cluster/selection has been validated by ensuring at least one instance carries the flag.
    """
    for inst in instances:
        props = get_properties(inst)
        if props.get(flag_field):
            return True
    return False


def get_instances_from_context(context: ActionContext) -> List[Instance]:
    """
    Extracts instances from the action context, supporting multiple attribute names.
    """
    for attr in ("instances", "images", "selection"):
        if hasattr(context, attr):
            candidates = getattr(context, attr)
            if candidates:
                return list(candidates)
    return []
