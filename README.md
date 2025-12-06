# Panoptic Databases Merger Plugin

Plugin to help merge metadata across similar images detected by [Panoptic](https://github.com/CERES-Sorbonne/Panoptic).
It rides on top of Panoptic’s existing similar-image detection and clustering to guide validation and merge metadata coming from multiple databases.
If you're new to plugin development, see https://github.com/CERES-Sorbonne/Panoptic/wiki/Plugin.

# Summary

Panoptic handles detecting similar images and creating clusters. This plugin adds a guided workflow so you can validate clusters and merge metadata fields coming from different sources (e.g., different image databases).

# Installation

Copy the URL of the git repository directly in Panoptic via “add a plugin with git”; the URL should look like `https://github.com/{{username}}/{{repo_name}}`.

# Usage Flow

0) **Prepare source tagging**: ensure every image has a metadata field `merge-source` set to the name of the originating database.
1) **Validate clusters**: for each Panoptic-created cluster, the UI lets you confirm whether the cluster truly contains images whose metadata should be merged.
2) **Configure merging**: open the plugin configuration window and map source metadata names to a destination field (e.g., merge `Auteur` + `Author` → `Auteur fusionné`; add more pairs as needed).
3) **Execute merge**: trigger “execute metadata merge” once mappings are set.
4) **Merge behavior**: for every image inside validated “similar” clusters, all mapped metadata are copied into the destination field. Each value is suffixed with its `merge-source`, e.g., `Victor Hugo [source-1];Hugo Victor [source-2]` (if missing, it shows `[merge-source not provided]`).

# Notes

- Panoptic must be installed before adding this plugin.
- Make sure `merge-source` is consistently populated; merge results include this label to preserve provenance.
