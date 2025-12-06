# PanopticPlugin

This is the readme for the default panoptic plugin.
Installation and functions part NEED to be modified since they are fake examples.
<br>
If you're new to plugin developpment, refer to: https://github.com/CERES-Sorbonne/Panoptic/wiki/Plugin
To get started, you can create a new repository by using this one as a template with the dedicated github button.

# Summary

This plugin is a panoptic plugin. [Panoptic](https://github.com/CERES-Sorbonne/Panoptic) is required before installing it. 
It provides new functions to panoptic such as new embeddings, new clustering, text-image similarity, and OCR.
<br>


# Installation

Copy the url of the git repository directly in panoptic in the add a plugin with git interface: the url should look like this: `https://github.com/{{username}}/{{repo_name}}`

# Functions

## Embeddings
In addition to panoptic default CLIP embeddings, this plugin adds embeddings using ImageNet finetuned on historical archives.

## Clusters
In addition to panoptic default KMeans clustering, this plugins adds the possibility to use a DBScan algorithm.

## Similarity
Instead of using only image embedding similarity, this plugin offers to user image + text similarity. Note: since no parameters can be used in panoptic similarity system, you need to manually set in the plugin parameters the name of the property you want to use for text similarity.

## Group Action
This plugin adds an OCR function using TrOCR, you can trigger it on a group of image, it will create a new property containing the OCRized image text for each image.
