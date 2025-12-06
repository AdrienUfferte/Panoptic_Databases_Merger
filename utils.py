import json
import os

from panoptic.utils import get_datadir

def after_install():
    """
    Adds plugin to the list of registered panoptic plugins
    """
    project_path = get_datadir() / 'panoptic' / 'projects.json'
    with open(project_path, 'r') as f:
        projects = json.load(f)
        projects['plugins'].append(os.path.dirname(os.path.abspath(__file__)))
    with open(project_path, 'w') as f:
        json.dump(projects, f)