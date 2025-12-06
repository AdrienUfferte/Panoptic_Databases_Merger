from pydantic import BaseModel

from panoptic.core.plugin.plugin import APlugin
from panoptic.models import Instance, ActionContext
from panoptic.core.plugin.plugin_project_interface import PluginProjectInterface


class PluginParams(BaseModel):
    """
    @default_text_prop: the default text prop that will be used for text+image similarity
    @ocr_prop_name: the name of the prop that will be created after an ocr
    """
    default_text_prop: str = "tweet_text"

    ocr_prop_name: str = "ocr"
  
  
class PluginExample(APlugin):  
    def __init__(self, project: PluginProjectInterface, plugin_path: str, name: str):  
        super().__init__(name=name,project=project, plugin_path=plugin_path)  
        self.params = PluginParams()  

        self.project.on_instance_import(self.compute_image_vector)
        self.add_action_easy(self.create_clusters, ['group'])
        self.add_action_easy(self.find_images, ['similar'])
        self.add_action_easy(self.ocr, ['execute'])
  
    async def compute_image_vector(self, instance: Instance):  
        pass

    async def find_images(self, context: ActionContext):
        pass

    async def create_clusters(self, context: ActionContext):
        pass

    async def make_ocr(self, context: ActionContext):
        pass

