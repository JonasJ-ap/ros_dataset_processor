from rostasks import Rostask
import pandas as pd
from typing import Dict
import os
from rosutils.bagimg import exportVideo

from rostasks.base import RGB_TOPIC, STATS_TOPIC, ODOM_TOPIC, SYSTEM_ID, POINTCLOUD_TOPIC, MAX_POINTS, DEFAULT_MAX_POINTS, RGB_TOPIC, THERMAL_TOPIC


class RgbTask(Rostask):

    def __init__(self):
        super().__init__()
        self.output_dir_name = "rgb_video_output"

    def get_out_dir_name(self):
        return self.output_dir_name

    def validate(self):
        if not super().validate():
            print("Basecheck failed")
            return False
        if self.system_id is None:
            print("System ID is not set")
            return False
        if not self.rgb_topic:
            print("RGB topic is not defined")
            return False
        if self.system_id not in self.rgb_topic:
            print("system id inconsistent with rgb topic")
            return False
        return True

    def initialize(self, project_name: str, run_name: str, bag_path: str, properties: Dict[str, str]):
        super().initialize(project_name, run_name, bag_path, properties)
        self.rgb_topic = properties.get(RGB_TOPIC)
        self.system_id = properties.get(SYSTEM_ID)

    def execute(self):
        print(
            f"Execute RGB video task for {self.project_name}.{self.run_name}")
        os.makedirs(self.output_path, exist_ok=True)
        out_video_path = os.path.join(
            self.output_path, f"{self.project_name}_{self.run_name}_rgb.mp4")
        exportVideo(self.bags, out_video_path,
                    self.rgb_topic, 1, 30, None, False, None)
