from rostasks import Rostask
import pandas as pd
from typing import Dict
import os
from rosutils.bagimg import exportVideo

from rostasks.base import SYSTEM_ID, THERMAL_TOPIC, PRINT_TIMESTAMP, DEFAULT_PRINT_TIMESTAMP, INVERT_THERMAL, DEFAULT_INVERT_THERMAL


class ThermalTask(Rostask):

    def __init__(self):
        super().__init__()
        self.output_dir_name = "thermal_video_output"

    def get_out_dir_name(self):
        return self.output_dir_name

    def validate(self):
        if not super().validate():
            print("Basecheck failed")
            return False
        if self.system_id is None:
            print("System ID is not set")
            return False
        if not self.thermal_topic:
            print("thermal topic is not defined")
            return False
        return True

    def initialize(self, project_name: str, run_name: str, bag_path: str, properties: Dict[str, str]):
        super().initialize(project_name, run_name, bag_path, properties)
        self.thermal_topic = properties.get(THERMAL_TOPIC)
        self.system_id = properties.get(SYSTEM_ID)
        self.print_timestamp = properties.get(
            PRINT_TIMESTAMP, DEFAULT_PRINT_TIMESTAMP)
        self.invert_thermal = properties.get(
            INVERT_THERMAL, DEFAULT_INVERT_THERMAL) == "true"

    def execute(self):
        print(
            f"Execute thermal video task for {self.project_name}.{self.run_name}")
        os.makedirs(self.output_path, exist_ok=True)
        out_video_path = os.path.join(
            self.output_path, f"{self.project_name}_{self.run_name}_thermal.mp4")
        exportVideo(self.bags, out_video_path,
                    self.thermal_topic, 1, 30, self.print_timestamp, self.invert_thermal, None)
