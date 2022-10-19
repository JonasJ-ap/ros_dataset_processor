from rostasks import Rostask
import pandas as pd
import rosbag
from typing import Dict
import os
from rosutils.baglas import exportPointCloud

from rostasks.base import STATS_TOPIC, ODOM_TOPIC, SYSTEM_ID, POINTCLOUD_TOPIC, MAX_POINTS, DEFAULT_MAX_POINTS


class PointcloudTask(Rostask):

    def __init__(self):
        super().__init__()
        self.output_dir_name = "pointcloud_output"

    def get_out_dir_name(self):
        return self.output_dir_name

    def initialize(self, project_name: str, run_name: str, bag_path: str, properties: Dict[str, str]):
        super().initialize(project_name, run_name, bag_path, properties)
        self.pointcloud_topic = properties.get(POINTCLOUD_TOPIC)
        self.max_points = int(properties.get(MAX_POINTS, DEFAULT_MAX_POINTS))

    def execute(self):
        print(
            f"Execute Pointcloud task for {self.project_name}.{self.run_name}")
        os.makedirs(self.output_path, exist_ok=True)
        exportPointCloud(self.bags, self.pointcloud_topic, f"{self.project_name}-{self.run_name}",
                         self.output_path, self.max_points, None, 1, None, None, None)
