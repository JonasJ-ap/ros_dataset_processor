from rostasks import Rostask
from typing import Dict
import rosbag
import os

from rostasks.base import ODOM_TOPIC, SYSTEM_ID, RAW_VELODYNE_TOPIC, IMU_TOPIC


def processed_multiple_bag(bagNames, outBagName, odom_topic, groundtruth_odom_topic, raw_velodyne, imu_topic):
    with rosbag.Bag(outBagName, 'w') as outBag:
        for bagName in bagNames:
            current_bag = rosbag.Bag(bagName)
            for topic, msg, t in current_bag.read_messages(topics=[odom_topic, raw_velodyne, imu_topic]):
                if topic == odom_topic:
                    outBag.write(groundtruth_odom_topic, msg, t)
                else:
                    outBag.write(topic, msg, t)


class GroundTruthTask(Rostask):

    def __init__(self):
        super().__init__()
        self.output_dir_name = "groundtruth_output"

    def get_out_dir_name(self):
        return self.output_dir_name

    def validate(self):
        if not super().validate():
            print("Basecheck failed")
            return False
        if self.system_id is None:
            print("System ID is not set")
            return False
        if not self.odom_topic:
            print("Odom topic is not defined")
            return False
        if self.system_id not in self.odom_topic:
            print("system id inconsistent with odom topic")
            return False
        if not self.raw_velodyne:
            print("Raw velodyne topic is not defined")
            return False
        if self.system_id not in self.raw_velodyne:
            print("system id inconsistent with raw velodyne topic")
            return False
        if not self.imu_topic:
            print("Imu topic is not defined")
            return False
        if self.system_id not in self.imu_topic:
            print("system id inconsistent with imu topic")
            return False
        return True

    def initialize(self, project_name: str, run_name: str, bag_path: str, properties: Dict[str, str]):
        super().initialize(project_name, run_name, bag_path, properties)
        self.odom_topic = properties.get(ODOM_TOPIC)
        self.system_id = properties.get(SYSTEM_ID)
        self.raw_velodyne = properties.get(RAW_VELODYNE_TOPIC)
        self.imu_topic = properties.get(IMU_TOPIC)

    def execute(self):
        print(
            f"Execute groundtruth task for {self.project_name}.{self.run_name}")
        os.makedirs(self.output_path, exist_ok=True)
        output_bag = os.path.join(
            self.output_path, f"{self.project_name}_{self.run_name}_groundtruth.bag")
        groundtruth_odom_topic = f"/groundtruth_odom"
        processed_multiple_bag(self.bags, output_bag,
                               self.odom_topic, groundtruth_odom_topic, self.raw_velodyne, self.imu_topic)
