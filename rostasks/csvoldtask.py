from rostasks import Rostask
import pandas as pd
import rosbag
from typing import Dict
import os

from rostasks.base import STATS_TOPIC, ODOM_TOPIC, SYSTEM_ID


def process_bag(bagPath, cum_list, odom_topic, stats_topic):
    with rosbag.Bag(bagPath, 'r') as current_bag:
        for topic, msg, t in current_bag.read_messages(topics=[odom_topic]):
            temporal_dict = {}
            temporal_dict['x'] = msg.pose.pose.position.x
            temporal_dict['y'] = msg.pose.pose.position.y
            temporal_dict['z'] = msg.pose.pose.position.z
            temporal_dict['roll'] = msg.pose.pose.orientation.x
            temporal_dict['pitch'] = msg.pose.pose.orientation.y
            temporal_dict['yaw'] = msg.pose.pose.orientation.z
            temporal_dict['time'] = t.to_sec()
            temporal_dict['bag_time'] = str(msg.header.stamp.to_sec())
            cum_list.append(temporal_dict)
        return cum_list


def process_multiple_bags(bagPath, df, odom_topic, stats_topic):
    cum_list = []
    for bag in bagPath:
        cum_list = process_bag(bag, cum_list, odom_topic, stats_topic)
    df = pd.concat([df, pd.DataFrame.from_records(cum_list)], sort=False)
    return df


class CsvOldTask(Rostask):

    def __init__(self):
        super().__init__()
        self.output_dir_name = "csv_output"

    def get_out_dir_name(self):
        return self.output_dir_name

    def validate(self):
        if not super().validate():
            print("Basecheck failed")
            return False
        if self.system_id is None:
            print("Missing system id")
            return False
        if self.odom_topic is None:
            print("Missing odom topic")
            return False
        if self.stats_topic is None:
            print("Missing stats topic")
            return False

        if self.system_id not in self.odom_topic or self.system_id not in self.stats_topic:
            print("System id inconsistent with odom and stats topics")
            return False
        return True

    def initialize(self, project_name: str, run_name: str, bag_path: str, properties: Dict[str, str]):
        super().initialize(project_name, run_name, bag_path, properties)
        self.system_id = properties.get(SYSTEM_ID)
        self.odom_topic = properties.get(ODOM_TOPIC)
        self.stats_topic = properties.get(STATS_TOPIC)

    def execute(self):
        print(f"Execute CSV Old task for {self.project_name}.{self.run_name}")
        df = pd.DataFrame(
            columns=['time', 'bag_time', 'x', 'y', 'z', 'roll', 'pitch', 'yaw'])
        os.makedirs(self.output_path, exist_ok=True)
        df = process_multiple_bags(
            self.bags, df, self.odom_topic, self.stats_topic)
        result_path = os.path.join(
            self.output_path, f"{self.project_name}_{self.run_name}.csv")
        df.to_csv(result_path, index=False)
