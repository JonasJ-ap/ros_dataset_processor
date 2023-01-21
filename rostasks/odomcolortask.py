from rostasks import Rostask
import pandas as pd
from typing import Dict
import rosbag
import struct
import os
from rosutils.baglas import exportPointCloud
from nav_msgs.msg import Odometry
from sensor_msgs import point_cloud2
from std_msgs.msg import Header
from sensor_msgs.msg import PointCloud2, PointField
from numpy import sqrt

from rostasks.base import MOCAP_TOPIC, STATS_TOPIC, ODOM_TOPIC, SYSTEM_ID, POINTCLOUD_TOPIC, MAX_POINTS, DEFAULT_MAX_POINTS


def compute_angular_speed(rotation1, rotation2, delta_time):
    rot_roll = rotation1.x - rotation2.x
    rot_pitch = rotation1.y - rotation2.y
    rot_yaw = rotation1.z - rotation2.z
    angular_roll = rot_roll / delta_time
    angular_pitch = rot_pitch / delta_time
    angular_yaw = rot_yaw / delta_time
    return sqrt(angular_roll ** 2 + angular_pitch ** 2 + angular_yaw ** 2)


def processed_multiple_bag(bagNames, outBagName, odom_topic, odom_path_topic):
    with rosbag.Bag(outBagName, 'w') as outBag:
        points = []
        prev_orientation = None
        for bagName in bagNames:
            current_bag = rosbag.Bag(bagName)
            count = 0
            for topic, msg, t in current_bag.read_messages(topics=[odom_topic]):

                pose = msg.pose.pose
                position = pose.position
                orientation = pose.orientation
                current_angular_speed = 0
                if prev_orientation is not None:
                    current_angular_speed = compute_angular_speed(
                        orientation, prev_orientation, 0.2)
                prev_orientation = orientation

                red_ratio = min(current_angular_speed / 1.5, 1.0)
                r = int(red_ratio * 255.0)
                g = int(0.0 * 255.0)
                b = int(0.8 * 255.0)
                a = 255
                rgb = struct.unpack('I', struct.pack('BBBB', b, g, r, a))[0]

                fields = [PointField('x', 0, PointField.FLOAT32, 1),
                          PointField('y', 4, PointField.FLOAT32, 1),
                          PointField('z', 8, PointField.FLOAT32, 1),
                          # PointField('rgb', 12, PointField.UINT32, 1),
                          PointField('rgba', 12, PointField.UINT32, 1),
                          ]
                header = Header()
                header.frame_id = msg.header.frame_id
                header.stamp = msg.header.stamp
                point = [position.x, position.y, position.z, rgb]
                points.append(point)
                if len(points) > 20:
                    points.pop(0)
                pointcloud = point_cloud2.create_cloud(header, fields, points)
                outBag.write(odom_path_topic, pointcloud, t)

                count += 1


class OdomColorTask(Rostask):

    def __init__(self):
        super().__init__()
        self.output_dir_name = "odom_path_output"

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
        return True

    def initialize(self, project_name: str, run_name: str, bag_path: str, properties: Dict[str, str]):
        super().initialize(project_name, run_name, bag_path, properties)
        self.odom_topic = properties.get(ODOM_TOPIC)
        self.system_id = properties.get(SYSTEM_ID)
        self.max_points = int(properties.get(MAX_POINTS, DEFAULT_MAX_POINTS))

    def execute(self):
        print(
            f"Execute odom path coloring task for {self.project_name}.{self.run_name}")
        os.makedirs(self.output_path, exist_ok=True)
        output_bag = os.path.join(
            self.output_path, f"{self.project_name}_{self.run_name}_odom_color_path.bag")
        odom_path_topic = f"/{self.system_id}/path_points"
        processed_multiple_bag(self.bags, output_bag,
                               self.odom_topic, odom_path_topic)
        exportPointCloud([output_bag], odom_path_topic, f"{self.project_name}_{self.run_name}_odom_color_path",
                         self.output_path, self.max_points, None, 1, None, None, None)
