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

from rostasks.base import MOCAP_TOPIC, STATS_TOPIC, ODOM_TOPIC, SYSTEM_ID, POINTCLOUD_TOPIC, MAX_POINTS, DEFAULT_MAX_POINTS


def processed_multiple_bag(bagNames, outBagName, system_id, mocap_topic, groundtruth_odom_topic, groundtruth_path_topic, odom_topic):
    with rosbag.Bag(outBagName, 'w') as outBag:
        points = []
        r = int(0.2 * 255.0)
        g = int(0.3 * 255.0)
        b = int(0.4 * 255.0)
        a = 255
        rgb = struct.unpack('I', struct.pack('BBBB', b, g, r, a))[0]
        once = True
        for bagName in bagNames:
            current_bag = rosbag.Bag(bagName)
            for topic, msg, t in current_bag.read_messages(topics=[mocap_topic, odom_topic]):
                if topic == mocap_topic:
                    ref_pose = msg.rigid_body_data.rigid_body_list[0].pose
                    ground_truth_odom = Odometry()
                    ground_truth_odom.header.frame_id = f"{system_id}_sensor_init"
                    ground_truth_odom.child_frame_id = f"{system_id}_sensor"
                    ground_truth_odom.header.stamp = t
                    ground_truth_odom.pose.pose.position.x = ref_pose.position.x
                    ground_truth_odom.pose.pose.position.y = ref_pose.position.y
                    ground_truth_odom.pose.pose.position.z = ref_pose.position.z
                    ground_truth_odom.pose.pose.orientation.x = ref_pose.orientation.x
                    ground_truth_odom.pose.pose.orientation.y = ref_pose.orientation.y
                    ground_truth_odom.pose.pose.orientation.z = ref_pose.orientation.z
                    ground_truth_odom.pose.pose.orientation.w = ref_pose.orientation.w
                    outBag.write(groundtruth_odom_topic, ground_truth_odom, t)

                    pose = ref_pose
                    position = pose.position

                    fields = [PointField('x', 0, PointField.FLOAT32, 1),
                              PointField('y', 4, PointField.FLOAT32, 1),
                              PointField('z', 8, PointField.FLOAT32, 1),
                              # PointField('rgb', 12, PointField.UINT32, 1),
                              PointField('rgba', 12, PointField.UINT32, 1),
                              ]
                    header = Header()
                    header.frame_id = ground_truth_odom.header.frame_id
                    header.stamp = t
                    point = [position.x, position.y, position.z, rgb]
                    points.append(point)
                    pointcloud = point_cloud2.create_cloud(
                        header, fields, points)
                    outBag.write(groundtruth_path_topic, pointcloud, t)
                elif topic == odom_topic:
                    outBag.write(odom_topic, msg, t)


class MocapTask(Rostask):

    def __init__(self):
        super().__init__()
        self.output_dir_name = "mocap_output"

    def get_out_dir_name(self):
        return self.output_dir_name

    def validate(self):
        if not super().validate():
            print("Basecheck failed")
            return False
        if self.system_id is None:
            print("System ID is not set")
            return False
        if not self.mocap_topic:
            print("Mocap topic is not defined")
            return False
        if not self.odom_topic:
            print("Odom topic is not defined")
            return False
        if "mil19" not in self.project_name.lower():
            print("Project is not mil19")
            return False
        return True

    def initialize(self, project_name: str, run_name: str, bag_path: str, properties: Dict[str, str]):
        super().initialize(project_name, run_name, bag_path, properties)
        self.mocap_topic = properties.get(MOCAP_TOPIC)
        self.odom_topic = properties.get(ODOM_TOPIC)
        self.system_id = properties.get(SYSTEM_ID)
        self.max_points = int(properties.get(MAX_POINTS, DEFAULT_MAX_POINTS))

    def execute(self):
        print(
            f"Execute Mocap task for {self.project_name}.{self.run_name}")
        os.makedirs(self.output_path, exist_ok=True)
        output_bag = os.path.join(
            self.output_path, f"{self.project_name}_{self.run_name}_mocap.bag")
        groundtruth_odom_topic = f"/{self.system_id}/groundtruth_odometry"
        groundtruth_path_topic = f"/{self.system_id}/groundtruth_path_points"
        processed_multiple_bag(self.bags, output_bag, self.system_id,
                               self.mocap_topic, groundtruth_odom_topic, groundtruth_path_topic, self.odom_topic)
        exportPointCloud([output_bag], groundtruth_path_topic, f"{self.project_name}_{self.run_name}_groundtruth",
                         self.output_path, self.max_points, None, 1, None, None, None)
