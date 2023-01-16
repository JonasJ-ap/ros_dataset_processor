import yaml
import argparse
from rosproject.rosproject import Project
from rostasks.csvtask import CsvTask
from rostasks.pointcloudtask import PointcloudTask
from rostasks.mocaptask import MocapTask
from rostasks.rgbtask import RgbTask
from rostasks.thermaltask import ThermalTask
from rostasks.odomtask import OdomTask
from rostasks.csvoldtask import CsvOldTask
from rostasks.groundtruthtask import GroundTruthTask


def get_properties(filename):
    with open(filename) as f:
        properties = yaml.load(f, Loader=yaml.FullLoader)
    return properties


def get_tasks(properties):
    tasks = []
    if properties.get("csv_task") == "true":
        tasks.append(CsvTask())
    if properties.get("pointcloud_task") == "true":
        tasks.append(PointcloudTask())
    if properties.get("mocap_task") == "true":
        tasks.append(MocapTask())
    if properties.get("rgb_task") == "true":
        tasks.append(RgbTask())
    if properties.get("thermal_task") == "true":
        tasks.append(ThermalTask())
    if properties.get("odom_task") == "true":
        tasks.append(OdomTask())
    if properties.get("csv_old_task") == "true":
        tasks.append(CsvOldTask())
    if properties.get("groundtruth_task") == "true":
        tasks.append(GroundTruthTask())
    return tasks


def main(config_file):
    properties = get_properties(config_file)
    rosproject = Project(properties=properties)
    rosproject.processAllRuns(get_tasks(properties))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ROS dataset processor")
    parser.add_argument("-c", "--config", help="Config file")
    args = parser.parse_args()
    if args.config:
        main(args.config)
    else:
        parser.print_help()
