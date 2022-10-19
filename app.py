import yaml
import argparse
from rosproject.rosproject import Project
from rostasks.csvtask import CsvTask
from rostasks.pointcloudtask import PointcloudTask


def get_properties(filename):
    with open(filename) as f:
        properties = yaml.load(f, Loader=yaml.FullLoader)
    return properties


def get_tasks(properties):
    tasks = []
    if properties['csv_task'] == 'true':
        tasks.append(CsvTask())
    if properties['pointcloud_task'] == 'true':
        tasks.append(PointcloudTask())
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
