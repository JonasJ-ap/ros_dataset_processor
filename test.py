from rosproject.rosproject import Project
from rostasks.csvtask import CsvTask
from rostasks.pointcloudtask import PointcloudTask

PROJECT_PATH = "/Users/jonasjiang/Workspace/AirLab/dataset/2022-08-03_rc1"
RUN_TO_BAGS = "bags"
PROPERTIES = {"system_id": "cmu_rc1",
              "odom_topic": "/cmu_rc1/aft_mapped_to_init_imu", "stats_topic": "/cmu_rc1/super_odometry_stats",
              "pointcloud_topic": "/cmu_rc1/velodyne_cloud_registered_imu"}


def main():
    rosproject = Project(project_path=PROJECT_PATH, project_name="2022-08-03_rc1",
                         run_to_bags=RUN_TO_BAGS, properties=PROPERTIES)
    # rosproject.processAllRuns(CsvTask())
    rosproject.processAllRuns([CsvTask(), PointcloudTask()])


if __name__ == "__main__":
    main()
