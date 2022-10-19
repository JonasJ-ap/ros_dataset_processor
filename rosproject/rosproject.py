import os
from typing import List, Dict
from rostasks import Rostask


PROJECT_PATH = "project_path"
RUN_TO_BAGS = "run_to_bags"
PROJECT_NAME = "project_name"


class Project:

    def __init__(self, properties: Dict[str, str]):
        self.properties = properties
        self.project_path = properties.get(PROJECT_PATH, "")
        self.run_to_bags = properties.get(RUN_TO_BAGS, "")
        self.project_name = properties.get(PROJECT_NAME, "default")
        if not self.validate():
            raise Exception("Invalid project properties")
        self.run_names = [runName for runName in os.listdir(
            self.project_path) if os.path.isdir(os.path.join(self.project_path, runName))]
        self.processedRuns = []

    def validate(self):
        if self.project_path == "" or self.run_to_bags == "" or self.project_name == "":
            return False
        return True

    def getRunPaths(self):
        return self.runPaths

    def getProcessedRuns(self):
        return self.processedRuns

    def getProjectPath(self):
        return self.project_path

    def getProjectName(self):
        return self.project_name

    def refreshProject(self):
        self.run_names = [runName for runName in os.listdir(
            self.project_path) if os.path.isdir(self.project_path + runName)]
        self.processedRuns = []

    def processRun(self, run_name, rostasks: List[Rostask]):
        if run_name in self.run_names:
            print(f"Processing {run_name}")
            runPath = os.path.join(self.project_path, run_name)
            bagPath = os.path.join(runPath, self.run_to_bags)
            for rostask in rostasks:
                print("=======================================")
                rostask.initialize(self.project_name, run_name,
                                   bagPath, self.properties)
                rostask.execute_task()
                print("=======================================")
            self.processedRuns.append(run_name)
            print(f"Finished processing {run_name}")
        else:
            print(f"{runPath} is not a valid run path")

    def processAllRuns(self, rostasks: List[Rostask]):
        for run_name in self.run_names:
            if run_name not in self.processedRuns:
                self.processRun(run_name, rostasks)
