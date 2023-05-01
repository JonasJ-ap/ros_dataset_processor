

from abc import ABC, abstractmethod
from typing import Dict
import os


class Rostask(ABC):

    def __init__(self):
        self.project_name = None
        self.run_name = None
        self.bag_path = None
        self.properties = None
        self.initialized = False

    @abstractmethod
    def get_out_dir_name(self):
        """
        Return the name of the output directory
        """

    def initialize(self, project_name: str, run_name: str, bag_path: str, properties: Dict[str, str]):
        """
        Initialize the task with the project name, bag path, and properties.
        Perform additional operations here.
        """
        self.project_name = project_name
        self.run_name = run_name
        self.bag_path = bag_path
        self.properties = properties
        self.bags = [os.path.join(self.bag_path, bag) for bag in os.listdir(
            self.bag_path) if bag.endswith(".bag")]
        self.bags.sort()
        self.output_path = os.path.join(self.bag_path, self.get_out_dir_name())
        self.initialized = True

    def validate(self):
        if not self.initialized:
            print("Task not initialized")
            return False
        if self.project_name is None or self.bag_path is None or self.properties is None or self.run_name is None:
            return False
        return True

    def execute_task(self):
        if self.validate():
            self.execute()
        else:
            print(
                f"invalid properties, fail to execute {self.__class__.__name__}")

    @abstractmethod
    def execute(self):
        """
        Execute the task
        """
