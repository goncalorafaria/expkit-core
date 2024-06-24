import copy
import json
import logging
import os
from dataclasses import dataclass
from typing import *
import uuid

from dataclasses import dataclass
from typing import *


@dataclass
class InstanceEval:
    results: Dict[str, Any]

    def __init__(self, **kwargs):
        """
        Initialize an InstanceEval object.

        Args:
            **kwargs: Key-value pairs representing the evaluation results.
        """
        self.results = kwargs

    def __str__(self):
        """
        Return a string representation of the InstanceEval object.
        """
        return f"InstanceEval(results={self.results})"

    def __getitem__(self, key):
        """
        Get the value associated with a specific key.

        Args:
            key: The key to retrieve the value for.

        Returns:
            The value associated with the key.
        """
        return self.results[key]

    def __getattr__(self, key):
        """
        Get the value associated with a specific key.

        Args:
            key: The key to retrieve the value for.

        Returns:
            The value associated with the key.
        """
        return self.results[key]

    def to_dict(self):
        """
        Convert the InstanceEval object to a dictionary.

        Returns:
            A dictionary representation of the InstanceEval object.
        """
        return self.results


@dataclass
class Instance:
    input_data: Dict[str, Any]
    outputs: List[Dict[str, Any]]

    def __str__(
        self,
    ):
        """
        Return a string representation of the Instance object.
        """
        return f"Instance(input_data={self.input_data}, outputs={self.outputs})"

    def to_dict(self):
        """
        Convert the Instance object to a dictionary.

        Returns:
            A dictionary representation of the Instance object.
        """
        return {
            "input": self.input_data,
            "outputs": self.outputs,
        }


class Exp:
    def __init__(self, name: str=None, meta: Dict[str, str]=None):
        """
        Initialize an Exp object.

        Args:
            name: The name of the experiment.
            meta: A dictionary containing metadata about the experiment.
        """
        self.evals = {}
        self.instances = []
        self.meta = {} if meta is None else meta
        self.name = str(uuid.uuid4()) if name is None else name

    def check_property(self, k, v):
        """
        Check if a specific property matches a given value.

        Args:
            k: The property to check.
            v: The value to compare against.

        Returns:
            True if the property matches the value, False otherwise.
        """
        return (k in self.meta) and (self.meta[k] == v)

    def get_name(
        self,
    ):
        """
        Get the name of the experiment.

        Returns:
            The name of the experiment.
        """
        return self.name

    def __str__(
        self,
    ):
        """
        Return a string representation of the Exp object.
        """
        return f"Experiment[{self.name}](instance={len(self.instances)} elements,evals={list(self.evals.keys())}, meta={self.meta})"

    def __repr__(self) -> str:
        """
        Return a string representation of the Exp object.
        """
        return self.__str__()
    
    def get(self, key):
        """
        Get the value associated with a specific key.

        Args:
            key: The key to retrieve the value for.

        Returns:
            The value associated with the key.

        Raises:
            ValueError: If the key is not found.
        """
        if key in self.__dict__:
            return self.__dict__[key]

        elif key in self.evals:
            return self.evals[key]

        elif key in self.meta:
            return self.meta[key]
        else:
            raise ValueError(f"key : {key} not found")

    def add_eval(self, key: str, data: List[Dict[str, Any]]):
        """
        Add evaluation data to the experiment.

        Args:
            key: The key to associate with the evaluation data.
            data: A list of dictionaries representing the evaluation data.
        """
        self.evals[key] = [InstanceEval(**d) for d in data]

    def add_instance(self, input_data=Dict[str, Any], output=List[Dict[str, Any]]):
        """
        Add an instance to the experiment.

        Args:
            input_data: A dictionary representing the input data for the instance.
            output: A list of dictionaries representing the output data for the instance.
        """
        self.instances.append(Instance(input_data, output))

    def add_instances(
        self, inputs: List[Dict[str, Any]], outputs: List[List[Dict[str, Any]]]
    ):
        """
        Add multiple instances to the experiment.

        Args:
            inputs: A list of dictionaries representing the input data for each instance.
            outputs: A list of lists of dictionaries representing the output data for each instance.
        """
        for input_data, output in zip(inputs, outputs):
            self.add_instance(input_data, output)

    def save(self, save_path):
        """
        Save the experiment to disk.

        Args:
            save_path: The path to save the experiment to.
        """
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        if not os.path.exists(os.path.join(save_path, self.name)):
            os.makedirs(os.path.join(save_path, self.name))

        save_path = os.path.join(save_path, self.name)
        data = [i.to_dict() for i in self.instances]

        evals = {
            "eval_" + k + ".json": [e.to_dict() for e in v]
            for k, v in self.evals.items()
        }

        files_to_write = {
            "data.json": data,
            "meta.json": self.meta,
            **evals,
        }

        for fn, data in files_to_write.items():
            with open(os.path.join(save_path, fn), "w") as fp:
                json.dump(data, fp=fp)

    
    def has_eval(self, key):
        return key in self.evals
    
    @staticmethod
    def load(base_path, name):
        """
        Load an experiment from disk.

        Args:
            base_path: The base path where the experiment is located.
            experiment_name: The name of the experiment to load.

        Returns:
            The loaded Exp object.

        Raises:
            ValueError: If required files are missing.
        """
        required_file_names = [
            "meta.json",
            "data.json",
        ]

        dir_path = os.path.join(base_path, name)

        run_files = os.listdir(dir_path)

        are_required_present = all([rf in run_files for rf in required_file_names])

        if not are_required_present:
            raise ValueError(f"Missing files in {dir_path} : {required_file_names}")

        with open(os.path.join(dir_path, "meta.json"), "r") as fp:
            meta = json.load(fp)

        with open(os.path.join(dir_path, "data.json"), "r") as fp:
            data = json.load(fp)

        exp = Exp(
            name=name,
            meta=meta,
        )

        inputs, outputs = zip(*[(d["input"], d["outputs"]) for d in data])

        exp.add_instances(
            inputs=list(inputs),
            outputs=list(outputs),
        )

        eval_files = {
            rf.split(".")[0].split("-")[1]: rf for rf in run_files if "eval-" in rf
        }

        for k, fn in eval_files.items():
            with open(os.path.join(dir_path, fn), "r") as fp:
                edata = json.load(fp)

                exp.add_eval(k, edata)

        return exp
