import json
from expkit.exp import Exp
from expkit.pexp import PExp
from expkit.eval import Evalutor
from typing import *
from dataclasses import dataclass
from functools import partial
import copy
import os


class ExpSetup:
    """
    This class is responsible for loading and processing experiment data.

    Attributes:
        base_path (str): The base path where the experiment data is located.
        experiments (list): A list of experiment objects.
        ops (dict): A dictionary of functions to be applied to each experiment's full results.
    """

    def __init__(
        self,
        base_path,
        ops={},
    ):
        """
        Initialize the ExperimentData object.

        Args:
            base_path (str): The base path where the experiment data is located.
            ops (dict): A dictionary of functions to be applied to each experiment's full results.
        """

        self.base_path = base_path
        self.experiments = []
        self.ops = ops

        self._load_data()

    def _load_data(
        self,
    ):
        """
        Load the experiment data from the base path.
        """

        experiments_to_add = list(
            filter(None, map(self._process_experiment, os.listdir(self.base_path)))
        )

        for e in experiments_to_add:
            self.add_experiment(e)

    def add_experiment(self, exp):
        """
        Add an experiment to the list.

        Args:
            exp: The experiment object to be added.

        Returns:
            None
        """
        self.experiments.append(exp)

    def __getitem__(self, index):
        return self.experiments[index]

    def _process_experiment(
        self,
        experiment_name,
    ):
        """
        Process a single experiment.

        Args:
            experiment_name (str): The name of the experiment to process.

        Returns:
            experiment: The processed experiment object.
        """

        try:
            experiment = PExp.load(self.base_path, experiment_name, ops=self.ops)
            experiment.run_ops()
            return experiment
        except Exception as e:
            print(f"Missing data for : {experiment_name}: {e}")
            return

    def meta(
        self,
    ):
        """
        Get the metadata of all experiments.

        Returns:
            list: A list of metadata dictionaries for each experiment.
        """
        return [e.meta for e in self.experiments]

    def query(
        self,
        criteria,
    ):
        """
        Get the experiments data based on criteria.

        Args:
            criteria (dict): A dictionary of criteria to filter the experiments.

        Returns:
            GetExperimentOutput: An object containing the filtered experiments.
        """

        base_experiments = self.experiments

        for (
            k,
            v,
        ) in criteria.items():
            base_experiments = [
                x
                for x in base_experiments
                if x.check_property(
                    k,
                    v,
                )
            ]

        return SetupResults(experiments=base_experiments)

    def _map(self, func):
        """
        Apply a function to each experiment in the list.

        Args:
            func (callable): The function to be applied to each experiment.

        Returns:
            None
        """
        self.experiments = list(map(func, self.experiments))

    def run_evaluation(
        self,
        evaluator: Evalutor,
    ):
        """
        Run evaluation on the experiments.

        Args:
            evaluator (Evalutor): The evaluator object to perform the evaluation.
            key (str): The key to be used for evaluation.

        Returns:
            None
        """
        self.experiments = self._map(func=evaluator)

    def save(self):
        """
        Save the experiments.

        Returns:
            None
        """
        for e in self.experiments:
            e.save(self.base_path)


@dataclass
class SetupResults(ExpSetup):

    def __init__(self, experiments):
        self.experiments = experiments
        self.ops = []
        self.base_path = ""

    def __str__(
        self,
    ):
        return f"SetupResults(experiments={self.experiments})"

    def get_and_stack(self, key):
        return [exp.get(key) for exp in self.experiments]

    def print_get_table(self, *gets):
        metric_names = "".join([f"\t{m}" for m in gets])

        print(f"Experiment:\t\t\t{metric_names}")
        for (
            i,
            exp,
        ) in enumerate(self.experiments):

            metric_values = ""
            for m in gets:
                try:
                    value = exp.get(m)
                    metric_values += f"\t{value:.4f}"
                except:
                    metric_values += "\t-"

            print(f"ExperimentResults:--{exp.get_name()}{metric_values}")

    def get_support(self, axis_of_variation):
        """
        Get the support for metadata based on common criteria and axis of variation.

        Args:
            common (dict): A dictionary of common criteria.
            axis_of_variation (list): A list of metadata keys representing the axis of variation.

        Returns:
            dict: A dictionary containing the support for each axis of variation.
        """
        exps = self.experiments

        exp_on_axis = exps.map(
            lambda x: {k: x.meta.get(k, -1) for k in axis_of_variation}
        )

        support = {k: [] for k in axis_of_variation}

        for e in exp_on_axis:
            for k, v in e.items():
                support[k].append(v)

        for k in support.keys():
            v = list(set(support[k]))
            v.sort()
            support[k] = v

        return support

    def __len__(
        self,
    ):
        return len(self.experiments)

    def map(self, func):
        return SetupResults(
            experiments=list(
                map(
                    func,
                    copy.deepcopy(self.experiments),
                )
            )
        )

    def filter(self, func):
        return SetupResults(list(filter(func, self.experiments)))

    def unique(self):
        return SetupResults(
            list({json.dumps(e.meta): e for e in self.experiments}.values())
        )

    def sort(self, key: str):
        self.experiments.sort(key=lambda exp: exp.meta[key])
        return self
