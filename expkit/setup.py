import json
from expkit.exp import Exp
from expkit.pexp import PExp
from expkit.eval import Evalutor
from expkit.storage import Storage
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
        storage,
        ops={},
    ):
        """
        Initialize the ExperimentData object.

        Args:
            base_path (str): The base path where the experiment data is located.
            ops (dict): A dictionary of functions to be applied to each experiment's full results.
        """

        self.storage = storage
        self.experiments = []
        self.ops = ops

        self._load_data()

    def _load_data(
        self,
    ):
        """
        Load the experiment data from the base path.
        """

        self.experiments = list(
            filter(
                None,
                map(
                    self._process_experiment,
                    self.storage.keys(),
                ),
            )
        )

    def add_experiment(self, exp):
        """
        Add an experiment to the list.

        Args:
            exp: The experiment object to be added.

        Returns:
            None
        """
        self.experiments.append(exp)

    def __len__(self):
        return len(self.experiments)

    def __getitem__(self, index):

        if isinstance(index, int):
            return self.experiments[index]

        else:
            matches = self.query({"name": index})
            if len(matches) > 0:
                return matches[0]

            else:
                print(index)
                raise KeyError(f"No experiment with name {index}")

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
            experiment = PExp.load(
                storage=self.storage,
                name=experiment_name,
                ops=self.ops,
            )
            # experiment.run_ops()

            return experiment
        except Exception as e:
            print(f"Missing data for : {self.base_path}- {experiment_name}: {e}")
            return None

    def run_ops(self):

        def safe_callable(x):

            try:
                return x.run_ops()
            except Exception as e:
                print("ops-error:", e)
                return None

        self._map(safe_callable)
        return self

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

        new_setup = copy.deepcopy(self)

        new_setup.experiments = base_experiments

        return new_setup

    def _map(self, func):
        """
        Apply a function to each experiment in the list.

        Args:
            func (callable): The function to be applied to each experiment.

        Returns:
            None
        """
        self.experiments = list(
            filter(
                None,
                (
                    map(
                        func,
                        self.experiments,
                    )
                ),
            )
        )

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

    def save(self, new_storage: Storage):
        """
        Save the experiments.

        Returns:
            None
        """
        self.storage.to(new_storage)

    def keys(self):
        return [e.get_name() for e in self.experiments]

    def __str__(
        self,
    ):
        return f"ExpSetup(experiments={self.experiments})"

    def __repr__(self) -> str:
        return self.__str__()

    def get_all(self, key):
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

        exp_on_axis = map(
            lambda x: {k: x.meta.get(k, -1) for k in axis_of_variation}, exps
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

    def safe_map(self, func):

        def safe_apply_func(experiment):
            try:
                return (
                    experiment.call_locked(func)
                    if not experiment.islocked()
                    else experiment
                )
            except Exception as e:
                print(f"Error in applying function: {e} - {experiment.get_name()}")
                return experiment

        self._map(safe_apply_func)

        return self

    def map(self, func):

        new_setup = copy.deepcopy(self)
        new_setup._map(func)

        return new_setup

    def filter(self, func):
        new_setup = copy.deepcopy(self)

        new_setup.experiments = list(filter(func, self.experiments))

        return new_setup

    def unique(self, key=None):
        new_setup = copy.deepcopy(self)

        if key is None:

            def key_factory(exp):
                return json.dumps(exp.meta)

        else:

            def key_factory(exp):
                return exp.get(key)

        new_setup.experiments = list(
            {key_factory(e): e for e in self.experiments}.values()
        )

        return new_setup

    def sort(self, key: str):
        new_setup = copy.deepcopy(self)
        new_setup.experiments.sort(key=lambda exp: exp.get(key))
        return new_setup
