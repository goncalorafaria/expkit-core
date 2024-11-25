import copy
import json
import logging
import os
from dataclasses import dataclass
from typing import *
import uuid

from dataclasses import dataclass
from typing import *
import fcntl
import types

from expkit.storage import (
    Storage,
    StorageDocument,
    DiskStorage,
    MemoryStorage,
    MongoStorage,
)


def create_lock(
    document,
):
    document.write("lock", "closed")


def release_lock(
    document,
):
    document.write("lock", "open")


def has_lock(
    document,
):
    if "lock" in document.keys():
        return document.read("lock") == "closed"
    else:
        return False


class Exp:
    def __init__(
        self,
        name: str = None,
        meta: Dict[str, str] = None,
        storage: Storage = None,
        **env_context_variables,
    ):
        """
        Initialize an Exp object.

        Args:
            name: The name of the experiment.
            meta: A dictionary containing metadata about the experiment.
        """

        self.name = str(uuid.uuid4()) if name is None else name

        if storage is None:
            storage = MemoryStorage(mode="rw")

        if not storage.exists(self.name):
            document_storage = storage.create(self.name)

            env_context_variables = {
                k: v
                for k, v in env_context_variables.items()
                if not k.startswith("__")
                and isinstance(
                    v,
                    (
                        Dict,
                        List,
                        Tuple,
                        str,
                        int,
                        float,
                        bool,
                    ),
                )
            }

            if meta is None:
                meta = env_context_variables
            else:
                meta = {
                    **meta,
                    **{k: v for k, v in env_context_variables.items() if k not in meta},
                }

            document_storage.write("meta", meta)
        else:

            document_storage = storage.document(self.name)

            if meta is not None:
                assert (
                    document_storage.read("meta") == meta
                ), f"Meta data mismatch {document_storage.read('meta')} != {meta}"
            else:
                meta = document_storage.read("meta")

        self.meta = {} if meta is None else meta

        self.document_storage = document_storage

    def instances(self, lazy_iterable=False):

        try:
            if lazy_iterable:
                return self.document_storage.iterable("data")
            else:
                return self.document_storage.read("data")
        except FileNotFoundError:
            return []

    def evals(self):

        return {
            key: self.get_eval(value) for key, value in self.load_eval_meta().items()
        }

    def check_property(self, k, v):
        """
        Check if a specific property matches a given value.

        Args:
            k: The property to check.
            v: The value to compare against.

        Returns:
            True if the property matches the value, False otherwise.
        """

        if k == "name":
            return self.name == v
        else:
            return (k in self.meta) and (self.meta[k] == v)

    def __deepcopy__(self, memo):
        e = Exp(
            name=self.name,
            meta=copy.deepcopy(self.meta, memo),
            storage=copy.deepcopy(self.document_storage.storage()),
            # lazy=self.lazy,
        )

        return e

    def islocked(self):

        return has_lock(self.document_storage)

    def call_locked(self, func):

        create_lock(self.document_storage)

        result = func(self)

        release_lock(self.document_storage)

        return result

    def get_name(
        self,
    ):
        """
        Get the name of the experiment.

        Returns:
            The name of the experiment.
        """
        return self.name

    def get_eval(self, eval_key):

        path = self.load_eval_meta()[eval_key]
        return self.document_storage.read(
            path,
        )

    def __str__(
        self,
    ):
        """
        Return a string representation of the Exp object.
        """
        return f"Experiment[{self.name}](instance=...,evals={list(self.load_eval_meta())}, meta={self.meta})"

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

        if key == "name":
            return self.name

        elif key in self.load_eval_meta():
            return self.get_eval(key)

        elif key in self.meta:
            return self.meta[key]
        else:
            raise ValueError(f"key : {key} not found")

    def add_eval(
        self,
        key: str,
        data: List[Dict[str, Any]],
    ):
        """
        Add evaluation data to the experiment.

        Args:
            key: The key to associate with the evaluation data.
            data: A list of dictionaries representing the evaluation data.
        """

        self.document_storage.write("eval_" + key, data)

        # self.evals[key] = [
        #    InstanceEval(**d) for d in data
        # ]

    def add_instance(
        self,
        input_data=Dict[str, Any],
        output=List[Dict[str, Any]],
    ):
        """
        Add an instance to the experiment.

        Args:
            input_data: A dictionary representing the input data for the instance.
            output: A list of dictionaries representing the output data for the instance.
        """

        self.document_storage.append_subfield(
            "data",
            {
                "input": input_data,
                "outputs": output,
            },
        )

    def add_instances(
        self,
        inputs: List[Dict[str, Any]],
        outputs: List[List[Dict[str, Any]]],
    ):
        """
        Add multiple instances to the experiment.

        Args:
            inputs: A list of dictionaries representing the input data for each instance.
            outputs: A list of lists of dictionaries representing the output data for each instance.
        """

        for input_data, output in zip(inputs, outputs):
            self.add_instance(input_data, output)

    def save(self, storage: Storage, **kwargs):
        """
        Save the experiment to disk.

        Args:
            save_path: The path to save the experiment to.
        """

        self.document_storage.to(storage, **kwargs)

    def has_eval(self, key):
        return key in self.load_eval_meta()

    def has_data(self):
        return "data" in self.document_storage.fields()

    def load_eval_meta(self):
        run_files = self.document_storage.keys()

        eval_files = {rf.split("_")[1]: rf for rf in run_files if "eval_" in rf}

        return eval_files

    @staticmethod
    def load(
        storage: Storage,
        name,
        **kwargs,
    ):
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

        exp = Exp(
            name=name,
            meta=None,
            storage=storage,
            **kwargs,
        )

        return exp


if __name__ == "__main__":

    storage = DiskStorage(
        base_dir="quest-rlhf/gemma-outputs/",
        mode="r",
    )
    new_storage = MongoStorage(
        "mongodb://localhost:27017/",
        mode="rw",
    )

    data_exemp = new_storage.document("83c9f882-8922-4cac-a334-b0efcb735fe4")
    cool_outer_local_variable = 4
    exp1 = Exp.load(
        storage=storage,
        name="83c9f882-8922-4cac-a334-b0efcb735fe4",
    )

    new_exp = Exp()
    import pdb

    pdb.set_trace()
    # new_exp.save(new_storage, force=True)

    new_exp.add_instance(
        input_data="test",
        output=["test"],
    )

    import pdb

    pdb.set_trace()
    new_exp = Exp.load(
        storage=new_storage,
        name="testg151",
    )
    import pdb

    pdb.set_trace()

    new_exp.add_instance(
        input_data="test",
        output=["test"],
    )

    # storage.to(new_storage, force=True)

    d = storage.document("1a698368-1ff1-4f57-8929-7cf3b1973307")

    exp = Exp.load(
        storage=new_storage,
        name="83c9f882-8922-4cac-a334-b0efcb735fe4",
        lazy=False,
    )

    exp.add_instance(
        input_data="test",
        output=["test"],
    )
    import pdb

    pdb.set_trace()
