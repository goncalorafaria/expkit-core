from dataclasses import dataclass
import pymongo
from typing import List, Any
from copy import deepcopy
import json
import os
import shutil
from tqdm import tqdm
from functools import partial
from concurrent.futures import (
    ThreadPoolExecutor,
)
import orjson
import motor.motor_asyncio
from pymongo import WriteConcern
import asyncio
from types import MappingProxyType
import itertools
import ijson

LIST_SYM = ">>"


class Storage:

    def __init__(self, mode: str):
        self.write_mode = "w" in mode
        self.read_mode = "r" in mode

    def is_write_mode(self):
        return self.write_mode

    def is_read_mode(self):
        return self.read_mode

    def create(
        self,
        exp_id: str,
        force: bool = False,
    ):
        pass

    def delete(self, exp_id: str):
        pass

    def exists(self, exp_id: str):
        pass

    def read(self, exp_id: str, field: str):  # field = {meta, evals, data}
        pass

    def iterable(self, exp_id: str, field: str):
        return self.read(exp_id, field)

    def keys(
        self,
    ):  # field = {meta, evals, data}
        pass

    def fields(self, exp_id: str):  # field = {meta, evals, data}
        pass

    def read_field_keys(self, exp_id: str, field: str):
        pass

    def read_subfield(
        self,
        exp_id: str,
        field: str,
        key: str,
    ):
        pass

    def write(
        self,
        exp_id: str,
        field: str,
        data: dict,
    ):
        pass

    def write_subfield(
        self,
        exp_id: str,
        field: str,
        key: str,
        data: List[dict],
    ):
        pass

    def document(self, exp_id: str):
        return StorageDocument(exp_id, self)

    def to(self, storage, **kwargs):

        for exp_id in tqdm(self.keys()):
            self.document(exp_id).to(storage, **kwargs)

    def append_subfield(
        self,
        exp_id: str,
        field: str,
        data: Any,
    ):

        if self.is_write_mode():

            if not self.exists(exp_id):

                raise ValueError(f"Collection {exp_id} does not exist.")

            try:
                list_data = self.read(exp_id, field)
            except (
                KeyError,
                FileNotFoundError,
            ):  # not initialized.
                list_data = []

            if not isinstance(list_data, list):
                raise ValueError(f"Field:{field} is not a list.")

            else:
                list_data.append(data)

            self.write(
                exp_id,
                field,
                list_data,
            )

        else:
            raise ValueError("Write mode is not enabled.")

    def __str__(self) -> str:
        return f"Storage(documents={self.keys()})"

    def __repr__(self) -> str:
        return self.__str__()


class StorageDocument:
    def __init__(self, exp_id: str, storage: Storage):
        self._storage = storage
        self._exp_id = exp_id

    def __setitem__(self, field: str, data: Any):

        if "." not in field:
            return self.write(field, data)
        else:  # subfield
            fk = field.split(".")[0]
            key = ".".join(field.split(".")[1:])

            return self.write_subfield(fk, key, data)

    def __getitem__(self, field: str) -> Any:

        if "." not in field:
            return self.read(field)
        else:  # subfield
            fk = field.split(".")[0]
            key = ".".join(field.split(".")[1:])

            return self.read_subfield(fk, key)

    def __getattr__(self, method_name: str) -> Any:

        if method_name in [
            "to",
            "id",
            "storage",
            "__str__",
            "__repr__",
        ]:
            return super().__getattribute__(method_name)
        else:
            storage = self.storage()
            exp_id = self.id()

            method_name = "fields" if method_name == "keys" else method_name

            return partial(
                getattr(storage, method_name),
                exp_id,
            )

    def to(self, storage: Storage, **kwargs):
        exp_id = self.id()

        document = storage.create(exp_id=exp_id, **kwargs)

        for k in self.keys():
            data = self.read(k)
            print(f"~read {len(data)} elements from {k}")
            document.write(k, data)

        return document

    def id(self):
        exp_id = super().__getattribute__("_exp_id")

        return exp_id

    def storage(self):
        return super().__getattribute__("_storage")

    def __str__(self) -> str:
        return f"StorageDocument(exp_id={self.id()},data={self.keys()}, storage={self.storage()})"

    def __repr__(self) -> str:
        return self.__str__()


def test_generic_storage(STORAGE: Storage):
    outputs = []
    STORAGE.create("exp1")
    STORAGE.write("exp1", "meta", {"name": "test1"})
    outputs.append(STORAGE.read("exp1", "meta"))

    outputs.append(STORAGE.fields("exp1"))

    STORAGE.write_subfield(
        "exp1",
        "meta",
        "time",
        "now",
    )

    outputs.append(STORAGE.read("exp1", "meta"))

    outputs.append(STORAGE.read_field_keys("exp1", "meta"))

    outputs.append(STORAGE.fields("exp1"))

    doc = STORAGE.document("exp1")

    outputs.append((doc.read("meta")))
    outputs.append(doc["meta"])
    outputs.append(doc["meta.name"])

    STORAGE.write(
        "exp1",
        "evals_xpto",
        [{"scores": [32, 41, 62, 73]}],
    )

    outputs.append(STORAGE.read_subfield("exp1", "meta", "time"))

    outputs.append(STORAGE.get("exp1"))
    outputs.append(STORAGE.keys())
    print((STORAGE, outputs))
    return outputs


def make_list_invariant(
    outputs: List[Any],
) -> List[Any]:
    converted_outputs = []
    for output in outputs:
        if isinstance(output, list):
            converted_outputs.append(sorted(output))
        else:
            converted_outputs.append(output)
    return converted_outputs


def invariant_assert(output1: List[Any], output2: List[Any]):

    assert make_list_invariant(output1) == make_list_invariant(output2), "Test failed!"


if __name__ == "__main__":

    invariant_assert(
        test_generic_storage(MemoryStorage(mode="rw")),
        test_generic_storage(
            DiskStorage(
                base_dir="test124/",
                mode="rw",
            )
        ),
    )

    invariant_assert(
        test_generic_storage(MemoryStorage(mode="rw")),
        test_generic_storage(
            MongoStorage(
                "mongodb://localhost:27017/",
                mode="rw",
            )
        ),
    )

    print("ALL DBS PASSED")
    print("----" * 20)

    storage = DiskStorage(
        base_dir="quest-rlhf/gemma-outputs/",
        mode="r",
    )

    d = storage.document("1a698368-1ff1-4f57-8929-7cf3b1973307")

    x = d["meta"]

    import pdb

    pdb.set_trace()
