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

from expkit.storage.base import Storage


class MemoryStorage(Storage):
    def __init__(self, mode: str = "r"):
        super().__init__(mode)
        self.db = {}

    def create(
        self,
        exp_id: str,
        force: bool = False,
        exists_ok=False,
    ):
        if self.is_write_mode():

            if self.exists(exp_id):
                if exists_ok:
                    return self.document(exp_id)
                elif force:
                    self.delete(exp_id)
                else:
                    raise ValueError(f"Document {exp_id} already exists.")

            self.db[exp_id] = {
                "_id": exp_id,
                # "meta": {},
                # "evals": {},
                # "data": {},
            }

            return self.document(exp_id)
        else:
            raise ValueError("Write mode is not enabled.")

    def delete(self, exp_id: str):
        if self.is_write_mode():
            self.db.pop(exp_id)
        else:
            raise ValueError("Write mode is not enabled.")

    def get(self, exp_id: str):
        if self.is_read_mode():
            return deepcopy(self.db[exp_id])
        else:
            raise ValueError("Read mode is not enabled.")

    def keys(self):
        if self.is_read_mode():
            return list(self.db.keys()).copy()
        else:
            raise ValueError("Read mode is not enabled.")

    def exists(self, exp_id: str):
        if self.is_read_mode():
            return exp_id in self.db
        else:
            raise ValueError("Read mode is not enabled.")

    def read(self, exp_id: str, field: str):

        if self.is_read_mode():
            collection = self.db[exp_id]
            return collection[field]
        else:
            raise ValueError("Read mode is not enabled.")

    def fields(self, exp_id: str):
        collection = self.db[exp_id]
        if self.is_read_mode():
            return list(
                filter(
                    lambda x: x != "_id",
                    collection.keys(),
                )
            )
        else:
            raise ValueError("Read mode is not enabled.")

    def read_field_keys(self, exp_id: str, field: str):
        collection = self.db[exp_id]
        if self.is_read_mode():
            return list(collection[field].keys())
        else:
            raise ValueError("Read mode is not enabled.")

    def read_subfield(
        self,
        exp_id: str,
        field: str,
        key: str,
    ):  # cover meta, eval, and data  fields
        collection = self.db[exp_id]
        if self.is_read_mode():
            return collection[field][key]
        else:
            raise ValueError("Write mode is not enabled.")

    def write(
        self,
        exp_id: str,
        field: str,
        data: dict,
    ):
        collection = self.db[exp_id]
        if self.is_write_mode():
            collection[field] = data
        else:
            raise ValueError("Write mode is not enabled.")

    def write_subfield(
        self,
        exp_id: str,
        field: str,
        key: str,
        data: List[dict],
    ):
        collection = self.db[exp_id]
        if self.is_write_mode():
            collection[field][key] = data
        else:
            raise ValueError("Write mode is not enabled.")
