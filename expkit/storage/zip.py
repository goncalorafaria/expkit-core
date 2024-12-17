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


import os
import zipfile
import orjson
from typing import List, Any


class ZipStorage(Storage):
    def __init__(self, base_dir: str, mode: str = "r"):
        super().__init__(mode)
        self.base_dir = base_dir

    def _get_zip_path(self, exp_id: str) -> str:
        return f"{self.base_dir}/{exp_id}.zip"

    def create(self, exp_id: str, force: bool = False, exists_ok=False):
        if not self.is_write_mode():
            raise ValueError("Write mode is not enabled.")

        zip_path = self._get_zip_path(exp_id)

        if self.exists(exp_id):
            if exists_ok:
                return self.document(exp_id)
            elif force:
                self.delete(exp_id)
            else:
                raise ValueError(f"Document {exp_id} already exists.")

        # Create base directory if it doesn't exist
        os.makedirs(os.path.dirname(zip_path), exist_ok=True)

        # Create empty zip file
        with zipfile.ZipFile(
            zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
        ) as _:
            pass

        return self.document(exp_id)

    def delete(self, exp_id: str):
        if not self.is_write_mode():
            raise ValueError("Write mode is not enabled.")

        zip_path = self._get_zip_path(exp_id)
        if os.path.exists(zip_path):
            os.remove(zip_path)

    def exists(self, exp_id: str) -> bool:
        if not self.is_read_mode():
            raise ValueError("Read mode is not enabled.")

        return os.path.exists(self._get_zip_path(exp_id))

    def keys(self) -> List[str]:
        if not self.is_read_mode():
            raise ValueError("Read mode is not enabled.")

        return [
            f.replace(".zip", "")
            for f in os.listdir(self.base_dir)
            if f.endswith(".zip")
        ]

    def get(self, exp_id: str) -> dict:
        if not self.is_read_mode():
            raise ValueError("Read mode is not enabled.")

        data = {"_id": exp_id}
        with zipfile.ZipFile(self._get_zip_path(exp_id), "r") as zf:
            for filename in zf.namelist():
                field = filename.replace(".json", "")
                data[field] = self.read(exp_id, field)
        return data

    def read(self, exp_id: str, field: str):
        if not self.is_read_mode():
            raise ValueError("Read mode is not enabled.")

        with zipfile.ZipFile(self._get_zip_path(exp_id), "r") as zf:
            with zf.open(f"{field}.json") as f:
                return orjson.loads(f.read())

    def fields(self, exp_id: str) -> List[str]:
        if not self.is_read_mode():
            raise ValueError("Read mode is not enabled.")

        with zipfile.ZipFile(self._get_zip_path(exp_id), "r") as zf:
            return [f.replace(".json", "") for f in zf.namelist()]

    def read_field_keys(self, exp_id: str, field: str) -> List[str]:
        if not self.is_read_mode():
            raise ValueError("Read mode is not enabled.")

        data = self.read(exp_id, field)
        return list(data.keys())

    def read_subfield(self, exp_id: str, field: str, key: str):
        if not self.is_read_mode():
            raise ValueError("Read mode is not enabled.")

        data = self.read(exp_id, field)
        return data[key]

    def write(self, exp_id: str, field: str, data: dict):
        if not self.is_write_mode():
            raise ValueError("Write mode is not enabled.")

        with zipfile.ZipFile(
            self._get_zip_path(exp_id),
            "a",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
        ) as zf:
            zf.writestr(f"{field}.json", orjson.dumps(data))

    def write_subfield(self, exp_id: str, field: str, key: str, data: List[dict]):
        if not self.is_write_mode():
            raise ValueError("Write mode is not enabled.")

        # Read existing data
        try:
            existing_data = self.read(exp_id, field)
        except:
            existing_data = {}

        existing_data[key] = data

        # Write back the entire field
        self.write(exp_id, field, existing_data)

    def append_subfield(self, exp_id: str, field: str, data: Any):
        if not self.is_write_mode():
            raise ValueError("Write mode is not enabled.")

        if not self.exists(exp_id):
            raise ValueError(f"Collection {exp_id} does not exist.")

        try:
            existing_data = self.read(exp_id, field)
            if not isinstance(existing_data, list):
                existing_data = []
        except:
            existing_data = []

        existing_data.append(data)

        # Write back the entire field
        self.write(exp_id, field, existing_data)
