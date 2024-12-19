import os
import shutil

import orjson

import ijson

from expkit.storage.base import Storage
from expkit.storage.cache import CachedRO
from typing import Any, List


class DiskStorage(Storage):
    def __init__(self, base_dir: str, mode: str = "r"):
        super().__init__(mode)
        self.base_dir = base_dir

        if not self.valid_storage():
            raise ValueError(
                "Invalid storage. This path already has dir files. It has a storage of other type."
            )

    def valid_storage(self) -> List[str]:
        if not self.is_read_mode():
            raise ValueError("Read mode is not enabled.")

        return not (len([1 for f in os.listdir(self.base_dir) if "." in f]) > 0)

    def create(
        self,
        exp_id: str,
        force: bool = False,
        exists_ok=False,
    ):
        if self.is_write_mode():
            dir_path = f"{self.base_dir}/{exp_id}"

            if self.exists(exp_id):
                if exists_ok:
                    return self.document(exp_id)
                elif force:
                    self.delete(exp_id)
                else:
                    raise ValueError(f"Document {exp_id} already exists.")

            os.makedirs(dir_path, exist_ok=True)

            return self.document(exp_id)

        else:
            raise ValueError("Write mode is not enabled.")

    def delete(self, exp_id: str):
        if self.is_write_mode():
            dir_path = f"{self.base_dir}/{exp_id}"
            shutil.rmtree(dir_path)
        else:
            raise ValueError("Write mode is not enabled.")

    def exists(self, exp_id: str):
        if self.is_read_mode():
            dir_path = f"{self.base_dir}/{exp_id}"
            return os.path.exists(dir_path)
        else:
            raise ValueError("Read mode is not enabled.")

    def keys(self):
        if self.is_read_mode():
            return os.listdir(self.base_dir)
        else:
            raise ValueError("Read mode is not enabled.")

    def get(self, exp_id: str):
        if self.is_read_mode():

            return {
                "_id": exp_id,
                **{
                    subdir.split(".")[0]: self.read(
                        exp_id,
                        subdir.split(".")[0],
                    )
                    for subdir in os.listdir(f"{self.base_dir}/{exp_id}")
                },
            }

        else:
            raise ValueError("Read mode is not enabled.")

    def read(self, exp_id: str, field: str):  # field = {meta, evals, data}
        if self.is_read_mode():
            file_path = f"{self.base_dir}/{exp_id}/{field}.json"

            with open(file_path, "rb") as file:
                return orjson.loads(file.read())
        else:
            raise ValueError("Read mode is not enabled.")

    def fields(self, exp_id: str):  # field = {meta, evals, data}
        if self.is_read_mode():
            dir_path = f"{self.base_dir}/{exp_id}"
            files = os.listdir(dir_path)
            return [file.split(".")[0] for file in files]
        else:
            raise ValueError("Read mode is not enabled.")

    def read_field_keys(self, exp_id: str, field: str):
        if self.is_read_mode():

            data = self.read(exp_id, field)

            return list(data.keys())
        else:
            raise ValueError("Read mode is not enabled.")

    def read_subfield(
        self,
        exp_id: str,
        field: str,
        key: str,
    ):
        if self.is_read_mode():

            data = self.read(exp_id, field)

            return data[key]
        else:
            raise ValueError("Read mode is not enabled.")

    def write(
        self,
        exp_id: str,
        field: str,
        data: dict,
    ):
        if self.is_write_mode():
            file_path = f"{self.base_dir}/{exp_id}/{field}.json"

            with open(file_path, "wb") as f:
                f.write(orjson.dumps(data))

        else:
            raise ValueError("Write mode is not enabled.")

    def write_subfield(
        self,
        exp_id: str,
        field: str,
        key: str,
        data: List[dict],
    ):
        if self.is_write_mode():

            existing_data = self.read(exp_id, field)

            file_path = f"{self.base_dir}/{exp_id}/{field}.json"

            existing_data[key] = data

            with open(file_path, "wb") as f:
                f.write(orjson.dumps(existing_data))

        else:
            raise ValueError("Write mode is not enabled.")

    def iterable(self, exp_id: str, field: str):
        if self.is_read_mode():
            file_path = f"{self.base_dir}/{exp_id}/{field}.json"
            with open(file_path, "rb") as file:
                # ijson.items() returns an iterator over the items in the array
                for item in ijson.items(file, "item"):
                    yield item

        else:
            raise ValueError("Read mode is not enabled.")

    def append_subfield(
        self,
        exp_id: str,
        field: str,
        data: Any,
    ):

        if self.is_write_mode():

            if not self.exists(exp_id):

                raise ValueError(f"Collection {exp_id} does not exist.")

            file_path = f"{self.base_dir}/{exp_id}/{field}.json"

            if not os.path.exists(file_path):
                # If file doesn't exist, create it with an empty list and add the first element
                with open(file_path, "wb") as file:
                    file.write(b"[" + orjson.dumps(data) + b"]")
            else:
                # If file exists, append to the list while keeping the JSON valid
                with open(file_path, "r+b") as file:
                    file.seek(0, os.SEEK_END)  # Move to the end of the file
                    file_size = file.tell()

                    # We need to move the file pointer back to before the closing bracket
                    file.seek(file_size - 1)

                    # Insert a comma if it's not the first item being added
                    if (
                        file_size > 2
                    ):  # File size is greater than 2 means it's not an empty list (just [])
                        file.write(b",")

                    # Write the new instance and close the list with ']'
                    file.write(orjson.dumps(data) + b"]")

        else:
            raise ValueError("Write mode is not enabled.")


class CachedRODiskStorage(CachedRO):
    def __init__(self, base_dir: str):
        storage = DiskStorage(
            base_dir,
            mode="r",
        )
        super().__init__(storage)
