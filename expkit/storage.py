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


class DiskStorage(Storage):
    def __init__(self, base_dir: str, mode: str = "r"):
        super().__init__(mode)
        self.base_dir = base_dir

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


class CachedRODiskStorage(DiskStorage):
    def __init__(self, base_dir: str):
        super().__init__(base_dir, "r")
        self.cache = MemoryStorage("rw")

    def clear(self):
        self.cache = MemoryStorage("rw")

    def read(self, exp_id: str, field: str):

        if field == "meta":
            return super().read(exp_id, field)
        else:
            try:
                return self.cache.read(exp_id, field)
            except Exception as e:
                print("GET:", e, field)
                data = super().read(exp_id, field)

                self.cache.create(exp_id, exists_ok=True)

                self.cache.write(exp_id, field, data)
                return data

    def read_subfield(
        self,
        exp_id: str,
        field: str,
        key: str,
    ):
        try:
            return self.cache.read_subfield(exp_id, field, key)
        except:
            data = super().read_subfield(exp_id, field, key)

            self.cache.create(exp_id, exists_ok=True)

            self.cache.write_subfield(exp_id, field, key, data)
            return data


def decode_mongo_format(data):

    if isinstance(data, dict) and len(data) > 0:

        if LIST_SYM in list(data.keys())[0]:
            #
            new_data = map(
                lambda x: (
                    int(x[0].replace(LIST_SYM, "")),
                    x[1],
                ),
                data.items(),
            )

            new_data = list(
                map(
                    lambda x: decode_mongo_format(x[1]),
                    sorted(
                        new_data,
                        key=lambda x: x[0],
                    ),
                )
            )

            return new_data
        else:
            for k, v in data.items():
                data[k] = decode_mongo_format(v)

    return data


def chunked_iterable(iterable, size):
    """Helper function to split iterable into chunks of given size."""
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


class MongoStorage(Storage):
    # data structure
    # run_id : {meta: {key: value}, evals: {key: [ {key:value}] }, data: {input: { key:value}, outputs: [ { key:value}] }

    def __init__(
        self,
        uri: str,
        mode: str = "r",  # can be w, r, and rw
        database_name: str = "test3",
    ):
        super().__init__(mode)
        self.uri = uri
        self.client = pymongo.MongoClient(uri)
        self.db = self.client.get_database(database_name)

        self.async_client = motor.motor_asyncio.AsyncIOMotorClient(uri)

        self.async_db = self.async_client.get_database(database_name)

    def get(self, exp_id: str):
        if self.is_read_mode():
            return decode_mongo_format(list(self.db[exp_id].find())[0])

    def delete(self, exp_id: str):
        if self.is_write_mode():
            self.db.drop_collection(exp_id)
        else:
            raise ValueError("Write mode is not enabled.")

    def keys(self):
        if self.is_read_mode():
            return self.db.list_collection_names()
        else:
            raise ValueError("Read mode is not enabled.")

    def exists(self, exp_id: str):
        if self.is_read_mode():
            return exp_id in self.db.list_collection_names()
        else:

            raise ValueError("Read mode is not enabled.")

    def create(
        self,
        exp_id: str,
        force: bool = False,
        exists_ok=False,
    ):
        collection = self.db[exp_id]

        if self.is_write_mode():

            if self.exists(exp_id):
                if exists_ok:
                    return self.document(exp_id)
                elif force:
                    self.delete(exp_id)
                else:
                    raise ValueError(f"Document {exp_id} already exists.")

            collection.insert_one(
                {
                    "_id": exp_id,
                    # "meta": {},
                    # "evals": {},
                    # "data": {},
                }
            )

            return self.document(exp_id)
        else:
            raise ValueError("Write mode is not enabled.")

    def read(self, exp_id: str, field: str):
        collection = self.db[exp_id]
        if self.is_read_mode():
            return decode_mongo_format(collection.find_one({}, {field: 1})[field])
        else:
            raise ValueError("Read mode is not enabled.")

    def read_subfield(
        self,
        exp_id: str,
        field: str,
        key: str,
    ):
        collection = self.db[exp_id]

        if self.is_read_mode():
            return decode_mongo_format(
                collection.find_one(
                    {},
                    {f"{field}.{key}": 1},
                )[
                    field
                ][key]
            )
        else:
            raise ValueError("Read mode is not enabled.")

    def fields(self, exp_id: str):
        collection = self.db[exp_id]
        if self.is_read_mode():
            return list(
                filter(
                    lambda x: x != "_id",
                    collection.find_one().keys(),
                )
            )
        else:
            raise ValueError("Read mode is not enabled.")

    def read_field_keys(self, exp_id: str, field: str):
        collection = self.db[exp_id]
        if self.is_read_mode():
            return list(
                filter(
                    lambda x: x != "_id",
                    collection.find_one()[field].keys(),
                )
            )
        else:
            raise ValueError("Read mode is not enabled.")

    def write(
        self,
        exp_id: str,
        field: str,
        data: Any,
        batch_size: int = 1,
    ):
        collection = self.db[exp_id]

        if self.is_write_mode():

            if isinstance(data, List):

                async def write_async():

                    for batch in tqdm(
                        list(
                            chunked_iterable(
                                data,
                                batch_size,
                            )
                        )
                    ):

                        tasks = [
                            self.write_subfield_async(
                                exp_id,
                                field,
                                f"{LIST_SYM}{i}",
                                d,
                            )
                            for i, d in enumerate(batch)
                        ]
                        await asyncio.gather(*tasks)

                asyncio.run(write_async())

                """for i, d in enumerate(data):
                    self.write_subfield(
                        exp_id,
                        field,
                        f"{LIST_SYM}{i}",
                        d,
                )"""
            else:
                collection.update_one(
                    {},
                    {"$set": {field: data}},
                    upsert=True,
                )
        else:
            raise ValueError("Write mode is not enabled.")

    def write_subfield(
        self,
        exp_id: str,
        field: str,
        key: str,
        data: Any,
    ):
        asyncio.run(self.write_subfield_async(exp_id, field, key, data))

    async def write_subfield_async(
        self,
        exp_id: str,
        field: str,
        key: str,
        data: Any,
    ):

        if self.is_write_mode():
            if not self.exists(exp_id):

                raise ValueError(f"Collection {exp_id} does not exist.")

            if isinstance(data, dict):

                tasks = [
                    self.write_subfield_async(
                        exp_id,
                        f"{field}.{key}",
                        k,
                        v,
                    )
                    for k, v in data.items()
                ]
                await asyncio.gather(*tasks)

            elif isinstance(data, List):

                tasks = [
                    self.write_subfield_async(
                        exp_id,
                        f"{field}.{key}",
                        f"{LIST_SYM}{i}",
                        d,
                    )
                    for i, d in enumerate(data)
                ]
                await asyncio.gather(*tasks)

            else:

                collection = self.async_db.get_collection(
                    exp_id,
                    write_concern=WriteConcern(w=0),
                )

                await collection.update_one(
                    {"_id": exp_id},
                    {"$set": {f"{field}.{key}": data}},
                    # upsert=True,
                )
        else:
            raise ValueError("Write mode is not enabled.")

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
                list_indexes = self.read_field_keys(exp_id, field)
            except KeyError:
                list_indexes = []

            if len(list_indexes) == 0:
                i = 0

            else:

                if not (LIST_SYM in list_indexes[0]):
                    raise ValueError(f"Field:{field} is not a list.")

                max_ind = max(
                    map(
                        lambda x: int(x.replace(LIST_SYM, "")),
                        list_indexes,
                    )
                )

                i = max_ind + 1

            self.write_subfield(
                exp_id,
                field,
                f"{LIST_SYM}{i}",
                data,
            )

        else:
            raise ValueError("Write mode is not enabled.")


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
