from dataclasses import dataclass
import pymongo
from typing import List


@dataclass
class ExpPath:
    uri: str


@dataclass
class FileSystemExpPath(ExpPath):
    exp_id: str
    base_dir: str

    def __init__(
        self, exp_id: str, base_dir: str
    ):
        uri = f"{base_dir}/{exp_id}"
        super().__init__(uri)


@dataclass
class MongoExpPath(ExpPath):
    exp_id: str
    mongo_server_uri: str

    def __init__(
        self,
        exp_id: str,
        mongo_server_uri: str,
    ):
        uri = f"{mongo_server_uri}/{exp_id}"
        super().__init__(uri)


class Storage:

    def __init__(self, mode: str):
        self._write_mode = "w" in mode
        self._read_mode = "r" in mode

    def is_write_mode(self):
        return self._write_mode

    def is_read_mode(self):
        return self._read_mode

    def write(self, exp_id: str):
        pass

    def read(
        self, exp_id: str, field: str
    ):  # field = {meta, evals, data}
        pass

    def keys(
        self, exp_id: str
    ):  # field = {meta, evals, data}
        pass


class PersistentStorage(Storage):

    def __init__(self, mode: str):
        super().__init__(mode)

    def load_to_mem(self, exp_id: str):
        pass


class MemoryStorage(Storage):
    def __init__(self, mode: str = "wb"):
        self.db = {}

    def read(self, exp_id: str, field: str):
        collection = self.db[exp_id]

        if self.is_read_mode():
            return collection[field]
        else:
            raise ValueError(
                "Read mode is not enabled."
            )

    def keys(self, exp_id: str):
        collection = self.db[exp_id]
        if self.is_read_mode():
            return list(collection.keys())
        else:
            raise ValueError(
                "Read mode is not enabled."
            )

    def read_field_keys(
        self, exp_id: str, field: str
    ):
        collection = self.db[exp_id]
        if self.is_read_mode():
            return list(
                collection[field].keys()
            )
        else:
            raise ValueError(
                "Read mode is not enabled."
            )

    def read_subfield(
        self,
        exp_id: str,
        field: str,
        key: str,
    ):  # cover meta, eval, and data  fields
        collection = self.db[exp_id]
        if self.is_read_mode():
            collection[field][field][key]
        else:
            raise ValueError(
                "Write mode is not enabled."
            )

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
            raise ValueError(
                "Write mode is not enabled."
            )

    def write_eval(
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
            raise ValueError(
                "Write mode is not enabled."
            )


class DiskStorage(PersistentStorage):
    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def save(self, p: FileSystemExpPath):
        pass

    def load_to_mem(
        self, p: FileSystemExpPath
    ):
        pass


class MongoStorage(PersistentStorage):
    # data structure
    # run_id : {meta: {key: value}, evals: {key: [ {key:value}] }, data: {input: { key:value}, outputs: [ { key:value}] }

    def __init__(
        self,
        uri: str,
        mode: str = "r",  # can be w, r, and rw
    ):
        super().__init__(mode)
        self.uri = uri
        self.client = pymongo.MongoClient(
            uri
        )
        self.db = (
            self.client.get_default_database()
        )
        self._write_mode = "w" in mode
        self._read_mode = "r" in mode

    def read(self, exp_id: str, field: str):
        collection = self.db[exp_id]
        if self._read_mode:
            return collection.find_one(
                {}, {field: 1}
            )
        else:
            raise ValueError(
                "Read mode is not enabled."
            )

    def keys(self, exp_id: str):
        collection = self.db[exp_id]
        if self._read_mode:
            return (
                collection.find_one().keys()
            )
        else:
            raise ValueError(
                "Read mode is not enabled."
            )

    def write(
        self,
        exp_id: str,
        field: str,
        data: dict,
    ):
        collection = self.db[exp_id]
        if self._write_mode:
            collection.update_one(
                {},
                {"$set": {field: data}},
                upsert=True,
            )
        else:
            raise ValueError(
                "Write mode is not enabled."
            )

    def write_eval(
        self,
        exp_id: str,
        eval_key: str,
        eval_data: List[dict],
    ):
        collection = self.db[exp_id]
        if self._write_mode:
            collection.update_one(
                {},
                {
                    "$push": {
                        f"evals.{eval_key}": {
                            "$each": eval_data
                        }
                    }
                },
                upsert=True,
            )
        else:
            raise ValueError(
                "Write mode is not enabled."
            )
