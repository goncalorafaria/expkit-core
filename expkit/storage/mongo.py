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
