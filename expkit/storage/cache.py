import asyncio
from types import MappingProxyType
import itertools
import ijson

from expkit.storage.base import Storage
from expkit.storage.memory import MemoryStorage


class CachedRO(Storage):

    def __init__(self, storage: Storage):

        super().__init__("r")

        self.source_storage = storage
        assert storage.is_read_mode(), "Storage must be in read mode."

        self.cache = MemoryStorage("rw")

    def clear(self):
        self.cache = MemoryStorage("rw")

    def keys(self):
        return self.source_storage.keys()

    def fields(self, exp_id):
        return self.source_storage.fields(exp_id)

    def get(self, exp_id: str):
        g = self.source_storage.get(exp_id)

        return g

    def read(self, exp_id: str, field: str):

        if field == "meta":
            return self.source_storage.read(exp_id, field)
        else:
            try:
                return self.cache.read(exp_id, field)
            except Exception as e:
                print("GET:", e, field)
                data = self.source_storage.read(exp_id, field)

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
            data = self.source_storage.read_subfield(exp_id, field, key)

            self.cache.create(exp_id, exists_ok=True)

            self.cache.write_subfield(exp_id, field, key, data)
            return data

    def exists(self, exp_id: str) -> bool:
        return self.source_storage.exists(exp_id)
