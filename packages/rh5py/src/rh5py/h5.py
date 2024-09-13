from typing import Union

import numpy as np

from .api import HTTPApi
from .task import encode_index, SLICE_TYPE


class File:
    def __init__(self, file_path: str, mode: str = "r", api: HTTPApi | None = None):
        assert mode == "r", "Only read mode is supported"

        self.file_path = file_path
        self.mode = mode
        self.api = api or HTTPApi()

        self.value_path = tuple()

    def keys(self):
        return self.api.get_keys(self.file_path, self.value_path)

    def __len__(self):
        return self.api.get_length(self.file_path, self.value_path)

    def __getitem__(self, index: str) -> "GroupOrDataset":
        assert isinstance(index, str), "Only string index is supported"
        return GroupOrDataset(parent=self, name=index)


class GroupOrDataset:
    def __init__(self, parent: Union[File, "GroupOrDataset"], name: str, api: HTTPApi | None = None):
        self.parent = parent
        self.name = name

        self.file_path = parent.file_path
        self.value_path = parent.value_path + (name,)

        api = api or parent.api
        if api is None:
            api = HTTPApi()
        self.api = api

    def keys(self):
        return self.api.get_keys(self.file_path, self.value_path)

    def __len__(self):
        return self.api.get_length(self.file_path, self.value_path)

    def __getitem__(
        self, index: str | SLICE_TYPE | tuple[SLICE_TYPE, ...]
    ) -> Union["GroupOrDataset", np.ndarray]:
        if isinstance(index, str):
            return GroupOrDataset(parent=self, name=index)
        else:
            return self.api.get_value(
                self.file_path, self.value_path + ("_|@|" + encode_index(index),)
            )

    @property
    def shape(self):
        return self.api.get_shape(self.file_path, self.value_path)

    @property
    def dtype(self):
        return self.api.get_dtype(self.file_path, self.value_path)
