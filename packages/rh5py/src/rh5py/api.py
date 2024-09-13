import os

import blosc2
import numpy as np
import requests

from .schema import Task, TaskType


class HTTPApi:
    def __init__(self, base_url: str | None = None):
        if base_url is None:
            base_url = os.environ["RH5PY_SERVER_URL"]

        self.base_url = base_url

        self.http = requests.Session()

    def get_keys(self, file_path: str, value_path: list[str]) -> list[str]:
        resp = self.http.post(
            f"{self.base_url}/task",
            json={
                "type": "get_keys",
                "file_path": file_path,
                "value_path": value_path,
            },
        )
        resp.raise_for_status()

        data = resp.json()

        assert isinstance(data, list), "get_keys should return a list"
        assert all(isinstance(key, str) for key in data), "All keys should be string"

        return data

    def get_value(self, file_path: str, value_path: list[str]) -> np.ndarray:
        resp = self.http.post(
            f"{self.base_url}/task",
            json={
                "type": "get_value",
                "file_path": file_path,
                "value_path": value_path,
            },
        )
        resp.raise_for_status()

        data = resp.content

        assert isinstance(data, bytes), "get_value should return bytes"

        print(len(data))

        data = blosc2.unpack_array(data)

        return data

    def get_length(self, file_path: str, value_path: list[str]) -> int:
        resp = self.http.post(
            f"{self.base_url}/task",
            json={
                "type": "get_length",
                "file_path": file_path,
                "value_path": value_path,
            },
        )
        resp.raise_for_status()

        data = resp.json()

        assert isinstance(data, int), "get_length should return an integer"

        return data

    def get_shape(self, file_path: str, value_path: list[str]) -> tuple[int]:
        resp = self.http.post(
            f"{self.base_url}/task",
            json={
                "type": "get_shape",
                "file_path": file_path,
                "value_path": value_path,
            },
        )
        resp.raise_for_status()

        data = resp.json()

        assert isinstance(data, list), "get_shape should return a list"
        assert all(isinstance(dim, int) for dim in data), "All dimensions should be integer"

        return tuple(data)

    def get_dtype(self, file_path: str, value_path: list[str]) -> np.dtype:
        resp = self.http.post(
            f"{self.base_url}/task",
            json={
                "type": "get_dtype",
                "file_path": file_path,
                "value_path": value_path,
            },
        )
        resp.raise_for_status()

        data = resp.json()

        assert isinstance(data, str), "get_dtype should return a string"

        return np.dtype(data)

    def execute(self, task: Task):
        if task.type == TaskType.get_keys:
            return self.get_keys(task.file_path, task.value_path)
        elif task.type == TaskType.get_value:
            return self.get_value(task.file_path, task.value_path)
        elif task.type == TaskType.get_length:
            return self.get_length(task.file_path, task.value_path)
        elif task.type == TaskType.get_shape:
            return self.get_shape(task.file_path, task.value_path)
        elif task.type == TaskType.get_dtype:
            return self.get_dtype(task.file_path, task.value_path)
        else:
            raise ValueError(f"Invalid task type: {task.type}")
