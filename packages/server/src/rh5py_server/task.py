import blosc2
import numpy as np
from cachetools import cached, TTLCache
from h5py import File, Group, Dataset

from .schema import Task, TaskType, TaskResult, TaskResultStatus


@cached(cache=TTLCache(maxsize=1024, ttl=5 * 60))
def open_h5_with_cache(file_path: str, mode: str = "r") -> File:
    return File(file_path, mode=mode)


def int_or_none(value: str) -> int | None:
    return int(value) if value.lower() != "none" else None


def access_h5_by_value_path(fp: File, value_path: list[str]):
    cur = fp
    for key in value_path:
        if key.startswith("_|@|"):
            key = key[4:]
            indices = []
            for index_str in key.split("|"):
                index_type = index_str[0]
                if index_type == "i":
                    index = int(index_str[1:])
                elif index_type == "s":
                    index = slice(*map(int_or_none, index_str[1:].split(",")))
                elif index_type == "l":
                    index = list(map(int, index_str[1:].split(",")))
                else:
                    raise ValueError(f"Invalid index type: {index_type}")
                indices.append(index)

            if len(indices) == 1:
                key = indices[0]
            else:
                key = tuple(indices)

        cur = cur[key]

    return cur


def execute_task(task: Task) -> TaskResult:
    try:
        fp = open_h5_with_cache(task.file_path, "r")

        value = access_h5_by_value_path(fp, task.value_path)

        assert isinstance(value, (File, Group, Dataset, np.ndarray)), f"Invalid value type: {type(value)}"

        if task.type == TaskType.get_keys:
            assert isinstance(value, (Group, File)), "Only Group or File can call `get_keys`"
            is_binary = False
            result = list(value.keys())
        elif task.type == TaskType.get_value:
            assert isinstance(value, np.ndarray), "Only np.ndarray can call `get_value`"
            is_binary = True
            result = value[:]
        elif task.type == TaskType.get_length:
            assert isinstance(value, (File, Group, Dataset)), "Only File, Group or Dataset can call `get_length`"
            is_binary = False
            result = len(value)
        elif task.type == TaskType.get_shape:
            assert isinstance(value, Dataset), "Only Dataset can call `get_shape`"
            is_binary = False
            result = value.shape
        elif task.type == TaskType.get_dtype:
            assert isinstance(value, Dataset), "Only Dataset can call `get_dtype`"
            is_binary = False
            result = str(value.dtype)
        else:
            return TaskResult(status=TaskResultStatus.error, error=f"Invalid task type: {task.type}")
    except Exception as e:
        return TaskResult(status=TaskResultStatus.error, error=str(e))

    if is_binary:
        assert isinstance(result, np.ndarray), "Only numpy.ndarray can be binary"
        result = blosc2.pack_array(result)
        if isinstance(result, str):
            result = result.encode("utf-8")

    return TaskResult(status=TaskResultStatus.success, data=result)
