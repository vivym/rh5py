from enum import Enum
from typing import Any

from pydantic import BaseModel


class TaskType(str, Enum):
    get_keys = "get_keys"
    get_value = "get_value"
    get_length = "get_length"
    get_shape = "get_shape"
    get_dtype = "get_dtype"


class Task(BaseModel):
    type: TaskType
    file_path: str
    value_path: list[str]
