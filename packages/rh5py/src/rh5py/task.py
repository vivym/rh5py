from typing import Optional, Union

from .schema import Task, TaskType

SLICE_TYPE = int | slice | list[int] | tuple[int, ...]


class TaskBuilder:
    def __init__(
        self,
        file_path: str | None = None,
        name: str = None,
        parent: Optional["TaskBuilder"] = None,
    ):
        if file_path is None:
            assert parent is not None, "Parent should be provided"

        if name is None:
            assert file_path is not None, "file_path should be provided"
            assert parent is None, "Parent should not be provided"

        self.file_path = file_path if file_path is not None else parent.file_path
        self.name = name
        self.parent = parent

        if parent is not None:
            if file_path is not None:
                assert file_path == parent.file_path, "File path should be the same"
            assert name is not None, "Name should be provided"
            self.value_path = parent.value_path + (name,)
        else:
            if name is not None:
                self.value_path = (name,)
            else:
                self.value_path = tuple()

    def keys(self) -> Task:
        return Task(type=TaskType.get_keys, file_path=self.file_path, value_path=self.value_path)

    def __getitem__(self, index: str | SLICE_TYPE | tuple[SLICE_TYPE, ...]) -> Union["TaskBuilder", "Task"]:
        if isinstance(index, str):
            return TaskBuilder(name=index, parent=self)
        else:
            return Task(
                type=TaskType.get_value,
                file_path=self.file_path,
                value_path=self.value_path + ("_|@|" + encode_index(index),),
            )

    def __len__(self) -> Task:
        return Task(type=TaskType.get_length, file_path=self.file_path, value_path=self.value_path)

    @property
    def shape(self) -> Task:
        return Task(type=TaskType.get_shape, file_path=self.file_path, value_path=self.value_path)

    @property
    def dtype(self) -> Task:
        return Task(type=TaskType.get_dtype, file_path=self.file_path, value_path=self.value_path)


def encode_index(index: SLICE_TYPE | tuple[SLICE_TYPE, ...], recursive: bool = True) -> str:
    if isinstance(index, int):
        return f"i{index}"
    elif isinstance(index, slice):
        return f"s{index.start},{index.stop},{index.step}"
    elif isinstance(index, (list, tuple)):
        return f"l{','.join(map(str, index))}"
    else:
        if recursive:
            return "|".join(encode_index(i, recursive=False) for i in index)
        else:
            raise ValueError(f"Invalid index: {index}")
