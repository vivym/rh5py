from .api import HTTPApi
from .schema import Task


def batch_execute(task: Task, http: HTTPApi | None = None):
    if http is None:
        http = HTTPApi()
