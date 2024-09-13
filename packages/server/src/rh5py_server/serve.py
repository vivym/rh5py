from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool

from .schema import Task
from .task import execute_task

app = FastAPI()


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.post("/task")
async def post_task(task: Task):
    result = await run_in_threadpool(execute_task, task)
    if result.success():
        data = result.data
        if isinstance(data, bytes):
            return Response(data, media_type="application/octet-stream")
        else:
            return JSONResponse(content=data)
    else:
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result.error)
