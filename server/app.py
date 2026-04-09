"""
server/app.py - FastAPI server for the SQL Repair Environment.
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "server"))

from fastapi.responses import JSONResponse, HTMLResponse
from openenv.core.env_server import create_fastapi_app
from models import SQLAction, SQLObservation
from server.environment import SQLRepairEnvironment, TASKS

app = create_fastapi_app(SQLRepairEnvironment, SQLAction, SQLObservation)

# ── read the UI HTML once at startup ─────────────────────────────────────────
_UI_PATH = os.path.join(ROOT, "ui.html")
with open(_UI_PATH, "r") as f:
    _UI_HTML = f.read()


@app.get("/", response_class=HTMLResponse)
def root():
    return HTMLResponse(content=_UI_HTML)


@app.get("/tasks", tags=["Competition"])
def get_tasks():
    return JSONResponse(content={
        "tasks": SQLRepairEnvironment.list_tasks(),
        "total": len(TASKS),
        "action_schema": {
            "sql_query":   "string - The fixed SQL query to submit",
            "explanation": "string (optional) - Agent reasoning",
        },
    })


@app.post("/grader", tags=["Competition"])
def run_grader(task_id: str, sql_query: str):
    result = SQLRepairEnvironment.run_grader(task_id, sql_query)
    return JSONResponse(content=result)


@app.get("/baseline", tags=["Competition"])
def run_baseline():
    baseline_scores = {}
    for task_id, task in TASKS.items():
        result = SQLRepairEnvironment.run_grader(task_id, task["expected_query"])
        baseline_scores[task_id] = {
            "score":    result["score"],
            "passed":   result["passed"],
            "feedback": result["feedback"],
        }
    avg = sum(v["score"] for v in baseline_scores.values()) / len(baseline_scores)
    return JSONResponse(content={
        "baseline_agent": "oracle (submits known correct query)",
        "results":        baseline_scores,
        "average_score":  round(avg, 4),
    })


def main():
    import uvicorn
    port    = int(os.environ.get("PORT", 7860))
    host    = os.environ.get("HOST", "0.0.0.0")
    workers = int(os.environ.get("WORKERS", 4))
    uvicorn.run("server.app:app", host=host, port=port, workers=workers)


if __name__ == "__main__":
    main()
