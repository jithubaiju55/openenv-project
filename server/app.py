"""
server/app.py - FastAPI server for the SQL Repair Environment.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.responses import JSONResponse
from openenv.core.env_server import create_fastapi_app
from models import SQLAction, SQLObservation
from server.environment import SQLRepairEnvironment, TASKS

# Auto-creates /reset /step /state /health /ws /docs
app = create_fastapi_app(SQLRepairEnvironment, SQLAction, SQLObservation)


@app.get("/")
def root():
    return JSONResponse(content={
        "name":        "SQL Repair Environment",
        "version":     "1.0.0",
        "status":      "running",
        "description": "OpenEnv environment for AI agents to fix broken SQL queries",
        "endpoints": {
            "health":   "/health",
            "docs":     "/docs",
            "tasks":    "/tasks",
            "grader":   "/grader",
            "baseline": "/baseline",
        }
    })


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
    """Entry point for 'uv run server' command."""
    import uvicorn
    port = int(os.environ.get("PORT", 7860))
    host = os.environ.get("HOST", "0.0.0.0")
    uvicorn.run("server.app:app", host=host, port=port, workers=4)


if __name__ == "__main__":
    main()
