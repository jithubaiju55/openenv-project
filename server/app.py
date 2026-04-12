"""
server/app.py - SQL Repair Environment v4 FastAPI Server
All endpoints: standard OpenEnv + competition + UI
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from fastapi import Request
from fastapi.responses import JSONResponse, HTMLResponse
from openenv.core.env_server import create_fastapi_app
from models import SQLAction, SQLObservation
from server.environment import SQLRepairEnvironment, TASKS, BUG_CATEGORY_DESCRIPTIONS

# ── Core app (auto-creates /reset /step /state /health /ws /docs) ─────────────
app = create_fastapi_app(SQLRepairEnvironment, SQLAction, SQLObservation)

# ── Load UI ───────────────────────────────────────────────────────────────────
_UI_PATH = os.path.join(ROOT, "ui.html")
try:
    with open(_UI_PATH, "r", encoding="utf-8") as f:
        _UI_HTML = f.read()
except FileNotFoundError:
    _UI_HTML = "<h1>SQL Repair Environment v4</h1><p>UI file not found. Use <a href='/docs'>/docs</a> for API.</p>"


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def root():
    """Serve the interactive web UI."""
    return HTMLResponse(content=_UI_HTML)


@app.get("/info", tags=["Meta"])
def info():
    """Environment metadata and statistics."""
    easy   = [t for t in TASKS.values() if t["difficulty"] == "easy"]
    medium = [t for t in TASKS.values() if t["difficulty"] == "medium"]
    hard   = [t for t in TASKS.values() if t["difficulty"] == "hard"]
    return JSONResponse(content={
        "name":        "SQL Repair Environment",
        "version":     "4.0.0",
        "status":      "running",
        "description": (
            "AI agent training environment for SQL query repair. "
            "20 tasks covering all real-world SQL bug categories. "
            "Features diagnostic mode, execution diffs, and progress rewards."
        ),
        "total_tasks": len(TASKS),
        "difficulty_breakdown": {
            "easy":   len(easy),
            "medium": len(medium),
            "hard":   len(hard),
        },
        "bug_categories": BUG_CATEGORY_DESCRIPTIONS,
        "innovations": [
            "Diagnostic mode: run free SQL queries to explore DB before fixing",
            "Execution diffs: agent sees its result vs expected result",
            "Progress reward: bonus for improving score across attempts",
            "Anti-hack grader: penalises unchanged/duplicate submissions",
            "Bug categories: structured curriculum learning support",
        ],
        "endpoints": {
            "ui":       "/",
            "health":   "/health",
            "docs":     "/docs",
            "info":     "/info",
            "tasks":    "/tasks",
            "grader":   "/grader",
            "baseline": "/baseline",
            "reset":    "/reset",
            "step":     "/step",
            "state":    "/state",
        },
    })


@app.get("/tasks", tags=["Competition"])
def get_tasks():
    """List all 20 tasks with descriptions, bug categories, and action schema."""
    return JSONResponse(content={
        "tasks": SQLRepairEnvironment.list_tasks(),
        "total": len(TASKS),
        "difficulty_breakdown": {
            "easy":   len([t for t in TASKS.values() if t["difficulty"] == "easy"]),
            "medium": len([t for t in TASKS.values() if t["difficulty"] == "medium"]),
            "hard":   len([t for t in TASKS.values() if t["difficulty"] == "hard"]),
        },
        "bug_categories": BUG_CATEGORY_DESCRIPTIONS,
        "action_schema": {
            "sql_query":   "string (required for grading) — your corrected SQL query",
            "diagnostic":  "string (optional, free) — any SQL to explore the DB without using an attempt",
            "explanation": "string (optional) — your reasoning",
        },
    })


@app.post("/grader", tags=["Competition"])
def run_grader(task_id: str, sql_query: str):
    """
    Grade a SQL query against a specific task without starting a full episode.
    Returns score (0.001–0.999), feedback, result diff, and pass/fail.
    """
    result = SQLRepairEnvironment.run_grader(task_id, sql_query)
    return JSONResponse(content=result)


@app.get("/baseline", tags=["Competition"])
def run_baseline():
    """
    Run the oracle baseline agent against all 20 tasks.
    The oracle submits the known-correct query for each task.
    Returns scores per task and aggregated by difficulty.
    """
    baseline_scores = {}
    for task_id, task in TASKS.items():
        result = SQLRepairEnvironment.run_grader(task_id, task["expected_query"])
        baseline_scores[task_id] = {
            "score":      result["score"],
            "passed":     result["passed"],
            "difficulty": task["difficulty"],
            "bug_category": task["bug_category"],
            "feedback":   result["feedback"],
        }

    by_diff: dict = {"easy": [], "medium": [], "hard": []}
    for res in baseline_scores.values():
        by_diff[res["difficulty"]].append(res["score"])

    avg = sum(v["score"] for v in baseline_scores.values()) / len(baseline_scores)

    return JSONResponse(content={
        "baseline_agent":  "oracle (submits known correct query)",
        "results":         baseline_scores,
        "average_score":   round(avg, 4),
        "by_difficulty": {
            d: round(sum(scores) / len(scores), 4)
            for d, scores in by_diff.items() if scores
        },
    })


def main():
    """Entry point for uv run server and [project.scripts] server."""
    import uvicorn
    port    = int(os.environ.get("PORT", 7860))
    host    = os.environ.get("HOST", "0.0.0.0")
    workers = int(os.environ.get("WORKERS", 4))
    uvicorn.run("server.app:app", host=host, port=port, workers=workers)


if __name__ == "__main__":
    main()
