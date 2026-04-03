"""
server/environment.py — Core SQL Repair Environment Logic.

Contains:
  - SQLite database schema + seed data
  - 3 tasks: easy (syntax), medium (JOIN), hard (aggregation)
  - Grader that gives partial-credit rewards 0.0–1.0
  - SQLRepairEnvironment class implementing the OpenEnv interface
"""

import sqlite3
import uuid
import random
from typing import Optional, Dict, List, Tuple, Any

from openenv.core.env_server import Environment
from models import SQLAction, SQLObservation, SQLState


# ─── Database Schema ──────────────────────────────────────────────────────────

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS employees (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    department  TEXT    NOT NULL,
    salary      REAL    NOT NULL,
    hire_date   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS departments (
    id       INTEGER PRIMARY KEY,
    name     TEXT  NOT NULL,
    budget   REAL  NOT NULL,
    location TEXT  NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    id            INTEGER PRIMARY KEY,
    name          TEXT  NOT NULL,
    department_id INTEGER NOT NULL,
    budget        REAL  NOT NULL,
    status        TEXT  NOT NULL
);

CREATE TABLE IF NOT EXISTS employee_projects (
    employee_id  INTEGER NOT NULL,
    project_id   INTEGER NOT NULL,
    role         TEXT,
    hours_worked REAL DEFAULT 0,
    PRIMARY KEY (employee_id, project_id)
);
"""

DB_SEED = """
INSERT INTO departments VALUES (1, 'Engineering', 500000, 'San Francisco');
INSERT INTO departments VALUES (2, 'Marketing',   200000, 'New York');
INSERT INTO departments VALUES (3, 'Finance',     300000, 'Chicago');

INSERT INTO employees VALUES (1, 'Alice Johnson', 'Engineering', 95000,  '2020-01-15');
INSERT INTO employees VALUES (2, 'Bob Smith',     'Engineering', 85000,  '2019-03-20');
INSERT INTO employees VALUES (3, 'Carol White',   'Marketing',   72000,  '2021-06-01');
INSERT INTO employees VALUES (4, 'David Brown',   'Finance',     88000,  '2018-11-10');
INSERT INTO employees VALUES (5, 'Eve Davis',     'Engineering', 105000, '2017-07-22');
INSERT INTO employees VALUES (6, 'Frank Miller',  'Marketing',   65000,  '2022-02-14');
INSERT INTO employees VALUES (7, 'Grace Wilson',  'Finance',     92000,  '2019-09-30');
INSERT INTO employees VALUES (8, 'Henry Moore',   'Engineering', 78000,  '2020-12-05');

INSERT INTO projects VALUES (1, 'AI Platform',   1, 150000, 'active');
INSERT INTO projects VALUES (2, 'Brand Refresh', 2,  80000, 'completed');
INSERT INTO projects VALUES (3, 'Budget System', 3, 120000, 'active');
INSERT INTO projects VALUES (4, 'API Gateway',   1,  90000, 'active');

INSERT INTO employee_projects VALUES (1, 1, 'Lead',      320.0);
INSERT INTO employee_projects VALUES (2, 1, 'Developer', 280.0);
INSERT INTO employee_projects VALUES (5, 4, 'Lead',      200.0);
INSERT INTO employee_projects VALUES (8, 4, 'Developer', 150.0);
INSERT INTO employee_projects VALUES (3, 2, 'Lead',      400.0);
INSERT INTO employee_projects VALUES (6, 2, 'Designer',  300.0);
INSERT INTO employee_projects VALUES (4, 3, 'Lead',      250.0);
INSERT INTO employee_projects VALUES (7, 3, 'Analyst',   180.0);
"""

SCHEMA_DESCRIPTION = """Tables in this database:

employees(id INTEGER, name TEXT, department TEXT, salary REAL, hire_date TEXT)
departments(id INTEGER, name TEXT, budget REAL, location TEXT)
projects(id INTEGER, name TEXT, department_id INTEGER, budget REAL, status TEXT)
employee_projects(employee_id INTEGER, project_id INTEGER, role TEXT, hours_worked REAL)

Relationships:
  employees.department  → matches departments.name
  projects.department_id → references departments.id
  employee_projects.employee_id → references employees.id
  employee_projects.project_id  → references projects.id
"""


# ─── Task Definitions ─────────────────────────────────────────────────────────

TASKS: Dict[str, Dict[str, Any]] = {

    "easy": {
        "id": "easy",
        "difficulty": "easy",
        "description": (
            "Fix the syntax errors in this SQL query. "
            "The query should return the name, department, and salary "
            "of all Engineering employees earning more than $80,000, "
            "ordered by salary descending."
        ),
        "broken_query": (
            "SELCT name, department, salary "
            "FORM employees "
            "WERE department = 'Engineering' AND salary > 80000 "
            "ORDR BY salary DESC"
        ),
        "expected_query": (
            "SELECT name, department, salary "
            "FROM employees "
            "WHERE department = 'Engineering' AND salary > 80000 "
            "ORDER BY salary DESC"
        ),
        "hint": (
            "Look for misspelled SQL keywords. "
            "Correct spellings: SELECT (not SELCT), FROM (not FORM), "
            "WHERE (not WERE), ORDER BY (not ORDR BY)."
        ),
    },

    "medium": {
        "id": "medium",
        "difficulty": "medium",
        "description": (
            "Fix the JOIN conditions in this query. "
            "The query should return each employee's name, "
            "the project they work on, and their hours worked — "
            "ordered by employee name."
        ),
        "broken_query": (
            "SELECT e.name, p.name AS project_name, ep.hours_worked\n"
            "FROM employees e\n"
            "JOIN employee_projects ep ON e.id = ep.project_id\n"
            "JOIN projects p ON ep.employee_id = p.id\n"
            "ORDER BY e.name"
        ),
        "expected_query": (
            "SELECT e.name, p.name AS project_name, ep.hours_worked\n"
            "FROM employees e\n"
            "JOIN employee_projects ep ON e.id = ep.employee_id\n"
            "JOIN projects p ON ep.project_id = p.id\n"
            "ORDER BY e.name"
        ),
        "hint": (
            "The JOIN column names are swapped on both JOIN lines. "
            "employees links to employee_projects via employee_id (not project_id). "
            "employee_projects links to projects via project_id (not employee_id)."
        ),
    },

    "hard": {
        "id": "hard",
        "difficulty": "hard",
        "description": (
            "Fix the aggregation logic in this query. "
            "The query should return each department's name, "
            "employee count, and average salary — "
            "but only for departments where the average salary exceeds $85,000, "
            "ordered by average salary descending."
        ),
        "broken_query": (
            "SELECT department, COUNT(*) AS emp_count, AVG(salary) AS avg_salary\n"
            "FROM employees\n"
            "GROUP BY department\n"
            "WHERE AVG(salary) > 85000\n"
            "ORDER BY avg_salary DESC"
        ),
        "expected_query": (
            "SELECT department, COUNT(*) AS emp_count, AVG(salary) AS avg_salary\n"
            "FROM employees\n"
            "GROUP BY department\n"
            "HAVING AVG(salary) > 85000\n"
            "ORDER BY avg_salary DESC"
        ),
        "hint": (
            "You cannot use WHERE with aggregate functions like AVG(). "
            "WHERE filters individual rows before grouping. "
            "To filter groups after aggregation, use HAVING instead."
        ),
    },
}


# ─── Database Helpers ─────────────────────────────────────────────────────────

def create_db() -> sqlite3.Connection:
    """Create a fresh in-memory SQLite database with seed data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(DB_SCHEMA + DB_SEED)
    return conn


def run_query(
    conn: sqlite3.Connection,
    query: str
) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """
    Execute a SQL query safely.
    Returns (rows, None) on success, (None, error_message) on failure.
    """
    try:
        cursor = conn.execute(query)
        rows = [dict(row) for row in cursor.fetchall()]
        return rows, None
    except Exception as exc:
        return None, str(exc)


# ─── Grader ───────────────────────────────────────────────────────────────────

def _normalize_rows(rows: List[Dict]) -> List[tuple]:
    """Convert rows to a sorted, comparable format (order-independent)."""
    def normalize_val(v: Any) -> str:
        if isinstance(v, float):
            return str(round(v, 2))
        return str(v)

    normalized = [
        tuple(sorted((k, normalize_val(v)) for k, v in row.items()))
        for row in rows
    ]
    return sorted(normalized)


def grade_submission(
    task_id: str,
    submitted_rows: Optional[List[Dict]],
    error: Optional[str],
    conn: sqlite3.Connection,
) -> Tuple[float, str]:
    """
    Grade a submitted query against the expected result.

    Scoring breakdown:
      +0.30  query executes without error
      +0.20  returned columns are correct
      +0.10  row count matches expected
      +0.40  all row values match exactly (partial credit for partial matches)

    Returns (score: float 0.0–1.0, feedback: str)
    """
    task = TASKS[task_id]
    feedback_parts: List[str] = []
    score = 0.0

    # ── Gate: did the query even run? ───────────────────────────────────────
    if error is not None:
        return 0.0, f"❌ Query failed to execute: {error}"

    # ── Run expected query for ground truth ─────────────────────────────────
    expected_rows, exp_err = run_query(conn, task["expected_query"])
    if exp_err:
        return 0.0, "Internal error running expected query."

    # ── +0.30 Executes without error ────────────────────────────────────────
    score += 0.30
    feedback_parts.append("✅ Query executes without error (+0.30)")

    # ── +0.20 Correct columns ───────────────────────────────────────────────
    sub_cols = set(submitted_rows[0].keys()) if submitted_rows else set()
    exp_cols = set(expected_rows[0].keys()) if expected_rows else set()

    if sub_cols == exp_cols:
        score += 0.20
        feedback_parts.append("✅ Correct columns returned (+0.20)")
    else:
        missing = exp_cols - sub_cols
        extra   = sub_cols - exp_cols
        msg = "❌ Wrong columns."
        if missing:
            msg += f" Missing: {sorted(missing)}."
        if extra:
            msg += f" Unexpected: {sorted(extra)}."
        feedback_parts.append(msg)

    # ── +0.10 Correct row count ─────────────────────────────────────────────
    sub_count = len(submitted_rows) if submitted_rows else 0
    exp_count = len(expected_rows) if expected_rows else 0

    if sub_count == exp_count:
        score += 0.10
        feedback_parts.append(f"✅ Correct row count: {exp_count} rows (+0.10)")
    else:
        feedback_parts.append(
            f"❌ Wrong row count: got {sub_count}, expected {exp_count}"
        )

    # ── +0.40 Row values match ──────────────────────────────────────────────
    if submitted_rows and expected_rows:
        sub_norm = _normalize_rows(submitted_rows)
        exp_norm = _normalize_rows(expected_rows)

        if sub_norm == exp_norm:
            score += 0.40
            feedback_parts.append("✅ All row values match exactly! (+0.40)")
        elif sub_count == exp_count:
            # Partial credit: count matching rows
            matching = sum(1 for s, e in zip(sub_norm, exp_norm) if s == e)
            partial  = (matching / exp_count) * 0.40
            score   += partial
            feedback_parts.append(
                f"⚠️  Partial row match: {matching}/{exp_count} rows correct "
                f"(+{partial:.2f})"
            )
        else:
            feedback_parts.append("❌ Row values do not match expected output.")

    score = min(1.0, round(score, 4))
    return score, " | ".join(feedback_parts)


# ─── Environment ──────────────────────────────────────────────────────────────

class SQLRepairEnvironment(Environment):
    """
    SQL Query Repair Environment.

    An AI agent is given a broken SQL query and must fix it.
    Three tasks of increasing difficulty cover real-world bug categories:

      easy   — Syntax errors    (typos in SQL keywords)
      medium — Logic errors     (wrong JOIN column references)
      hard   — Semantic errors  (WHERE vs HAVING with aggregates)

    Episode flow:
      1. reset(task_id="easy"|"medium"|"hard")  →  broken query + schema
      2. step(SQLAction(sql_query="..."))        →  grader feedback + partial reward
      3. Repeat until done=True (score=1.0 or max attempts reached)
    """

    SUPPORTS_CONCURRENT_SESSIONS = True
    MAX_ATTEMPTS = 5

    def __init__(self):
        self._state      = SQLState()
        self._task: Optional[Dict]               = None
        self._conn: Optional[sqlite3.Connection] = None
        self._attempt    = 0
        self._last_score = 0.0

    # ── reset ────────────────────────────────────────────────────────────────

    def reset(
        self,
        seed=None,
        episode_id: Optional[str] = None,
        task_id: Optional[str] = None,
        **kwargs,
    ) -> SQLObservation:
        """
        Start a new episode.

        Args:
            task_id: "easy", "medium", or "hard".
                     Random task selected if omitted.
        """
        # Select task
        if task_id and task_id in TASKS:
            self._task = TASKS[task_id]
        else:
            self._task = random.choice(list(TASKS.values()))

        # Fresh database for this episode
        if self._conn:
            self._conn.close()
        self._conn   = create_db()
        self._attempt    = 0
        self._last_score = 0.0

        self._state = SQLState(
            episode_id   = episode_id or str(uuid.uuid4()),
            step_count   = 0,
            task_id      = self._task["id"],
            difficulty   = self._task["difficulty"],
            max_attempts = self.MAX_ATTEMPTS,
            last_score   = 0.0,
            completed    = False,
        )

        return SQLObservation(
            done             = False,
            reward           = 0.0,
            broken_query     = self._task["broken_query"],
            db_schema        = SCHEMA_DESCRIPTION,
            error_message    = "",
            task_description = self._task["description"],
            task_id          = self._task["id"],
            difficulty       = self._task["difficulty"],
            attempt_number   = 0,
            max_attempts     = self.MAX_ATTEMPTS,
            feedback         = "Episode started. Submit your fixed SQL query.",
            hint             = "",
        )

    # ── step ─────────────────────────────────────────────────────────────────

    def step(
        self,
        action: SQLAction,
        timeout_s=None,
        **kwargs,
    ) -> SQLObservation:
        """
        Submit a fixed SQL query.

        The grader runs the query, compares results to expected,
        and returns a score with detailed feedback.
        """
        self._attempt         += 1
        self._state.step_count += 1

        # Run submitted query
        rows, error = run_query(self._conn, action.sql_query)

        # Grade it
        score, feedback = grade_submission(
            self._task["id"], rows, error, self._conn
        )
        self._last_score      = score
        self._state.last_score = score

        # Episode ends when solved or out of attempts
        done = (score >= 1.0) or (self._attempt >= self.MAX_ATTEMPTS)
        self._state.completed = score >= 1.0

        # Reward shaping:
        #   - Intermediate steps: return current score (partial progress signal)
        #   - Terminal step with perfect score: 1.0
        #   - Terminal step after exhausting attempts: slight penalty
        if done and score < 1.0 and self._attempt >= self.MAX_ATTEMPTS:
            reward = round(score * 0.85, 4)   # Penalty for using all attempts
        else:
            reward = score                     # Full score at every step

        # Reveal hint after 2 failed attempts
        hint = ""
        if self._attempt >= 2 and score < 0.5:
            hint = self._task["hint"]

        return SQLObservation(
            done             = done,
            reward           = reward,
            broken_query     = self._task["broken_query"],
            db_schema        = SCHEMA_DESCRIPTION,
            error_message    = error or "",
            task_description = self._task["description"],
            task_id          = self._task["id"],
            difficulty       = self._task["difficulty"],
            attempt_number   = self._attempt,
            max_attempts     = self.MAX_ATTEMPTS,
            feedback         = feedback,
            hint             = hint,
        )

    # ── state ─────────────────────────────────────────────────────────────────

    @property
    def state(self) -> SQLState:
        """Return current episode metadata."""
        return self._state

    # ── helpers (used by app.py extra endpoints) ──────────────────────────────

    def get_last_score(self) -> float:
        return self._last_score

    def get_current_task(self) -> Dict:
        return self._task or {}

    @staticmethod
    def list_tasks() -> List[Dict]:
        """Return all task definitions for the /tasks endpoint."""
        return [
            {
                "task_id":     t["id"],
                "difficulty":  t["difficulty"],
                "description": t["description"],
                "action_schema": {
                    "sql_query":   "string — Your fixed SQL query",
                    "explanation": "string (optional) — Your reasoning",
                },
            }
            for t in TASKS.values()
        ]

    @staticmethod
    def run_grader(task_id: str, sql_query: str) -> Dict:
        """
        Standalone grader — used by the /grader endpoint.
        Creates a fresh DB, runs the query, and returns the score.
        """
        if task_id not in TASKS:
            return {"error": f"Unknown task_id: {task_id}. Choose: easy, medium, hard"}

        conn = create_db()
        rows, error = run_query(conn, sql_query)
        score, feedback = grade_submission(task_id, rows, error, conn)
        conn.close()

        return {
            "task_id":  task_id,
            "score":    score,
            "feedback": feedback,
            "passed":   score >= 1.0,
        }
