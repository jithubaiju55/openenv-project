# SQL Repair Environment

A production-grade OpenEnv environment where AI agents learn to diagnose and fix broken SQL queries through multi-turn agentic interaction with a live relational database.

**20 tasks · 7 database tables · Diagnostic mode · Execution diffs · Progress rewards · Anti-hack grading**

---

## Why This Environment Matters

SQL bugs cost real engineering hours every day. An agent that can autonomously diagnose and fix broken queries has immediate value in:

- IDE plugins that auto-fix SQL errors as developers type
- Automated code review systems that catch query bugs before deployment
- Database maintenance tools that validate and repair legacy queries
- RL training benchmarks for code understanding and repair agents

This environment provides a rigorous, reproducible training ground for exactly that capability.

---

## What Makes This Environment Exceptional

### 1. Diagnostic Mode — True Multi-Turn Agentic Behaviour

Most RL environments only support: observe → act → reward. This environment introduces a third action type: **diagnostic queries**.

An agent can submit any SQL query as a diagnostic action — exploring the database, checking relationships, counting rows, running EXPLAIN — **without using an attempt**. This enables genuine multi-turn agentic reasoning:

```python
# Phase 1: Diagnose
env.step({"action": {"diagnostic": "SELECT * FROM employee_projects LIMIT 5"}})
# → Returns up to 10 rows, no attempt used

# Phase 2: Fix based on what you learned
env.step({"action": {"sql_query": "SELECT e.name, p.name FROM employees e JOIN ..."}})
# → Graded, returns reward
```

This mirrors how a real developer debugs: they first explore, then fix.

### 2. Execution Diffs — Agent Sees Exactly What Went Wrong

Every observation after a submission includes:

- `your_result_preview` — first 5 rows your query returned
- `expected_result_preview` — first 5 rows of the correct output (shown from reset)
- `result_diff` — human-readable diff: "Row 3 differs: got {name: Bob} expected {name: Alice}"

This transforms the environment from a black box into a transparent training signal. Agents can learn from the *shape* of their mistakes, not just their score.

### 3. Progress Tracking — Reward for Improvement

The reward function tracks `best_score_so_far` across the episode and adds a **progress bonus** when an agent improves:

```
progress_bonus = (current_score - best_previous_score) * 0.1
```

This prevents reward flattening in multi-attempt episodes and encourages convergent behaviour — the agent is rewarded for getting progressively closer to the correct answer.

### 4. Bug Categories — Structured Curriculum

Every task is tagged with its bug category:

| Category | Description | Tasks |
|----------|-------------|-------|
| `syntax` | Misspelled SQL keywords | easy_1 through easy_7 |
| `join` | Wrong JOIN column references | medium_1,3,5,6 |
| `having` | WHERE used with aggregates | medium_2,7, hard_4,6 |
| `subquery` | Wrong aggregate in correlated subquery | medium_4 |
| `join_type` | LEFT vs INNER JOIN with WHERE filter | hard_1 |
| `self_join` | Self-referential join with wrong column | hard_2 |
| `group_by` | Non-aggregated column in SELECT | hard_3 |
| `duplicate_count` | Duplicate rows from multi-join | hard_5 |

This enables **curriculum learning**: train on syntax first, then joins, then hard semantic bugs.

### 5. Anti-Hack Grader — No Reward Exploitation

The grader detects and penalises gaming:
- Submitting the original broken query unchanged → score 0.001
- Submitting an exact duplicate of a previous attempt → score 0.001
- All scores clamped strictly to (0.001, 0.999) — never exactly 0 or 1

---

## Observation Space

```python
class SQLObservation(Observation):
    # Core task
    broken_query: str           # The broken SQL to fix
    db_schema: str              # Full schema with relationships
    task_description: str       # What the fixed query should return
    task_id: str                # e.g. "hard_2"
    difficulty: str             # easy | medium | hard
    bug_category: str           # syntax | join | having | ...

    # Episode progress
    attempt_number: int         # Current attempt (0 = just reset)
    max_attempts: int           # Always 5
    best_score_so_far: float    # Best score this episode
    improving: bool             # True if last attempt beat previous best

    # Grader feedback
    error_message: str          # SQL execution error
    feedback: str               # Per-component score breakdown
    hint: str                   # Bug-specific hint after 2 failed attempts

    # Execution results (key differentiator)
    your_result_preview: list   # First 5 rows your query returned
    expected_result_preview: list  # First 5 rows of correct output
    result_diff: str            # Human-readable diff

    # Metadata
    expected_columns: list      # Expected column names
    expected_row_count: int     # Expected row count
    your_row_count: int         # Rows your query returned

    # Diagnostic
    diagnostic_result: list     # Result of diagnostic query
    diagnostic_error: str       # Error if diagnostic failed
```

## Action Space

```python
class SQLAction(Action):
    sql_query: str    # Graded fix — uses one attempt
    diagnostic: str   # Free exploratory query — no attempt used
    explanation: str  # Optional reasoning
```

---

## Reward Function

| Component | Points | Condition |
|-----------|--------|-----------|
| Execution | +0.30 | Query runs without SQL error |
| Columns | +0.20 | Returned column set matches exactly |
| Row count | +0.10 | Number of rows matches expected |
| Values | +0.40 | All values match (partial credit) |
| Progress | +bonus | Score improved from previous best |
| Unchanged | 0.001 | Submitted original broken query |
| Duplicate | 0.001 | Exact same query as previous attempt |
| Exhausted | ×0.85 | Used all 5 attempts without solving |

---

## Tasks

### Easy (7) — Syntax Errors
Agents learn to recognise and fix misspelled SQL keywords.

| Task | Bug |
|------|-----|
| easy_1 | SELCT, FORM, WERE, ORDR BY |
| easy_2 | ORER BY |
| easy_3 | CONT(*), GROUB BY |
| easy_4 | SELCT, FEOM |
| easy_5 | WHER, ORDER BE |
| easy_6 | SELEC, FORM |
| easy_7 | DESTINCT, FORME, ORDRE BY |

### Medium (7) — Logic Errors
Agents learn to reason about table relationships and aggregate semantics.

| Task | Bug |
|------|-----|
| medium_1 | Swapped JOIN columns (ep.project_id ↔ ep.employee_id) |
| medium_2 | WHERE AVG() → HAVING AVG() |
| medium_3 | p.id = d.id → p.department_id = d.id |
| medium_4 | MAX() → AVG() in correlated subquery |
| medium_5 | s.id = e.id → s.employee_id = e.id |
| medium_6 | pr.reviewer_id = e.id → pr.employee_id = e.id |
| medium_7 | WHERE SUM() → HAVING SUM() |

### Hard (6) — Semantic Errors
Agents learn deep SQL semantics: join types, self-referential joins, GROUP BY rules.

| Task | Bug |
|------|-----|
| hard_1 | LEFT JOIN + WHERE filter → INNER JOIN |
| hard_2 | Self-join wrong column + missing LEFT JOIN |
| hard_3 | Non-aggregated column in SELECT with GROUP BY |
| hard_4 | WHERE SUM() with multi-column GROUP BY → HAVING |
| hard_5 | Multi-join duplicate counting → SUM(DISTINCT) + LEFT JOIN |
| hard_6 | WHERE COUNT() > 1 → HAVING COUNT() > 1 |

---

## Database Schema (7 Tables)

```sql
employees(id, name, department, salary, hire_date, manager_id, status)
departments(id, name, budget, location, head_id)
projects(id, name, department_id, budget, status, start_date, end_date, priority)
employee_projects(employee_id, project_id, role, hours_worked, start_date)
sales(id, employee_id, amount, sale_date, product, region, quarter)
products(id, name, category, price, stock, supplier)
performance_reviews(id, employee_id, year, rating, reviewer_id, notes)
```

---

## Quick Start

```python
import requests
ENV = "https://WALKMAN303-sql-repair-env.hf.space"

# Start episode
r   = requests.post(f"{ENV}/reset", json={"task_id": "hard_2"})
obs = r.json()["observation"]
print("Broken:", obs["broken_query"])
print("Expected preview:", obs["expected_result_preview"])

# Phase 1: Diagnose (free)
r   = requests.post(f"{ENV}/step", json={"action": {"diagnostic": "SELECT id, manager_id FROM employees LIMIT 5"}})
obs = r.json()["observation"]
print("Diagnostic:", obs["diagnostic_result"])

# Phase 2: Fix
r      = requests.post(f"{ENV}/step", json={"action": {"sql_query": "SELECT e.name, m.name AS manager FROM employees e LEFT JOIN employees m ON m.id = e.manager_id ORDER BY e.name"}})
result = r.json()
print(f"Score: {result['reward']:.3f}")
print(f"Diff:  {result['observation']['result_diff']}")
```

---

## Setup

```bash
git clone https://huggingface.co/spaces/WALKMAN303/sql-repair-env
cd sql-repair-env
pip install -r requirements.txt
git clone https://github.com/meta-pytorch/OpenEnv.git
export PYTHONPATH=$PYTHONPATH:$(pwd)/OpenEnv:$(pwd)/OpenEnv/src:$(pwd)
uvicorn server.app:app --host 0.0.0.0 --port 7860 --reload
```

Docker:
```bash
docker build -t sql-repair-env .
docker run -p 7860:7860 sql-repair-env
```

Run baseline:
```bash
export API_KEY=your_token
export API_BASE_URL=https://router.huggingface.co/v1
export ENV_URL=https://WALKMAN303-sql-repair-env.hf.space
python inference.py
```

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Interactive web UI |
| `/reset` | POST | Start episode |
| `/step` | POST | Submit sql_query or diagnostic |
| `/state` | GET | Episode metadata |
| `/tasks` | GET | All 20 tasks |
| `/grader` | POST | Grade without episode |
| `/baseline` | GET | Oracle scores all 20 tasks |
| `/health` | GET | Health check |
| `/info` | GET | Environment info |
| `/docs` | GET | Swagger API |

---

## Baseline Scores

| Agent | Easy | Medium | Hard | Overall |
|-------|------|--------|------|---------|
| Oracle | 0.999 | 0.999 | 0.999 | 0.999 |
| Qwen2.5-72B | ~0.95 | ~0.78 | ~0.60 | ~0.78 |
| Llama-3.1-8B | ~0.90 | ~0.65 | ~0.42 | ~0.66 |

---

## Project Structure

```
sql-repair-env/
├── __init__.py
├── models.py          ← Pydantic models with Field() descriptors
├── client.py          ← WebSocket client
├── inference.py       ← Two-phase diagnostic+fix baseline agent
├── ui.html            ← Interactive web UI
├── openenv.yaml       ← OpenEnv spec
├── pyproject.toml     ← Package with server entry point
├── uv.lock
├── requirements.txt
├── Dockerfile
└── server/
    ├── __init__.py
    ├── environment.py  ← v4: diagnostic mode, diffs, progress rewards
    └── app.py          ← FastAPI server
```

---

## Links

- GitHub: https://github.com/arjunkr303/openenv-project
- HF Space: https://huggingface.co/spaces/WALKMAN303/sql-repair-env  
- Live API: https://WALKMAN303-sql-repair-env.hf.space/docs  
- OpenEnv: https://github.com/meta-pytorch/OpenEnv
