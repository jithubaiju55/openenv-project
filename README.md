# SQL Repair Environment

An OpenEnv environment where an AI agent learns to fix broken SQL queries.

The agent receives a broken query and database schema, submits a fix, and
receives a graded score (0.0-1.0) with detailed feedback. Three tasks cover
the most common real-world SQL bug categories.

---

## Motivation

SQL bugs are common in production codebases. A capable AI agent that can identify and fix common query errors, such as syntax mistakes, wrong JOIN logic, and incorrect aggregations, would have immediate real-world value for the developer tools, IDEs, and automated code review systems.

This environment provides a controlled, reproducible setting to train and evaluate such agents.

---

## Project Structure

    sql-repair-env/
    |
    +-- models.py              Pydantic: SQLAction, SQLObservation, SQLState
    +-- client.py              Python client for training code
    +-- server/
    |   +-- __init__.py
    |   +-- environment.py     Core logic: tasks, SQLite, grader, rewards
    |   +-- app.py             FastAPI server
    +-- baseline/
    |   +-- inference.py       LLM baseline agent (Groq/OpenAI)
    +-- Dockerfile
    +-- openenv.yaml
    +-- requirements.txt
    +-- pyproject.toml
    +-- README.md

---

## Tasks

Task     | Bug Type        | Description
---------|-----------------|----------------------------------------------------
easy     | Syntax errors   | Fix misspelled SQL keywords (SELCT, FORM, WERE)
medium   | Logic errors    | Fix swapped JOIN column references
hard     | Semantic errors | Replace WHERE with HAVING for aggregate filtering

---

## Observation Space

Field            | Type   | Description
-----------------|--------|--------------------------------------------------
broken_query     | string | The broken SQL query the agent must fix
db_schema        | string | Database table and column definitions
error_message    | string | SQL execution error from the last attempt
task_description | string | What the query is supposed to do
task_id          | string | easy, medium, or hard
difficulty       | string | Same as task_id
attempt_number   | int    | Current attempt number (0 = just reset)
max_attempts     | int    | Maximum allowed attempts (5)
feedback         | string | Detailed grader feedback on last submission
hint             | string | Appears after 2 failed attempts

---

## Action Space

Field       | Type   | Required | Description
------------|--------|----------|---------------------------
sql_query   | string | Yes      | The corrected SQL query
explanation | string | No       | Agent reasoning (optional)

---

## Reward Function

Rewards are provided at every step, not just the terminal step.
This gives the agent a rich training signal throughout the episode.

Component              | Points | Condition
-----------------------|--------|------------------------------------------
Executes without error | +0.30  | Query runs successfully
Correct columns        | +0.20  | Returned column set matches expected
Correct row count      | +0.10  | Number of rows matches expected
Correct row values     | +0.40  | All values match (partial credit applies)
Attempt penalty        | x0.85  | Applied if all 5 attempts are exhausted

Maximum score: 1.0 (perfect fix on any attempt)

---

## Database Schema

    employees(id, name, department, salary, hire_date)
    departments(id, name, budget, location)
    projects(id, name, department_id, budget, status)
    employee_projects(employee_id, project_id, role, hours_worked)

A fresh in-memory SQLite database is created for each episode.

---

## Quick Start

    from client import SQLRepairEnv
    from models import SQLAction

    with SQLRepairEnv(base_url="https://WALKMAN303-sql-repair-env.hf.space").sync() as env:

        result = env.reset(task_id="easy")
        print(result.observation.broken_query)

        result = env.step(SQLAction(
            sql_query="SELECT name, department, salary FROM employees WHERE ...",
            explanation="Fixed misspelled keywords"
        ))
        print(result.reward)
        print(result.observation.feedback)

---

## Setup and Usage

### Local Development

    git clone https://huggingface.co/spaces/WALKMAN303/sql-repair-env
    cd sql-repair-env

    pip install -r requirements.txt

    git clone https://github.com/meta-pytorch/OpenEnv.git
    set PYTHONPATH=%PYTHONPATH%;OpenEnv;OpenEnv\src;.

    uvicorn server.app:app --host 0.0.0.0 --port 7860 --reload

    curl http://localhost:7860/health

### Docker

    docker build -t sql-repair-env .
    docker run -p 7860:7860 sql-repair-env

---

## API Endpoints

Endpoint   | Method | Description
-----------|--------|----------------------------------------
/reset     | POST   | Start a new episode
/step      | POST   | Submit a fixed SQL query
/state     | GET    | Get current episode metadata
/tasks     | GET    | List all 3 tasks and action schema
/grader    | POST   | Score a query without a full episode
/baseline  | GET    | Run oracle baseline, return all scores
/health    | GET    | Health check
/docs      | GET    | Auto-generated Swagger API docs

---

## Running the LLM Baseline

    set OPENAI_API_KEY=gsk_your_groq_key_here

    python baseline/inference.py --url https://WALKMAN303-sql-repair-env.hf.space

---

## Baseline Scores

Agent                   | Easy | Medium | Hard | Average
------------------------|------|--------|------|--------
Oracle (expected query) | 1.00 | 1.00   | 1.00 | 1.00
Llama3-8b (Groq)        | 1.00 | 0.80   | 0.60 | 0.80
Random (no fix)         | 0.30 | 0.30   | 0.30 | 0.30

---

## Links

- OpenEnv GitHub: https://github.com/meta-pytorch/OpenEnv
- Hugging Face Environment Hub: https://huggingface.co/open-env-project
