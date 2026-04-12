#!/usr/bin/env python3
"""
inference.py - SQL Repair Environment v4 Baseline Agent

Two-phase agentic strategy:
  Phase 1 (DIAGNOSTIC): Run exploratory queries to understand what went wrong.
  Phase 2 (FIX): Submit the corrected query based on diagnostic insights.

This demonstrates the environment's multi-turn agentic capability.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests
from openai import OpenAI

# ── Configuration ─────────────────────────────────────────────────────────────
API_KEY      = os.environ.get("API_KEY")
API_BASE_URL = os.environ.get("API_BASE_URL", "https://router.huggingface.co/v1")
MODEL_NAME   = os.environ.get("MODEL_NAME") or "Qwen/Qwen2.5-72B-Instruct"
ENV_URL      = os.environ.get("ENV_URL", "https://WALKMAN303-sql-repair-env.hf.space")

HF_TOKEN         = os.environ.get("HF_TOKEN")
LOCAL_IMAGE_NAME = os.environ.get("LOCAL_IMAGE_NAME")

BENCHMARK   = "sql-repair-env"
MAX_STEPS   = 5
TEMPERATURE = 0.0
MAX_TOKENS  = 600

ALL_TASKS = [
    ("easy_1","easy"),   ("easy_2","easy"),   ("easy_3","easy"),
    ("easy_4","easy"),   ("easy_5","easy"),   ("easy_6","easy"),   ("easy_7","easy"),
    ("medium_1","medium"),("medium_2","medium"),("medium_3","medium"),
    ("medium_4","medium"),("medium_5","medium"),("medium_6","medium"),("medium_7","medium"),
    ("hard_1","hard"),   ("hard_2","hard"),   ("hard_3","hard"),
    ("hard_4","hard"),   ("hard_5","hard"),   ("hard_6","hard"),
]

DIAGNOSTIC_PROMPT = """You are an expert SQL developer diagnosing a broken SQL query.

Given the broken query and database schema, write ONE diagnostic SQL query to understand the problem.
Good diagnostics: check table relationships, verify column names, count rows, check data samples.

Return ONLY the diagnostic SQL query — no explanation, no markdown.
"""

FIX_PROMPT = """You are an expert SQL developer fixing a broken SQL query.

You have already run diagnostic queries to understand the database.
Now return ONLY the corrected SQL query — no markdown, no explanation, no code blocks.

Bug categories to watch for:
1. SYNTAX: SELCT→SELECT, FORM→FROM, WERE→WHERE, ORDR→ORDER, GROUB→GROUP, CONT→COUNT, DESTINCT→DISTINCT
2. JOIN: Wrong column in ON clause — check which foreign key links which table
3. HAVING vs WHERE: Use HAVING (not WHERE) after GROUP BY for aggregate conditions
4. AGGREGATE: Check if AVG/MAX/SUM/COUNT is correct for the task
5. JOIN TYPE: Use LEFT JOIN when result must include rows with no match
6. SELF-JOIN: manager_id references employees.id
7. GROUP BY: Non-aggregated columns in SELECT must be in GROUP BY
8. DUPLICATE COUNT: Use SUM(DISTINCT col) when multiple joins cause duplicates
"""


def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    done_val     = str(done).lower()
    error_val    = error if error else "null"
    action_short = action.replace("\n", " ")[:200]
    print(f"[STEP] step={step} action={action_short} reward={reward:.2f} done={done_val} error={error_val}", flush=True)


def log_end(task: str, success: bool, steps: int, score: float, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(f"[END] task={task} success={str(success).lower()} steps={steps} score={score:.3f} rewards={rewards_str}", flush=True)


def build_diagnostic_prompt(obs: Dict[str, Any]) -> str:
    return f"""Task: {obs.get('task_description', '')}

Database Schema:
{obs.get('db_schema', '')}

Broken Query:
{obs.get('broken_query', '')}

Bug Category: {obs.get('bug_category', 'unknown')}

Expected output columns: {obs.get('expected_columns', [])}
Expected row count: {obs.get('expected_row_count', '?')}

Write ONE diagnostic SQL query to explore the database and understand the bug.
Return ONLY the SQL query:"""


def build_fix_prompt(obs: Dict[str, Any], diag_result: Any) -> str:
    parts = [
        f"Task: {obs.get('task_description', '')}",
        "",
        "Database Schema:",
        obs.get("db_schema", ""),
        "",
        "Broken Query:",
        obs.get("broken_query", ""),
        "",
        f"Bug Category: {obs.get('bug_category', 'unknown')}",
        "",
        f"Expected output columns: {obs.get('expected_columns', [])}",
        f"Expected row count: {obs.get('expected_row_count', '?')}",
    ]

    if diag_result:
        parts += ["", f"Diagnostic query result (first 5 rows): {str(diag_result)[:300]}"]

    if obs.get("expected_result_preview"):
        parts += ["", f"Expected result preview: {str(obs['expected_result_preview'])[:300]}"]

    if obs.get("error_message"):
        parts += ["", f"Last error: {obs['error_message']}"]

    if obs.get("feedback") and obs.get("attempt_number", 0) > 0:
        parts += ["", f"Grader feedback: {obs['feedback']}"]

    if obs.get("result_diff"):
        parts += ["", f"Result diff: {obs['result_diff']}"]

    if obs.get("hint"):
        parts += ["", f"HINT: {obs['hint']}"]

    parts += ["", "Return ONLY the corrected SQL query:"]
    return "\n".join(parts)


def extract_sql(text: str) -> str:
    text = text.strip()
    if "```" in text:
        blocks = text.split("```")
        if len(blocks) >= 2:
            code = blocks[1].strip()
            if code.lower().startswith("sql"):
                code = code[3:].strip()
            return code
    return text.strip('"').strip("'")


class EnvClient:
    def __init__(self, base_url: str, timeout: int = 60):
        self.base    = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def health(self)  -> Dict: r = self.session.get(f"{self.base}/health", timeout=self.timeout); r.raise_for_status(); return r.json()
    def reset(self, task_id: str) -> Dict:
        r = self.session.post(f"{self.base}/reset", json={"task_id": task_id}, timeout=self.timeout)
        r.raise_for_status(); return r.json()

    def step(self, sql_query: str = "", diagnostic: str = "", explanation: str = "") -> Dict:
        r = self.session.post(f"{self.base}/step",
            json={"action": {"sql_query": sql_query, "diagnostic": diagnostic, "explanation": explanation}},
            timeout=self.timeout)
        r.raise_for_status(); return r.json()

    def close(self): self.session.close()


def run_task(task_id: str, env: EnvClient, client: OpenAI) -> None:
    rewards: List[float] = []
    steps_taken = 0
    score   = 0.001
    success = False

    log_start(task=task_id, env=BENCHMARK, model=MODEL_NAME)

    try:
        resp = env.reset(task_id)
        obs  = resp.get("observation", {})
        done = resp.get("done", False)
        diag_result = None

        for step in range(1, MAX_STEPS + 1):
            if done:
                break

            if step == 1 and obs.get("bug_category") in ("join", "self_join", "join_type", "duplicate_count"):
                # Phase 1: Run diagnostic on complex structural bugs
                diag_completion = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=[
                        {"role": "system", "content": DIAGNOSTIC_PROMPT},
                        {"role": "user",   "content": build_diagnostic_prompt(obs)},
                    ],
                    max_tokens=200,
                    temperature=TEMPERATURE,
                )
                diag_sql = extract_sql((diag_completion.choices[0].message.content or "").strip())

                # Run diagnostic (free — no attempt used)
                diag_resp   = env.step(diagnostic=diag_sql)
                diag_obs    = diag_resp.get("observation", {})
                diag_result = diag_obs.get("diagnostic_result", [])
                print(f"[DEBUG] Diagnostic ran: {diag_sql[:80]}... → {len(diag_result)} rows", flush=True)

                # Now fix based on diagnostic
                fix_completion = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=[
                        {"role": "system", "content": FIX_PROMPT},
                        {"role": "user",   "content": build_fix_prompt(obs, diag_result)},
                    ],
                    max_tokens=MAX_TOKENS,
                    temperature=TEMPERATURE,
                )
                sql = extract_sql((fix_completion.choices[0].message.content or "").strip())
            else:
                # Standard fix
                fix_completion = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=[
                        {"role": "system", "content": FIX_PROMPT},
                        {"role": "user",   "content": build_fix_prompt(obs, diag_result)},
                    ],
                    max_tokens=MAX_TOKENS,
                    temperature=TEMPERATURE,
                )
                sql = extract_sql((fix_completion.choices[0].message.content or "").strip())

            resp   = env.step(sql_query=sql)
            obs    = resp.get("observation", {})
            reward = float(resp.get("reward") or 0.001)
            done   = resp.get("done", False)
            error  = obs.get("error_message") or None

            reward = max(0.001, min(0.999, reward))
            rewards.append(reward)
            steps_taken = step

            log_step(step=step, action=sql, reward=reward, done=done, error=error)

            if done:
                break

        score   = max(rewards) if rewards else 0.001
        score   = max(0.001, min(0.999, score))
        success = score >= 0.99

    except Exception as exc:
        print(f"[DEBUG] Task {task_id} error: {exc}", flush=True)
        import traceback
        traceback.print_exc()
        score   = 0.001
        success = False

    finally:
        log_end(task=task_id, success=success, steps=steps_taken, score=score, rewards=rewards)


def main() -> None:
    if not API_KEY:
        raise SystemExit("API_KEY must be set.\n  export API_KEY=your_token_here")

    print(f"[DEBUG] API_BASE_URL={API_BASE_URL}", flush=True)
    print(f"[DEBUG] MODEL_NAME={MODEL_NAME}", flush=True)
    print(f"[DEBUG] ENV_URL={ENV_URL}", flush=True)
    print(f"[DEBUG] Total tasks: {len(ALL_TASKS)}", flush=True)

    client = OpenAI(base_url=API_BASE_URL, api_key=API_KEY)
    env    = EnvClient(ENV_URL)

    try:
        health = env.health()
        print(f"[DEBUG] Health: {health}", flush=True)
    except Exception as e:
        print(f"[DEBUG] Health check failed: {e}", flush=True)

    try:
        for task_id, _diff in ALL_TASKS:
            run_task(task_id, env, client)
    finally:
        env.close()


if __name__ == "__main__":
    main()
