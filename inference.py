"""
inference.py - SQL Repair Environment Baseline Agent
"""

import os
import sys
from typing import List, Optional

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from openai import OpenAI
from client import SQLRepairEnv
from models import SQLAction

HF_TOKEN         = os.getenv("HF_TOKEN")
LOCAL_IMAGE_NAME = os.getenv("LOCAL_IMAGE_NAME")

# SPACE_URL — validator injects this pointing to our HF Space
SPACE_URL = os.getenv(
    "SPACE_URL",
    "https://WALKMAN303-sql-repair-env.hf.space"
)
BENCHMARK = "sql-repair-env"
TASK_IDS  = ["easy", "medium", "hard"]
MAX_STEPS = 5

SYSTEM_PROMPT = """You are an expert SQL developer who fixes broken SQL queries.
Return ONLY the corrected SQL query. No explanation, no markdown, no code blocks.

Common bugs:
- Misspelled keywords: SELCT->SELECT, FORM->FROM, WERE->WHERE, ORDR->ORDER
- Wrong JOIN columns: check which columns link which tables
- WHERE vs HAVING: use HAVING with aggregate functions like AVG(), COUNT()
"""


def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    error_val    = error if error else "null"
    done_val     = str(done).lower()
    action_clean = action.replace("\n", " ")[:60]
    print(
        f"[STEP] step={step} action={action_clean!r} reward={reward:.2f} done={done_val} error={error_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(
        f"[END] success={str(success).lower()} steps={steps} score={score:.2f} rewards={rewards_str}",
        flush=True,
    )


def build_prompt(obs) -> str:
    parts = [
        f"Task: {obs.task_description}",
        "",
        "Database Schema:",
        obs.db_schema,
        "",
        "Broken Query:",
        obs.broken_query,
    ]
    if obs.error_message:
        parts += ["", f"Error: {obs.error_message}"]
    if obs.feedback and obs.attempt_number > 0:
        parts += ["", f"Grader feedback: {obs.feedback}"]
    if obs.hint:
        parts += ["", f"Hint: {obs.hint}"]
    parts += ["", "Return ONLY the fixed SQL query:"]
    return "\n".join(parts)


def run_task(env, client, model_name: str, task_id: str) -> float:
    rewards:     List[float] = []
    steps_taken: int         = 0
    score:       float       = 0.0
    success:     bool        = False

    log_start(task=task_id, env=BENCHMARK, model=model_name)

    try:
        result = env.reset(task_id=task_id)
        obs    = result.observation

        for step in range(1, MAX_STEPS + 1):
            if result.done:
                break

            prompt = build_prompt(obs)

            response = client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.7,
                max_tokens=500,
                stream=False,
            )
            fixed_query = response.choices[0].message.content.strip()
            fixed_query = fixed_query.replace("```sql", "").replace("```", "").strip()

            result      = env.step(SQLAction(sql_query=fixed_query))
            obs         = result.observation
            reward      = result.reward or 0.0
            done        = result.done
            error       = obs.error_message if obs.error_message else None

            rewards.append(reward)
            steps_taken = step

            log_step(step=step, action=fixed_query, reward=reward, done=done, error=error)

            if done:
                break

        score   = max(rewards) if rewards else 0.01
        score   = min(max(score, 0.01), 0.99)
        success = score >= 0.99

    except Exception as e:
        print(f"[DEBUG] Task error: {e}", flush=True)
        score   = 0.01
        success = False

    finally:
        log_end(success=success, steps=steps_taken, score=score, rewards=rewards)

    return score


def main():
    # Strict env vars — exactly as validator requires
    api_base_url = os.environ["API_BASE_URL"]
    api_key      = os.environ["API_KEY"]
    model_name   = os.getenv("MODEL_NAME", "Qwen/Qwen2.5-72B-Instruct")

    print(f"[DEBUG] API_BASE_URL={api_base_url}", flush=True)
    print(f"[DEBUG] MODEL_NAME={model_name}", flush=True)
    print(f"[DEBUG] SPACE_URL={SPACE_URL}", flush=True)
    print(f"[DEBUG] API_KEY present={bool(api_key)}", flush=True)

    client = OpenAI(
        base_url=api_base_url,
        api_key=api_key,
    )

    all_scores = {}

    # Wrap environment connection in try/except
    try:
        env_client = SQLRepairEnv(base_url=SPACE_URL)
        with env_client.sync() as env:
            for task_id in TASK_IDS:
                try:
                    score = run_task(env, client, model_name, task_id)
                except Exception as e:
                    print(f"[DEBUG] Task {task_id} error: {e}", flush=True)
                    log_start(task=task_id, env=BENCHMARK, model=model_name)
                    log_end(success=False, steps=0, score=0.01, rewards=[0.01])
                    score = 0.01
                all_scores[task_id] = score

    except Exception as e:
        # WebSocket connection failed — still emit required output
        print(f"[DEBUG] Connection error: {e}", flush=True)
        for task_id in TASK_IDS:
            if task_id not in all_scores:
                log_start(task=task_id, env=BENCHMARK, model=model_name)
                log_end(success=False, steps=0, score=0.01, rewards=[0.01])
                all_scores[task_id] = 0.0

    avg = sum(all_scores.values()) / len(all_scores) if all_scores else 0.0
    print(f"[SUMMARY] scores={all_scores} average={avg:.2f}", flush=True)


if __name__ == "__main__":
    main()
