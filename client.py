"""
client.py — Python client for the SQL Repair Environment.

Usage:
    from client import SQLRepairEnv
    from models import SQLAction

    with SQLRepairEnv(base_url="https://your-space.hf.space").sync() as env:
        result = env.reset(task_id="easy")
        print(result.observation.broken_query)

        result = env.step(SQLAction(sql_query="SELECT name FROM employees"))
        print(f"Score: {result.reward}")
        print(f"Done:  {result.done}")

    # Async usage (for training loops)
    async with SQLRepairEnv(base_url=url) as env:
        result = await env.reset(task_id="hard")
        result = await env.step(SQLAction(sql_query="..."))
"""

from openenv.core.env_client import EnvClient
from openenv.core.client_types import StepResult
from models import SQLAction, SQLObservation, SQLState


class SQLRepairEnv(EnvClient[SQLAction, SQLObservation, SQLState]):
    """
    Client for the SQL Repair Environment.

    Handles all WebSocket communication with the server.
    You only need reset(), step(), and state().
    """

    # ── Serialize action → wire format ───────────────────────────────────────
    def _step_payload(self, action: SQLAction) -> dict:
        return {
            "sql_query":   action.sql_query,
            "explanation": action.explanation,
        }

    # ── Deserialize wire format → StepResult ─────────────────────────────────
    def _parse_result(self, payload: dict) -> StepResult:
        obs_data = payload.get("observation", {})
        done     = payload.get("done", False)
        reward   = payload.get("reward")

        return StepResult(
            observation=SQLObservation(
                done             = done,
                reward           = reward,
                broken_query     = obs_data.get("broken_query",     ""),
                db_schema        = obs_data.get("db_schema",        ""),
                error_message    = obs_data.get("error_message",    ""),
                task_description = obs_data.get("task_description", ""),
                task_id          = obs_data.get("task_id",          ""),
                difficulty       = obs_data.get("difficulty",       ""),
                attempt_number   = obs_data.get("attempt_number",   0),
                max_attempts     = obs_data.get("max_attempts",     5),
                feedback         = obs_data.get("feedback",         ""),
                hint             = obs_data.get("hint",             ""),
            ),
            reward=reward,
            done=done,
        )

    # ── Deserialize state payload → SQLState ─────────────────────────────────
    def _parse_state(self, payload: dict) -> SQLState:
        return SQLState(
            episode_id   = payload.get("episode_id"),
            step_count   = payload.get("step_count",   0),
            task_id      = payload.get("task_id",      ""),
            difficulty   = payload.get("difficulty",   ""),
            max_attempts = payload.get("max_attempts", 5),
            last_score   = payload.get("last_score",   0.0),
            completed    = payload.get("completed",    False),
        )
