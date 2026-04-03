"""
models.py — Type-safe data contracts for the SQL Repair Environment.

Every field is documented. Your IDE will autocomplete everything.
"""

from typing import Optional, List
from openenv.core.env_server import Action, Observation, State


class SQLAction(Action):
    """
    The action an AI agent takes: submit a (hopefully fixed) SQL query.

    Fields:
        sql_query   : The fixed SQL query the agent wants to run.
        explanation : Optional reasoning — useful for debugging agent behaviour.
    """
    sql_query: str
    explanation: str = ""


class SQLObservation(Observation):
    """
    What the agent sees after reset() or step().

    Inherited from Observation base:
        done   : bool              — True when the episode is over.
        reward : Optional[float]   — Score 0.0–1.0 at every step.

    Custom fields:
        broken_query     : The original broken SQL query the agent must fix.
        db_schema        : Human-readable description of available tables/columns.
        error_message    : SQL execution error from the last step (empty if no error).
        task_description : Natural-language description of what the query should do.
        task_id          : "easy" | "medium" | "hard"
        difficulty       : Same as task_id — for clarity.
        attempt_number   : How many step() calls have been made this episode.
        max_attempts     : Maximum allowed attempts before episode ends.
        feedback         : Detailed grader feedback on the last submission.
        hint             : Appears after 2 failed attempts to guide the agent.
    """
    broken_query: str
    db_schema: str
    error_message: str = ""
    task_description: str = ""
    task_id: str = ""
    difficulty: str = ""
    attempt_number: int = 0
    max_attempts: int = 5
    feedback: str = ""
    hint: str = ""


class SQLState(State):
    """
    Episode metadata returned by state().

    Inherited from State base:
        episode_id : Optional[str]  — Unique episode identifier.
        step_count : int            — Total steps taken this episode.

    Custom fields:
        task_id    : Which task is running.
        difficulty : Difficulty level of the current task.
        max_attempts : Maximum steps allowed.
        last_score   : Score from the most recent step.
        completed    : True if agent achieved score >= 1.0.
    """
    task_id: str = ""
    difficulty: str = ""
    max_attempts: int = 5
    last_score: float = 0.0
    completed: bool = False
