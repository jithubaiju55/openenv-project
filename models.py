"""
models.py - SQL Repair Environment v4
Advanced observation with execution results, diffs, and progress tracking.
"""

from typing import Optional, List, Dict, Any
from pydantic import Field
from openenv.core.env_server import Action, Observation, State


class SQLAction(Action):
    """
    Two action types:
      1. sql_query  — Submit a fix attempt (graded, counts as an attempt)
      2. diagnostic — Run any SQL to explore the database (not graded, free)
    """
    sql_query: str = Field(
        default="",
        description="The corrected SQL query to submit for grading"
    )
    diagnostic: str = Field(
        default="",
        description="Optional: run a diagnostic SQL query to explore the DB without using an attempt"
    )
    explanation: str = Field(
        default="",
        description="Optional agent reasoning — useful for debugging"
    )


class SQLObservation(Observation):
    """
    Rich observation with execution results, diffs, and progress signals.
    done and reward are inherited from Observation base class.
    """
    # ── Core task info ─────────────────────────────────────────────────────────
    broken_query: str = Field(..., description="The broken SQL query the agent must fix")
    db_schema: str = Field(..., description="Full database schema with table definitions and relationships")
    task_description: str = Field(default="", description="Natural language description of what the fixed query should return")
    task_id: str = Field(default="", description="Task identifier e.g. hard_3")
    difficulty: str = Field(default="", description="easy | medium | hard")
    bug_category: str = Field(default="", description="The bug category: syntax | join | having | subquery | self_join | group_by | duplicate_count")

    # ── Episode progress ───────────────────────────────────────────────────────
    attempt_number: int = Field(default=0, description="Current attempt number (0 = just reset)")
    max_attempts: int = Field(default=5, description="Maximum allowed attempts per episode")
    best_score_so_far: float = Field(default=0.001, description="Best score achieved so far in this episode")
    improving: bool = Field(default=False, description="True if last attempt scored higher than previous best")

    # ── Grader feedback ────────────────────────────────────────────────────────
    error_message: str = Field(default="", description="SQL execution error from the last attempt")
    feedback: str = Field(default="", description="Detailed grader feedback with per-component scores")
    hint: str = Field(default="", description="Bug-specific hint, revealed after 2 failed attempts")

    # ── Execution results (key differentiator) ─────────────────────────────────
    your_result_preview: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="First 5 rows returned by your last submitted query (empty on first reset)"
    )
    expected_result_preview: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="First 5 rows of the expected output — shows agent what to aim for"
    )
    result_diff: str = Field(
        default="",
        description="Human-readable diff showing what's different between your result and expected"
    )

    # ── Metadata hints ─────────────────────────────────────────────────────────
    expected_columns: List[str] = Field(default_factory=list, description="Expected output column names")
    expected_row_count: int = Field(default=-1, description="Expected number of output rows")
    your_row_count: int = Field(default=-1, description="Rows returned by your last query")

    # ── Diagnostic result ──────────────────────────────────────────────────────
    diagnostic_result: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Result of diagnostic SQL query (if action.diagnostic was set)"
    )
    diagnostic_error: str = Field(
        default="",
        description="Error from diagnostic query (if it failed)"
    )


class SQLState(State):
    """
    Episode metadata. episode_id and step_count inherited from State.
    """
    task_id: str = Field(default="", description="Current task identifier")
    difficulty: str = Field(default="", description="Task difficulty level")
    bug_category: str = Field(default="", description="Bug category being tested")
    max_attempts: int = Field(default=5, description="Maximum steps per episode")
    last_score: float = Field(default=0.001, description="Score from most recent step")
    best_score: float = Field(default=0.001, description="Best score achieved in this episode")
    completed: bool = Field(default=False, description="True if agent achieved score >= 0.99")
    attempts_used: int = Field(default=0, description="Total attempts made")
    total_diagnostic_queries: int = Field(default=0, description="Total diagnostic queries run")
