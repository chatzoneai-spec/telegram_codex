from __future__ import annotations

import json
import os
from copy import deepcopy
from typing import Any


def scope_key(chat_id: int, thread_id: int | None) -> str:
    return f"{chat_id}:{thread_id or 0}"


def load_state(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return _default_state()

    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return _default_state()

    if not isinstance(data, dict):
        return _default_state()

    result = _default_state()
    result["update_offset"] = int(data.get("update_offset") or 0)
    scopes = data.get("scopes")
    if isinstance(scopes, dict):
        result["scopes"] = scopes
    return result


def save_state(path: str, state: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)


def get_scope(state: dict[str, Any], chat_id: int, thread_id: int | None) -> dict[str, Any] | None:
    scopes = state.setdefault("scopes", {})
    return scopes.get(scope_key(chat_id, thread_id))


def ensure_scope(
    state: dict[str, Any],
    chat_id: int,
    thread_id: int | None,
    title: str | None = None,
) -> dict[str, Any]:
    scopes = state.setdefault("scopes", {})
    key = scope_key(chat_id, thread_id)
    scope = scopes.get(key)
    if not isinstance(scope, dict):
        scope = {
            "chat_id": chat_id,
            "thread_id": thread_id,
            "title": title or "",
            "status": "active",
            "engine": "",
            "project": "",
            "sessions": {
                "claude": None,
                "codex": None,
            },
        }
        scopes[key] = scope

    if title:
        scope["title"] = title
    scope["chat_id"] = chat_id
    scope["thread_id"] = thread_id
    scope.setdefault("status", "active")
    scope.setdefault("engine", "")
    scope.setdefault("project", "")
    journal = scope.get("task_journal")
    if not isinstance(journal, dict):
        scope["task_journal"] = {"active_task_id": "", "tasks": []}
    else:
        active_task_id = journal.get("active_task_id")
        if not isinstance(active_task_id, str):
            journal["active_task_id"] = ""
        tasks = journal.get("tasks")
        if not isinstance(tasks, list):
            journal["tasks"] = []
    sessions = scope.get("sessions")
    if not isinstance(sessions, dict):
        scope["sessions"] = {"claude": None, "codex": None}
    else:
        sessions.setdefault("claude", None)
        sessions.setdefault("codex", None)
    return scope


def delete_scope(state: dict[str, Any], chat_id: int, thread_id: int | None) -> None:
    state.setdefault("scopes", {}).pop(scope_key(chat_id, thread_id), None)


def set_scope_status(
    state: dict[str, Any],
    chat_id: int,
    thread_id: int | None,
    status: str,
) -> dict[str, Any]:
    scope = ensure_scope(state, chat_id, thread_id)
    scope["status"] = status
    return scope


def get_update_offset(state: dict[str, Any]) -> int:
    return int(state.get("update_offset") or 0)


def set_update_offset(state: dict[str, Any], offset: int) -> None:
    state["update_offset"] = max(0, int(offset))


def clone_scope(scope: dict[str, Any] | None) -> dict[str, Any] | None:
    if scope is None:
        return None
    return deepcopy(scope)


def _default_state() -> dict[str, Any]:
    return {
        "update_offset": 0,
        "scopes": {},
    }
