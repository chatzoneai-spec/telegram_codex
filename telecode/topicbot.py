from __future__ import annotations

import os
import subprocess
import threading
import time
import uuid
from copy import deepcopy
from contextlib import contextmanager
from typing import Any, Optional

from telecode.claude import ask_claude_code
from telecode.codex import ask_codex_exec
from telecode.projects import ProjectConfig, ProjectRegistry, format_project_list, load_project_registry, project_keyboard
from telecode.state import (
    delete_scope,
    ensure_scope,
    get_update_offset,
    load_state as _state_load,
    save_state as _state_save,
    scope_key,
    set_scope_status,
    set_update_offset,
)
from telecode.telegram import (
    TelegramConfig,
    telegram_answer_callback_query,
    telegram_delete_forum_topic,
    telegram_delete_webhook,
    telegram_download_file,
    telegram_get_my_commands,
    telegram_get_updates,
    telegram_send_message,
    telegram_set_my_commands,
)


_BOT_COMMANDS = [
    {"command": "projects", "description": "List allowed projects"},
    {"command": "project", "description": "Choose project for this topic"},
    {"command": "engine", "description": "Switch engine: /engine claude|codex"},
    {"command": "claude", "description": "Use Claude for this topic"},
    {"command": "codex", "description": "Use Codex for this topic"},
    {"command": "status", "description": "Show git status for this project"},
    {"command": "deploy", "description": "Run this project's deploy command"},
    {"command": "cli", "description": "Run a shell command in this project"},
    {"command": "stop", "description": "Stop the running task in this topic"},
    {"command": "end", "description": "Delete this topic session"},
]

_STATE_LOCK = threading.RLock()
_TASKS_LOCK = threading.RLock()
_ACTIVE_TASKS: dict[str, dict[str, Any]] = {}
_TASK_HISTORY_LIMIT = 20
_TASK_LOG_BATCH_SIZE = 8
_TASK_DELIVERY_POLL_S = 1.0
_TASK_HEARTBEAT_INTERVAL_S = 30.0
_TASK_HEARTBEAT_POLL_S = 5.0
_TERMINAL_TASK_STATUSES = {"done", "failed", "stopped"}


def load_state(path: str) -> dict[str, Any]:
    with _STATE_LOCK:
        return _state_load(path)


def save_state(path: str, state: dict[str, Any]) -> None:
    with _STATE_LOCK:
        _state_save(path, state)


def _mutate_state(path: str, mutator) -> Any:
    with _STATE_LOCK:
        state = _state_load(path)
        result = mutator(state)
        _state_save(path, state)
        return result


def _read_state(path: str, reader) -> Any:
    with _STATE_LOCK:
        state = _state_load(path)
        return reader(state)


def _scope_id(chat_id: int, thread_id: int | None) -> str:
    return scope_key(chat_id, thread_id)


def _get_active_task(chat_id: int, thread_id: int | None) -> dict[str, Any] | None:
    key = _scope_id(chat_id, thread_id)
    with _TASKS_LOCK:
        task = _ACTIVE_TASKS.get(key)
        if not task:
            return None
        thread = task.get("thread")
        if isinstance(thread, threading.Thread) and not thread.is_alive():
            _ACTIVE_TASKS.pop(key, None)
            return None
        return task


def _set_task_process(chat_id: int, thread_id: int | None, process: subprocess.Popen[str] | None) -> None:
    task = _get_active_task(chat_id, thread_id)
    if task is not None:
        task["process"] = process


def _clear_active_task(chat_id: int, thread_id: int | None, expected: dict[str, Any]) -> None:
    key = _scope_id(chat_id, thread_id)
    with _TASKS_LOCK:
        if _ACTIVE_TASKS.get(key) is expected:
            _ACTIVE_TASKS.pop(key, None)


def _stop_active_task(chat_id: int, thread_id: int | None) -> bool:
    task = _get_active_task(chat_id, thread_id)
    if task is None:
        return False
    task["cancel_requested"] = True
    process = task.get("process")
    if process is not None and process.poll() is None:
        try:
            process.terminate()
        except Exception:
            pass
    return True


def _launch_topic_task(
    chat_id: int,
    thread_id: int | None,
    runner,
) -> bool:
    key = _scope_id(chat_id, thread_id)
    with _TASKS_LOCK:
        existing = _ACTIVE_TASKS.get(key)
        if existing:
            thread = existing.get("thread")
            if isinstance(thread, threading.Thread) and thread.is_alive():
                return False
        task: dict[str, Any] = {
            "process": None,
            "cancel_requested": False,
        }

        def _target() -> None:
            try:
                runner(task)
            finally:
                _clear_active_task(chat_id, thread_id, task)

        thread = threading.Thread(
            target=_target,
            name=f"telecode-topic-{chat_id}-{thread_id or 0}",
            daemon=True,
        )
        task["thread"] = thread
        _ACTIVE_TASKS[key] = task
        thread.start()
    return True


def _ensure_task_journal(scope: dict[str, Any]) -> dict[str, Any]:
    journal = scope.get("task_journal")
    if not isinstance(journal, dict):
        journal = {"active_task_id": "", "tasks": []}
        scope["task_journal"] = journal
    active_task_id = journal.get("active_task_id")
    if not isinstance(active_task_id, str):
        journal["active_task_id"] = ""
    tasks = journal.get("tasks")
    if not isinstance(tasks, list):
        journal["tasks"] = []
    return journal


def _prune_task_history(journal: dict[str, Any]) -> None:
    tasks = journal.get("tasks")
    if not isinstance(tasks, list) or len(tasks) <= _TASK_HISTORY_LIMIT:
        return
    active_task_id = str(journal.get("active_task_id") or "")
    kept: list[dict[str, Any]] = []
    to_drop = len(tasks) - _TASK_HISTORY_LIMIT
    for task in tasks:
        task_id = str(task.get("id") or "")
        if to_drop > 0 and task_id != active_task_id:
            to_drop -= 1
            continue
        kept.append(task)
    if len(kept) > _TASK_HISTORY_LIMIT:
        kept = kept[-_TASK_HISTORY_LIMIT:]
    journal["tasks"] = kept


def _find_task_record(journal: dict[str, Any], task_id: str) -> dict[str, Any] | None:
    tasks = journal.get("tasks")
    if not isinstance(tasks, list):
        return None
    for task in tasks:
        if isinstance(task, dict) and str(task.get("id") or "") == task_id:
            return task
    return None


def _create_task_record(
    state_file: str,
    chat_id: int,
    thread_id: int | None,
    *,
    task_id: str,
    kind: str,
    project: str,
    engine: str,
    title: str | None,
    detail: str,
) -> None:
    now = time.time()

    def mutator(state: dict[str, Any]) -> None:
        scope = ensure_scope(state, chat_id, thread_id, title=title)
        journal = _ensure_task_journal(scope)
        task = {
            "id": task_id,
            "kind": kind,
            "status": "running",
            "project": project,
            "engine": engine,
            "title": title or "",
            "detail": detail,
            "created_at": now,
            "updated_at": now,
            "last_activity_at": now,
            "last_heartbeat_at": 0.0,
            "last_line": "Started.",
            "log_lines": ["Started."],
            "next_unsent_index": 0,
            "final_message_text": "",
            "final_message_sent": False,
        }
        journal["tasks"].append(task)
        journal["active_task_id"] = task_id
        _prune_task_history(journal)

    _mutate_state(state_file, mutator)


def _discard_task_record(state_file: str, chat_id: int, thread_id: int | None, task_id: str) -> None:
    def mutator(state: dict[str, Any]) -> None:
        scope = ensure_scope(state, chat_id, thread_id)
        journal = _ensure_task_journal(scope)
        tasks = journal.get("tasks")
        if isinstance(tasks, list):
            journal["tasks"] = [
                task
                for task in tasks
                if not (isinstance(task, dict) and str(task.get("id") or "") == task_id)
            ]
        if str(journal.get("active_task_id") or "") == task_id:
            journal["active_task_id"] = ""

    _mutate_state(state_file, mutator)


def _append_task_log_line(
    state_file: str,
    chat_id: int,
    thread_id: int | None,
    task_id: str,
    line: str,
    *,
    heartbeat: bool = False,
) -> bool:
    now = time.time()

    def mutator(state: dict[str, Any]) -> bool:
        scope = ensure_scope(state, chat_id, thread_id)
        task = _find_task_record(_ensure_task_journal(scope), task_id)
        if task is None:
            return False
        log_lines = task.setdefault("log_lines", [])
        if log_lines and log_lines[-1] == line:
            return False
        log_lines.append(line)
        task["updated_at"] = now
        task["last_line"] = line
        if heartbeat:
            task["last_heartbeat_at"] = now
        else:
            task["last_activity_at"] = now
        return True

    return bool(_mutate_state(state_file, mutator))


def _get_task_snapshot(
    state_file: str,
    chat_id: int,
    thread_id: int | None,
    task_id: str,
) -> dict[str, Any] | None:
    def reader(state: dict[str, Any]) -> dict[str, Any] | None:
        scope = ensure_scope(state, chat_id, thread_id)
        task = _find_task_record(_ensure_task_journal(scope), task_id)
        if task is None:
            return None
        return deepcopy(task)

    return _read_state(state_file, reader)


def _peek_task_log_batch(
    state_file: str,
    chat_id: int,
    thread_id: int | None,
    task_id: str,
    batch_size: int = _TASK_LOG_BATCH_SIZE,
) -> list[str]:
    def reader(state: dict[str, Any]) -> list[str]:
        scope = ensure_scope(state, chat_id, thread_id)
        task = _find_task_record(_ensure_task_journal(scope), task_id)
        if task is None:
            return []
        log_lines = task.get("log_lines")
        if not isinstance(log_lines, list):
            return []
        next_unsent_index = int(task.get("next_unsent_index") or 0)
        return list(log_lines[next_unsent_index : next_unsent_index + batch_size])

    return _read_state(state_file, reader)


def _ack_task_log_batch(
    state_file: str,
    chat_id: int,
    thread_id: int | None,
    task_id: str,
    count: int,
) -> None:
    def mutator(state: dict[str, Any]) -> None:
        scope = ensure_scope(state, chat_id, thread_id)
        task = _find_task_record(_ensure_task_journal(scope), task_id)
        if task is None:
            return
        log_lines = task.get("log_lines")
        if not isinstance(log_lines, list):
            return
        next_unsent_index = int(task.get("next_unsent_index") or 0)
        task["next_unsent_index"] = min(len(log_lines), next_unsent_index + max(0, count))
        task["updated_at"] = time.time()

    _mutate_state(state_file, mutator)


def _mark_task_terminal(
    state_file: str,
    chat_id: int,
    thread_id: int | None,
    task_id: str,
    *,
    status: str,
    final_message_text: str = "",
) -> None:
    now = time.time()

    def mutator(state: dict[str, Any]) -> None:
        scope = ensure_scope(state, chat_id, thread_id)
        journal = _ensure_task_journal(scope)
        task = _find_task_record(journal, task_id)
        if task is None:
            return
        task["status"] = status
        task["updated_at"] = now
        if final_message_text:
            task["final_message_text"] = final_message_text
            task["final_message_sent"] = False
        if str(journal.get("active_task_id") or "") == task_id:
            journal["active_task_id"] = ""

    _mutate_state(state_file, mutator)


def _mark_task_final_message_sent(
    state_file: str,
    chat_id: int,
    thread_id: int | None,
    task_id: str,
) -> None:
    def mutator(state: dict[str, Any]) -> None:
        scope = ensure_scope(state, chat_id, thread_id)
        task = _find_task_record(_ensure_task_journal(scope), task_id)
        if task is None:
            return
        task["final_message_sent"] = True
        task["updated_at"] = time.time()

    _mutate_state(state_file, mutator)


def _start_task_delivery_loop(
    telegram: TelegramConfig,
    state_file: str,
    chat_id: int,
    thread_id: int | None,
    message_id: int,
    task_id: str,
) -> None:
    def _delivery() -> None:
        while True:
            batch = _peek_task_log_batch(state_file, chat_id, thread_id, task_id)
            if batch:
                try:
                    _send_scope_message(
                        telegram,
                        chat_id,
                        thread_id,
                        message_id,
                        _render_progress_log_text(batch),
                    )
                except Exception as exc:
                    time.sleep(_delivery_retry_delay(exc))
                    continue
                _ack_task_log_batch(state_file, chat_id, thread_id, task_id, len(batch))
                time.sleep(_TASK_DELIVERY_POLL_S)
                continue

            snapshot = _get_task_snapshot(state_file, chat_id, thread_id, task_id)
            if snapshot is None:
                return

            final_message_text = str(snapshot.get("final_message_text") or "")
            if snapshot.get("status") in _TERMINAL_TASK_STATUSES and final_message_text and not snapshot.get("final_message_sent"):
                try:
                    _send_scope_message(telegram, chat_id, thread_id, message_id, final_message_text)
                except Exception as exc:
                    time.sleep(_delivery_retry_delay(exc))
                    continue
                _mark_task_final_message_sent(state_file, chat_id, thread_id, task_id)
                time.sleep(_TASK_DELIVERY_POLL_S)
                continue

            log_lines = snapshot.get("log_lines")
            if not isinstance(log_lines, list):
                log_lines = []
            next_unsent_index = int(snapshot.get("next_unsent_index") or 0)
            final_sent = bool(snapshot.get("final_message_sent"))
            if (
                snapshot.get("status") in _TERMINAL_TASK_STATUSES
                and next_unsent_index >= len(log_lines)
                and (final_sent or not final_message_text)
            ):
                return
            time.sleep(_TASK_DELIVERY_POLL_S)

    thread = threading.Thread(
        target=_delivery,
        name=f"telecode-delivery-{chat_id}-{thread_id or 0}-{task_id[:8]}",
        daemon=True,
    )
    thread.start()


def _start_task_heartbeat_loop(
    state_file: str,
    chat_id: int,
    thread_id: int | None,
    task_id: str,
) -> None:
    def _heartbeat() -> None:
        while True:
            snapshot = _get_task_snapshot(state_file, chat_id, thread_id, task_id)
            if snapshot is None:
                return
            if snapshot.get("status") != "running":
                return
            now = time.time()
            last_activity_at = float(snapshot.get("last_activity_at") or snapshot.get("created_at") or now)
            last_heartbeat_at = float(snapshot.get("last_heartbeat_at") or 0.0)
            if (
                now - last_activity_at >= _TASK_HEARTBEAT_INTERVAL_S
                and now - last_heartbeat_at >= _TASK_HEARTBEAT_INTERVAL_S
            ):
                last_line = str(snapshot.get("last_line") or "Working.")
                _append_task_log_line(
                    state_file,
                    chat_id,
                    thread_id,
                    task_id,
                    f"Still working. Last step: {_truncate_inline(last_line, 120)}",
                    heartbeat=True,
                )
            time.sleep(_TASK_HEARTBEAT_POLL_S)

    thread = threading.Thread(
        target=_heartbeat,
        name=f"telecode-heartbeat-{chat_id}-{thread_id or 0}-{task_id[:8]}",
        daemon=True,
    )
    thread.start()


def run_polling(
    telegram: TelegramConfig,
    state_file: str,
    projects_file: str,
    default_engine: str,
    timeout_s: int | None = None,
    poll_timeout_s: int = 30,
) -> None:
    registry = load_project_registry(projects_file)
    _ensure_bot_commands(telegram)
    telegram_delete_webhook(telegram, drop_pending_updates=False)

    while True:
        state = load_state(state_file)
        offset = get_update_offset(state)
        try:
            updates = telegram_get_updates(
                telegram,
                offset=offset,
                timeout=poll_timeout_s,
                allowed_updates=["message", "callback_query"],
            )
        except Exception as exc:
            print(f"Warning: Telegram polling failed: {exc}")
            time.sleep(2)
            continue
        if not updates:
            continue

        for update in updates:
            update_id = int(update.get("update_id") or 0)
            try:
                process_update(update, telegram, state_file, registry, default_engine, timeout_s)
            except Exception as exc:
                print(f"Warning: failed to process update {update_id}: {exc}")
            state = load_state(state_file)
            set_update_offset(state, update_id + 1)
            save_state(state_file, state)


def process_update(
    update: dict[str, Any],
    telegram: TelegramConfig,
    state_file: str,
    registry: ProjectRegistry,
    default_engine: str,
    timeout_s: int | None = None,
) -> None:
    callback = update.get("callback_query")
    if isinstance(callback, dict):
        _handle_callback_query(callback, telegram, state_file, registry, default_engine, timeout_s)
        return

    message = update.get("message")
    if isinstance(message, dict):
        _handle_message(message, telegram, state_file, registry, default_engine, timeout_s)


def _handle_message(
    msg: dict[str, Any],
    telegram: TelegramConfig,
    state_file: str,
    registry: ProjectRegistry,
    default_engine: str,
    timeout_s: int | None,
) -> None:
    chat_id = int(msg["chat"]["id"])
    thread_id = _message_thread_id(msg)
    message_id = int(msg["message_id"])
    title = _topic_title(msg)

    state = load_state(state_file)
    scope = ensure_scope(state, chat_id, thread_id, title=title)
    if not scope.get("engine"):
        scope["engine"] = default_engine
    if not scope.get("project"):
        _maybe_assign_default_project(scope, registry)
    save_state(state_file, state)

    if "forum_topic_created" in msg:
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            _welcome_message(scope, registry),
            reply_markup=_project_keyboard_if_needed(scope, registry),
        )
        return

    if "forum_topic_closed" in msg:
        state = load_state(state_file)
        set_scope_status(state, chat_id, thread_id, "closed")
        save_state(state_file, state)
        return

    if "forum_topic_reopened" in msg:
        state = load_state(state_file)
        scope = set_scope_status(state, chat_id, thread_id, "active")
        if not scope.get("project"):
            _maybe_assign_default_project(scope, registry)
        save_state(state_file, state)
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            _welcome_message(scope, registry),
            reply_markup=_project_keyboard_if_needed(scope, registry),
        )
        return

    text = (msg.get("text") or "").strip()
    image_paths: list[str] = []
    has_image = "photo" in msg or _is_image_document(msg.get("document"))
    prompt = text

    if has_image:
        try:
            prompt, image_paths = _extract_image_prompt_and_paths(msg, telegram)
        except ValueError as exc:
            _send_scope_message(telegram, chat_id, thread_id, message_id, f"Error: {exc}")
            return
        except Exception as exc:
            _send_scope_message(telegram, chat_id, thread_id, message_id, f"Error: {exc}")
            return
    elif not text:
        return

    if not has_image and _handle_command(
        text,
        chat_id,
        thread_id,
        message_id,
        telegram,
        state_file,
        registry,
        default_engine,
        timeout_s,
    ):
        return

    state = load_state(state_file)
    scope = ensure_scope(state, chat_id, thread_id, title=title)
    _maybe_assign_default_project(scope, registry)
    save_state(state_file, state)
    if not scope.get("project"):
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            "Choose a project for this topic first with /project.",
            reply_markup=project_keyboard(registry),
        )
        return

    project = registry.projects[scope["project"]]
    _start_prompt_task(
        telegram=telegram,
        state_file=state_file,
        registry=registry,
        default_engine=default_engine,
        timeout_s=timeout_s,
        chat_id=chat_id,
        thread_id=thread_id,
        message_id=message_id,
        title=title,
        prompt=prompt,
        project=project,
        scope_snapshot=scope,
        image_paths=image_paths,
    )


def _handle_callback_query(
    callback: dict[str, Any],
    telegram: TelegramConfig,
    state_file: str,
    registry: ProjectRegistry,
    default_engine: str,
    timeout_s: int | None,
) -> None:
    callback_id = callback.get("id")
    if callback_id:
        telegram_answer_callback_query(telegram, callback_id)

    message = callback.get("message") or {}
    data = str(callback.get("data") or "").strip()
    if not data or not isinstance(message, dict):
        return

    chat = message.get("chat") or {}
    if "id" not in chat or "message_id" not in message:
        return

    chat_id = int(chat["id"])
    thread_id = _message_thread_id(message)
    message_id = int(message["message_id"])

    if data.startswith("project:"):
        project_name = data.split(":", 1)[1]
        try:
            _set_project_for_scope(state_file, chat_id, thread_id, project_name, registry)
        except Exception as exc:
            _send_scope_message(telegram, chat_id, thread_id, message_id, f"Error: {exc}")
            return
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            f"Switched this topic to project '{project_name}'.",
        )
        return

    # Reuse the normal message path for future callback types by converting them into text prompts.
    state = load_state(state_file)
    scope = ensure_scope(state, chat_id, thread_id)
    _maybe_assign_default_project(scope, registry)
    save_state(state_file, state)
    if not scope.get("project"):
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            "Choose a project for this topic first with /project.",
            reply_markup=project_keyboard(registry),
        )
        return

    project = registry.projects[scope["project"]]
    _start_prompt_task(
        telegram=telegram,
        state_file=state_file,
        registry=registry,
        default_engine=default_engine,
        timeout_s=timeout_s,
        chat_id=chat_id,
        thread_id=thread_id,
        message_id=message_id,
        title=None,
        prompt=data,
        project=project,
        scope_snapshot=scope,
    )


def _handle_command(
    text: str,
    chat_id: int,
    thread_id: int | None,
    message_id: int,
    telegram: TelegramConfig,
    state_file: str,
    registry: ProjectRegistry,
    default_engine: str,
    timeout_s: int | None,
) -> bool:
    if not text.startswith("/"):
        return False

    command, _, rest = text.partition(" ")
    command = command.split("@", 1)[0].lower()
    rest = rest.strip()
    state = load_state(state_file)
    scope = ensure_scope(state, chat_id, thread_id)
    if not scope.get("engine"):
        scope["engine"] = default_engine
    _maybe_assign_default_project(scope, registry)

    if command == "/projects":
        save_state(state_file, state)
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            format_project_list(registry, current=scope.get("project") or None),
            reply_markup=project_keyboard(registry),
        )
        return True

    if command == "/project":
        if rest:
            try:
                _set_project_for_scope(state_file, chat_id, thread_id, rest, registry)
            except Exception as exc:
                _send_scope_message(telegram, chat_id, thread_id, message_id, f"Error: {exc}")
                return True
            _send_scope_message(
                telegram,
                chat_id,
                thread_id,
                message_id,
                f"Switched this topic to project '{rest}'.",
            )
            return True

        save_state(state_file, state)
        current = scope.get("project") or "not selected"
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            f"Current project: {current}",
            reply_markup=project_keyboard(registry),
        )
        return True

    if command in {"/codex", "/claude"}:
        engine = command.lstrip("/")
        scope["engine"] = engine
        save_state(state_file, state)
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            f"Switched this topic to {engine}.",
        )
        return True

    if command == "/engine":
        if not rest:
            save_state(state_file, state)
            _send_scope_message(
                telegram,
                chat_id,
                thread_id,
                message_id,
                f"Current engine: {scope.get('engine') or default_engine}.",
            )
            return True
        engine = rest.lower()
        if engine not in {"claude", "codex"}:
            _send_scope_message(
                telegram,
                chat_id,
                thread_id,
                message_id,
                "Usage: /engine claude or /engine codex",
            )
            return True
        scope["engine"] = engine
        save_state(state_file, state)
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            f"Switched this topic to {engine}.",
        )
        return True

    if command == "/status":
        save_state(state_file, state)
        project = _require_project(scope, registry, telegram, chat_id, thread_id, message_id)
        if project is None:
            return True
        output = _run_cli_command("git status --short --branch", project.path, timeout_s=30)
        _send_scope_message(telegram, chat_id, thread_id, message_id, output)
        return True

    if command == "/deploy":
        save_state(state_file, state)
        project = _require_project(scope, registry, telegram, chat_id, thread_id, message_id)
        if project is None:
            return True
        if not project.deploy:
            _send_scope_message(
                telegram,
                chat_id,
                thread_id,
                message_id,
                f"No deploy command is configured for '{project.name}'.",
            )
            return True
        _start_cli_task(
            telegram=telegram,
            state_file=state_file,
            chat_id=chat_id,
            thread_id=thread_id,
            message_id=message_id,
            command=project.deploy,
            cwd=project.path,
            timeout_s=timeout_s or 300,
        )
        return True

    if command == "/cli":
        save_state(state_file, state)
        project = _require_project(scope, registry, telegram, chat_id, thread_id, message_id)
        if project is None:
            return True
        if not rest:
            _send_scope_message(telegram, chat_id, thread_id, message_id, "Usage: /cli <command>")
            return True
        _start_cli_task(
            telegram=telegram,
            state_file=state_file,
            chat_id=chat_id,
            thread_id=thread_id,
            message_id=message_id,
            command=rest,
            cwd=project.path,
            timeout_s=timeout_s or 300,
        )
        return True

    if command == "/stop":
        if _stop_active_task(chat_id, thread_id):
            _send_scope_message(telegram, chat_id, thread_id, message_id, "Stopping the current task in this topic.")
        else:
            _send_scope_message(telegram, chat_id, thread_id, message_id, "No running task in this topic.")
        return True

    if command == "/end":
        _stop_active_task(chat_id, thread_id)
        delete_scope(state, chat_id, thread_id)
        save_state(state_file, state)
        if thread_id:
            try:
                telegram_delete_forum_topic(telegram, chat_id, thread_id)
            except Exception as exc:
                _send_scope_message(
                    telegram,
                    chat_id,
                    thread_id,
                    message_id,
                    f"Session deleted. Telegram topic delete failed: {exc}",
                )
                return True
        _send_scope_message(telegram, chat_id, thread_id, message_id, "Session deleted.")
        return True

    return False


def _start_prompt_task(
    telegram: TelegramConfig,
    state_file: str,
    registry: ProjectRegistry,
    default_engine: str,
    timeout_s: int | None,
    chat_id: int,
    thread_id: int | None,
    message_id: int,
    title: str | None,
    prompt: str,
    project: ProjectConfig,
    scope_snapshot: dict[str, Any],
    image_paths: list[str] | None = None,
) -> bool:
    if _get_active_task(chat_id, thread_id) is not None:
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            "This topic already has a running task. Use /stop or wait.",
        )
        return False

    task_id = uuid.uuid4().hex
    _create_task_record(
        state_file,
        chat_id,
        thread_id,
        task_id=task_id,
        kind="prompt",
        project=project.name,
        engine=scope_snapshot.get("engine", default_engine),
        title=title,
        detail=_truncate_inline(prompt, 160),
    )

    def _progress_callback(event: dict[str, Any]) -> None:
        line = _format_progress_event(event)
        if not line:
            return
        _append_task_log_line(state_file, chat_id, thread_id, task_id, line)

    def _runner(task: dict[str, Any]) -> None:
        try:
            answer = _run_prompt(
                prompt=prompt,
                scope=scope_snapshot,
                project=project,
                timeout_s=timeout_s,
                default_engine=default_engine,
                image_paths=image_paths,
                process_callback=lambda process: _set_task_process(chat_id, thread_id, process),
                event_callback=_progress_callback,
            )
            if task.get("cancel_requested"):
                _append_task_log_line(state_file, chat_id, thread_id, task_id, "Stopped.")
                _mark_task_terminal(state_file, chat_id, thread_id, task_id, status="stopped")
                return
            state = load_state(state_file)
            updated_scope = ensure_scope(state, chat_id, thread_id, title=title)
            updated_scope.update(
                {
                    "engine": scope_snapshot.get("engine", default_engine),
                    "project": project.name,
                    "sessions": scope_snapshot.get("sessions", {"claude": None, "codex": None}),
                    "status": "active",
                }
            )
            save_state(state_file, state)
            _append_task_log_line(state_file, chat_id, thread_id, task_id, "Completed.")
            _append_task_log_line(state_file, chat_id, thread_id, task_id, "Final answer follows below.")
            _mark_task_terminal(
                state_file,
                chat_id,
                thread_id,
                task_id,
                status="done",
                final_message_text=answer,
            )
        except Exception as exc:
            if task.get("cancel_requested"):
                _append_task_log_line(state_file, chat_id, thread_id, task_id, "Stopped.")
                _mark_task_terminal(state_file, chat_id, thread_id, task_id, status="stopped")
                return
            _append_task_log_line(state_file, chat_id, thread_id, task_id, f"Failed: {_truncate_inline(str(exc), 120)}")
            _mark_task_terminal(
                state_file,
                chat_id,
                thread_id,
                task_id,
                status="failed",
                final_message_text=f"Error: {exc}",
            )
        finally:
            _cleanup_temp_paths(image_paths or [])

    if not _launch_topic_task(chat_id, thread_id, _runner):
        _discard_task_record(state_file, chat_id, thread_id, task_id)
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            "This topic already has a running task. Use /stop or wait.",
        )
        return False
    _start_task_delivery_loop(telegram, state_file, chat_id, thread_id, message_id, task_id)
    _start_task_heartbeat_loop(state_file, chat_id, thread_id, task_id)
    return True


def _start_cli_task(
    telegram: TelegramConfig,
    state_file: str,
    chat_id: int,
    thread_id: int | None,
    message_id: int,
    command: str,
    cwd: str,
    timeout_s: int,
) -> bool:
    if _get_active_task(chat_id, thread_id) is not None:
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            "This topic already has a running task. Use /stop or wait.",
        )
        return False

    task_id = uuid.uuid4().hex
    _create_task_record(
        state_file=state_file,
        chat_id=chat_id,
        thread_id=thread_id,
        task_id=task_id,
        kind="cli",
        project=cwd,
        engine="cli",
        title=None,
        detail=_truncate_inline(command, 160),
    )

    def _runner(task: dict[str, Any]) -> None:
        try:
            _append_task_log_line(state_file, chat_id, thread_id, task_id, f"Running: {_normalize_command(command)}")
            output = _run_cli_command(
                command,
                cwd,
                timeout_s=timeout_s,
                process_callback=lambda process: _set_task_process(chat_id, thread_id, process),
            )
            if task.get("cancel_requested"):
                _append_task_log_line(state_file, chat_id, thread_id, task_id, "Stopped.")
                _mark_task_terminal(state_file, chat_id, thread_id, task_id, status="stopped")
                return
            _append_task_log_line(state_file, chat_id, thread_id, task_id, "Completed.")
            _append_task_log_line(state_file, chat_id, thread_id, task_id, "Command output follows below.")
            _mark_task_terminal(
                state_file,
                chat_id,
                thread_id,
                task_id,
                status="done",
                final_message_text=output,
            )
        except Exception as exc:
            _append_task_log_line(state_file, chat_id, thread_id, task_id, f"Failed: {_truncate_inline(str(exc), 120)}")
            _mark_task_terminal(
                state_file,
                chat_id,
                thread_id,
                task_id,
                status="failed",
                final_message_text=f"Error: {exc}",
            )

    if not _launch_topic_task(chat_id, thread_id, _runner):
        _discard_task_record(state_file, chat_id, thread_id, task_id)
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            "This topic already has a running task. Use /stop or wait.",
        )
        return False

    _start_task_delivery_loop(telegram, state_file, chat_id, thread_id, message_id, task_id)
    _start_task_heartbeat_loop(state_file, chat_id, thread_id, task_id)
    return True


def _run_prompt(
    prompt: str,
    scope: dict[str, Any],
    project: ProjectConfig,
    timeout_s: int | None,
    default_engine: str,
    image_paths: list[str] | None = None,
    process_callback=None,
    event_callback=None,
) -> str:
    engine = (scope.get("engine") or default_engine or "codex").strip().lower()
    if engine not in {"claude", "codex"}:
        engine = "codex"

    sessions = scope.setdefault("sessions", {"claude": None, "codex": None})
    session_id = sessions.get(engine)

    if engine == "claude":
        session_id = session_id or str(uuid.uuid4())
        with _working_directory(project.path):
            answer = ask_claude_code(
                _format_agent_prompt(project, prompt),
                session_id=session_id,
                timeout_s=timeout_s,
                image_paths=image_paths or [],
            )
        sessions["claude"] = session_id
        return answer.strip()

    answer, new_session_id, _ = ask_codex_exec(
        _format_agent_prompt(project, prompt),
        session_id=session_id,
        timeout_s=timeout_s,
        image_paths=image_paths or [],
        cwd=project.path,
        sandbox_mode="danger-full-access",
        approval_policy="dangerous",
        process_callback=process_callback,
        event_callback=event_callback,
    )
    if new_session_id:
        sessions["codex"] = new_session_id
    return answer.strip()


def _set_project_for_scope(
    state_file: str,
    chat_id: int,
    thread_id: int | None,
    project_name: str,
    registry: ProjectRegistry,
) -> None:
    if project_name not in registry.projects:
        raise RuntimeError(f"Unknown project '{project_name}'.")

    state = load_state(state_file)
    scope = ensure_scope(state, chat_id, thread_id)
    scope["project"] = project_name
    scope["status"] = "active"
    scope["sessions"] = {"claude": None, "codex": None}
    save_state(state_file, state)


def _maybe_assign_default_project(scope: dict[str, Any], registry: ProjectRegistry) -> None:
    if scope.get("project"):
        return
    if registry.default_project:
        scope["project"] = registry.default_project


def _require_project(
    scope: dict[str, Any],
    registry: ProjectRegistry,
    telegram: TelegramConfig,
    chat_id: int,
    thread_id: int | None,
    message_id: int,
) -> ProjectConfig | None:
    _maybe_assign_default_project(scope, registry)
    project_name = scope.get("project") or ""
    if not project_name:
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            "Choose a project for this topic first with /project.",
            reply_markup=project_keyboard(registry),
        )
        return None
    project = registry.projects.get(project_name)
    if project is None:
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            f"Configured project '{project_name}' is missing from the registry.",
            reply_markup=project_keyboard(registry),
        )
        return None
    return project


def _ensure_bot_commands(telegram: TelegramConfig) -> None:
    existing = telegram_get_my_commands(telegram)
    existing_commands = {cmd.get("command") for cmd in existing if isinstance(cmd, dict)}
    missing = [cmd for cmd in _BOT_COMMANDS if cmd["command"] not in existing_commands]
    if missing:
        telegram_set_my_commands(telegram, existing + missing)


def _send_scope_message(
    telegram: TelegramConfig,
    chat_id: int,
    thread_id: int | None,
    reply_to_message_id: int | None,
    text: str,
    reply_markup: dict[str, Any] | None = None,
) -> int:
    return telegram_send_message(
        telegram,
        chat_id,
        _truncate_message(text),
        reply_to_message_id=reply_to_message_id,
        reply_markup=reply_markup,
        message_thread_id=thread_id,
    )


def _message_thread_id(message: dict[str, Any]) -> int | None:
    value = message.get("message_thread_id")
    return int(value) if isinstance(value, int) else None


def _topic_title(message: dict[str, Any]) -> str | None:
    created = message.get("forum_topic_created")
    if isinstance(created, dict):
        name = created.get("name")
        return str(name).strip() if name else None
    return None


def _extract_image_prompt_and_paths(
    msg: dict[str, Any],
    telegram: TelegramConfig,
) -> tuple[str, list[str]]:
    caption = msg.get("caption")
    prompt = caption if isinstance(caption, str) else ""
    if "photo" in msg:
        photo_id = _pick_best_photo_id(msg.get("photo", []))
        if not photo_id:
            raise ValueError("No photo data found.")
        image_bytes, file_path = telegram_download_file(telegram, photo_id)
        return prompt, [_write_temp_image(image_bytes, file_path)]

    document = msg.get("document") or {}
    file_id = document.get("file_id")
    if not file_id:
        raise ValueError("No document data found.")
    image_bytes, file_path = telegram_download_file(telegram, file_id)
    return prompt, [_write_temp_image(image_bytes, file_path)]


def _is_image_document(document: Optional[dict]) -> bool:
    if not isinstance(document, dict):
        return False
    mime = (document.get("mime_type") or "").lower()
    return mime.startswith("image/")


def _pick_best_photo_id(photos: list[dict]) -> Optional[str]:
    if not photos:
        return None

    def score(photo: dict) -> int:
        file_size = photo.get("file_size") or 0
        width = photo.get("width") or 0
        height = photo.get("height") or 0
        return file_size or (width * height)

    best = max(photos, key=score)
    return best.get("file_id")


def _write_temp_image(image_bytes: bytes, file_path: str) -> str:
    _, ext = os.path.splitext(file_path)
    suffix = ext if ext else ".jpg"
    temp_dir = _ensure_temp_dir()
    filename = f"image_{uuid.uuid4().hex}{suffix}"
    path = os.path.join(temp_dir, filename)
    with open(path, "wb") as handle:
        handle.write(image_bytes)
    return path


def _ensure_temp_dir() -> str:
    path = os.path.join(os.getcwd(), ".telecode_tmp")
    os.makedirs(path, exist_ok=True)
    return path


def _cleanup_temp_paths(paths: list[str]) -> None:
    for path in paths:
        if not path:
            continue
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError:
            pass


def _welcome_message(scope: dict[str, Any], registry: ProjectRegistry) -> str:
    project_name = scope.get("project") or "not selected"
    if project_name == "not selected":
        return (
            "New topic session created.\n"
            "Choose a project for this topic with /project."
        )
    return (
        "New topic session created.\n"
        f"Project: {project_name}\n"
        f"Engine: {scope.get('engine') or 'codex'}"
    )


def _project_keyboard_if_needed(
    scope: dict[str, Any],
    registry: ProjectRegistry,
) -> dict[str, list[list[dict[str, str]]]] | None:
    if scope.get("project"):
        return None
    return project_keyboard(registry)


def _format_agent_prompt(project: ProjectConfig, prompt: str) -> str:
    lines = [
        f"Project: {project.name}",
        f"Working directory: {project.path}",
    ]
    if project.branch:
        lines.append(f"Default branch: {project.branch}")
    if project.repo:
        lines.append(f"Git remote: {project.repo}")
    lines.extend(
        [
            "",
            "You are operating inside the selected project directory.",
            "Make code changes and run checks when the user asks for them.",
            "Keep replies concise and practical.",
            "",
            "User request:",
            prompt,
        ]
    )
    return "\n".join(lines)


def _run_cli_command(
    cmd: str,
    cwd: str,
    timeout_s: int,
    process_callback=None,
) -> str:
    process: subprocess.Popen[str] | None = None
    try:
        process = subprocess.Popen(
            cmd,
            shell=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=cwd,
        )
        if process_callback:
            process_callback(process)
        stdout, stderr = process.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        if process is not None:
            process.kill()
            process.communicate()
        return f"Command timed out after {timeout_s}s."
    except Exception as exc:
        return f"Command failed: {exc}"
    finally:
        if process_callback:
            process_callback(None)

    stdout = (stdout or "").strip()
    stderr = (stderr or "").strip()
    output = "\n".join(part for part in [stdout, stderr] if part)
    if not output:
        output = "Command finished with no output."
    return _truncate_message(output)


def _truncate_message(text: str, limit: int = 3500) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}\n...[truncated]"


def _truncate_inline(text: str, limit: int = 160) -> str:
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 3]}..."


def _render_progress_text(lines: list[str]) -> str:
    body = "\n".join(f"- {line}" for line in lines[-8:])
    return f"Progress\n{body}"


def _render_progress_log_text(lines: list[str]) -> str:
    body = "\n".join(f"- {line}" for line in lines)
    return f"Progress Log\n{body}"


def _delivery_retry_delay(exc: Exception) -> float:
    response = getattr(exc, "response", None)
    if response is not None and getattr(response, "status_code", None) == 429:
        try:
            payload = response.json()
        except Exception:
            payload = None
        if isinstance(payload, dict):
            parameters = payload.get("parameters")
            if isinstance(parameters, dict):
                retry_after = parameters.get("retry_after")
                try:
                    if retry_after is not None:
                        return max(float(retry_after), _TASK_DELIVERY_POLL_S)
                except (TypeError, ValueError):
                    pass
    return _TASK_DELIVERY_POLL_S


def _format_progress_event(event: dict[str, Any]) -> str | None:
    event_type = str(event.get("type") or "")
    if event_type == "thread.started":
        thread_id = event.get("thread_id")
        if isinstance(thread_id, str) and thread_id:
            return f"Session {thread_id[:12]} started."
        return "Session started."
    if event_type == "turn.started":
        return "Planning."
    if event_type != "item.started" and event_type != "item.completed":
        return None

    item = event.get("item")
    if not isinstance(item, dict):
        return None

    item_type = str(item.get("type") or "")
    if item_type == "agent_message":
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            return f"Agent: {_truncate_inline(text.strip(), 140)}"
        return None

    if item_type == "command_execution":
        command = _normalize_command(str(item.get("command") or ""))
        status = str(item.get("status") or "")
        if event_type == "item.started" or status == "in_progress":
            return f"Running: {command}"
        exit_code = item.get("exit_code")
        if exit_code is None:
            return f"Finished: {command}"
        return f"Finished ({exit_code}): {command}"

    return None


def _normalize_command(command: str) -> str:
    text = " ".join(command.split())
    prefix = "/bin/bash -lc "
    if text.startswith(prefix):
        text = text[len(prefix) :].strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        text = text[1:-1]
    return _truncate_inline(text, 140)


@contextmanager
def _working_directory(path: str):
    previous = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)
