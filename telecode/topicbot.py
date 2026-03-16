from __future__ import annotations

import os
import subprocess
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any

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
    telegram_edit_message_text,
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


def load_state(path: str) -> dict[str, Any]:
    with _STATE_LOCK:
        return _state_load(path)


def save_state(path: str, state: dict[str, Any]) -> None:
    with _STATE_LOCK:
        _state_save(path, state)


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
        updates = telegram_get_updates(
            telegram,
            offset=offset,
            timeout=poll_timeout_s,
            allowed_updates=["message", "callback_query"],
        )
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
    if not text:
        return

    if _handle_command(
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
        prompt=text,
        project=project,
        scope_snapshot=scope,
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

    progress_lines = ["Started."]
    progress_message_id = _send_scope_message(
        telegram,
        chat_id,
        thread_id,
        message_id,
        _render_progress_text(progress_lines),
    )

    def _progress_callback(event: dict[str, Any]) -> None:
        line = _format_progress_event(event)
        if not line:
            return
        if progress_lines and progress_lines[-1] == line:
            return
        progress_lines.append(line)
        del progress_lines[:-8]
        try:
            telegram_edit_message_text(
                telegram,
                chat_id,
                progress_message_id,
                _render_progress_text(progress_lines),
            )
        except Exception:
            pass

    def _runner(task: dict[str, Any]) -> None:
        try:
            answer = _run_prompt(
                prompt=prompt,
                scope=scope_snapshot,
                project=project,
                timeout_s=timeout_s,
                default_engine=default_engine,
                process_callback=lambda process: _set_task_process(chat_id, thread_id, process),
                event_callback=_progress_callback,
            )
            if task.get("cancel_requested"):
                try:
                    telegram_edit_message_text(
                        telegram,
                        chat_id,
                        progress_message_id,
                        _render_progress_text(progress_lines + ["Stopped."]),
                    )
                except Exception:
                    pass
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
            try:
                telegram_edit_message_text(
                    telegram,
                    chat_id,
                    progress_message_id,
                    _render_progress_text(progress_lines + ["Done."]),
                )
            except Exception:
                pass
            _send_scope_message(telegram, chat_id, thread_id, message_id, answer)
        except Exception as exc:
            if task.get("cancel_requested"):
                try:
                    telegram_edit_message_text(
                        telegram,
                        chat_id,
                        progress_message_id,
                        _render_progress_text(progress_lines + ["Stopped."]),
                    )
                except Exception:
                    pass
                return
            try:
                telegram_edit_message_text(
                    telegram,
                    chat_id,
                    progress_message_id,
                    _render_progress_text(progress_lines + [f"Error: {_truncate_inline(str(exc), 120)}"]),
                )
            except Exception:
                pass
            _send_scope_message(telegram, chat_id, thread_id, message_id, f"Error: {exc}")

    if not _launch_topic_task(chat_id, thread_id, _runner):
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            "This topic already has a running task. Use /stop or wait.",
        )
        return False
    return True


def _start_cli_task(
    telegram: TelegramConfig,
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

    def _runner(task: dict[str, Any]) -> None:
        output = _run_cli_command(
            command,
            cwd,
            timeout_s=timeout_s,
            process_callback=lambda process: _set_task_process(chat_id, thread_id, process),
        )
        if task.get("cancel_requested"):
            return
        _send_scope_message(telegram, chat_id, thread_id, message_id, output)

    if not _launch_topic_task(chat_id, thread_id, _runner):
        _send_scope_message(
            telegram,
            chat_id,
            thread_id,
            message_id,
            "This topic already has a running task. Use /stop or wait.",
        )
        return False

    _send_scope_message(telegram, chat_id, thread_id, message_id, "Working...")
    return True


def _run_prompt(
    prompt: str,
    scope: dict[str, Any],
    project: ProjectConfig,
    timeout_s: int | None,
    default_engine: str,
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
                image_paths=[],
            )
        sessions["claude"] = session_id
        return answer.strip()

    answer, new_session_id, _ = ask_codex_exec(
        _format_agent_prompt(project, prompt),
        session_id=session_id,
        timeout_s=timeout_s,
        image_paths=[],
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
