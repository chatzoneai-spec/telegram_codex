import json
import re
import subprocess
import threading
from typing import Any, Callable, Optional


def ask_codex_exec(
    prompt: str,
    session_id: Optional[str],
    timeout_s: Optional[int],
    image_paths: Optional[list[str]] = None,
    cwd: Optional[str] = None,
    sandbox_mode: str = "workspace-write",
    approval_policy: str = "never",
    process_callback: Callable[[subprocess.Popen[str] | None], None] | None = None,
    event_callback: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[str, Optional[str], str]:
    """Run codex exec, optionally resuming a session, and return answer + session_id + logs."""
    use_images = image_paths or []
    cmd = _build_cmd(
        prompt,
        session_id,
        image_paths=use_images,
        cwd=cwd,
        sandbox_mode=sandbox_mode,
        approval_policy=approval_policy,
    )
    prompt_input = prompt if use_images else None
    stdout, stderr = _run_codex(
        cmd,
        timeout_s,
        prompt_input=prompt_input,
        cwd=cwd,
        process_callback=process_callback,
        event_callback=event_callback,
    )

    new_session_id = _extract_session_id(stdout + "\n" + stderr)
    answer = _extract_last_agent_message(stdout) or _extract_last_message(stdout)
    if not new_session_id:
        new_session_id = _extract_session_id(answer)

    if not answer:
        raise RuntimeError("Codex returned empty output.")

    logs = "\n".join([stdout, stderr]).strip()
    return answer, new_session_id or session_id, logs


def _build_cmd(
    prompt: str,
    session_id: Optional[str],
    image_paths: list[str],
    cwd: Optional[str],
    sandbox_mode: str,
    approval_policy: str,
) -> list[str]:
    if session_id:
        base = ["codex", "exec", "resume"]
        base.append("--json")
        if approval_policy in {"dangerous", "bypass"}:
            base.append("--dangerously-bypass-approvals-and-sandbox")
        elif approval_policy == "never":
            base.append("--full-auto")
        for path in image_paths:
            base.extend(["--image", path])
        base.append(session_id)
        if image_paths:
            return base
        base.append(prompt)
        return base
    base = ["codex", "exec"]
    base.append("--json")
    if cwd:
        base.extend(["--cd", cwd])
    if approval_policy in {"dangerous", "bypass"}:
        pass
    elif sandbox_mode:
        base.extend(["--sandbox", sandbox_mode])
    if approval_policy in {"dangerous", "bypass"}:
        base.append("--dangerously-bypass-approvals-and-sandbox")
    elif approval_policy == "never":
        base.append("--full-auto")
    for path in image_paths:
        base.extend(["--image", path])
    if image_paths:
        return base
    base.append(prompt)
    return base


def _run_codex(
    cmd: list[str],
    timeout_s: Optional[int],
    prompt_input: Optional[str] = None,
    cwd: Optional[str] = None,
    process_callback: Callable[[subprocess.Popen[str] | None], None] | None = None,
    event_callback: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[str, str]:
    process: subprocess.Popen[str] | None = None
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    stdout_exc: list[BaseException] = []
    stderr_exc: list[BaseException] = []

    def _reader(
        stream,
        sink: list[str],
        errors: list[BaseException],
        parse_events: bool,
    ) -> None:
        try:
            for line in iter(stream.readline, ""):
                sink.append(line)
                if parse_events and event_callback is not None:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    try:
                        event = json.loads(stripped)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(event, dict):
                        event_callback(event)
        except BaseException as exc:  # pragma: no cover - defensive
            errors.append(exc)
        finally:
            try:
                stream.close()
            except Exception:
                pass

    try:
        process = subprocess.Popen(
            cmd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE if prompt_input is not None else None,
            cwd=cwd,
            bufsize=1,
        )
        if process_callback:
            process_callback(process)
        stdout_thread = threading.Thread(
            target=_reader,
            args=(process.stdout, stdout_lines, stdout_exc, True),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_reader,
            args=(process.stderr, stderr_lines, stderr_exc, False),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        if prompt_input is not None and process.stdin is not None:
            process.stdin.write(prompt_input)
            process.stdin.close()
        process.wait(timeout=timeout_s)
        stdout_thread.join(timeout=2)
        stderr_thread.join(timeout=2)
    except subprocess.TimeoutExpired as exc:
        if process is not None:
            process.kill()
            process.wait(timeout=5)
        raise RuntimeError(f"Codex timed out after {timeout_s}s") from exc
    finally:
        if process_callback:
            process_callback(None)

    if process is None:
        raise RuntimeError("Codex failed: process did not start.")

    if stdout_exc:
        raise RuntimeError(f"Codex failed while reading stdout: {stdout_exc[0]}")
    if stderr_exc:
        raise RuntimeError(f"Codex failed while reading stderr: {stderr_exc[0]}")

    stdout = "".join(stdout_lines)
    stderr = "".join(stderr_lines)

    if process.returncode != 0:
        detail = (stderr or "").strip() or (stdout or "").strip() or f"exit code {process.returncode}"
        raise RuntimeError(f"Codex failed: {detail}")

    return stdout, stderr


def _extract_session_id(stdout: str) -> Optional[str]:
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        session_id = _pick_session_id(data)
        if session_id:
            return session_id
    return _extract_session_id_from_text(stdout)


def _pick_session_id(data: object, parent_key: Optional[str] = None) -> Optional[str]:
    if isinstance(data, dict):
        for key in (
            "session_id",
            "sessionId",
            "sessionID",
            "conversation_id",
            "conversationId",
            "conversationID",
            "thread_id",
            "threadId",
        ):
            value = data.get(key)
            if isinstance(value, str) and value:
                return value
        if parent_key in {"session", "conversation"}:
            value = data.get("id")
            if isinstance(value, str) and value:
                return value
        for key, value in data.items():
            found = _pick_session_id(value, parent_key=key)
            if found:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _pick_session_id(item, parent_key=parent_key)
            if found:
                return found
    return None


def _extract_session_id_from_text(text: str) -> Optional[str]:
    patterns = [
        r'"(?:session_id|sessionId|sessionID|conversation_id|conversationId|conversationID)"\s*:\s*"([^"]+)"',
        r'"session"\s*:\s*{[^}]*"id"\s*:\s*"([^"]+)"',
        r'"conversation"\s*:\s*{[^}]*"id"\s*:\s*"([^"]+)"',
        r'(?:session_id|sessionId|sessionID|conversation_id|conversationId|conversationID)\s*[:=]\s*([A-Za-z0-9_-]+)',
        r'session\s+id\s*[:=]\s*([A-Za-z0-9_-]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def _extract_last_message(output: str) -> str:
    if not output:
        return ""
    lines = [line.rstrip() for line in output.splitlines()]
    while lines and not lines[-1].strip():
        lines.pop()
    if not lines:
        return ""
    role_markers = {"assistant", "codex"}
    stop_prefixes = (
        "tokens used",
        "mcp startup",
        "reasoning summaries",
        "reasoning effort",
        "workdir:",
        "model:",
        "provider:",
        "approval:",
        "sandbox:",
        "session id:",
        "user",
        "thinking",
    )
    start_index = None
    for idx, line in enumerate(lines):
        if line.strip().lower() in role_markers:
            start_index = idx
    if start_index is None:
        return "\n".join(lines).strip()
    message_lines: list[str] = []
    for line in lines[start_index + 1 :]:
        stripped = line.strip()
        lowered = stripped.lower()
        if lowered in role_markers:
            break
        if any(lowered.startswith(prefix) for prefix in stop_prefixes):
            break
        message_lines.append(line)
    return "\n".join(message_lines).strip()


def _extract_last_agent_message(output: str) -> str:
    last = ""
    current_turn_messages: list[str] = []
    last_turn_messages: list[str] = []
    saw_turn = False
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            data = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if not isinstance(data, dict):
            continue
        event_type = data.get("type")
        if event_type == "turn.started":
            saw_turn = True
            current_turn_messages = []
            continue
        if event_type == "turn.completed":
            if current_turn_messages:
                last_turn_messages = list(current_turn_messages)
            continue
        if event_type != "item.completed":
            continue
        item = data.get("item")
        if not isinstance(item, dict):
            continue
        if item.get("type") != "agent_message":
            continue
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            last = text.strip()
            if saw_turn:
                current_turn_messages.append(last)
    if last_turn_messages:
        return last_turn_messages[-1]
    if current_turn_messages:
        return current_turn_messages[-1]
    return last
