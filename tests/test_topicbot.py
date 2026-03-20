import json
import time

import telecode.codex as codex
import telecode.topicbot as topicbot


def _dummy_telegram():
    return topicbot.TelegramConfig(bot_token="test-token")


def _capture_text_message(sink, value=1):
    def sender(*args, **kwargs):
        payload = dict(kwargs)
        if len(args) >= 2:
            payload.setdefault("chat_id", args[1])
        if len(args) >= 3:
            payload.setdefault("text", args[2])
        sink.append(payload)
        return value

    return sender


def _write_projects(tmp_path, projects, default_project=None):
    path = tmp_path / ".telecode.projects.json"
    payload = {"projects": projects}
    if default_project is not None:
        payload["default_project"] = default_project
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _message(chat_id, thread_id, text=None, message_id=1, **extra):
    message = {
        "message_id": message_id,
        "chat": {"id": chat_id},
        "message_thread_id": thread_id,
        "from": {"id": 672372661, "username": "shubhbali"},
    }
    if text is not None:
        message["text"] = text
    message.update(extra)
    return message


def test_plain_text_requires_project_selection_without_default(monkeypatch, tmp_path):
    state_file = str(tmp_path / ".telecode.state.json")
    project_a = tmp_path / "alpha"
    project_b = tmp_path / "beta"
    project_a.mkdir()
    project_b.mkdir()
    projects_file = _write_projects(
        tmp_path,
        [
            {"name": "alpha", "path": str(project_a)},
            {"name": "beta", "path": str(project_b)},
        ],
    )
    registry = topicbot.load_project_registry(projects_file)
    sent = []

    monkeypatch.setattr(topicbot, "_run_prompt", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError))
    monkeypatch.setattr(
        topicbot,
        "telegram_send_message",
        _capture_text_message(sent),
    )

    topicbot.process_update(
        {"update_id": 1, "message": _message(100, 10, "hello")},
        _dummy_telegram(),
        state_file,
        registry,
        "codex",
        timeout_s=None,
    )

    assert sent
    assert "Choose a project" in sent[0]["text"]
    assert sent[0]["message_thread_id"] == 10
    assert "inline_keyboard" in sent[0]["reply_markup"]


def test_project_selection_is_scoped_per_topic(monkeypatch, tmp_path):
    state_file = str(tmp_path / ".telecode.state.json")
    project_a = tmp_path / "alpha"
    project_b = tmp_path / "beta"
    project_a.mkdir()
    project_b.mkdir()
    projects_file = _write_projects(
        tmp_path,
        [
            {"name": "alpha", "path": str(project_a)},
            {"name": "beta", "path": str(project_b)},
        ],
    )
    registry = topicbot.load_project_registry(projects_file)

    monkeypatch.setattr(topicbot, "telegram_answer_callback_query", lambda *args, **kwargs: None)
    monkeypatch.setattr(topicbot, "telegram_send_message", lambda *args, **kwargs: 1)

    callback_a = {
        "update_id": 1,
        "callback_query": {
            "id": "cb-a",
            "data": "project:alpha",
            "message": {
                "message_id": 1,
                "chat": {"id": 100},
                "message_thread_id": 10,
            },
        },
    }
    callback_b = {
        "update_id": 2,
        "callback_query": {
            "id": "cb-b",
            "data": "project:beta",
            "message": {
                "message_id": 1,
                "chat": {"id": 100},
                "message_thread_id": 11,
            },
        },
    }

    topicbot.process_update(callback_a, _dummy_telegram(), state_file, registry, "codex", timeout_s=None)
    topicbot.process_update(callback_b, _dummy_telegram(), state_file, registry, "codex", timeout_s=None)

    state = topicbot.load_state(state_file)
    assert state["scopes"]["100:10"]["project"] == "alpha"
    assert state["scopes"]["100:11"]["project"] == "beta"


def test_default_project_runs_codex_in_selected_repo(monkeypatch, tmp_path):
    state_file = str(tmp_path / ".telecode.state.json")
    project_a = tmp_path / "alpha"
    project_a.mkdir()
    projects_file = _write_projects(
        tmp_path,
        [{"name": "alpha", "path": str(project_a)}],
        default_project="alpha",
    )
    registry = topicbot.load_project_registry(projects_file)
    sent = []
    captured = {}

    def fake_codex(
        prompt,
        session_id,
        timeout_s,
        image_paths,
        cwd,
        sandbox_mode,
        approval_policy,
        process_callback=None,
        event_callback=None,
    ):
        captured["prompt"] = prompt
        captured["cwd"] = cwd
        captured["sandbox_mode"] = sandbox_mode
        captured["approval_policy"] = approval_policy
        captured["image_paths"] = image_paths
        return ("done", "sess-123", "logs")

    monkeypatch.setattr(topicbot, "ask_codex_exec", fake_codex)
    monkeypatch.setattr(topicbot, "telegram_send_message", _capture_text_message(sent))

    topicbot.process_update(
        {"update_id": 1, "message": _message(200, 22, "fix the bug")},
        _dummy_telegram(),
        state_file,
        registry,
        "codex",
        timeout_s=None,
    )

    for _ in range(50):
        if topicbot._get_active_task(200, 22) is None and sent:
            break
        time.sleep(0.02)
    state = topicbot.load_state(state_file)
    assert captured["cwd"] == str(project_a)
    assert captured["sandbox_mode"] == "danger-full-access"
    assert captured["approval_policy"] == "dangerous"
    assert captured["image_paths"] == []
    assert state["scopes"]["200:22"]["sessions"]["codex"] == "sess-123"
    assert sent[-1]["text"] == "done"


def test_end_command_clears_scope_and_requests_topic_delete(monkeypatch, tmp_path):
    state_file = str(tmp_path / ".telecode.state.json")
    project_a = tmp_path / "alpha"
    project_a.mkdir()
    projects_file = _write_projects(
        tmp_path,
        [{"name": "alpha", "path": str(project_a)}],
        default_project="alpha",
    )
    registry = topicbot.load_project_registry(projects_file)
    state = topicbot.load_state(state_file)
    scope = topicbot.ensure_scope(state, 300, 33)
    scope["project"] = "alpha"
    topicbot.save_state(state_file, state)
    deleted = []

    monkeypatch.setattr(topicbot, "telegram_delete_forum_topic", lambda *args, **kwargs: deleted.append((args, kwargs)))
    monkeypatch.setattr(topicbot, "telegram_send_message", lambda *args, **kwargs: 1)

    topicbot.process_update(
        {"update_id": 1, "message": _message(300, 33, "/end")},
        _dummy_telegram(),
        state_file,
        registry,
        "codex",
        timeout_s=None,
    )

    state = topicbot.load_state(state_file)
    assert "300:33" not in state["scopes"]
    assert deleted


def test_photo_message_starts_prompt_task_with_caption_and_image(monkeypatch, tmp_path):
    state_file = str(tmp_path / ".telecode.state.json")
    project_a = tmp_path / "alpha"
    project_a.mkdir()
    projects_file = _write_projects(
        tmp_path,
        [{"name": "alpha", "path": str(project_a)}],
        default_project="alpha",
    )
    registry = topicbot.load_project_registry(projects_file)
    captured = {}

    monkeypatch.setattr(topicbot, "telegram_download_file", lambda *args, **kwargs: (b"img", "photo.jpg"))

    def fake_start_prompt_task(**kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(topicbot, "_start_prompt_task", fake_start_prompt_task)
    monkeypatch.setattr(topicbot, "telegram_send_message", lambda *args, **kwargs: 1)

    topicbot.process_update(
        {
            "update_id": 1,
            "message": _message(
                400,
                44,
                text=None,
                caption="inspect this",
                photo=[{"file_id": "file-1", "file_size": 10}],
            ),
        },
        _dummy_telegram(),
        state_file,
        registry,
        "codex",
        timeout_s=None,
    )

    assert captured["prompt"] == "inspect this"
    assert len(captured["image_paths"]) == 1
    assert captured["project"].name == "alpha"
    assert captured["scope_snapshot"]["project"] == "alpha"
    assert captured["image_paths"][0].endswith(".jpg")
    for path in captured["image_paths"]:
        assert topicbot.os.path.exists(path)
        topicbot.os.remove(path)


def test_image_document_starts_prompt_task_with_empty_prompt(monkeypatch, tmp_path):
    state_file = str(tmp_path / ".telecode.state.json")
    project_a = tmp_path / "alpha"
    project_a.mkdir()
    projects_file = _write_projects(
        tmp_path,
        [{"name": "alpha", "path": str(project_a)}],
        default_project="alpha",
    )
    registry = topicbot.load_project_registry(projects_file)
    captured = {}

    monkeypatch.setattr(topicbot, "telegram_download_file", lambda *args, **kwargs: (b"img", "image.png"))

    def fake_start_prompt_task(**kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(topicbot, "_start_prompt_task", fake_start_prompt_task)
    monkeypatch.setattr(topicbot, "telegram_send_message", lambda *args, **kwargs: 1)

    topicbot.process_update(
        {
            "update_id": 1,
            "message": _message(
                500,
                55,
                text=None,
                document={"file_id": "file-2", "mime_type": "image/png"},
            ),
        },
        _dummy_telegram(),
        state_file,
        registry,
        "claude",
        timeout_s=None,
    )

    assert captured["prompt"] == ""
    assert len(captured["image_paths"]) == 1
    assert captured["image_paths"][0].endswith(".png")
    for path in captured["image_paths"]:
        assert topicbot.os.path.exists(path)
        topicbot.os.remove(path)


def test_run_prompt_passes_image_paths_to_claude(monkeypatch, tmp_path):
    project_a = tmp_path / "alpha"
    project_a.mkdir()
    project = topicbot.ProjectConfig(name="alpha", path=str(project_a))
    scope = {"engine": "claude", "sessions": {"claude": None, "codex": None}}
    captured = {}

    def fake_claude(prompt, session_id, timeout_s, image_paths=None):
        captured["prompt"] = prompt
        captured["session_id"] = session_id
        captured["image_paths"] = image_paths
        return "done"

    monkeypatch.setattr(topicbot, "ask_claude_code", fake_claude)

    result = topicbot._run_prompt(
        prompt="look at this",
        scope=scope,
        project=project,
        timeout_s=None,
        default_engine="claude",
        image_paths=["/tmp/image.jpg"],
    )

    assert result == "done"
    assert captured["image_paths"] == ["/tmp/image.jpg"]
    assert scope["sessions"]["claude"]


def test_start_prompt_task_cleans_up_temp_images(monkeypatch, tmp_path):
    state_file = str(tmp_path / ".telecode.state.json")
    project_a = tmp_path / "alpha"
    project_a.mkdir()
    state = topicbot.load_state(state_file)
    scope = topicbot.ensure_scope(state, 600, 66)
    scope["engine"] = "codex"
    scope["project"] = "alpha"
    topicbot.save_state(state_file, state)
    project = topicbot.ProjectConfig(name="alpha", path=str(project_a))
    temp_image = tmp_path / "temp.png"
    temp_image.write_bytes(b"img")

    monkeypatch.setattr(topicbot, "_run_prompt", lambda *args, **kwargs: "done")
    monkeypatch.setattr(topicbot, "telegram_send_message", lambda *args, **kwargs: 1)
    monkeypatch.setattr(topicbot, "telegram_edit_message_text", lambda *args, **kwargs: None)

    started = topicbot._start_prompt_task(
        telegram=_dummy_telegram(),
        state_file=state_file,
        registry=topicbot.ProjectRegistry(projects={"alpha": project}, default_project="alpha"),
        default_engine="codex",
        timeout_s=None,
        chat_id=600,
        thread_id=66,
        message_id=1,
        title="topic",
        prompt="inspect",
        project=project,
        scope_snapshot=scope,
        image_paths=[str(temp_image)],
    )

    assert started is True
    for _ in range(50):
        if not temp_image.exists():
            break
        time.sleep(0.02)
    assert not temp_image.exists()


def test_start_prompt_task_emits_append_only_progress_log_and_final_answer(monkeypatch, tmp_path):
    state_file = str(tmp_path / ".telecode.state.json")
    project_a = tmp_path / "alpha"
    project_a.mkdir()
    state = topicbot.load_state(state_file)
    scope = topicbot.ensure_scope(state, 700, 77)
    scope["engine"] = "codex"
    scope["project"] = "alpha"
    topicbot.save_state(state_file, state)
    project = topicbot.ProjectConfig(name="alpha", path=str(project_a))

    sent = []
    message_ids = iter(range(1, 50))

    def fake_send_message(*args, **kwargs):
        payload = dict(kwargs)
        if len(args) >= 3:
            payload.setdefault("text", args[2])
        sent.append(payload["text"])
        return next(message_ids)

    def fake_run_prompt(*args, **kwargs):
        callback = kwargs["event_callback"]
        callback({"type": "thread.started", "thread_id": "thread-1234"})
        callback(
            {
                "type": "item.started",
                "item": {"type": "command_execution", "command": "echo hi", "status": "in_progress"},
            }
        )
        callback(
            {
                "type": "item.completed",
                "item": {"type": "command_execution", "command": "echo hi", "exit_code": 0},
            }
        )
        callback(
            {
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "done"},
            }
        )
        return "final answer"

    monkeypatch.setattr(topicbot, "_run_prompt", fake_run_prompt)
    monkeypatch.setattr(topicbot, "telegram_send_message", fake_send_message)

    started = topicbot._start_prompt_task(
        telegram=_dummy_telegram(),
        state_file=state_file,
        registry=topicbot.ProjectRegistry(projects={"alpha": project}, default_project="alpha"),
        default_engine="codex",
        timeout_s=None,
        chat_id=700,
        thread_id=77,
        message_id=1,
        title="topic",
        prompt="inspect",
        project=project,
        scope_snapshot=scope,
        image_paths=[],
    )

    assert started is True
    for _ in range(50):
        if topicbot._get_active_task(700, 77) is None:
            break
        time.sleep(0.02)
    time.sleep(0.05)

    assert any(text.startswith("Progress Log\n") for text in sent)
    assert not any(text.startswith("Progress\n") for text in sent)
    assert sent[-1] == "final answer"
    state = topicbot.load_state(state_file)
    task_journal = state["scopes"]["700:77"]["task_journal"]
    assert task_journal["active_task_id"] == ""
    task = task_journal["tasks"][-1]
    assert task["status"] == "done"
    assert task["final_message_sent"] is True
    assert "Completed." in task["log_lines"]
    assert "Final answer follows below." in task["log_lines"]


def test_send_scope_message_splits_long_text_without_truncation(monkeypatch):
    sent = []
    message_ids = iter(range(1, 20))

    def fake_send(*args, **kwargs):
        payload = dict(kwargs)
        if len(args) >= 2:
            payload.setdefault("chat_id", args[1])
        if len(args) >= 3:
            payload.setdefault("text", args[2])
        sent.append(payload)
        return next(message_ids)

    monkeypatch.setattr(topicbot, "telegram_send_message", fake_send)

    text = ("A" * 3400) + "\n" + ("B" * 3400) + "\n" + ("C" * 400)
    last_message_id = topicbot._send_scope_message(
        _dummy_telegram(),
        123,
        45,
        99,
        text,
        reply_markup={"inline_keyboard": [[{"text": "x", "callback_data": "y"}]]},
    )

    assert len(sent) == 3
    assert "".join(item["text"] for item in sent) == text
    assert sent[0]["reply_to_message_id"] == 99
    assert "reply_to_message_id" not in sent[1]
    assert "reply_to_message_id" not in sent[2]
    assert "reply_markup" not in sent[0]
    assert "reply_markup" not in sent[1]
    assert sent[2]["reply_markup"]["inline_keyboard"][0][0]["text"] == "x"
    assert all(item["message_thread_id"] == 45 for item in sent)
    assert last_message_id == 3


def test_extract_last_agent_message_prefers_last_turn():
    output = "\n".join(
        [
            json.dumps({"type": "thread.started", "thread_id": "thread-1"}),
            json.dumps({"type": "turn.started"}),
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "old answer"}}),
            json.dumps({"type": "turn.completed"}),
            json.dumps({"type": "turn.started"}),
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "new answer"}}),
            json.dumps({"type": "turn.completed"}),
        ]
    )

    assert codex._extract_last_agent_message(output) == "new answer"
