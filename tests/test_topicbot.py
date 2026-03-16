import json

import telecode.topicbot as topicbot


def _dummy_telegram():
    return topicbot.TelegramConfig(bot_token="test-token")


def _write_projects(tmp_path, projects, default_project=None):
    path = tmp_path / ".telecode.projects.json"
    payload = {"projects": projects}
    if default_project is not None:
        payload["default_project"] = default_project
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _message(chat_id, thread_id, text, message_id=1):
    return {
        "message_id": message_id,
        "chat": {"id": chat_id},
        "message_thread_id": thread_id,
        "text": text,
        "from": {"id": 672372661, "username": "shubhbali"},
    }


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
        lambda *args, **kwargs: sent.append(kwargs) or 1,
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

    def fake_codex(prompt, session_id, timeout_s, image_paths, cwd, sandbox_mode, approval_policy):
        captured["prompt"] = prompt
        captured["cwd"] = cwd
        captured["sandbox_mode"] = sandbox_mode
        captured["approval_policy"] = approval_policy
        return ("done", "sess-123", "logs")

    monkeypatch.setattr(topicbot, "ask_codex_exec", fake_codex)
    monkeypatch.setattr(topicbot, "telegram_send_message", lambda *args, **kwargs: sent.append(kwargs) or 1)

    topicbot.process_update(
        {"update_id": 1, "message": _message(200, 22, "fix the bug")},
        _dummy_telegram(),
        state_file,
        registry,
        "codex",
        timeout_s=None,
    )

    state = topicbot.load_state(state_file)
    assert captured["cwd"] == str(project_a)
    assert captured["sandbox_mode"] == "workspace-write"
    assert captured["approval_policy"] == "never"
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
