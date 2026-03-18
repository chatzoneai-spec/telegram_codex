import httpx

from telecode.telegram import TelegramConfig, telegram_get_updates


def test_telegram_get_updates_returns_empty_on_timeout(monkeypatch):
    config = TelegramConfig(bot_token="test-token")

    def fake_post_json(*args, **kwargs):
        raise httpx.ReadTimeout("timed out")

    monkeypatch.setattr("telecode.telegram._post_json", fake_post_json)

    assert telegram_get_updates(config, timeout=30) == []
