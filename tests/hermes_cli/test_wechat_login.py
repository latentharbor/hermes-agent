import json
import sys

from hermes_cli.wechat_login import _print_qr, save_credentials


def test_save_credentials_uses_active_hermes_home(tmp_path, monkeypatch):
    hermes_home = tmp_path / ".hermes-profile"
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))

    saved = save_credentials(
        account_id="bot@example.com",
        token="secret-token",
        base_url="https://ilinkai.weixin.qq.com",
        user_id="wx-user-1",
    )

    assert saved == hermes_home / "weixin" / "accounts" / "bot-example-com.json"
    payload = json.loads(saved.read_text(encoding="utf-8"))
    assert payload["token"] == "secret-token"
    assert payload["baseUrl"] == "https://ilinkai.weixin.qq.com"
    assert payload["userId"] == "wx-user-1"

    index_path = hermes_home / "weixin" / "accounts.json"
    assert json.loads(index_path.read_text(encoding="utf-8")) == ["bot-example-com"]


def test_print_qr_warns_when_qrcode_dependency_missing(monkeypatch, capsys):
    monkeypatch.setitem(sys.modules, "qrcode", None)

    _print_qr("https://example.com/qr")

    output = capsys.readouterr().out
    assert "Terminal QR rendering is unavailable" in output
    assert "https://example.com/qr" in output
