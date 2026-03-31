from gateway.config import Platform, load_gateway_config
from gateway.platforms.weixin import _markdown_to_plain


def test_gateway_config_loads_weixin_from_env(monkeypatch):
    monkeypatch.setenv("WEIXIN_TOKEN", "token-123")
    monkeypatch.setenv("WEIXIN_ACCOUNT_ID", "bot-456")
    monkeypatch.setenv("WEIXIN_HOME_CHANNEL", "user-789")

    config = load_gateway_config()
    pconfig = config.platforms[Platform.WEIXIN]

    assert pconfig.enabled is True
    assert pconfig.token == "token-123"
    assert pconfig.extra["account_id"] == "bot-456"
    assert pconfig.home_channel.chat_id == "user-789"


def test_markdown_to_plain_strips_formatting():
    content = """# Title

**bold** and [link](https://example.com)

```python
print("hi")
```
"""

    plain = _markdown_to_plain(content)

    assert "**" not in plain
    assert "[link]" not in plain
    assert "print(\"hi\")" in plain
    assert "link" in plain
