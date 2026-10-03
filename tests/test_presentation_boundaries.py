from html.parser import HTMLParser
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import NetworkError

from app.bot import ui


class BalancedHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self.stack = []

    def handle_starttag(self, tag, attrs):
        self.stack.append(tag)

    def handle_endtag(self, tag):
        assert self.stack.pop() == tag


@pytest.mark.parametrize(
    "text",
    ["<b>heading\n" + "x" * 6000 + "</b>", "<pre><code>" + "&amp;" * 2000 + "</code></pre>", "😀" * 4000],
    ids=["multiline", "entities", "emoji"],
)
def test_clipped_html_stays_balanced_and_within_utf16_budget(text):
    clipped = ui.clip_html_message(text, limit=4000)
    assert len(clipped.encode("utf-16-le")) // 2 <= 4000
    checker = BalancedHTML()
    checker.feed(clipped)
    checker.close()
    assert checker.stack == []


def test_escaping_and_plain_clipping_include_the_suffix_in_the_limit():
    assert len(ui.clip_html("&" * 1000, limit=50)) <= 50
    assert len(ui.clip_text("x" * 1000, limit=50)) <= 50


@pytest.mark.asyncio
async def test_uncertain_edit_failure_does_not_send_a_duplicate_reply():
    message = SimpleNamespace(edit_text=AsyncMock(side_effect=NetworkError("response lost")), reply_text=AsyncMock())
    with pytest.raises(NetworkError):
        await ui.safe_edit_or_reply(message, "notice")
    message.reply_text.assert_not_awaited()
