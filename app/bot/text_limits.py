"""Conservative Telegram text budgets without broken HTML or UTF-16 characters."""

from __future__ import annotations

import html
from html.parser import HTMLParser

_SUFFIX = "\n…(сообщение сокращено)"


def utf16_length(text: str) -> int:
    return sum(2 if ord(char) > 0xFFFF else 1 for char in text)


def _prefix(text: str, budget: int) -> str:
    end = 0
    for char in text:
        budget -= 2 if ord(char) > 0xFFFF else 1
        if budget < 0:
            break
        end += 1
    return text[:end]


def clip_plain_text(text: str, limit: int) -> str:
    if utf16_length(text) <= limit:
        return text
    suffix = _SUFFIX if limit >= len(_SUFFIX) else "…"[: max(0, limit)]
    return _prefix(text, limit - len(suffix)) + suffix


class _HTMLBudget(HTMLParser):
    def __init__(self, budget: int) -> None:
        super().__init__(convert_charrefs=False)
        self.budget = budget
        self.parts: list[str] = []
        self.stack: list[str] = []
        self.stopped = False

    def _append(self, text: str, *, reserve: int = 0) -> bool:
        size = utf16_length(text)
        if self.stopped or size + reserve > self.budget:
            self.stopped = True
            return False
        self.parts.append(text)
        self.budget -= size + reserve
        return True

    def handle_starttag(self, tag: str, _attrs) -> None:
        closing = f"</{tag}>"
        if self._append(self.get_starttag_text() or f"<{tag}>", reserve=len(closing)):
            self.stack.append(tag)

    def handle_startendtag(self, tag: str, _attrs) -> None:
        self._append(self.get_starttag_text() or f"<{tag}/>")

    def handle_endtag(self, tag: str) -> None:
        if not self.stopped and self.stack and self.stack[-1] == tag:
            self.parts.append(f"</{self.stack.pop()}>")

    def handle_data(self, data: str) -> None:
        if self.stopped:
            return
        for char in data:
            if not self._append(html.escape(char, quote=False)):
                break

    def handle_entityref(self, name: str) -> None:
        self._append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self._append(f"&#{name};")

    def result(self) -> str:
        return "".join(self.parts) + "".join(f"</{tag}>" for tag in reversed(self.stack))


def clip_html_message(text: str, limit: int = 4000) -> str:
    """Keep complete tags/entities, reserving space for closing tags and the suffix.

    Counting serialized HTML is stricter than Telegram's post-entity limit, so
    both plain text and entity offsets remain within the delivery budget.
    """
    value = str(text or "")
    if utf16_length(value) <= limit:
        return value
    suffix = _SUFFIX if limit >= len(_SUFFIX) else "…"[: max(0, limit)]
    parser = _HTMLBudget(max(0, limit - len(suffix)))
    parser.feed(value)
    parser.close()
    return parser.result() + suffix
