import re

from ..config import PRIVILEGED_HELPER_BIN, SUBPROC_SHORT_TIMEOUT, SUDO_BIN, UFW_BIN
from .system_process import run_exec


def _parse_ufw_status(out: str) -> str:
    first = (out.strip().splitlines()[:1] or [""])[0].strip().lower()
    # ``inactive`` contains ``active``; negative states must always be checked first.
    if re.search(r"\b(inactive|disabled)\b", first) or any(word in first for word in ("неактив", "отключ", "выключ")):
        return "inactive"
    if re.search(r"\b(active|enabled)\b", first) or any(word in first for word in ("актив", "включ")):
        return "active"
    return "н/д"


def _ufw_candidates() -> list[list[str]]:
    bases: list[str] = []
    for b in [UFW_BIN, "ufw"]:
        if b and b not in bases:
            bases.append(b)
    cmds: list[list[str]] = []
    for b in bases:
        cmds.append([b, "status"])
    if SUDO_BIN and PRIVILEGED_HELPER_BIN:
        cmds.append([SUDO_BIN, "-n", PRIVILEGED_HELPER_BIN, "ufw-status"])
    return cmds


async def ufw_status_basic() -> str:
    out = ""
    for args in _ufw_candidates():
        rc, o, _ = await run_exec(args, timeout=SUBPROC_SHORT_TIMEOUT)
        if rc == 0 and (o or "").strip():
            out = o
            break

    if not out:
        return "н/д"

    return _parse_ufw_status(out)


def _parse_ufw_rules(out: str) -> tuple[list[str], list[str], list[str]]:
    allow: list[str] = []
    deny: list[str] = []
    reject: list[str] = []

    lines = [ln.rstrip() for ln in (out or "").splitlines()]
    if not lines:
        return allow, deny, reject

    start_idx = 0
    for i, ln in enumerate(lines[:10]):
        if re.search(r"\bTo\b", ln) and re.search(r"\bAction\b", ln):
            start_idx = i + 1
            break

    for ln in lines[start_idx:]:
        if not ln.strip():
            continue

        parts = [p.strip() for p in re.split(r"\s{2,}", ln.strip()) if p.strip()]
        if len(parts) < 2:
            continue
        to, action = parts[0], parts[1].upper()
        src = parts[2] if len(parts) > 2 else ""
        item = to.strip()
        if not item:
            continue
        if src and src.lower() not in {"anywhere", "anywhere (v6)"}:
            item = f"{item} <- {src}"
        if action.startswith("ALLOW"):
            allow.append(item)
        elif action.startswith("DENY"):
            deny.append(item)
        elif action.startswith("REJECT"):
            reject.append(item)

    def uniq(xs: list[str]) -> list[str]:
        seen: set[str] = set()
        outl: list[str] = []
        for x in xs:
            if x not in seen:
                seen.add(x)
                outl.append(x)
        return outl

    return uniq(allow), uniq(deny), uniq(reject)


async def ufw_summary_for_admin() -> tuple[str, list[str], list[str], list[str]]:
    out = ""
    for args in _ufw_candidates():
        rc, o, _ = await run_exec(args, timeout=SUBPROC_SHORT_TIMEOUT)
        if rc == 0 and (o or "").strip():
            out = o
            break

    if not out:
        return "н/д", [], [], []

    status = _parse_ufw_status(out)

    allow, deny, reject = _parse_ufw_rules(out)
    return status, allow, deny, reject
