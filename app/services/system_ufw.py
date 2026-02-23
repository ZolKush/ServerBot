import re
from typing import List, Tuple

from ..config import SUDO_BIN, SUBPROC_SHORT_TIMEOUT, UFW_BIN
from .system_process import run_exec


def _ufw_candidates() -> List[List[str]]:
    bases: List[str] = []
    for b in [UFW_BIN, "ufw"]:
        if b and b not in bases:
            bases.append(b)
    cmds: List[List[str]] = []
    for b in bases:
        cmds.append([b, "status"])
        if SUDO_BIN:
            cmds.append([SUDO_BIN, "-n", b, "status"])
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

    first = (out.strip().splitlines()[:1] or [""])[0].lower()
    if "active" in first:
        return "active"
    if "inactive" in first:
        return "inactive"
    return "н/д"


def _parse_ufw_rules(out: str) -> Tuple[List[str], List[str], List[str]]:
    allow: List[str] = []
    deny: List[str] = []
    reject: List[str] = []

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

    def uniq(xs: List[str]) -> List[str]:
        seen: set[str] = set()
        outl: List[str] = []
        for x in xs:
            if x not in seen:
                seen.add(x)
                outl.append(x)
        return outl

    return uniq(allow), uniq(deny), uniq(reject)


async def ufw_summary_for_admin() -> Tuple[str, List[str], List[str], List[str]]:
    out = ""
    for args in _ufw_candidates():
        rc, o, _ = await run_exec(args, timeout=SUBPROC_SHORT_TIMEOUT)
        if rc == 0 and (o or "").strip():
            out = o
            break

    if not out:
        return "н/д", [], [], []

    status = "н/д"
    first = (out.strip().splitlines()[:1] or [""])[0].lower()
    if "active" in first:
        status = "active"
    elif "inactive" in first:
        status = "inactive"

    allow, deny, reject = _parse_ufw_rules(out)
    return status, allow, deny, reject
