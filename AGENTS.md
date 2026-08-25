# AGENTS.md

Scope: entire repository.

This project is MaintBot, a Python 3.10+ Telegram bot for server monitoring, access management, subscriptions, maintenance windows, and support workflows. Treat this file as the working contract for automated agents and contributors making scoped changes in this repo.

## First-pass orientation

- Read [README.md](air-file://stcbk9ptdclb7s52j3k0/C:/Users/kiril/Documents/server_bot/README.md?type=file&root=C%3A) before non-trivial changes. It defines the product model, storage layout, deployment assumptions, and architectural constraints.
- Main code lives under [app](air-file://stcbk9ptdclb7s52j3k0/C:/Users/kiril/Documents/server_bot/app?type=directory&root=C%3A); tests live under [tests](air-file://stcbk9ptdclb7s52j3k0/C:/Users/kiril/Documents/server_bot/tests?type=directory&root=C%3A); migration and env helper scripts live under [tools](air-file://stcbk9ptdclb7s52j3k0/C:/Users/kiril/Documents/server_bot/tools?type=directory&root=C%3A).
- Runtime entry points are [launcher.py](air-file://stcbk9ptdclb7s52j3k0/C:/Users/kiril/Documents/server_bot/app/launcher.py?type=file&root=C%3A) for the real process and [main.py](air-file://stcbk9ptdclb7s52j3k0/C:/Users/kiril/Documents/server_bot/app/main.py?type=file&root=C%3A) as the thin Telegram application entry.

## Non-negotiable architecture rules

- Keep the feature-oriented package layout. Do not add global `handlers/` or `services/` packages.
- Keep the root of [app](air-file://stcbk9ptdclb7s52j3k0/C:/Users/kiril/Documents/server_bot/app?type=directory&root=C%3A) thin. The architecture test expects only `__init__.py`, `config_check.py`, `constants.py`, `launcher.py`, `main.py`, and `storage.py` at package root.
- Do not grow any Python module past 400 lines. Split by responsibility before crossing the limit.
- Avoid direct internal import cycles across `app.*` modules.
- Persistence imports must stay side-effect free. Migration and storage initialization must happen only through explicit commands or launcher flow, never on import.

## How to place changes

- Put business logic next to the relevant domain:
  - access flow in `app/access`
  - staff/admin management in `app/administration` and `app/users/admin`
  - bot wiring, routes, jobs, navigation, and UI in `app/bot`
  - configuration parsing and validation in `app/config`
  - maintenance workflow in `app/maintenance`
  - Telegram delivery and cleanup in `app/messaging`
  - monitoring adapters and status presentation in `app/monitoring/*`
  - persistence backend, repositories, migrations, and transactions in `app/persistence`
  - subscription requests and lifecycle in `app/subscriptions/requests`
  - support ticket workflow in `app/tickets`
  - user-facing profile and staff utilities in `app/users`
- Prefer extending an existing domain module set over creating a new top-level package.
- Keep transport/integration concerns separate from view formatting and policy logic when touching monitoring, tickets, or subscriptions.

## Configuration and secrets

- Do not commit real secrets. Examples live in [app/.env.example](air-file://stcbk9ptdclb7s52j3k0/C:/Users/kiril/Documents/server_bot/app/.env.example?type=file&root=C%3A) and [app/env.secrets.example](air-file://stcbk9ptdclb7s52j3k0/C:/Users/kiril/Documents/server_bot/app/env.secrets.example?type=file&root=C%3A).
- Server inventory examples live in [deploy/servers.toml.example](air-file://stcbk9ptdclb7s52j3k0/C:/Users/kiril/Documents/server_bot/deploy/servers.toml.example?type=file&root=C%3A).
- Preserve the current split-storage assumptions. Do not casually edit data files or lock files in a live environment.

## Validation and test expectations

Run the smallest relevant test slice first, then broader checks if the change touches shared infrastructure.

- Full test suite: `pytest`
- Architecture guardrails: `pytest tests/test_architecture.py`
- Type checks: `mypy app`
- Lint: `ruff check .`

Add or update tests when behavior changes. Prefer targeted tests in the matching domain area before broad integration coverage.

## Change discipline

- Keep edits minimal and local to the requested behavior.
- Preserve Russian-language user/admin copy unless the task explicitly changes wording.
- Avoid speculative refactors during feature or bug work.
- When touching storage schemas, migrations, auth rules, payment lifecycle, or maintenance scheduling, verify README and existing tests first; these areas have explicit operational constraints.
- If a requested change conflicts with the architecture tests or README contract, surface that conflict before proceeding blindly.

## Useful repo commands

- Create empty split-layout storage: `python -m app.persistence.bootstrap --data-dir data`
- Run configuration preflight: `python -m app.config_check`
- Start the bot process: `python -m app.launcher`
- Dry-run migration from legacy monolith: `python -m app.persistence.migration --data-dir <DATA_DIR> --dry-run`

## Deliverable expectations for agents

- Mention any architectural constraint that shaped the implementation.
- Reference the exact tests run, or state clearly if no tests were run.
- Call out any follow-up needed for deployment, secrets, inventory, or migration steps.
