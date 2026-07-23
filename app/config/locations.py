from __future__ import annotations

import os
from pathlib import Path

# Configuration files live beside the application package; repository-level
# runtime data lives one directory above it.
BASE_DIR = Path(__file__).resolve().parents[1]
ROOT_DIR = BASE_DIR.parent

_ENV_PATH = os.getenv("ENV_PATH", "").strip()
ENV_FILE = Path(_ENV_PATH) if _ENV_PATH else (BASE_DIR / ".env")

_SECRETS_ENV_PATH = os.getenv("SECRETS_ENV_PATH", "").strip()
SECRETS_ENV_FILE = Path(_SECRETS_ENV_PATH) if _SECRETS_ENV_PATH else (BASE_DIR / "env.secrets")

__all__ = [
    "BASE_DIR",
    "ENV_FILE",
    "ROOT_DIR",
    "SECRETS_ENV_FILE",
]
