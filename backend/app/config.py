from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


BACKEND_DIR = Path(__file__).resolve().parent.parent
load_env_file(BACKEND_DIR / ".env")


@dataclass(frozen=True)
class Settings:
    app_name: str = "Context Graph + LLM Query Interface"
    backend_dir: Path = BACKEND_DIR
    project_dir: Path = backend_dir.parent
    data_dir: Path = project_dir / "sap-o2c-data" #data in the parent repo fix
    sqlite_path: Path = backend_dir / "sap_o2c.sqlite3"
    groq_api_key: str = os.getenv("GROQ_API_KEY", "")
    groq_model: str = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b")
    groq_base_url: str = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1/chat/completions")


settings = Settings()
