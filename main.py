from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

from Multi_agen.router.api import build_app

logger = logging.getLogger("multi_agen.main")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def create_app():
    # 1) Load .env explicitly from Multi_agen/.env (NOT from CWD)
    env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(dotenv_path=env_path, override=False)
    logger.info("Loaded .env from %s", env_path)

    # 2) Load YAML config
    default_cfg_path = Path(__file__).resolve().parent / "apps" / "orchestrator" / "config.yaml"
    config_path = os.environ.get("ORC_CONFIG", str(default_cfg_path))

    cfg = load_yaml(config_path)
    logger.info("Loaded config.yaml from %s", config_path)

    # 3) Extract MCP server URLs (matches your YAML: mcp.servers.*.base_url)
    servers = (cfg.get("mcp", {}) or {}).get("servers", {}) or {}
    server_base_urls = {sid: info["base_url"] for sid, info in servers.items() if isinstance(info, dict) and "base_url" in info}

    # 4) Capabilities (matches your YAML: mcp.available_capabilities)
    available_capabilities = list((cfg.get("mcp", {}) or {}).get("available_capabilities", []) or [])

    # 5) LLM config (we only need enabled flag; Azure values come from .env)
    llm_config = cfg.get("llm", None)

    # 6) Build the FastAPI app
    return build_app(
        server_base_urls=server_base_urls,
        available_capabilities=available_capabilities,
        llm_config=llm_config,
    )


app = create_app()

if __name__ == "__main__":
    import uvicorn

    # Load config again for host/port
    default_cfg_path = Path(__file__).resolve().parent / "apps" / "orchestrator" / "config.yaml"
    config_path = os.environ.get("ORC_CONFIG", str(default_cfg_path))
    cfg = load_yaml(config_path)

    host = (cfg.get("app", {}) or {}).get("host", "127.0.0.1")
    port = int((cfg.get("app", {}) or {}).get("port", 8000))

    uvicorn.run("Multi_agen.main:app", host=host, port=port, reload=True)