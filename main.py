from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv
from openai import AzureOpenAI, APIStatusError

from Multi_agen.router.api import build_app

logger = logging.getLogger("multi_agen.main")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def check_model_connection() -> bool:
    """
    Verify connectivity to the Azure OpenAI model defined in .env.
    Returns True if connection succeeded, False otherwise.
    Never raises — startup must not be blocked by a failed probe.
    """
    endpoint   = (os.getenv("AZURE_OPENAI_GPT54_ENDPOINT")         or "").strip().rstrip("/")
    api_key    = (os.getenv("AZURE_OPENAI_GPT54_API_KEY")           or "").strip()
    api_version = (os.getenv("AZURE_OPENAI_GPT54_API_VERSION")      or "").strip()
    deployment = (os.getenv("AZURE_OPENAI_GPT54_DEPLOYMENT_NAME")   or "").strip()

    missing = [k for k, v in {
        "AZURE_OPENAI_GPT54_ENDPOINT":        endpoint,
        "AZURE_OPENAI_GPT54_API_KEY":         api_key,
        "AZURE_OPENAI_GPT54_API_VERSION":     api_version,
        "AZURE_OPENAI_GPT54_DEPLOYMENT_NAME": deployment,
    }.items() if not v]

    if missing:
        logger.warning(
            "Azure OpenAI env vars not set — skipping connectivity probe. Missing: %s",
            missing,
        )
        return False

    try:
        client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=api_version,
            timeout=15.0,
        )

        # ✅ GPT-5.4 requires max_completion_tokens — max_tokens raises 400
        client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": "ping"}],
            max_completion_tokens=10,
        )

        logger.info(
            "Azure OpenAI connectivity OK  deployment=%r  endpoint=%r  api_version=%r",
            deployment, endpoint, api_version,
        )
        return True

    except APIStatusError as e:
        # Structured API errors — surface the exact message for fast diagnosis
        logger.error(
            "Azure OpenAI probe failed  deployment=%r  status=%s  code=%s  message=%s",
            deployment,
            e.status_code,
            getattr(e, "code", "—"),
            getattr(e, "message", str(e)),
        )
    except Exception as e:
        logger.error(
            "Azure OpenAI probe failed  deployment=%r  error=%s: %s",
            deployment, type(e).__name__, e,
        )

    return False


def create_app():
    # 1) Load .env from Multi_agen/.env (not CWD)
    env_path = Path(__file__).resolve().parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
        logger.info("Loaded .env from %s", env_path)
    else:
        logger.warning(".env not found at %s — relying on shell environment", env_path)

    # 2) Probe LLM connectivity (non-blocking)
    check_model_connection()

    # 3) Load YAML config
    default_cfg_path = (
        Path(__file__).resolve().parent / "apps" / "orchestrator" / "config.yaml"
    )
    config_path = os.environ.get("ORC_CONFIG", str(default_cfg_path))
    cfg = load_yaml(config_path)
    logger.info("Loaded config.yaml from %s", config_path)

    # 4) MCP server configs
    mcp_cfg = cfg.get("mcp", {}) or {}
    servers = mcp_cfg.get("servers", {}) or {}

    server_base_urls = {
        sid: info["base_url"]
        for sid, info in servers.items()
        if isinstance(info, dict) and "base_url" in info
    }

    available_capabilities = list(mcp_cfg.get("available_capabilities", []) or [])
    llm_config             = cfg.get("llm", None)
    auto_start             = bool(mcp_cfg.get("auto_start", False))
    startup_timeout        = float(mcp_cfg.get("startup_timeout_seconds", 20))
    stop_on_shutdown       = bool(mcp_cfg.get("stop_on_shutdown", True))

    return build_app(
        server_base_urls=server_base_urls,
        available_capabilities=available_capabilities,
        llm_config=llm_config,
        mcp_servers_cfg=servers,
        mcp_auto_start=auto_start,
        mcp_startup_timeout_seconds=startup_timeout,
        mcp_stop_on_shutdown=stop_on_shutdown,
    )


app = create_app()


if __name__ == "__main__":
    import uvicorn

    default_cfg_path = (
        Path(__file__).resolve().parent / "apps" / "orchestrator" / "config.yaml"
    )
    config_path = os.environ.get("ORC_CONFIG", str(default_cfg_path))
    cfg         = load_yaml(config_path)
    host        = (cfg.get("app", {}) or {}).get("host", "127.0.0.1")
    port        = int((cfg.get("app", {}) or {}).get("port", 8000))

    uvicorn.run("Multi_agen.main:app", host=host, port=port, reload=True)