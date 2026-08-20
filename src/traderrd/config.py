from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from urllib.parse import urlparse


class ConfigurationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ObserverConfig:
    api_id: int
    api_hash: str
    source_chat_id: int
    source_topic_id: int
    source_seed_message_id: int
    expected_sender_id: int | None
    session_path: Path
    database_path: Path
    log_level: str
    bybit_public_base_url: str
    bybit_category: str
    bybit_timeout_seconds: float

    def __post_init__(self) -> None:
        for name, value in (
            ("source_chat_id", self.source_chat_id),
            ("source_topic_id", self.source_topic_id),
            ("source_seed_message_id", self.source_seed_message_id),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ConfigurationError(f"{name} must be a positive integer")
        if self.expected_sender_id == 0:
            raise ConfigurationError("expected_sender_id must be non-zero when set")


def load_dotenv(path: str | Path = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def load_config(env_path: str | Path = ".env") -> ObserverConfig:
    load_dotenv(env_path)
    api_id_raw = _required("TELEGRAM_API_ID")
    api_hash = _required("TELEGRAM_API_HASH")
    source_chat_raw = _required("TELEGRAM_SOURCE_CHAT_ID")
    source_topic_raw = _required("TELEGRAM_SOURCE_TOPIC_ID")
    source_seed_raw = _required("TELEGRAM_SOURCE_SEED_MESSAGE_ID")

    if api_id_raw.startswith("replace_") or api_hash.startswith("replace_"):
        raise ConfigurationError("Replace Telegram credential placeholders in .env")
    try:
        api_id = int(api_id_raw)
        source_chat_id = int(source_chat_raw)
        source_topic_id = int(source_topic_raw)
        source_seed_message_id = int(source_seed_raw)
    except ValueError as exc:
        raise ConfigurationError(
            "Telegram API, source chat, topic, and seed message IDs must be integers"
        ) from exc
    if min(api_id, source_chat_id, source_topic_id, source_seed_message_id) <= 0:
        raise ConfigurationError(
            "Telegram API, source chat, topic, and seed message IDs must be positive"
        )

    expected_sender_raw = os.getenv("TELEGRAM_EXPECTED_SENDER_ID", "").strip()
    expected_sender_id: int | None = None
    if expected_sender_raw:
        try:
            expected_sender_id = int(expected_sender_raw)
        except ValueError as exc:
            raise ConfigurationError(
                "TELEGRAM_EXPECTED_SENDER_ID must be an integer"
            ) from exc
        if expected_sender_id == 0:
            raise ConfigurationError(
                "TELEGRAM_EXPECTED_SENDER_ID must be non-zero"
            )

    bybit_public_base_url = os.getenv(
        "BYBIT_PUBLIC_BASE_URL", "https://api.bybit.com"
    ).rstrip("/")
    parsed_base_url = urlparse(bybit_public_base_url)
    if (
        parsed_base_url.scheme != "https"
        or not parsed_base_url.netloc
        or parsed_base_url.username is not None
        or parsed_base_url.path not in ("", "/")
        or parsed_base_url.query
        or parsed_base_url.fragment
    ):
        raise ConfigurationError("BYBIT_PUBLIC_BASE_URL must be a public HTTPS base URL")
    bybit_category = os.getenv("BYBIT_CATEGORY", "linear").strip().lower()
    if bybit_category != "linear":
        raise ConfigurationError("BYBIT_CATEGORY must be linear")
    try:
        bybit_timeout_seconds = float(os.getenv("BYBIT_TIMEOUT_SECONDS", "3.0"))
    except ValueError as exc:
        raise ConfigurationError("BYBIT_TIMEOUT_SECONDS must be numeric") from exc
    if not 0 < bybit_timeout_seconds <= 30:
        raise ConfigurationError(
            "BYBIT_TIMEOUT_SECONDS must be greater than 0 and at most 30"
        )

    return ObserverConfig(
        api_id=api_id,
        api_hash=api_hash,
        source_chat_id=source_chat_id,
        source_topic_id=source_topic_id,
        source_seed_message_id=source_seed_message_id,
        expected_sender_id=expected_sender_id,
        session_path=Path(os.getenv("TELEGRAM_SESSION_PATH", ".local/traderrd")),
        database_path=Path(os.getenv("TRADERRD_DB_PATH", "data/traderrd.sqlite3")),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        bybit_public_base_url=bybit_public_base_url,
        bybit_category=bybit_category,
        bybit_timeout_seconds=bybit_timeout_seconds,
    )


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigurationError(f"Missing required environment variable: {name}")
    return value
