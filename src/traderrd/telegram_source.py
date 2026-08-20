from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class SourceDecision(StrEnum):
    ACCEPTED = "accepted"
    OUTSIDE_TOPIC = "outside_topic"
    UNEXPECTED_SENDER = "unexpected_sender"


class SourceIsolationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SourceInspection:
    decision: SourceDecision
    topic_id: int | None
    sender_id: int | None


def inspect_message_source(
    message: Any,
    *,
    topic_id: int,
    expected_sender_id: int | None,
) -> SourceInspection:
    observed_topic_id = message_topic_id(message, topic_id)
    sender_id = message_sender_id(message)
    if observed_topic_id != topic_id:
        decision = SourceDecision.OUTSIDE_TOPIC
    elif expected_sender_id is not None and sender_id != expected_sender_id:
        decision = SourceDecision.UNEXPECTED_SENDER
    else:
        decision = SourceDecision.ACCEPTED
    return SourceInspection(decision, observed_topic_id, sender_id)


def inspect_topic_membership(
    message: Any,
    *,
    topic_id: int,
    scoped_topic_id: int | None = None,
) -> SourceInspection:
    observed_topic_id = message_topic_id(message, topic_id)
    sender_id = message_sender_id(message)
    if observed_topic_id == topic_id:
        return SourceInspection(SourceDecision.ACCEPTED, observed_topic_id, sender_id)
    if observed_topic_id is None and scoped_topic_id == topic_id:
        return SourceInspection(SourceDecision.ACCEPTED, topic_id, sender_id)
    return SourceInspection(SourceDecision.OUTSIDE_TOPIC, observed_topic_id, sender_id)


def message_topic_id(message: Any, configured_topic_id: int) -> int | None:
    message_id = _integer_or_none(getattr(message, "id", None))
    if message_id == configured_topic_id:
        return configured_topic_id

    reply_header = getattr(message, "reply_to", None)
    if reply_header is None:
        return None
    top_id = _integer_or_none(getattr(reply_header, "reply_to_top_id", None))
    if top_id is not None:
        return top_id
    reply_id = _integer_or_none(getattr(reply_header, "reply_to_msg_id", None))
    if reply_id == configured_topic_id:
        return configured_topic_id
    if getattr(reply_header, "forum_topic", False) is True:
        return reply_id
    return None


def is_non_operational_message(message: Any, raw_text: object) -> bool:
    if not isinstance(raw_text, str) or not raw_text.strip():
        return True
    if getattr(message, "action", None) is not None:
        return True
    return type(message).__name__ in {"MessageEmpty", "MessageService"}


def message_sender_id(message: Any) -> int | None:
    return _integer_or_none(getattr(message, "sender_id", None))


async def resolve_source_entity(client: Any, source_chat_id: int) -> Any:
    async for dialog in client.iter_dialogs():
        if getattr(dialog.entity, "id", None) == source_chat_id:
            return dialog.entity
    raise RuntimeError(
        f"Source chat {source_chat_id} was not found in the authorized account dialogs"
    )


async def inspect_seed_source(
    client: Any,
    source_entity: Any,
    *,
    topic_id: int,
    seed_message_id: int,
    expected_sender_id: int | None,
) -> tuple[Any, SourceInspection]:
    message = await client.get_messages(source_entity, ids=seed_message_id)
    if message is None or getattr(message, "id", None) != seed_message_id:
        raise RuntimeError("Configured seed message was not found in the source chat")
    return message, inspect_message_source(
        message,
        topic_id=topic_id,
        expected_sender_id=expected_sender_id,
    )


def _integer_or_none(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value == 0:
        return None
    return value
