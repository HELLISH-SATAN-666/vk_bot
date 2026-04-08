from __future__ import annotations

import logging
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import vk_api
from vk_api.exceptions import ApiError


LOGGER = logging.getLogger(__name__)
FULL_MESSAGE_FETCH_ATTEMPTS = 4
FULL_MESSAGE_FETCH_DELAY_SECONDS = 0.75
KEYBOARD_UNSUPPORTED_ERROR_CODE = 912
KEYBOARDS_AVAILABLE: bool | None = None


@dataclass
class PendingPost:
    text: str
    attachments: list[str] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.text and not self.attachments

    def preview(self, limit: int = 200) -> str:
        if not self.text:
            return "<без текста>"

        normalized = " ".join(self.text.split())
        if len(normalized) <= limit:
            return normalized

        return normalized[: limit - 3] + "..."


@dataclass(frozen=True)
class GroupTarget:
    group_id: int
    name: str
    api: object

    def wall_url(self, post_id: int) -> str:
        return f"https://vk.com/wall-{self.group_id}_{post_id}"

    def publish(self, message: str, attachments: list[str]) -> int:
        payload: dict[str, Any] = {
            "owner_id": -self.group_id,
            "from_group": 1,
        }

        if message:
            payload["message"] = message
        if attachments:
            payload["attachments"] = ",".join(attachments)

        LOGGER.info(
            "Publishing to group '%s' (id=%s): text_len=%s attachments=%s",
            self.name,
            self.group_id,
            len(message),
            len(attachments),
        )
        response = self.api.wall.post(**payload)
        return int(response["post_id"])


class PatternStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.ensure_exists()

    def ensure_exists(self) -> None:
        if self.path.exists():
            return

        LOGGER.info("Creating suffix file at %s", self.path)
        self.save("")

    def load(self) -> str:
        self.ensure_exists()
        pattern = self.path.read_text(encoding="utf-8").strip()
        LOGGER.info("Loaded suffix from %s (length=%s)", self.path, len(pattern))
        return pattern

    def save(self, pattern: str) -> None:
        normalized = pattern.strip()
        self.path.write_text(normalized, encoding="utf-8")
        LOGGER.info("Saved suffix to %s (length=%s)", self.path, len(normalized))


def build_api(token: str) -> tuple[vk_api.VkApi, object]:
    session = vk_api.VkApi(token=token)
    return session, session.get_api()


def normalize_screen_name(value: str) -> str:
    normalized = value.strip()
    if normalized.startswith("https://vk.com/"):
        normalized = normalized.removeprefix("https://vk.com/")
    elif normalized.startswith("http://vk.com/"):
        normalized = normalized.removeprefix("http://vk.com/")

    return normalized.lstrip("@").strip("/")


def resolve_admin_id(api: object, screen_name: str) -> int:
    response = api.utils.resolveScreenName(
        screen_name=normalize_screen_name(screen_name)
    )
    if not response or response.get("type") != "user":
        raise RuntimeError(f"Admin value '{screen_name}' does not point to a VK user")

    return int(response["object_id"])


def resolve_admin_ids(api: object, values: Iterable[str | int]) -> list[int]:
    resolved_ids: list[int] = []
    seen: set[int] = set()

    for value in values:
        raw_value = str(value).strip()
        if not raw_value:
            continue

        if raw_value.lstrip("-").isdigit():
            admin_id = abs(int(raw_value))
        else:
            admin_id = resolve_admin_id(api, raw_value)

        if admin_id in seen:
            continue

        seen.add(admin_id)
        resolved_ids.append(admin_id)

    return resolved_ids


def get_current_group(api: object) -> tuple[int, str]:
    groups = api.groups.getById()
    if not groups:
        raise RuntimeError("Unable to detect group for the current token")

    group = groups[0]
    return int(group["id"]), str(group["name"])


def build_group_targets(tokens: Iterable[str]) -> list[GroupTarget]:
    targets: list[GroupTarget] = []

    for token in tokens:
        _, api = build_api(token)
        group_id, name = get_current_group(api)
        LOGGER.info("Registered target group '%s' (id=%s)", name, group_id)
        targets.append(
            GroupTarget(
                group_id=group_id,
                name=name,
                api=api,
            )
        )

    return targets


def send_message(
    api: object,
    user_id: int,
    message: str,
    keyboard: str | None = None,
    attachments: list[str] | None = None,
) -> None:
    global KEYBOARDS_AVAILABLE

    payload: dict[str, Any] = {
        "user_id": user_id,
        "random_id": secrets.randbelow(2_147_483_647),
    }
    if message:
        payload["message"] = message
    if attachments:
        payload["attachment"] = ",".join(attachments)
    if keyboard and KEYBOARDS_AVAILABLE is not False:
        payload["keyboard"] = keyboard

    try:
        api.messages.send(**payload)
        if keyboard and KEYBOARDS_AVAILABLE is None:
            KEYBOARDS_AVAILABLE = True
        return
    except ApiError as error:
        if not keyboard or error.code != KEYBOARD_UNSUPPORTED_ERROR_CODE:
            raise

        KEYBOARDS_AVAILABLE = False
        LOGGER.warning(
            "VK keyboard is unavailable for this bot configuration; retrying message without keyboard"
        )

    fallback_payload = dict(payload)
    fallback_payload.pop("keyboard", None)
    api.messages.send(**fallback_payload)


def compose_post_text(text: str, suffix: str) -> str:
    base_text = text.strip()
    normalized_suffix = suffix.strip()

    if base_text and normalized_suffix:
        return f"{base_text}\n\n{normalized_suffix}"
    if base_text:
        return base_text
    if normalized_suffix:
        return normalized_suffix

    return ""


def get_message_text(message: Any) -> str:
    value = _get_value(message, "text", "")
    return str(value or "").strip()


def collect_photo_attachments(api: object, message: Any) -> list[str]:
    fetched_message = fetch_full_message(api, message)

    if fetched_message is not None:
        attachments = extract_photo_attachments(fetched_message, source="full_message")
        if attachments:
            return attachments

    return extract_photo_attachments(message, source="longpoll_event")


def fetch_full_message(api: object, message: Any) -> Any | None:
    peer_id = _get_value(message, "peer_id")
    conversation_message_id = _get_value(message, "conversation_message_id")
    should_retry = (
        not get_message_text(message)
        and not (_get_value(message, "attachments", []) or [])
    )

    if peer_id is not None and conversation_message_id is not None:
        for attempt in range(1, FULL_MESSAGE_FETCH_ATTEMPTS + 1):
            try:
                response = api.messages.getByConversationMessageId(
                    peer_id=peer_id,
                    conversation_message_ids=conversation_message_id,
                )
                fetched_message = extract_first_message(response)
                if fetched_message is not None:
                    LOGGER.info(
                        "Fetched full message by conversation_message_id=%s for peer_id=%s on attempt %s",
                        conversation_message_id,
                        peer_id,
                        attempt,
                    )
                    if not should_retry:
                        return fetched_message

                    fetched_attachments = _get_value(fetched_message, "attachments", []) or []
                    fetched_text = get_message_text(fetched_message)
                    if fetched_attachments or fetched_text or attempt == FULL_MESSAGE_FETCH_ATTEMPTS:
                        return fetched_message
            except Exception:  # noqa: BLE001
                LOGGER.exception(
                    "Failed to fetch full message by conversation_message_id=%s for peer_id=%s on attempt %s",
                    conversation_message_id,
                    peer_id,
                    attempt,
                )

            if should_retry and attempt < FULL_MESSAGE_FETCH_ATTEMPTS:
                LOGGER.info(
                    "Retrying full message fetch in %s second(s)",
                    FULL_MESSAGE_FETCH_DELAY_SECONDS,
                )
                time.sleep(FULL_MESSAGE_FETCH_DELAY_SECONDS)

    message_id = _get_value(message, "id")
    if message_id is not None:
        try:
            response = api.messages.getById(message_ids=message_id)
            fetched_message = extract_first_message(response)
            if fetched_message is not None:
                LOGGER.info("Fetched full message by message_id=%s", message_id)
                return fetched_message
        except Exception:  # noqa: BLE001
            LOGGER.exception("Failed to fetch full message by message_id=%s", message_id)

    LOGGER.info("Could not fetch full message details; using long poll payload only")
    return None


def extract_first_message(response: Any) -> Any | None:
    items = _get_value(response, "items")
    if items:
        return items[0]

    messages = _get_value(response, "messages")
    if messages:
        message_items = _get_value(messages, "items")
        if message_items:
            return message_items[0]

    return None


def extract_photo_attachments(message: Any, source: str) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    _collect_photo_attachments_from_message(
        message=message,
        source=source,
        result=result,
        seen=seen,
    )
    LOGGER.info("Collected %s photo attachment(s) from %s", len(result), source)
    return result


def _collect_photo_attachments_from_message(
    message: Any,
    source: str,
    result: list[str],
    seen: set[str],
) -> None:
    attachments = _get_value(message, "attachments", []) or []
    LOGGER.info("Inspecting %s attachment payload: count=%s", source, len(attachments))

    for attachment in attachments:
        attachment_type = _get_value(attachment, "type")
        if attachment_type != "photo":
            LOGGER.info("Skipping unsupported attachment type in %s: %s", source, attachment_type)
            continue

        photo = _get_value(attachment, "photo")
        if not photo:
            LOGGER.warning("Photo attachment payload is empty in %s", source)
            continue

        attachment_ref = build_photo_attachment(photo)
        if attachment_ref in seen:
            LOGGER.info("Skipping duplicate photo attachment in %s: %s", source, attachment_ref)
            continue

        seen.add(attachment_ref)
        result.append(attachment_ref)
        LOGGER.info("Prepared photo attachment from %s: %s", source, attachment_ref)

    reply_message = _get_value(message, "reply_message")
    if reply_message:
        _collect_photo_attachments_from_message(
            message=reply_message,
            source=f"{source}.reply_message",
            result=result,
            seen=seen,
        )

    forwarded_messages = _get_value(message, "fwd_messages", []) or []
    for index, forwarded_message in enumerate(forwarded_messages, start=1):
        _collect_photo_attachments_from_message(
            message=forwarded_message,
            source=f"{source}.fwd[{index}]",
            result=result,
            seen=seen,
        )


def build_photo_attachment(photo: Any) -> str:
    owner_id = _get_value(photo, "owner_id")
    media_id = _get_value(photo, "id")
    access_key = _get_value(photo, "access_key")
    if owner_id is None or media_id is None:
        raise RuntimeError("Uploaded photo data is missing owner_id or id")

    attachment = f"photo{owner_id}_{media_id}"
    if access_key:
        attachment += f"_{access_key}"
    else:
        LOGGER.warning(
            "Photo attachment %s_%s does not have access_key; VK may reject it",
            owner_id,
            media_id,
        )

    return attachment


def _get_value(data: Any, key: str, default: Any = None) -> Any:
    if isinstance(data, dict):
        return data.get(key, default)
    return getattr(data, key, default)
