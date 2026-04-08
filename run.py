from __future__ import annotations

import json
import logging
import os
import time
from logging import FileHandler, Formatter, StreamHandler

import vk_api
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll
from vk_api.keyboard import VkKeyboard, VkKeyboardColor

from config import ADMIN_USERS as CONFIG_ADMIN_USERS, GROUPS_TOKENS as CONFIG_GROUPS_TOKENS, VK_BOT_TOKEN as CONFIG_VK_BOT_TOKEN
from tools import (
    GroupTarget,
    PatternStore,
    PendingPost,
    build_group_targets,
    collect_photo_attachments,
    compose_post_text,
    get_current_group,
    get_message_text,
    resolve_admin_ids,
    send_message,
)


def _load_env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def _load_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def _load_env_tokens(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if not value:
        return list(default)

    stripped = value.strip()
    if stripped.startswith("["):
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            normalized = [str(item).strip() for item in parsed if str(item).strip()]
            if normalized:
                return normalized

    normalized = stripped.replace("\r", "\n").replace(",", "\n")
    tokens = [item.strip() for item in normalized.split("\n") if item.strip()]
    return tokens or list(default)


def _load_admin_users(default: list[str]) -> list[str]:
    admin_users = _load_env_tokens("ADMIN_USERS", default)
    legacy_admin = os.getenv("ADMIN_US")
    if legacy_admin:
        return [legacy_admin.strip()]
    return admin_users


LOGGER = logging.getLogger(__name__)
BOT_LOG_PATH = os.getenv("BOT_LOG_PATH", "bot.log")
CRITICAL_LOG_PATH = os.getenv("CRITICAL_LOG_PATH", "critical_errors.txt")
LONGPOLL_RETRY_DELAY_SECONDS = _load_env_int("LONGPOLL_RETRY_DELAY_SECONDS", 5)
SUFFIX_FILE_PATH = os.getenv("SUFFIX_FILE_PATH", "template.txt")
NON_ADMIN_REPLY_COOLDOWN_SECONDS = _load_env_int("NON_ADMIN_REPLY_COOLDOWN_SECONDS", 6 * 60 * 60)
LOG_TO_FILES = _load_env_flag("LOG_TO_FILES", True)
VK_BOT_TOKEN = os.getenv("VK_BOT_TOKEN", CONFIG_VK_BOT_TOKEN)
ADMIN_USERS = _load_admin_users(CONFIG_ADMIN_USERS)
GROUPS_TOKENS = _load_env_tokens("GROUPS_TOKENS", CONFIG_GROUPS_TOKENS)

HELP_COMMANDS = {"help", "/help", "menu", "/menu", "start", "/start", "commands", "/commands", "помощь", "/помощь", "команды", "/команды", "справка", "/справка"}
STATUS_COMMANDS = {"status", "/status", "draft", "/draft", "info", "/info", "статус", "/статус", "черновик"}
PREVIEW_COMMANDS = {"preview", "/preview", "show", "/show", "предпросмотр", "/предпросмотр"}
CANCEL_COMMANDS = {"cancel", "/cancel", "clear", "/clear", "reset", "/reset", "отмена", "/отмена", "сброс", "отменить", "/отменить"}
PUBLISH_COMMANDS = {"+", "publish", "/publish", "post", "/post", "send", "/send", "опубликовать", "/опубликовать"}
PATTERN_CLEAR_VALUES = {"clear", "reset", "none", "empty", "пусто", "очистить", "сброс"}
PATTERN_COMMANDS = {"pattern", "/pattern", "suffix", "/suffix", "суффикс", "/суффикс"}
PATTERN_INPUT_CANCEL_COMMANDS = {
    "отмена ввода",
    "/отмена_ввода",
    "отмена",
    "/отмена",
    "cancel",
    "/cancel",
    "cancel suffix",
    "/cancel_suffix",
}
PATTERN_INPUT_CLEAR_COMMANDS = {"очистить суффикс", "/очистить_суффикс", "clear suffix", "/clear_suffix"}


def configure_logging() -> None:
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)

    formatter = Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    console_handler = StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)
    if LOG_TO_FILES:
        try:
            file_handler = FileHandler(BOT_LOG_PATH, encoding="utf-8")
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

            critical_handler = FileHandler(CRITICAL_LOG_PATH, encoding="utf-8")
            critical_handler.setLevel(logging.CRITICAL)
            critical_handler.setFormatter(formatter)
            root_logger.addHandler(critical_handler)
        except OSError:
            root_logger.warning("File logging is unavailable; continuing with console logging only")


def notify_admin(
    bot_api: object,
    admin_id: int,
    message: str,
    keyboard: str | None = None,
    attachments: list[str] | None = None,
) -> None:
    try:
        send_message(bot_api, admin_id, message, keyboard=keyboard, attachments=attachments)
    except Exception:  # noqa: BLE001
        LOGGER.exception("Failed to send message to admin")


def _get_value(data: object, key: str, default: object = None) -> object:
    if isinstance(data, dict):
        return data.get(key, default)
    return getattr(data, key, default)


def is_private_message(message: object) -> bool:
    peer_id = _get_value(message, "peer_id")
    sender_id = _get_value(message, "from_id")
    return peer_id is not None and sender_id is not None and int(peer_id) == int(sender_id)


def build_non_admin_message() -> str:
    return (
        "Данный аккаунт используется как технический бот для внутренних публикаций.\n"
        "Обработка сообщений выполняется только для администратора."
    )


def decode_escaped_newlines(value: str) -> str:
    return value.replace("\\r\\n", "\n").replace("\\n", "\n").strip()


def validate_runtime_settings() -> None:
    if not VK_BOT_TOKEN.strip():
        raise RuntimeError("VK_BOT_TOKEN is empty")
    if not ADMIN_USERS:
        raise RuntimeError("ADMIN_USERS is empty")
    if not GROUPS_TOKENS:
        raise RuntimeError("GROUPS_TOKENS is empty")


def build_admin_help_message() -> str:
    return (
        "Доступные команды:\n\n"
        "Опубликовать - публикация текущего черновика\n"
        "Статус - сведения о текущем черновике и суффиксе\n"
        "Предпросмотр - просмотр итогового текста перед публикацией\n"
        "Отменить - удаление текущего черновика\n"
        "Суффикс - переход в режим обновления суффикса\n"
        "pattern <текст> - установка нового суффикса\n"
        "pattern clear - очистка суффикса\n"
        "Справка - повторный вывод этого сообщения\n\n"
        "Любое иное сообщение от администратора сохраняется как новый черновик. "
        "Фотографии необходимо отправлять как фотографии VK. "
        "Последовательность \\n в тексте и суффиксе преобразуется в перенос строки."
    )


def build_admin_keyboard(
    has_pending_draft: bool,
    awaiting_pattern_input: bool = False,
) -> str:
    keyboard = VkKeyboard(inline=True)

    if awaiting_pattern_input:
        keyboard.add_button("Очистить суффикс", color=VkKeyboardColor.NEGATIVE)
        keyboard.add_button("Отмена ввода", color=VkKeyboardColor.SECONDARY)
    elif has_pending_draft:
        keyboard.add_button("Опубликовать", color=VkKeyboardColor.POSITIVE)
        keyboard.add_button("Предпросмотр", color=VkKeyboardColor.PRIMARY)
        keyboard.add_line()
        keyboard.add_button("Отменить", color=VkKeyboardColor.NEGATIVE)
        keyboard.add_button("Суффикс", color=VkKeyboardColor.SECONDARY)
    else:
        keyboard.add_button("Суффикс", color=VkKeyboardColor.SECONDARY)

    return keyboard.get_keyboard()


def build_status_message(
    pending_post: PendingPost | None,
    suffix: str,
    targets: list[GroupTarget],
) -> str:
    suffix_preview = suffix if suffix else "<пусто>"
    targets_preview = ", ".join(target.name for target in targets) if targets else "<нет>"

    if pending_post is None:
        return (
            "Статус бота:\n"
            "Черновик: отсутствует\n"
            f"Суффикс: {suffix_preview}\n"
            f"Группы: {targets_preview}\n\n"
            "Допускается отправка нового текста или фотографий для формирования черновика."
        )

    return (
        "Статус бота:\n"
        "Черновик: сформирован\n"
        f"Текст: {pending_post.preview()}\n"
        f"Фото: {len(pending_post.attachments)}\n"
        f"Суффикс: {suffix_preview}\n"
        f"Группы: {targets_preview}\n\n"
        "Для публикации доступна кнопка «Опубликовать»."
    )


def build_preview_message(draft: PendingPost | None, suffix: str) -> str:
    if draft is None:
        return "Черновик отсутствует. Для продолжения необходимо отправить текст или фотографии."

    return compose_post_text(draft.text, suffix)


def handle_pattern_command(
    bot_api: object,
    admin_id: int,
    store: PatternStore,
    text: str,
    has_pending_draft: bool,
) -> bool:
    parts = text.split(maxsplit=1)
    new_pattern = parts[1].strip() if len(parts) > 1 else ""

    if not new_pattern:
        current_pattern = store.load()
        current_preview = current_pattern if current_pattern else "<пусто>"
        LOGGER.info("Admin entered suffix input mode")
        notify_admin(
            bot_api,
            admin_id,
            f"Текущий суффикс:\n{current_preview}\n\n"
            "Следующим сообщением отправьте новый суффикс.\n"
            "Последовательность \\n будет преобразована в перенос строки.\n"
            "Для очистки используйте кнопку «Очистить суффикс».\n"
            "Для выхода без изменений используйте кнопку «Отмена ввода».",
            keyboard=build_admin_keyboard(
                has_pending_draft=has_pending_draft,
                awaiting_pattern_input=True,
            ),
        )
        return True

    normalized_pattern = decode_escaped_newlines(new_pattern)
    if normalized_pattern.casefold() in PATTERN_CLEAR_VALUES:
        store.save("")
        LOGGER.info("Admin cleared suffix")
        notify_admin(
            bot_api,
            admin_id,
            "Суффикс очищен. Дополнительный текст к публикации применяться не будет.",
            keyboard=build_admin_keyboard(has_pending_draft=has_pending_draft),
        )
        return False

    store.save(normalized_pattern)
    LOGGER.info("Admin changed suffix to: %s", normalized_pattern)
    notify_admin(
        bot_api,
        admin_id,
        f"Суффикс обновлен:\n{normalized_pattern}",
        keyboard=build_admin_keyboard(has_pending_draft=has_pending_draft),
    )
    return False


def build_confirmation_message(draft: PendingPost, suffix: str) -> str:
    suffix_preview = suffix if suffix else "<пусто>"
    photo_count = len(draft.attachments)
    return (
        "Черновик сохранен.\n"
        f"Текст: {draft.preview()}\n"
        f"Фото: {photo_count}\n"
        f"Суффикс для поста: {suffix_preview}\n\n"
        "Для публикации используйте кнопку «Опубликовать»."
    )


def publish_message(
    bot_api: object,
    admin_id: int,
    targets: list[GroupTarget],
    draft: PendingPost,
    suffix: str,
) -> bool:
    post_text = compose_post_text(draft.text, suffix)
    success_lines: list[str] = []
    error_lines: list[str] = []

    LOGGER.info(
        "Publishing confirmed draft: text_len=%s attachments=%s suffix_len=%s",
        len(draft.text),
        len(draft.attachments),
        len(suffix),
    )

    for target in targets:
        try:
            post_id = target.publish(
                message=post_text,
                attachments=draft.attachments,
            )
            success_lines.append(
                f"{target.name}: {target.wall_url(post_id)} (post_id={post_id})"
            )
            LOGGER.info("Published to '%s' with post_id=%s", target.name, post_id)
        except Exception as error:  # noqa: BLE001
            LOGGER.exception("Failed to publish to %s", target.name)
            error_lines.append(f"{target.name}: {error}")

    if success_lines and not error_lines:
        notify_admin(
            bot_api,
            admin_id,
            "Публикация выполнена:\n" + "\n".join(success_lines),
            keyboard=build_admin_keyboard(has_pending_draft=False),
        )
        return True

    if success_lines and error_lines:
        notify_admin(
            bot_api,
            admin_id,
            "Публикация выполнена частично.\n\nУспешно:\n"
            + "\n".join(success_lines)
            + "\n\nОшибки:\n"
            + "\n".join(error_lines),
            keyboard=build_admin_keyboard(has_pending_draft=False),
        )
        return True

    notify_admin(
        bot_api,
        admin_id,
        "Публикация не выполнена. Черновик сохранен.\n\nОшибки:\n"
        + "\n".join(error_lines),
        keyboard=build_admin_keyboard(has_pending_draft=True),
    )
    return False


def handle_admin_message(
    bot_api: object,
    admin_id: int,
    store: PatternStore,
    targets: list[GroupTarget],
    pending_post: PendingPost | None,
    awaiting_pattern_input: bool,
    message: object,
) -> tuple[PendingPost | None, bool]:
    raw_text = get_message_text(message)
    text = decode_escaped_newlines(raw_text)
    normalized_text = raw_text.casefold()
    first_token = normalized_text.split(maxsplit=1)[0] if normalized_text else ""
    LOGGER.info("Received admin message: raw_text_len=%s decoded_text_len=%s", len(raw_text), len(text))

    if awaiting_pattern_input:
        if normalized_text in PATTERN_INPUT_CANCEL_COMMANDS:
            LOGGER.info("Admin canceled suffix input mode")
            notify_admin(
                bot_api,
                admin_id,
                "Изменения не внесены.",
                keyboard=build_admin_keyboard(has_pending_draft=pending_post is not None),
            )
            return pending_post, False

        if normalized_text in PATTERN_INPUT_CLEAR_COMMANDS or text.casefold() in PATTERN_CLEAR_VALUES:
            store.save("")
            LOGGER.info("Admin cleared suffix from input mode")
            notify_admin(
                bot_api,
                admin_id,
                "Суффикс очищен. Дополнительный текст к публикации применяться не будет.",
                keyboard=build_admin_keyboard(has_pending_draft=pending_post is not None),
            )
            return pending_post, False

        if not text:
            notify_admin(
                bot_api,
                admin_id,
                "Текст суффикса не получен. Следующим сообщением отправьте новый суффикс.",
                keyboard=build_admin_keyboard(
                    has_pending_draft=pending_post is not None,
                    awaiting_pattern_input=True,
                ),
            )
            return pending_post, True

        store.save(text)
        LOGGER.info("Admin updated suffix from input mode")
        notify_admin(
            bot_api,
            admin_id,
            f"Суффикс обновлен:\n{text}",
            keyboard=build_admin_keyboard(has_pending_draft=pending_post is not None),
        )
        return pending_post, False

    if normalized_text in HELP_COMMANDS:
        notify_admin(
            bot_api,
            admin_id,
            build_admin_help_message(),
            keyboard=build_admin_keyboard(has_pending_draft=pending_post is not None),
        )
        return pending_post, False

    if normalized_text in STATUS_COMMANDS:
        notify_admin(
            bot_api,
            admin_id,
            build_status_message(pending_post, store.load(), targets),
            keyboard=build_admin_keyboard(has_pending_draft=pending_post is not None),
        )
        return pending_post, False

    if normalized_text in PREVIEW_COMMANDS:
        if pending_post is None:
            notify_admin(
                bot_api,
                admin_id,
                build_preview_message(pending_post, store.load()),
                keyboard=build_admin_keyboard(has_pending_draft=False),
            )
        else:
            notify_admin(
                bot_api,
                admin_id,
                build_preview_message(pending_post, store.load()),
                keyboard=build_admin_keyboard(has_pending_draft=True),
                attachments=pending_post.attachments,
            )
        return pending_post, False

    if normalized_text in CANCEL_COMMANDS:
        if pending_post is None:
            notify_admin(
                bot_api,
                admin_id,
                "Черновик отсутствует. Удаление не требуется.",
                keyboard=build_admin_keyboard(has_pending_draft=False),
            )
            return None, False

        LOGGER.info("Admin cleared pending draft")
        notify_admin(
            bot_api,
            admin_id,
            "Черновик удален.",
            keyboard=build_admin_keyboard(has_pending_draft=False),
        )
        return None, False

    if first_token in PATTERN_COMMANDS:
        awaiting_pattern_input = handle_pattern_command(
            bot_api,
            admin_id,
            store,
            raw_text,
            has_pending_draft=pending_post is not None,
        )
        return pending_post, awaiting_pattern_input

    if normalized_text in PUBLISH_COMMANDS:
        LOGGER.info("Admin sent publish confirmation")
        if pending_post is None:
            LOGGER.info("Admin confirmed draft, but no pending draft exists")
            notify_admin(
                bot_api,
                admin_id,
                "Черновик отсутствует. Публикация невозможна.",
                keyboard=build_admin_keyboard(has_pending_draft=False),
            )
            return None, False

        suffix = store.load()
        should_clear_pending = publish_message(
            bot_api=bot_api,
            admin_id=admin_id,
            targets=targets,
            draft=pending_post,
            suffix=suffix,
        )
        if should_clear_pending:
            LOGGER.info("Pending draft cleared after publish attempt")
            return None, False

        LOGGER.info("Pending draft kept after failed publish attempt")
        return pending_post, False

    attachments = collect_photo_attachments(bot_api, message)

    LOGGER.info(
        "Admin draft payload prepared: text_len=%s attachments=%s",
        len(text),
        len(attachments),
    )

    new_draft = PendingPost(
        text=text,
        attachments=attachments,
    )
    if new_draft.is_empty():
        LOGGER.info("Ignoring empty admin message without supported attachments")
        notify_admin(
            bot_api,
            admin_id,
            "Сообщение пустое или не содержит поддерживаемых вложений. "
            "Фотографии должны быть отправлены как фотографии VK, а не как файл или документ.",
            keyboard=build_admin_keyboard(has_pending_draft=pending_post is not None),
        )
        return pending_post, False

    if pending_post is None:
        LOGGER.info("Saved new pending draft")
    else:
        LOGGER.info("Replaced existing pending draft with a new one")

    suffix = store.load()
    notify_admin(
        bot_api,
        admin_id,
        build_confirmation_message(new_draft, suffix),
        keyboard=build_admin_keyboard(has_pending_draft=True),
    )
    return new_draft, False


def main() -> None:
    validate_runtime_settings()
    pattern_store = PatternStore(SUFFIX_FILE_PATH)
    bot_session = vk_api.VkApi(token=VK_BOT_TOKEN)
    bot_api = bot_session.get_api()

    bot_group_id, bot_group_name = get_current_group(bot_api)
    admin_ids = resolve_admin_ids(bot_api, ADMIN_USERS)
    if not admin_ids:
        raise RuntimeError("No admin ids could be resolved")
    admin_id_set = set(admin_ids)
    targets = build_group_targets(GROUPS_TOKENS)
    pending_post: PendingPost | None = None
    awaiting_pattern_input = False
    non_admin_reply_timestamps: dict[int, float] = {}

    LOGGER.info("Bot group: %s (%s)", bot_group_name, bot_group_id)
    LOGGER.info("Admin ids: %s", ", ".join(str(admin_id) for admin_id in admin_ids))
    LOGGER.info("Target groups: %s", ", ".join(target.name for target in targets))
    LOGGER.info("Suffix file: %s", pattern_store.path)
    LOGGER.info("Regular logs file: %s", BOT_LOG_PATH)
    LOGGER.info("Critical logs file: %s", CRITICAL_LOG_PATH)

    while True:
        try:
            LOGGER.info("Starting long poll connection")
            longpoll = VkBotLongPoll(bot_session, bot_group_id)

            for event in longpoll.listen():
                if event.type != VkBotEventType.MESSAGE_NEW:
                    continue

                if not event.from_user:
                    LOGGER.info("Ignoring non-user incoming event")
                    continue

                message = event.message
                sender_id = int(message["from_id"])
                if sender_id not in admin_id_set:
                    if is_private_message(message):
                        current_time = time.monotonic()
                        last_reply_time = non_admin_reply_timestamps.get(sender_id, 0.0)
                        if current_time - last_reply_time >= NON_ADMIN_REPLY_COOLDOWN_SECONDS:
                            try:
                                send_message(bot_api, sender_id, build_non_admin_message())
                                non_admin_reply_timestamps[sender_id] = current_time
                                LOGGER.info("Sent technical-bot notice to non-admin user_id=%s", sender_id)
                            except Exception:  # noqa: BLE001
                                LOGGER.exception("Failed to send non-admin notice to user_id=%s", sender_id)
                        else:
                            LOGGER.info("Skipped non-admin notice due to cooldown for user_id=%s", sender_id)

                    LOGGER.info("Ignoring message from non-admin user_id=%s", sender_id)
                    continue

                try:
                    pending_post, awaiting_pattern_input = handle_admin_message(
                        bot_api=bot_api,
                        admin_id=sender_id,
                        store=pattern_store,
                        targets=targets,
                        pending_post=pending_post,
                        awaiting_pattern_input=awaiting_pattern_input,
                        message=message,
                    )
                except Exception:  # noqa: BLE001
                    LOGGER.exception("Unexpected error while handling admin message")
                    notify_admin(
                        bot_api,
                        sender_id,
                        "Произошла ошибка. Подробности смотрите в логах.",
                        keyboard=build_admin_keyboard(
                            has_pending_draft=pending_post is not None,
                            awaiting_pattern_input=awaiting_pattern_input,
                        ),
                    )

        except Exception:  # noqa: BLE001
            LOGGER.exception(
                "Long poll loop crashed. Restarting in %s second(s)",
                LONGPOLL_RETRY_DELAY_SECONDS,
            )
            time.sleep(LONGPOLL_RETRY_DELAY_SECONDS)


if __name__ == "__main__":
    configure_logging()
    try:
        main()
    except KeyboardInterrupt:
        LOGGER.info("Bot stopped by user")
    except Exception:
        LOGGER.critical("Bot stopped due to a critical error", exc_info=True)
        raise
