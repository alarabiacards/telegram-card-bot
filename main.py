import os
import json
import time
import re
import asyncio
import logging
import random
import secrets
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Tuple, List, Union
from datetime import datetime, timezone, timedelta

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, Response

from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.transport.requests import Request as GARequest
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("telegram-multi-card-bot")

# ---------------------------
# Tunables (Reliability / Load / Security)
# ---------------------------
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
EXPORT_TIMEOUT = int(os.getenv("EXPORT_TIMEOUT", "45"))
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "200"))
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "1"))

# IMPORTANT: rate limit ONLY on generation
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "10"))
PROGRESS_PING_SECONDS = float(os.getenv("PROGRESS_PING_SECONDS", "30"))

RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "5"))
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "0.7"))
RETRY_MAX_DELAY = float(os.getenv("RETRY_MAX_DELAY", "8.0"))

FP_DEDUP_SECONDS = int(os.getenv("FP_DEDUP_SECONDS", "60"))

# Per-queue generation concurrency
GEN_CONCURRENCY = int(os.getenv("GEN_CONCURRENCY", "1"))

# Instance name for tracking
INSTANCE_NAME = os.getenv("INSTANCE_NAME", "local").strip() or "local"

# Which bots are active on this server
ACTIVE_BOTS = os.getenv("ACTIVE_BOTS", "").strip()

# temp folder where copied presentations are created
OUTPUT_FOLDER_ID = os.getenv("OUTPUT_FOLDER_ID", "").strip()

# base url for public mini app / share links
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
SHARE_TTL_SECONDS = int(os.getenv("SHARE_TTL_SECONDS", "3600"))

# ---------------------------
# Google (shared)
# ---------------------------
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "").strip()
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON", "").strip()

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/spreadsheets",
]

PLACEHOLDER_AR = os.getenv("PLACEHOLDER_AR", "<<Name in Arabic>>")
PLACEHOLDER_EN = os.getenv("PLACEHOLDER_EN", "<<Name in English>>")

# ---------------------------
# Google Sheet tracking
# ---------------------------
SHEET_ID = os.getenv("SHEET_ID", "").strip()
SHEET_TAB = os.getenv("SHEET_TAB", "Tracking").strip()

# IMPORTANT:
# A Timestamp
# B Bot
# C Status
# D Ar Name
# E En Name
# F Chat ID
# G User ID
# H Username
# I Size
# J Design
# K Error
# L INSTANCE_NAME
# M QUEUE_WAIT_SEC
# N GEN_SEC
SHEET_COLUMNS_COUNT = 14

# ---------------------------
# Env (Bots)
# ---------------------------
BOT_TOKEN_ALARABIA = os.getenv("BOT_TOKEN_ALARABIA", "").strip()
TEMPLATE_SLIDES_ID_ALARABIA_SQUARE = os.getenv("TEMPLATE_SLIDES_ID_ALARABIA_SQUARE", "").strip()

BOT_TOKEN_ALHAFEZ = os.getenv("BOT_TOKEN_ALHAFEZ", "").strip()
TEMPLATE_SLIDES_ID_ALHAFEZ_SQUARE = os.getenv("TEMPLATE_SLIDES_ID_ALHAFEZ_SQUARE", "").strip()
TEMPLATE_SLIDES_ID_ALHAFEZ_VERTICAL = os.getenv("TEMPLATE_SLIDES_ID_ALHAFEZ_VERTICAL", "").strip()

BOT_TOKEN_ALFALAH = os.getenv("BOT_TOKEN_ALFALAH", "").strip()
TEMPLATE_SLIDES_ID_ALFALAH_SQUARE = os.getenv("TEMPLATE_SLIDES_ID_ALFALAH_SQUARE", "").strip()
TEMPLATE_SLIDES_ID_ALFALAH_VERTICAL = os.getenv("TEMPLATE_SLIDES_ID_ALFALAH_VERTICAL", "").strip()

BOT_TOKEN_KOUNUZ_ALWARD = os.getenv("BOT_TOKEN_KOUNUZ_ALWARD", "").strip()
TEMPLATE_SLIDES_ID_KOUNUZ_ALWARD_SQUARE = os.getenv("TEMPLATE_SLIDES_ID_KOUNUZ_ALWARD_SQUARE", "").strip()
TEMPLATE_SLIDES_ID_KOUNUZ_ALWARD_VERTICAL = os.getenv("TEMPLATE_SLIDES_ID_KOUNUZ_ALWARD_VERTICAL", "").strip()

BOT_TOKEN_AMRO = os.getenv("BOT_TOKEN_AMRO", "").strip()
TEMPLATE_SLIDES_ID_AMRO_SQUARE_1 = os.getenv("TEMPLATE_SLIDES_ID_AMRO_SQUARE_1", "").strip()
TEMPLATE_SLIDES_ID_AMRO_SQUARE_2 = os.getenv("TEMPLATE_SLIDES_ID_AMRO_SQUARE_2", "").strip()
TEMPLATE_SLIDES_ID_AMRO_SQUARE_3 = os.getenv("TEMPLATE_SLIDES_ID_AMRO_SQUARE_3", "").strip()
TEMPLATE_SLIDES_ID_AMRO_VERTICAL_1 = os.getenv("TEMPLATE_SLIDES_ID_AMRO_VERTICAL_1", "").strip()
TEMPLATE_SLIDES_ID_AMRO_VERTICAL_2 = os.getenv("TEMPLATE_SLIDES_ID_AMRO_VERTICAL_2", "").strip()
TEMPLATE_SLIDES_ID_AMRO_VERTICAL_3 = os.getenv("TEMPLATE_SLIDES_ID_AMRO_VERTICAL_3", "").strip()

# preview images via Telegram file_id
AMRO_PREVIEW_SQUARE = os.getenv("AMRO_PREVIEW_SQUARE", "").strip()
AMRO_PREVIEW_VERTICAL = os.getenv("AMRO_PREVIEW_VERTICAL", "").strip()

BOTS_CONFIG_JSON = os.getenv("BOTS_CONFIG_JSON", "").strip()

# generic share store for all bots
SHARE_STORE: Dict[str, Dict[str, Any]] = {}


def _default_bots() -> Dict[str, Dict[str, Any]]:
    return {
        "alarabia": {
            "token": BOT_TOKEN_ALARABIA,
            "template_square": TEMPLATE_SLIDES_ID_ALARABIA_SQUARE,
            "template_vertical": "",
            "lang_mode": "AR_EN",
            "branding": "alarabia",
            "design_count": 1,
            "supports_vertical": False,
        },
        "alhafez": {
            "token": BOT_TOKEN_ALHAFEZ,
            "template_square": TEMPLATE_SLIDES_ID_ALHAFEZ_SQUARE,
            "template_vertical": TEMPLATE_SLIDES_ID_ALHAFEZ_VERTICAL,
            "lang_mode": "AR_ONLY",
            "branding": "alhafez",
            "design_count": 1,
            "supports_vertical": True,
        },
        "alfalah": {
            "token": BOT_TOKEN_ALFALAH,
            "template_square": TEMPLATE_SLIDES_ID_ALFALAH_SQUARE,
            "template_vertical": TEMPLATE_SLIDES_ID_ALFALAH_VERTICAL,
            "lang_mode": "AR_ONLY",
            "branding": "alfalah",
            "design_count": 1,
            "supports_vertical": True,
        },
        "kounuz_alward": {
            "token": BOT_TOKEN_KOUNUZ_ALWARD,
            "template_square": TEMPLATE_SLIDES_ID_KOUNUZ_ALWARD_SQUARE,
            "template_vertical": TEMPLATE_SLIDES_ID_KOUNUZ_ALWARD_VERTICAL,
            "lang_mode": "AR_ONLY",
            "branding": "kounuz_alward",
            "design_count": 1,
            "supports_vertical": True,
        },
        "amro": {
            "token": BOT_TOKEN_AMRO,
            "template_square": [
                TEMPLATE_SLIDES_ID_AMRO_SQUARE_1,
                TEMPLATE_SLIDES_ID_AMRO_SQUARE_2,
                TEMPLATE_SLIDES_ID_AMRO_SQUARE_3,
            ],
            "template_vertical": [
                TEMPLATE_SLIDES_ID_AMRO_VERTICAL_1,
                TEMPLATE_SLIDES_ID_AMRO_VERTICAL_2,
                TEMPLATE_SLIDES_ID_AMRO_VERTICAL_3,
            ],
            "lang_mode": "AR_ONLY",
            "branding": "amro",
            "design_count": 3,
            "supports_vertical": True,
        },
    }


def load_bots_config() -> Dict[str, Dict[str, Any]]:
    if not BOTS_CONFIG_JSON:
        return _default_bots()
    try:
        cfg = json.loads(BOTS_CONFIG_JSON)
        if not isinstance(cfg, dict):
            raise ValueError("BOTS_CONFIG_JSON must be an object")
        for k, v in cfg.items():
            if not isinstance(v, dict):
                raise ValueError(f"BOTS_CONFIG_JSON[{k}] must be an object")
            if not (v.get("token") or "").strip():
                raise ValueError(f"BOTS_CONFIG_JSON[{k}].token missing")
            if v.get("lang_mode") not in ("AR_ONLY", "AR_EN"):
                raise ValueError(f"BOTS_CONFIG_JSON[{k}].lang_mode must be AR_ONLY or AR_EN")
            v.setdefault("branding", k)
            v.setdefault("design_count", 1)
            v.setdefault("supports_vertical", bool((v.get("template_vertical") or "")))
        return cfg
    except Exception as e:
        log.exception("Invalid BOTS_CONFIG_JSON, falling back to defaults: %s", e)
        return _default_bots()


def filter_active_bots(all_bots: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    if not ACTIVE_BOTS:
        return all_bots

    names = [x.strip() for x in ACTIVE_BOTS.split(",") if x.strip()]
    filtered = {}

    for name in names:
        if name not in all_bots:
            raise RuntimeError(f"ACTIVE_BOTS contains unknown bot: {name}")
        filtered[name] = all_bots[name]

    if not filtered:
        raise RuntimeError("No active bots selected in ACTIVE_BOTS")

    return filtered


BOTS: Dict[str, Dict[str, Any]] = load_bots_config()
BOTS = filter_active_bots(BOTS)

app = FastAPI()

# ---------------------------
# Retry helpers
# ---------------------------
def _is_retryable_status(code: int) -> bool:
    return code in (429, 500, 502, 503, 504)


def _sleep_backoff(attempt: int) -> None:
    base = min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2 ** max(0, attempt - 1)))
    delay = random.random() * base
    time.sleep(delay)


def request_with_retry(method: str, url: str, *, timeout: int, **kwargs) -> requests.Response:
    last_exc = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            r = requests.request(method, url, timeout=timeout, **kwargs)
            if r.status_code == 200:
                return r
            if _is_retryable_status(r.status_code):
                log.warning(
                    "Retryable HTTP %s on %s (attempt %s/%s): %s",
                    r.status_code, url, attempt, RETRY_MAX_ATTEMPTS, r.text[:300]
                )
                if attempt < RETRY_MAX_ATTEMPTS:
                    _sleep_backoff(attempt)
                    continue
            return r
        except (requests.Timeout, requests.ConnectionError, requests.RequestException) as e:
            last_exc = e
            log.warning(
                "Network error on %s %s (attempt %s/%s): %s",
                method, url, attempt, RETRY_MAX_ATTEMPTS, repr(e)
            )
            if attempt < RETRY_MAX_ATTEMPTS:
                _sleep_backoff(attempt)
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("request_with_retry failed unexpectedly")


def google_execute_with_retry(fn, *, label: str = "google_call"):
    last_exc = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            return fn()
        except HttpError as e:
            last_exc = e
            status = getattr(e, "status_code", None)
            try:
                status = int(getattr(e, "resp", {}).status)
            except Exception:
                pass
            if status and _is_retryable_status(status) and attempt < RETRY_MAX_ATTEMPTS:
                log.warning(
                    "Retryable Google HttpError %s (%s) attempt %s/%s",
                    status, label, attempt, RETRY_MAX_ATTEMPTS
                )
                _sleep_backoff(attempt)
                continue
            raise
        except Exception as e:
            last_exc = e
            if attempt < RETRY_MAX_ATTEMPTS and isinstance(e, (TimeoutError, ConnectionError)):
                log.warning(
                    "Retryable Google error (%s) attempt %s/%s: %s",
                    label, attempt, RETRY_MAX_ATTEMPTS, repr(e)
                )
                _sleep_backoff(attempt)
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("google_execute_with_retry failed unexpectedly")


def now_ts_riyadh() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


def normalize_sheet_row(values: List[str]) -> List[str]:
    vals = ["" if v is None else str(v) for v in values]
    if len(vals) < SHEET_COLUMNS_COUNT:
        vals.extend([""] * (SHEET_COLUMNS_COUNT - len(vals)))
    elif len(vals) > SHEET_COLUMNS_COUNT:
        vals = vals[:SHEET_COLUMNS_COUNT]
    return vals


def safe_sheet_append_row(values: List[str]) -> None:
    try:
        sheet_append_row(values)
    except Exception as e:
        log.warning("Sheet append failed: %s", repr(e))


def sheet_append_row(values: List[str]) -> None:
    if not SHEET_ID:
        return
    _, _, sheets, _ = build_clients()
    rng = f"{SHEET_TAB}!A1"
    body = {"values": [normalize_sheet_row(values)]}
    google_execute_with_retry(
        lambda: sheets.spreadsheets()
        .values()
        .append(
            spreadsheetId=SHEET_ID,
            range=rng,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        )
        .execute(),
        label="sheets.values.append",
    )


# ---------------------------
# Telegram helpers
# ---------------------------
TG_API = "https://api.telegram.org/bot{}/{}"


def tg(bot_token: str, method: str, data: Optional[dict] = None, files: Optional[dict] = None) -> requests.Response:
    url = TG_API.format(bot_token, method)
    r = request_with_retry("POST", url, timeout=HTTP_TIMEOUT, data=data, files=files)
    if r.status_code != 200:
        log.warning("TG error %s %s: %s", method, r.status_code, r.text[:800])
    return r


def tg_api_json(bot_token: str, method: str, payload: dict) -> dict:
    url = TG_API.format(bot_token, method)
    r = request_with_retry("POST", url, timeout=HTTP_TIMEOUT, json=payload)
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"Telegram {method} returned non-JSON response: HTTP {r.status_code}")

    if r.status_code != 200 or not data.get("ok"):
        desc = data.get("description") if isinstance(data, dict) else None
        raise RuntimeError(f"Telegram {method} failed: HTTP {r.status_code} - {desc or r.text[:300]}")
    return data


def tg_send_message(bot_token: str, chat_id: str, text: str, reply_markup: Optional[dict] = None) -> Optional[int]:
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup)
    r = tg(bot_token, "sendMessage", payload)
    try:
        j = r.json()
        return int(j["result"]["message_id"])
    except Exception:
        return None


def tg_answer_callback(bot_token: str, callback_query_id: str) -> None:
    if callback_query_id:
        tg(bot_token, "answerCallbackQuery", {"callback_query_id": callback_query_id})


def tg_toast(bot_token: str, callback_query_id: str, text: str, show_alert: bool = False) -> None:
    if callback_query_id:
        tg(
            bot_token,
            "answerCallbackQuery",
            {
                "callback_query_id": callback_query_id,
                "text": text,
                "show_alert": "true" if show_alert else "false",
            },
        )


def tg_send_photo(
    bot_token: str,
    chat_id: str,
    png_bytes: bytes,
    caption: str = "",
    reply_markup: Optional[dict] = None
) -> None:
    files = {"photo": ("card.png", png_bytes)}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    tg(bot_token, "sendPhoto", data=data, files=files)


def tg_send_photo_by_file_id(
    bot_token: str,
    chat_id: str,
    file_id: str,
    caption: str = "",
    reply_markup: Optional[dict] = None
) -> None:
    data = {
        "chat_id": chat_id,
        "photo": file_id,
    }
    if caption:
        data["caption"] = caption
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    tg(bot_token, "sendPhoto", data=data)


def tg_save_prepared_inline_photo(
    bot_token: str,
    *,
    user_id: str,
    image_url: str,
    title: str,
    caption: str = "",
) -> dict:
    """
    ينشئ PreparedInlineMessage خاص بالمستخدم الحالي،
    ليتم إرساله من داخل Telegram Mini App.
    """
    payload = {
        "user_id": int(user_id),
        "result": {
            "type": "photo",
            "id": secrets.token_hex(8),
            "photo_url": image_url,
            "thumbnail_url": image_url,
            "title": title,
            "caption": caption or "",
        },
        "allow_user_chats": True,
        "allow_bot_chats": True,
        "allow_group_chats": True,
        "allow_channel_chats": True,
    }
    return tg_api_json(bot_token, "savePreparedInlineMessage", payload)["result"]


# Async wrappers to avoid blocking event loop
async def atg_send_message(bot_token: str, chat_id: str, text: str, reply_markup: Optional[dict] = None) -> Optional[int]:
    return await asyncio.to_thread(tg_send_message, bot_token, chat_id, text, reply_markup)


async def atg_answer_callback(bot_token: str, callback_query_id: str) -> None:
    await asyncio.to_thread(tg_answer_callback, bot_token, callback_query_id)


async def atg_toast(bot_token: str, callback_query_id: str, text: str, show_alert: bool = False) -> None:
    await asyncio.to_thread(tg_toast, bot_token, callback_query_id, text, show_alert)


async def atg_send_photo(
    bot_token: str,
    chat_id: str,
    png_bytes: bytes,
    caption: str = "",
    reply_markup: Optional[dict] = None
) -> None:
    await asyncio.to_thread(tg_send_photo, bot_token, chat_id, png_bytes, caption, reply_markup)


async def atg_send_photo_by_file_id(
    bot_token: str,
    chat_id: str,
    file_id: str,
    caption: str = "",
    reply_markup: Optional[dict] = None
) -> None:
    await asyncio.to_thread(tg_send_photo_by_file_id, bot_token, chat_id, file_id, caption, reply_markup)


# ---------------------------
# Generic share helpers
# ---------------------------
def guess_base_url() -> str:
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL
    render_external = os.getenv("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
    if render_external:
        return render_external
    return ""


def make_public_url(path: str) -> str:
    base = guess_base_url()
    if not base:
        return path
    return f"{base}{path}"


def cleanup_share_store() -> None:
    now = time.time()
    expired = [k for k, v in SHARE_STORE.items() if float(v.get("expires_at", 0)) <= now]
    for k in expired:
        SHARE_STORE.pop(k, None)


def create_share_token(png_bytes: bytes, *, chat_id: str, user_id: str, bot_key: str) -> str:
    cleanup_share_store()
    token = secrets.token_urlsafe(18)
    SHARE_STORE[token] = {
        "png_bytes": png_bytes,
        "chat_id": chat_id,
        "user_id": user_id,
        "bot_key": bot_key,
        "created_at": time.time(),
        "expires_at": time.time() + SHARE_TTL_SECONDS,
    }
    return token


def get_share_item(token: str) -> Optional[Dict[str, Any]]:
    cleanup_share_store()
    item = SHARE_STORE.get(token)
    if not item:
        return None
    if float(item.get("expires_at", 0)) <= time.time():
        SHARE_STORE.pop(token, None)
        return None
    return item


def share_btn_text(is_ar_only: bool) -> str:
    return "📤 مشاركة البطاقة" if is_ar_only else "📤 مشاركة البطاقة / Share Card"


def start_btn_text(is_ar_only: bool) -> str:
    return "↩️ البداية" if is_ar_only else "↩️ Start / ابدأ"


def kb_after_ready_with_share(is_ar_only: bool, webapp_url: str) -> dict:
    return {
        "inline_keyboard": [
            [{"text": share_btn_text(is_ar_only), "web_app": {"url": webapp_url}}],
            [{"text": start_btn_text(is_ar_only), "callback_data": "START"}],
        ]
    }


# ---------------------------
# Google clients (cached)
# ---------------------------
_drive = None
_slides = None
_sheets = None
_creds = None


def require_env():
    for bot_key, bot in BOTS.items():
        if not (bot.get("token") or "").strip():
            raise RuntimeError(f"{bot_key}: token is missing")
        if bot.get("lang_mode") not in ("AR_ONLY", "AR_EN"):
            raise RuntimeError(f"{bot_key}: lang_mode must be AR_ONLY or AR_EN")

        design_count = int(bot.get("design_count") or 1)
        supports_vertical = bool(bot.get("supports_vertical"))
        tsq = bot.get("template_square")
        tv = bot.get("template_vertical")

        if design_count == 1:
            if not (isinstance(tsq, str) and tsq.strip()):
                raise RuntimeError(f"{bot_key}: template_square is missing")
            if supports_vertical and not (isinstance(tv, str) and tv.strip()):
                raise RuntimeError(f"{bot_key}: template_vertical is missing (supports_vertical=true)")
        else:
            if not (
                isinstance(tsq, list)
                and len(tsq) >= design_count
                and all(str(x).strip() for x in tsq[:design_count])
            ):
                raise RuntimeError(f"{bot_key}: template_square list is missing/invalid for design_count={design_count}")
            if supports_vertical:
                if not (
                    isinstance(tv, list)
                    and len(tv) >= design_count
                    and all(str(x).strip() for x in tv[:design_count])
                ):
                    raise RuntimeError(f"{bot_key}: template_vertical list is missing/invalid for design_count={design_count}")

    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN) and not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Provide OAuth vars (GOOGLE_CLIENT_ID/SECRET/REFRESH_TOKEN) or SERVICE_ACCOUNT_JSON")


def build_clients():
    global _drive, _slides, _sheets, _creds
    if _drive and _slides and _sheets and _creds:
        try:
            if not _creds.valid or _creds.expired:
                _creds.refresh(GARequest())
        except Exception:
            pass
        return _drive, _slides, _sheets, _creds

    if GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN:
        creds = Credentials(
            token=None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            scopes=SCOPES,
        )
        creds.refresh(GARequest())
        drive = build("drive", "v3", credentials=creds, cache_discovery=False)
        slides = build("slides", "v1", credentials=creds, cache_discovery=False)
        sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
        _drive, _slides, _sheets, _creds = drive, slides, sheets, creds
        log.info("Using OAuth user credentials")
        return drive, slides, sheets, creds

    info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    slides = build("slides", "v1", credentials=creds, cache_discovery=False)
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
    _drive, _slides, _sheets, _creds = drive, slides, sheets, creds
    log.info("Using Service Account credentials")
    return drive, slides, sheets, creds


# ---------------------------
# Validation
# ---------------------------
MAX_NAME_LEN = 40
AR_ALLOWED = re.compile(r"^[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF\s\-'.0-9]+$")
EN_ALLOWED = re.compile(r"^[A-Za-z\s\-'.0-9]+$")


def clean_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def validate_ar(name: str) -> Tuple[bool, str]:
    name = clean_text(name)
    if not name:
        return False, "الاسم فارغ."
    if len(name) > MAX_NAME_LEN:
        return False, f"الاسم طويل جدًا (أقصى {MAX_NAME_LEN} حرف)."
    if not AR_ALLOWED.match(name):
        return False, "اكتب الاسم بالعربية بدون رموز غريبة."
    return True, name


def validate_en(name: str) -> Tuple[bool, str]:
    name = clean_text(name)
    if not name:
        return False, "الاسم الإنجليزي فارغ."
    if len(name) > MAX_NAME_LEN:
        return False, f"الاسم طويل جدًا (أقصى {MAX_NAME_LEN} حرف)."
    if not EN_ALLOWED.match(name):
        return False, "اكتب الاسم بالإنجليزية (A-Z) بدون رموز."
    return True, name


# ---------------------------
# Messages / Keyboards
# ---------------------------
DIV = "\n\n--------------------\n\n"


def msg_high_load(ar_only: bool) -> str:
    if ar_only:
        return "الضغط عالي الآن. الرجاء المحاولة مرة أخرى بعد قليل."
    return "الضغط عالي الآن. الرجاء المحاولة مرة أخرى بعد قليل." + DIV + "High load right now. Please try again in a moment."


def msg_rate_limited(ar_only: bool, seconds: float) -> str:
    if ar_only:
        return f"تم استقبال طلب إصدار قبل قليل. الرجاء الانتظار {int(seconds)} ثانية ثم حاول مرة أخرى."
    ar = f"تم استقبال طلب إصدار قبل قليل. الرجاء الانتظار {int(seconds)} ثانية ثم حاول مرة أخرى."
    en = f"Please wait {int(seconds)} seconds then try again."
    return ar + DIV + en


BRANDING: Dict[str, Dict[str, str]] = {
    "alarabia": {
        "welcome_ar": (
            "مرحباً بكم في بوت إصدار بطاقات التهنئة الرقمية بشركة العربية\n\n"
            "يمكن لكل موظف إصدار البطاقة بسرعة وبشكل مستقل – مبادرة شخصية وحل رقمي – تطوير: عمرو إسماعيل"
        ),
        "welcome_en": (
            "Welcome to the Digital Greeting Card Bot at AlArabia Company\n\n"
            "Every employee can issue cards quickly and on their own – "
            "A personal initiative and digital solution – developed by Amro Ismail"
        ),
    },
    "alhafez": {
        "welcome_ar": (
            "مرحبا بكم في بوت إصدار بطاقات التهنئة لمنسوبي جمعية الحافظ لتأهيل حفاظ القرآن الكريم\n\n"
            "تطوير: عمرو إسماعيل"
        )
    },
    "alfalah": {
        "welcome_ar": (
            "مرحبا بكم في بوت إصدار بطاقات التهنئة لمنسوبي مدارس الفلاح\n\n"
            "تطوير: عمرو إسماعيل"
        )
    },
    "kounuz_alward": {
        "welcome_ar": (
            "مرحبا بكم في بوت إصدار بطاقات التهنئة من كنوز الورد\n\n"
            "تطوير: عمرو إسماعيل"
        )
    },
    "amro": {
        "welcome_ar": (
            "أنا عمرو.. أرحب بكم في بوت إصدار بطاقات التهنئة\n\n"
        )
    },
}


def get_branding(bot_key: str) -> Dict[str, str]:
    b = BOTS[bot_key]
    branding_key = b.get("branding") or bot_key
    return BRANDING.get(branding_key, BRANDING.get(bot_key, {"welcome_ar": "مرحباً بك"}))


def ar_msg_welcome(bot_key: str) -> str:
    br = get_branding(bot_key)
    ar = br.get("welcome_ar", "مرحباً بك")
    en = br.get("welcome_en", "Welcome")
    return ar + DIV + en


def ar_msg_need_start() -> str:
    return "للعودة للبداية، الرجاء إرسال /start أو اضغط زر (ابدأ) أدناه." + DIV + "To start again, send /start or tap Start below."


def ar_msg_ask_ar() -> str:
    return "اكتب اسمك بالعربية:" + DIV + "Enter your name in Arabic:"


def ar_msg_ask_en() -> str:
    return "اكتب اسمك بالإنجليزية:" + DIV + "Enter your name in English:"


def ar_msg_invalid_ar(reason_ar: str) -> str:
    return f"غير صحيح: {reason_ar}\n\nاكتب الاسم بالعربية فقط." + DIV + "Invalid Arabic name.\n\nPlease type Arabic letters only."


def ar_msg_invalid_en(reason_ar: str) -> str:
    return f"غير صحيح: {reason_ar}\n\nاكتب الاسم بالإنجليزية فقط." + DIV + "Invalid English name.\n\nPlease type English letters only."


def ar_msg_confirm(name_ar: str, name_en: str) -> str:
    ar = f"تأكيد البيانات:\n\nالاسم بالعربية: {name_ar}\nالاسم بالإنجليزية: {name_en}"
    en = f"Confirm details:\n\nArabic: {name_ar}\nEnglish: {name_en}"
    return ar + DIV + en


def ar_msg_creating() -> str:
    return "جاري إصدار البطاقة..." + DIV + "Generating your card..."


def ar_msg_still_working() -> str:
    return "لا يزال جاري إصدار البطاقة..." + DIV + "Still generating your card..."


def ar_msg_ready() -> str:
    return "تم إصدار البطاقة بنجاح." + DIV + "Your card is ready."


def ar_msg_error(err: str) -> str:
    return "خطأ أثناء إصدار البطاقة:\n" + err + DIV + "Error while generating the card:\n" + err


def ar_kb_start_card() -> dict:
    return {"inline_keyboard": [[{"text": "إصدار بطاقة تهنئة / Generate Card", "callback_data": "START_CARD"}]]}


def ar_kb_start_again() -> dict:
    return {"inline_keyboard": [[{"text": "↩️ Start / ابدأ", "callback_data": "START"}]]}


def ar_kb_wait_en() -> dict:
    return {"inline_keyboard": [[{"text": "تعديل الاسم العربي / Edit Arabic", "callback_data": "EDIT_AR"}]]}


def ar_kb_confirm() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "✅ إصدار البطاقة / Generate", "callback_data": "GEN"}],
            [
                {"text": "تعديل العربي / Edit Arabic", "callback_data": "EDIT_AR"},
                {"text": "تعديل الإنجليزي / Edit English", "callback_data": "EDIT_EN"},
            ],
            [{"text": "❌ إلغاء العملية / Cancel", "callback_data": "CANCEL"}],
        ]
    }


def hz_msg_welcome(bot_key: str) -> str:
    br = get_branding(bot_key)
    return br.get("welcome_ar", "مرحباً بك")


def hz_msg_need_start() -> str:
    return "للعودة للبداية، الرجاء إرسال /start أو اضغط زر (البداية) أدناه."


def hz_msg_ask_name() -> str:
    return "اكتب اسمك:"


def hz_msg_invalid_ar(reason_ar: str) -> str:
    return f"غير صحيح: {reason_ar}\n\nاكتب الاسم بالعربية فقط."


def hz_msg_review_name(name_ar: str) -> str:
    return f"مراجعة الاسم:\n\nالاسم: {name_ar}"


def hz_msg_choose_size(supports_vertical: bool) -> str:
    return "اختر مقاس البطاقة" if supports_vertical else "المقاس المتاح: مربع"


def hz_msg_choose_design(design_count: int) -> str:
    return "اختر رقم التصميم" if design_count > 1 else "التصميم الافتراضي"


def hz_msg_preview(bot_key: str, name_ar: str, size_label: str, design_number: int) -> str:
    base = (
        "ملخص البطاقة قبل الإصدار:\n\n"
        f"الاسم: {name_ar}\n"
        f"المقاس: {size_label}\n"
    )
    if bot_key == "amro":
        base += f"رقم التصميم: {design_number}\n"
    base += "\nهل تريد التأكيد؟"
    return base


def hz_msg_creating() -> str:
    return "جاري إنشاء البطاقة..."


def hz_msg_still_working() -> str:
    return "لا يزال جاري إصدار البطاقة..."


def hz_msg_ready() -> str:
    return "تم إصدار البطاقة بنجاح."


def hz_msg_error(err: str) -> str:
    return "خطأ أثناء إصدار البطاقة:\n" + err


def hz_kb_start_card() -> dict:
    return {"inline_keyboard": [[{"text": "إصدار بطاقة تهنئة", "callback_data": "START_CARD"}]]}


def hz_kb_start_again() -> dict:
    return {"inline_keyboard": [[{"text": "↩️ البداية", "callback_data": "START"}]]}


def hz_kb_review_name() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "تأكيد الاسم", "callback_data": "CONFIRM_NAME"}],
            [{"text": "تعديل الاسم", "callback_data": "EDIT_AR"}],
        ]
    }


def hz_kb_choose_size(supports_vertical: bool) -> dict:
    if supports_vertical:
        return {
            "inline_keyboard": [
                [{"text": "مربع", "callback_data": "GEN_SQUARE"}],
                [{"text": "طولي", "callback_data": "GEN_VERTICAL"}],
            ]
        }
    return {"inline_keyboard": [[{"text": "مربع", "callback_data": "GEN_SQUARE"}]]}


def kb_choose_design(size_key: str, design_count: int) -> dict:
    s_prefix = "S" if size_key == "SQUARE" else "V"
    rows = []
    for i in range(1, max(1, design_count) + 1):
        rows.append([{"text": f"{i}", "callback_data": f"DESIGN_{s_prefix}_{i}"}])
    return {"inline_keyboard": rows}


def kb_preview_ar(supports_vertical: bool, design_count: int) -> dict:
    rows = [
        [{"text": "✅ تأكيد الإصدار", "callback_data": "CONFIRM_GEN"}],
        [{"text": "تعديل الاسم", "callback_data": "EDIT_AR"}],
    ]
    if supports_vertical:
        rows.append([{"text": "تغيير المقاس", "callback_data": "BACK_SIZE"}])
    if design_count > 1:
        rows.append([{"text": "تغيير التصميم", "callback_data": "BACK_DESIGN"}])
    rows.append([{"text": "❌ إلغاء العملية", "callback_data": "CANCEL"}])
    return {"inline_keyboard": rows}


# ---------------------------
# Session
# ---------------------------
STATE_MENU = "MENU"
STATE_WAIT_AR = "WAIT_AR"
STATE_WAIT_EN = "WAIT_EN"
STATE_REVIEW_NAME = "REVIEW_NAME"
STATE_CHOOSE_SIZE = "CHOOSE_SIZE"
STATE_CHOOSE_DESIGN = "CHOOSE_DESIGN"
STATE_PREVIEW_AR = "PREVIEW_AR"
STATE_CONFIRM = "CONFIRM"
STATE_CREATING = "CREATING"


@dataclass
class Session:
    chat_id: str
    bot_key: str
    state: str = STATE_MENU
    name_ar: str = ""
    name_en: str = ""
    last_update_id: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    seq: int = 0
    chosen_size: str = ""
    chosen_design: int = 1
    last_name_ar: str = ""
    last_gen_ts: float = 0
    recent_fps: Dict[str, float] = field(default_factory=dict)
    user_id: str = ""
    username: str = ""


sessions: Dict[str, Session] = {}


def session_key(bot_key: str, chat_id: str) -> str:
    return f"{bot_key}:{chat_id}"


def get_session(bot_key: str, chat_id: str) -> Session:
    k = session_key(bot_key, chat_id)
    s = sessions.get(k)
    if not s:
        s = Session(chat_id=chat_id, bot_key=bot_key)
        sessions[k] = s
    return s


def reset_session(s: Session, keep_last_name: bool = True):
    s.state = STATE_MENU
    s.name_ar = ""
    s.name_en = ""
    s.chosen_size = ""
    s.chosen_design = 1
    if not keep_last_name:
        s.last_name_ar = ""


def bump_seq(s: Session):
    s.seq += 1


# ---------------------------
# Queue groups
# ---------------------------
QUEUE_ARABIA_WARD = "queue_arabia_ward"
QUEUE_HAFEZ_FALAH = "queue_hafez_falah"
QUEUE_AMRO = "queue_amro"

BOT_TO_QUEUE: Dict[str, str] = {
    "alarabia": QUEUE_ARABIA_WARD,
    "kounuz_alward": QUEUE_ARABIA_WARD,
    "alhafez": QUEUE_HAFEZ_FALAH,
    "alfalah": QUEUE_HAFEZ_FALAH,
    "amro": QUEUE_AMRO,
}

job_queues: Dict[str, asyncio.Queue] = {
    QUEUE_ARABIA_WARD: asyncio.Queue(maxsize=MAX_QUEUE_SIZE),
    QUEUE_HAFEZ_FALAH: asyncio.Queue(maxsize=MAX_QUEUE_SIZE),
    QUEUE_AMRO: asyncio.Queue(maxsize=MAX_QUEUE_SIZE),
}

queue_sems: Dict[str, asyncio.Semaphore] = {
    QUEUE_ARABIA_WARD: asyncio.Semaphore(max(1, GEN_CONCURRENCY)),
    QUEUE_HAFEZ_FALAH: asyncio.Semaphore(max(1, GEN_CONCURRENCY)),
    QUEUE_AMRO: asyncio.Semaphore(max(1, GEN_CONCURRENCY)),
}


def get_queue_name_for_bot(bot_key: str) -> str:
    qn = BOT_TO_QUEUE.get(bot_key)
    if not qn:
        raise RuntimeError(f"No queue mapped for bot: {bot_key}")
    return qn


def get_queue_for_bot(bot_key: str) -> asyncio.Queue:
    return job_queues[get_queue_name_for_bot(bot_key)]


def get_sem_for_bot(bot_key: str) -> asyncio.Semaphore:
    return queue_sems[get_queue_name_for_bot(bot_key)]


# ---------------------------
# Inflight dedupe
# ---------------------------
_inflight_lock = asyncio.Lock()
_inflight: set = set()


@dataclass
class Job:
    bot_key: str
    chat_id: str
    user_id: str
    username: str
    name_ar: str
    name_en: str
    size_key: str
    design_number: int
    template_id: str
    requested_at: float
    seq: int
    queue_name: str


async def worker_loop(queue_name: str, worker_id: int):
    require_env()
    q = job_queues[queue_name]
    log.info("Worker %s started for %s", worker_id, queue_name)
    while True:
        job: Job = await q.get()
        try:
            await process_job(job)
        except Exception as e:
            log.exception("Job failed in %s: %s", queue_name, e)
        finally:
            async with _inflight_lock:
                _inflight.discard((job.bot_key, job.chat_id, job.seq))
            q.task_done()


async def _progress_ping(bot_token: str, bot_key: str, chat_id: str, seq: int):
    await asyncio.sleep(PROGRESS_PING_SECONDS)
    s = get_session(bot_key, chat_id)
    async with s.lock:
        if s.seq != seq or s.state != STATE_CREATING:
            return
    bot = BOTS[bot_key]
    if bot.get("lang_mode") == "AR_EN":
        await atg_send_message(bot_token, chat_id, ar_msg_still_working())
    else:
        await atg_send_message(bot_token, chat_id, hz_msg_still_working())


def size_label_ar(size_key: str) -> str:
    return "مربع" if size_key == "SQUARE" else "طولي"


async def process_job(job: Job):
    bot = BOTS[job.bot_key]
    bot_token = bot["token"]
    s = get_session(job.bot_key, job.chat_id)
    queue_sem = get_sem_for_bot(job.bot_key)

    async with s.lock:
        if job.seq != s.seq:
            log.info("Skip stale job for %s", job.chat_id)
            return

    started_processing_at = time.time()
    queue_wait_sec = max(0.0, started_processing_at - float(job.requested_at or started_processing_at))

    gen_sec = 0.0
    gen_started_at = None

    try:
        async with queue_sem:
            gen_started_at = time.time()
            png_bytes = await asyncio.to_thread(
                generate_card_png,
                template_id=job.template_id,
                name_ar=job.name_ar,
                name_en=job.name_en,
                lang_mode=bot["lang_mode"],
            )
            gen_sec = max(0.0, time.time() - gen_started_at)

        async with s.lock:
            if job.seq != s.seq:
                log.info("Skip stale result for %s", job.chat_id)
                return

        await atg_send_photo(bot_token, job.chat_id, png_bytes, caption="", reply_markup=None)

        share_token = create_share_token(
            png_bytes,
            chat_id=job.chat_id,
            user_id=job.user_id,
            bot_key=job.bot_key,
        )
        share_url = make_public_url(f"/share-mini/{share_token}")
        is_ar_only = bot["lang_mode"] == "AR_ONLY"

        if is_ar_only:
            await atg_send_message(
                bot_token,
                job.chat_id,
                hz_msg_ready(),
                kb_after_ready_with_share(True, share_url),
            )
        else:
            await atg_send_message(
                bot_token,
                job.chat_id,
                ar_msg_ready(),
                kb_after_ready_with_share(False, share_url),
            )

        await asyncio.to_thread(
            safe_sheet_append_row,
            [
                now_ts_riyadh(),
                job.bot_key,
                "SUCCESS",
                job.name_ar or "",
                job.name_en or "",
                job.chat_id or "",
                job.user_id or "",
                job.username or "",
                size_label_ar(job.size_key),
                str(job.design_number or 1),
                "",
                INSTANCE_NAME,
                f"{queue_wait_sec:.2f}",
                f"{gen_sec:.2f}",
            ],
        )

        async with s.lock:
            s.last_name_ar = job.name_ar or s.last_name_ar
            reset_session(s, keep_last_name=True)

    except Exception as e:
        if gen_started_at is not None and gen_sec <= 0.0:
            gen_sec = max(0.0, time.time() - gen_started_at)

        if bot["lang_mode"] == "AR_EN":
            await atg_send_message(bot_token, job.chat_id, ar_msg_error(str(e)), ar_kb_start_again())
        else:
            await atg_send_message(bot_token, job.chat_id, hz_msg_error(str(e)), hz_kb_start_again())

        await asyncio.to_thread(
            safe_sheet_append_row,
            [
                now_ts_riyadh(),
                job.bot_key,
                "ERROR",
                job.name_ar or "",
                job.name_en or "",
                job.chat_id or "",
                job.user_id or "",
                job.username or "",
                size_label_ar(job.size_key),
                str(job.design_number or 1),
                str(e)[:400],
                INSTANCE_NAME,
                f"{queue_wait_sec:.2f}",
                f"{gen_sec:.2f}",
            ],
        )

        async with s.lock:
            reset_session(s, keep_last_name=True)


# ---------------------------
# Google: generate PNG
# ---------------------------
def export_png(pres_id: str, slide_object_id: str, creds) -> bytes:
    if not creds.valid or creds.expired:
        creds.refresh(GARequest())
    url = f"https://docs.google.com/presentation/d/{pres_id}/export/png?pageid={slide_object_id}"
    headers = {"Authorization": f"Bearer {creds.token}"}
    r = request_with_retry("GET", url, timeout=EXPORT_TIMEOUT, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"Export PNG failed: HTTP {r.status_code} - {r.text[:300]}")
    return r.content


def generate_card_png(template_id: str, name_ar: str, name_en: str, lang_mode: str) -> bytes:
    drive, slides, _, creds = build_clients()
    pres_id = None

    try:
        copy_body = {"name": f"tg_card_{int(time.time())}"}
        if OUTPUT_FOLDER_ID:
            copy_body["parents"] = [OUTPUT_FOLDER_ID]

        copied = google_execute_with_retry(
            lambda: drive.files()
            .copy(
                fileId=template_id,
                body=copy_body,
                supportsAllDrives=True,
            )
            .execute(),
            label="drive.files.copy",
        )
        pres_id = copied["id"]

        reqs = [{"replaceAllText": {"containsText": {"text": PLACEHOLDER_AR}, "replaceText": name_ar}}]
        if lang_mode == "AR_EN":
            reqs.append({"replaceAllText": {"containsText": {"text": PLACEHOLDER_EN}, "replaceText": name_en}})

        google_execute_with_retry(
            lambda: slides.presentations()
            .batchUpdate(
                presentationId=pres_id,
                body={"requests": reqs},
            )
            .execute(),
            label="slides.presentations.batchUpdate",
        )

        pres = google_execute_with_retry(
            lambda: slides.presentations().get(presentationId=pres_id).execute(),
            label="slides.presentations.get",
        )
        slide_id = pres["slides"][0]["objectId"]

        return export_png(pres_id, slide_id, creds)

    finally:
        if pres_id:
            try:
                google_execute_with_retry(
                    lambda: drive.files().delete(fileId=pres_id, supportsAllDrives=True).execute(),
                    label="drive.files.delete",
                )
            except Exception:
                pass


# ---------------------------
# Update parsing + smart intent
# ---------------------------
ARABIC_DIACRITICS_RE = re.compile(r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06ED]")


def extract_update(data: Dict[str, Any]) -> Tuple[int, Optional[str], Optional[str], Optional[int], Optional[str], Optional[str], Optional[str]]:
    update_id = int(data.get("update_id") or 0)

    if "callback_query" in data:
        cq = data["callback_query"]
        cq_id = str(cq.get("id") or "")
        frm = cq.get("from") or {}
        user_id = str(frm.get("id") or "")
        username = str(frm.get("username") or "")

        msg = cq.get("message") or {}
        chat_id = str((msg.get("chat") or {}).get("id") or "")
        message_id = int(msg.get("message_id") or 0)
        text = str(cq.get("data") or "")
        return update_id, chat_id, text, message_id, cq_id, user_id, username

    msg = data.get("message") or {}
    frm = msg.get("from") or {}
    user_id = str(frm.get("id") or "")
    username = str(frm.get("username") or "")

    chat_id = str((msg.get("chat") or {}).get("id") or "")
    message_id = int(msg.get("message_id") or 0)
    text = str(msg.get("text") or "")
    return update_id, chat_id, text, message_id, None, user_id, username


def normalize_intent_text(text: str) -> str:
    s = clean_text(text).lower()
    s = s.translate(str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789"))
    s = s.translate(str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789"))
    s = ARABIC_DIACRITICS_RE.sub("", s)

    repl = {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ٱ": "ا",
        "ى": "ي",
        "ؤ": "و",
        "ئ": "ي",
        "ة": "ه",
        "ـ": " ",
    }
    for k, v in repl.items():
        s = s.replace(k, v)

    s = s.replace("/", " ")
    s = s.replace("|", " ")
    s = s.replace("-", " ")
    s = s.replace("_", " ")

    s = re.sub(r"[^0-9A-Za-z\u0600-\u06FF\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def levenshtein_distance(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)

    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            curr.append(min(
                prev[j] + 1,
                curr[j - 1] + 1,
                prev[j - 1] + cost
            ))
        prev = curr
    return prev[-1]


def typo_tolerant_match(token: str, keyword: str) -> bool:
    if not token or not keyword:
        return False

    if token == keyword:
        return True

    if keyword in token or token in keyword:
        shorter = min(len(token), len(keyword))
        if shorter >= 4:
            return True

    dist = levenshtein_distance(token, keyword)
    max_len = max(len(token), len(keyword))

    if max_len <= 4:
        return dist <= 1
    if max_len <= 7:
        return dist <= 2
    return dist <= 2


def contains_any_phrase(text: str, phrases: List[str]) -> bool:
    nt = normalize_intent_text(text)
    if not nt:
        return False

    nt_tokens = nt.split()

    for phrase in phrases:
        np = normalize_intent_text(phrase)
        if not np:
            continue

        if np in nt:
            return True

        p_tokens = np.split()
        if not p_tokens:
            continue

        for start in range(0, len(nt_tokens) - len(p_tokens) + 1):
            ok = True
            for i, p_tok in enumerate(p_tokens):
                if not typo_tolerant_match(nt_tokens[start + i], p_tok):
                    ok = False
                    break
            if ok:
                return True

        all_found = True
        for p_tok in p_tokens:
            found = any(typo_tolerant_match(t, p_tok) for t in nt_tokens)
            if not found:
                all_found = False
                break
        if all_found:
            return True

    return False


def extract_design_number(text: str, max_design: int) -> Optional[int]:
    nt = normalize_intent_text(text)
    if not nt:
        return None

    m = re.search(r"\b([1-9]\d*)\b", nt)
    if m:
        idx = int(m.group(1))
        if 1 <= idx <= max_design:
            return idx

    en_map = {
        "one": 1, "first": 1,
        "two": 2, "second": 2,
        "three": 3, "third": 3,
        "four": 4, "fourth": 4,
        "five": 5, "fifth": 5,
    }
    for word, idx in en_map.items():
        if contains_any_phrase(nt, [word]) and 1 <= idx <= max_design:
            return idx

    ar_map = {
        "الاول": 1, "اول": 1, "واحد": 1,
        "الثاني": 2, "ثاني": 2, "اثنين": 2, "اثنان": 2,
        "الثالث": 3, "ثالث": 3, "ثلاثه": 3, "ثلاثة": 3,
        "الرابع": 4, "رابع": 4, "اربعه": 4, "أربعة": 4,
        "الخامس": 5, "خامس": 5, "خمسه": 5, "خمسة": 5,
    }
    for word, idx in ar_map.items():
        if contains_any_phrase(nt, [word]) and 1 <= idx <= max_design:
            return idx

    return None


def normalize_cmd(text: str) -> str:
    nt = normalize_intent_text(text)

    if nt in {"start", "home", "menu", "ابدا", "ابدأ", "البدايه", "البداية"}:
        return "START"

    if nt in {"cancel", "الغاء", "إلغاء", "الغاء العمليه", "ايقاف", "stop"}:
        return "CANCEL"

    return ""


def infer_command(
    text: str,
    state: str,
    is_ar_only: bool,
    supports_vertical: bool,
    design_count: int,
    chosen_size: str = "",
) -> str:
    raw = clean_text(text)
    if not raw:
        return ""

    if raw in {
        "EDIT_AR", "EDIT_EN", "GEN", "GEN_SQUARE", "GEN_VERTICAL",
        "START_CARD", "START", "CONFIRM_NAME", "CANCEL",
        "BACK_SIZE", "BACK_DESIGN", "CONFIRM_GEN",
    }:
        return raw

    if raw.startswith("DESIGN_"):
        return raw

    basic = normalize_cmd(raw)
    if basic:
        return basic

    start_card_phrases = [
        "اصدار بطاقه", "اصدار بطاقة",
        "انشاء بطاقه", "انشاء بطاقة",
        "بطاقه تهنئه", "بطاقة تهنئة",
        "generate card", "create card",
        "generate", "create",
    ]

    confirm_phrases = [
        "تأكيد", "تاكيد", "تأكيد الاصدار", "تاكيد الاصدار",
        "تأكيد البطاقه", "تأكيد البطاقة",
        "confirm", "confirm generate", "confirm card",
        "generate", "create", "yes", "ok", "okay", "done",
        "ابدأ الاصدار", "ابدا الاصدار", "ابدأ", "ابدا",
    ]

    confirm_name_phrases = [
        "تأكيد الاسم", "تاكيد الاسم", "تأكيد", "تاكيد",
        "confirm name", "confirm", "yes", "ok", "okay",
    ]

    cancel_phrases = [
        "الغاء", "إلغاء", "الغاء العمليه", "إلغاء العملية",
        "cancel", "stop", "abort",
    ]

    edit_ar_phrases = [
        "تعديل العربي", "تعديل الاسم العربي", "تعديل الاسم",
        "عدل العربي", "عدل الاسم", "غير الاسم", "تغيير الاسم",
        "edit arabic", "edit ar", "edit name", "change name",
    ]

    edit_en_phrases = [
        "تعديل الانجليزي", "تعديل الإنجليزي", "تعديل الاسم الانجليزي", "تعديل الاسم الإنجليزي",
        "عدل الانجليزي", "عدل الإنجليزي",
        "edit english", "edit en",
    ]

    square_phrases = [
        "مربع", "مربعه", "مربعة", "square", "squar",
    ]

    vertical_phrases = [
        "طولي", "طوليه", "طولية", "عمودي", "vertical", "portrait", "vertcal",
    ]

    back_size_phrases = [
        "تغيير المقاس", "غير المقاس", "تعديل المقاس",
        "change size", "edit size", "back size", "size",
    ]

    back_design_phrases = [
        "تغيير التصميم", "غير التصميم", "تعديل التصميم",
        "change design", "edit design", "back design", "design",
    ]

    if state == STATE_MENU:
        if contains_any_phrase(raw, start_card_phrases):
            return "START_CARD"

    if state == STATE_WAIT_EN and contains_any_phrase(raw, edit_ar_phrases):
        return "EDIT_AR"

    if state == STATE_REVIEW_NAME and is_ar_only:
        if contains_any_phrase(raw, cancel_phrases):
            return "CANCEL"
        if contains_any_phrase(raw, edit_ar_phrases):
            return "EDIT_AR"
        if contains_any_phrase(raw, confirm_name_phrases):
            return "CONFIRM_NAME"

    if state == STATE_CONFIRM and (not is_ar_only):
        if contains_any_phrase(raw, cancel_phrases):
            return "CANCEL"
        if contains_any_phrase(raw, edit_ar_phrases):
            return "EDIT_AR"
        if contains_any_phrase(raw, edit_en_phrases):
            return "EDIT_EN"
        if contains_any_phrase(raw, confirm_phrases):
            return "GEN"

    if state == STATE_CHOOSE_SIZE and is_ar_only:
        if contains_any_phrase(raw, cancel_phrases):
            return "CANCEL"
        if contains_any_phrase(raw, square_phrases):
            return "GEN_SQUARE"
        if supports_vertical and contains_any_phrase(raw, vertical_phrases):
            return "GEN_VERTICAL"

    if state == STATE_CHOOSE_DESIGN and is_ar_only:
        if contains_any_phrase(raw, cancel_phrases):
            return "CANCEL"

        idx = extract_design_number(raw, design_count)
        if idx is not None:
            s_prefix = "S" if (chosen_size or "SQUARE") == "SQUARE" else "V"
            return f"DESIGN_{s_prefix}_{idx}"

    if state == STATE_PREVIEW_AR and is_ar_only:
        if contains_any_phrase(raw, cancel_phrases):
            return "CANCEL"
        if contains_any_phrase(raw, edit_ar_phrases):
            return "EDIT_AR"
        if supports_vertical and contains_any_phrase(raw, back_size_phrases):
            return "BACK_SIZE"
        if design_count > 1 and contains_any_phrase(raw, back_design_phrases):
            return "BACK_DESIGN"
        if contains_any_phrase(raw, confirm_phrases):
            return "CONFIRM_GEN"

    return ""


# ---------------------------
# Template picking
# ---------------------------
def pick_template_id(bot: Dict[str, Any], size_key: str, design_idx_1based: int) -> str:
    design_count = int(bot.get("design_count") or 1)
    supports_vertical = bool(bot.get("supports_vertical"))

    if size_key == "VERTICAL" and not supports_vertical:
        size_key = "SQUARE"

    field = bot["template_square"] if size_key == "SQUARE" else bot["template_vertical"]

    if design_count <= 1:
        tid = (str(field) or "").strip()
        if not tid:
            raise RuntimeError("Template id is empty")
        return tid

    if not isinstance(field, list):
        raise RuntimeError("Template list is not configured correctly")
    if design_idx_1based < 1 or design_idx_1based > len(field):
        raise RuntimeError("Design index out of range")
    tid = (field[design_idx_1based - 1] or "").strip()
    if not tid:
        raise RuntimeError("Template id is empty")
    return tid


# ---------------------------
# Core handler
# ---------------------------
GEN_COMMANDS = {"GEN", "CONFIRM_GEN"}


async def handle_webhook(req: Request, bot_key: str):
    try:
        if bot_key not in BOTS:
            return {"ok": True, "message": f"bot '{bot_key}' is not active on this server"}

        bot = BOTS[bot_key]
        bot_token = bot["token"]
        lang_mode = bot.get("lang_mode")
        is_ar_only = (lang_mode == "AR_ONLY")
        supports_vertical = bool(bot.get("supports_vertical"))
        design_count = int(bot.get("design_count") or 1)
        queue_name = get_queue_name_for_bot(bot_key)
        job_queue = get_queue_for_bot(bot_key)

        data = await req.json()
        update_id, chat_id, text_raw, msg_id, cq_id, user_id, username = extract_update(data)

        if not chat_id:
            return {"ok": True}

        s = get_session(bot_key, chat_id)

        async with s.lock:
            if user_id:
                s.user_id = user_id
            if username:
                s.username = username

        text = clean_text(text_raw)

        async with s.lock:
            current_state = s.state
            current_chosen_size = s.chosen_size

        cmd = infer_command(
            text=text,
            state=current_state,
            is_ar_only=is_ar_only,
            supports_vertical=supports_vertical,
            design_count=design_count,
            chosen_size=current_chosen_size,
        )

        fp = f"{update_id}|{msg_id}|{cq_id or ''}|{cmd}|{text}"
        now = time.time()

        async with s.lock:
            if update_id and s.last_update_id >= update_id:
                return {"ok": True}

            if s.recent_fps:
                stale = [k for k, ts in s.recent_fps.items() if (now - ts) > FP_DEDUP_SECONDS]
                for k in stale:
                    s.recent_fps.pop(k, None)

            if fp in s.recent_fps:
                return {"ok": True}

            s.recent_fps[fp] = now

            if update_id:
                s.last_update_id = update_id

        if cq_id:
            try:
                await atg_answer_callback(bot_token, cq_id)
            except Exception as e:
                log.warning("answerCallbackQuery failed: %s", repr(e))

        async with s.lock:
            state_now = s.state
            last_gen_ts = s.last_gen_ts

        if state_now == STATE_CREATING and cmd in GEN_COMMANDS:
            if cq_id:
                try:
                    await atg_toast(bot_token, cq_id, "⏳ جاري توليد البطاقة... الرجاء الانتظار", False)
                except Exception as e:
                    log.warning("tg_toast failed: %s", repr(e))
            return {"ok": True}

        if cmd in GEN_COMMANDS:
            if (now - last_gen_ts) < RATE_LIMIT_SECONDS:
                remaining = RATE_LIMIT_SECONDS - (now - last_gen_ts)
                if cq_id:
                    try:
                        await atg_toast(bot_token, cq_id, f"⏳ انتظر {int(remaining)} ثانية ثم حاول", False)
                    except Exception as e:
                        log.warning("tg_toast rate-limit failed: %s", repr(e))
                else:
                    await atg_send_message(bot_token, s.chat_id, msg_rate_limited(is_ar_only, remaining))
                return {"ok": True}

            async with s.lock:
                s.last_gen_ts = now

        if cmd == "CANCEL":
            async with s.lock:
                bump_seq(s)
                reset_session(s, keep_last_name=True)
                s.state = STATE_MENU

            if not is_ar_only:
                await atg_send_message(bot_token, s.chat_id, ar_msg_welcome(bot_key), ar_kb_start_card())
            else:
                await atg_send_message(bot_token, s.chat_id, hz_msg_welcome(bot_key), hz_kb_start_card())
            return {"ok": True}

        if cmd == "START":
            async with s.lock:
                bump_seq(s)
                reset_session(s, keep_last_name=True)
                s.state = STATE_MENU

            if not is_ar_only:
                await atg_send_message(bot_token, s.chat_id, ar_msg_welcome(bot_key), ar_kb_start_card())
            else:
                await atg_send_message(bot_token, s.chat_id, hz_msg_welcome(bot_key), hz_kb_start_card())
            return {"ok": True}

        if cmd == "START_CARD":
            async with s.lock:
                bump_seq(s)
                s.state = STATE_WAIT_AR
                s.name_ar = ""
                s.name_en = ""
                s.chosen_size = ""
                s.chosen_design = 1

            if not is_ar_only:
                await atg_send_message(bot_token, s.chat_id, ar_msg_ask_ar())
            else:
                await atg_send_message(bot_token, s.chat_id, hz_msg_ask_name())
            return {"ok": True}

        async with s.lock:
            state_now = s.state

        if state_now == STATE_WAIT_AR:
            ok, val = validate_ar(text)
            if not ok:
                if not is_ar_only:
                    await atg_send_message(bot_token, s.chat_id, ar_msg_invalid_ar(val))
                else:
                    await atg_send_message(bot_token, s.chat_id, hz_msg_invalid_ar(val), hz_kb_start_again())
                return {"ok": True}

            async with s.lock:
                s.name_ar = val
                if not is_ar_only:
                    s.state = STATE_WAIT_EN
                else:
                    s.state = STATE_REVIEW_NAME

            if not is_ar_only:
                await atg_send_message(bot_token, s.chat_id, ar_msg_ask_en(), ar_kb_wait_en())
            else:
                await atg_send_message(bot_token, s.chat_id, hz_msg_review_name(val), hz_kb_review_name())
            return {"ok": True}

        async with s.lock:
            state_now = s.state
            name_ar_now = s.name_ar

        if state_now == STATE_WAIT_EN and (not is_ar_only):
            if cmd == "EDIT_AR":
                async with s.lock:
                    s.state = STATE_WAIT_AR
                await atg_send_message(bot_token, s.chat_id, ar_msg_ask_ar())
                return {"ok": True}

            ok, val = validate_en(text)
            if not ok:
                await atg_send_message(bot_token, s.chat_id, ar_msg_invalid_en(val), ar_kb_wait_en())
                return {"ok": True}

            async with s.lock:
                s.name_en = val
                s.state = STATE_CONFIRM

            await atg_send_message(
                bot_token,
                s.chat_id,
                ar_msg_confirm(name_ar_now, val),
                ar_kb_confirm(),
            )
            return {"ok": True}

        async with s.lock:
            state_now = s.state
            name_ar_now = s.name_ar

        if state_now == STATE_REVIEW_NAME and is_ar_only:
            if cmd == "EDIT_AR":
                async with s.lock:
                    s.state = STATE_WAIT_AR
                await atg_send_message(bot_token, s.chat_id, hz_msg_ask_name())
                return {"ok": True}

            if cmd == "CONFIRM_NAME":
                async with s.lock:
                    s.state = STATE_CHOOSE_SIZE
                await atg_send_message(
                    bot_token,
                    s.chat_id,
                    hz_msg_choose_size(supports_vertical),
                    hz_kb_choose_size(supports_vertical),
                )
                return {"ok": True}

            await atg_send_message(bot_token, s.chat_id, hz_msg_review_name(name_ar_now), hz_kb_review_name())
            return {"ok": True}

        async with s.lock:
            state_now = s.state
            name_ar_now = s.name_ar
            name_en_now = s.name_en
            seq_now = s.seq

        if state_now == STATE_CONFIRM and (not is_ar_only):
            if cmd == "EDIT_AR":
                async with s.lock:
                    s.state = STATE_WAIT_AR
                await atg_send_message(bot_token, s.chat_id, ar_msg_ask_ar())
                return {"ok": True}

            if cmd == "EDIT_EN":
                async with s.lock:
                    s.state = STATE_WAIT_EN
                await atg_send_message(bot_token, s.chat_id, ar_msg_ask_en(), ar_kb_wait_en())
                return {"ok": True}

            if cmd == "GEN":
                if job_queue.qsize() >= MAX_QUEUE_SIZE:
                    async with s.lock:
                        reset_session(s, keep_last_name=True)
                    await atg_send_message(
                        bot_token,
                        s.chat_id,
                        msg_high_load(ar_only=False),
                        ar_kb_start_again(),
                    )
                    return {"ok": True}

                async with s.lock:
                    s.state = STATE_CREATING
                    seq_now = s.seq

                await atg_send_message(bot_token, s.chat_id, ar_msg_creating())

                async with _inflight_lock:
                    key = (bot_key, s.chat_id, seq_now)
                    if key in _inflight:
                        return {"ok": True}
                    _inflight.add(key)

                asyncio.create_task(_progress_ping(bot_token, bot_key, s.chat_id, seq_now))

                try:
                    job_queue.put_nowait(
                        Job(
                            bot_key=bot_key,
                            chat_id=s.chat_id,
                            user_id=s.user_id,
                            username=s.username,
                            name_ar=name_ar_now,
                            name_en=name_en_now,
                            size_key="SQUARE",
                            design_number=1,
                            template_id=pick_template_id(bot, "SQUARE", 1),
                            requested_at=time.time(),
                            seq=seq_now,
                            queue_name=queue_name,
                        )
                    )
                except asyncio.QueueFull:
                    async with s.lock:
                        reset_session(s, keep_last_name=True)
                    await atg_send_message(
                        bot_token,
                        s.chat_id,
                        msg_high_load(ar_only=False),
                        ar_kb_start_again(),
                    )
                    return {"ok": True}

                return {"ok": True}

            await atg_send_message(
                bot_token,
                s.chat_id,
                ar_msg_confirm(name_ar_now, name_en_now),
                ar_kb_confirm(),
            )
            return {"ok": True}

        async with s.lock:
            state_now = s.state
            name_ar_now = s.name_ar

        if state_now == STATE_CHOOSE_SIZE and is_ar_only:
            if cmd == "GEN_SQUARE":
                async with s.lock:
                    s.chosen_size = "SQUARE"
                    chosen_size_now = s.chosen_size

                    if design_count > 1:
                        s.state = STATE_CHOOSE_DESIGN
                    else:
                        s.chosen_design = 1
                        s.state = STATE_PREVIEW_AR

                if design_count > 1:
                    if bot_key == "amro" and AMRO_PREVIEW_SQUARE:
                        await atg_send_photo_by_file_id(
                            bot_token,
                            s.chat_id,
                            AMRO_PREVIEW_SQUARE,
                            "نماذج التصاميم للمقاس المربع"
                        )

                    await atg_send_message(
                        bot_token,
                        s.chat_id,
                        hz_msg_choose_design(design_count),
                        kb_choose_design("SQUARE", design_count),
                    )
                else:
                    await atg_send_message(
                        bot_token,
                        s.chat_id,
                        hz_msg_preview(bot_key, name_ar_now, size_label_ar(chosen_size_now), 1),
                        kb_preview_ar(supports_vertical, design_count),
                    )
                return {"ok": True}

            if cmd == "GEN_VERTICAL" and supports_vertical:
                async with s.lock:
                    s.chosen_size = "VERTICAL"
                    chosen_size_now = s.chosen_size

                    if design_count > 1:
                        s.state = STATE_CHOOSE_DESIGN
                    else:
                        s.chosen_design = 1
                        s.state = STATE_PREVIEW_AR

                if design_count > 1:
                    if bot_key == "amro" and AMRO_PREVIEW_VERTICAL:
                        await atg_send_photo_by_file_id(
                            bot_token,
                            s.chat_id,
                            AMRO_PREVIEW_VERTICAL,
                            "نماذج التصاميم للمقاس الطولي"
                        )

                    await atg_send_message(
                        bot_token,
                        s.chat_id,
                        hz_msg_choose_design(design_count),
                        kb_choose_design("VERTICAL", design_count),
                    )
                else:
                    await atg_send_message(
                        bot_token,
                        s.chat_id,
                        hz_msg_preview(bot_key, name_ar_now, size_label_ar(chosen_size_now), 1),
                        kb_preview_ar(supports_vertical, design_count),
                    )
                return {"ok": True}

            await atg_send_message(
                bot_token,
                s.chat_id,
                hz_msg_choose_size(supports_vertical),
                hz_kb_choose_size(supports_vertical),
            )
            return {"ok": True}

        async with s.lock:
            state_now = s.state
            name_ar_now = s.name_ar
            chosen_size_now = s.chosen_size

        if state_now == STATE_CHOOSE_DESIGN and is_ar_only:
            if cmd.startswith("DESIGN_"):
                parts = cmd.split("_")
                if len(parts) == 3:
                    sv = parts[1]
                    try:
                        idx = int(parts[2])
                    except Exception:
                        idx = 1

                    size_key = "SQUARE" if sv == "S" else "VERTICAL"
                    idx = max(1, min(design_count, idx))

                    async with s.lock:
                        s.chosen_size = size_key
                        s.chosen_design = idx
                        s.state = STATE_PREVIEW_AR

                    await atg_send_message(
                        bot_token,
                        s.chat_id,
                        hz_msg_preview(bot_key, name_ar_now, size_label_ar(size_key), idx),
                        kb_preview_ar(supports_vertical, design_count),
                    )
                    return {"ok": True}

            if not chosen_size_now:
                chosen_size_now = "SQUARE"

            await atg_send_message(
                bot_token,
                s.chat_id,
                hz_msg_choose_design(design_count),
                kb_choose_design(chosen_size_now, design_count),
            )
            return {"ok": True}

        async with s.lock:
            state_now = s.state
            name_ar_now = s.name_ar
            chosen_size_now = s.chosen_size or "SQUARE"
            chosen_design_now = s.chosen_design or 1
            seq_now = s.seq

        if state_now == STATE_PREVIEW_AR and is_ar_only:
            if cmd == "BACK_SIZE" and supports_vertical:
                async with s.lock:
                    s.state = STATE_CHOOSE_SIZE
                await atg_send_message(
                    bot_token,
                    s.chat_id,
                    hz_msg_choose_size(supports_vertical),
                    hz_kb_choose_size(supports_vertical),
                )
                return {"ok": True}

            if cmd == "BACK_DESIGN" and design_count > 1:
                async with s.lock:
                    s.state = STATE_CHOOSE_DESIGN
                await atg_send_message(
                    bot_token,
                    s.chat_id,
                    hz_msg_choose_design(design_count),
                    kb_choose_design(chosen_size_now, design_count),
                )
                return {"ok": True}

            if cmd == "EDIT_AR":
                async with s.lock:
                    s.state = STATE_WAIT_AR
                await atg_send_message(bot_token, s.chat_id, hz_msg_ask_name())
                return {"ok": True}

            if cmd == "CONFIRM_GEN":
                if job_queue.qsize() >= MAX_QUEUE_SIZE:
                    async with s.lock:
                        reset_session(s, keep_last_name=True)
                    await atg_send_message(
                        bot_token,
                        s.chat_id,
                        msg_high_load(ar_only=True),
                        hz_kb_start_again(),
                    )
                    return {"ok": True}

                async with s.lock:
                    s.state = STATE_CREATING
                    seq_now = s.seq

                await atg_send_message(bot_token, s.chat_id, hz_msg_creating())

                async with _inflight_lock:
                    key = (bot_key, s.chat_id, seq_now)
                    if key in _inflight:
                        return {"ok": True}
                    _inflight.add(key)

                asyncio.create_task(_progress_ping(bot_token, bot_key, s.chat_id, seq_now))

                template_id = pick_template_id(bot, chosen_size_now, chosen_design_now)

                try:
                    job_queue.put_nowait(
                        Job(
                            bot_key=bot_key,
                            chat_id=s.chat_id,
                            user_id=s.user_id,
                            username=s.username,
                            name_ar=name_ar_now,
                            name_en="",
                            size_key=chosen_size_now,
                            design_number=int(chosen_design_now),
                            template_id=template_id,
                            requested_at=time.time(),
                            seq=seq_now,
                            queue_name=queue_name,
                        )
                    )
                except asyncio.QueueFull:
                    async with s.lock:
                        reset_session(s, keep_last_name=True)
                    await atg_send_message(
                        bot_token,
                        s.chat_id,
                        msg_high_load(ar_only=True),
                        hz_kb_start_again(),
                    )
                    return {"ok": True}

                return {"ok": True}

            await atg_send_message(
                bot_token,
                s.chat_id,
                hz_msg_preview(bot_key, name_ar_now, size_label_ar(chosen_size_now), chosen_design_now),
                kb_preview_ar(supports_vertical, design_count),
            )
            return {"ok": True}

        if not is_ar_only:
            await atg_send_message(bot_token, s.chat_id, ar_msg_need_start(), ar_kb_start_again())
        else:
            await atg_send_message(bot_token, s.chat_id, hz_msg_need_start(), hz_kb_start_again())
        return {"ok": True}

    except Exception as e:
        log.exception("Webhook error for bot=%s: %s", bot_key, repr(e))
        return {"ok": True}


# ---------------------------
# Generic share routes
# ---------------------------
@app.get("/share-file/{token}.png")
async def share_file(token: str):
    item = get_share_item(token)
    if not item:
        raise HTTPException(status_code=404, detail="Card not found or expired")

    return Response(
        content=item["png_bytes"],
        media_type="image/png",
        headers={
            "Cache-Control": "private, max-age=300",
            "Content-Disposition": f'attachment; filename="card-{token}.png"',
        },
    )


@app.post("/share-prepared/{token}")
async def share_prepared(token: str):
    item = get_share_item(token)
    if not item:
        raise HTTPException(status_code=404, detail="Card not found or expired")

    bot_key = str(item.get("bot_key") or "").strip()
    if not bot_key or bot_key not in BOTS:
        raise HTTPException(status_code=400, detail="Invalid bot")

    user_id = str(item.get("user_id") or "").strip()
    if not user_id.isdigit():
        raise HTTPException(status_code=400, detail="Missing or invalid user_id")

    bot_token = BOTS[bot_key]["token"]
    image_url = make_public_url(f"/share-file/{token}.png")

    is_ar_only = BOTS[bot_key].get("lang_mode") == "AR_ONLY"
    if is_ar_only:
        title = "بطاقة تهنئة"
        caption = ""
    else:
        title = "Greeting Card"
        caption = ""

    try:
        prepared = await asyncio.to_thread(
            tg_save_prepared_inline_photo,
            bot_token,
            user_id=user_id,
            image_url=image_url,
            title=title,
            caption=caption,
        )
    except Exception as e:
        log.exception("share_prepared failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to prepare share message: {e}")

    return {
        "ok": True,
        "id": prepared["id"],
        "expiration_date": prepared.get("expiration_date"),
    }


@app.get("/share-mini/{token}", response_class=HTMLResponse)
async def share_mini(token: str):
    item = get_share_item(token)
    if not item:
        raise HTTPException(status_code=404, detail="Card not found or expired")

    image_url = make_public_url(f"/share-file/{token}.png")
    prepared_url = make_public_url(f"/share-prepared/{token}")

    html = f"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="UTF-8" />
  <meta
    name="viewport"
    content="width=device-width, initial-scale=1.0, viewport-fit=cover, user-scalable=no"
  />
  <title>Share Card</title>
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <style>
    * {{
      box-sizing: border-box;
      -webkit-tap-highlight-color: transparent;
    }}
    html, body {{
      margin: 0;
      padding: 0;
      background: #f8fafc;
      color: #0f172a;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Tahoma, Arial, sans-serif;
    }}
    body {{
      min-height: 100vh;
    }}
    .wrap {{
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 18px;
    }}
    .card {{
      width: 100%;
      max-width: 540px;
      background: #ffffff;
      border-radius: 24px;
      box-shadow: 0 10px 35px rgba(15, 23, 42, 0.12);
      padding: 18px;
    }}
    .title {{
      text-align: center;
      font-size: 26px;
      font-weight: 800;
      margin-bottom: 10px;
    }}
    .sub {{
      text-align: center;
      font-size: 16px;
      line-height: 1.9;
      color: #475569;
      margin-bottom: 14px;
    }}
    .preview {{
      background: #eef2f7;
      border-radius: 18px;
      padding: 10px;
      margin-bottom: 14px;
    }}
    .preview img {{
      width: 100%;
      height: auto;
      display: block;
      border-radius: 14px;
      background: #fff;
    }}
    .btn {{
      width: 100%;
      border: 0;
      border-radius: 18px;
      padding: 18px 14px;
      font-size: 24px;
      font-weight: 800;
      cursor: pointer;
      background: #16a34a;
      color: #fff;
      margin-top: 8px;
    }}
    .btn:disabled {{
      opacity: 0.65;
      cursor: default;
    }}
    .btn-secondary {{
      width: 100%;
      border: 0;
      border-radius: 18px;
      padding: 16px 14px;
      font-size: 20px;
      font-weight: 700;
      cursor: pointer;
      background: #e2e8f0;
      color: #0f172a;
      margin-top: 10px;
    }}
    .btn-secondary:disabled {{
      opacity: 0.65;
      cursor: default;
    }}
    .note {{
      text-align: center;
      margin-top: 12px;
      font-size: 15px;
      line-height: 1.8;
      color: #64748b;
      min-height: 30px;
      white-space: pre-line;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div id="title" class="title"></div>
      <div id="subtitle" class="sub"></div>

      <div class="preview">
        <img id="cardImage" src="{image_url}" alt="" />
      </div>

      <button id="shareBtn" class="btn"></button>
      <button id="openBtn" class="btn-secondary"></button>
      <div id="note" class="note"></div>
    </div>
  </div>

  <script>
    const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
    const absoluteImageUrl = {json.dumps(image_url)};
    const preparedApiUrl = {json.dumps(prepared_url)};

    function getLang() {{
      const tgLang = tg?.initDataUnsafe?.user?.language_code || "";
      const navLang = navigator.language || navigator.userLanguage || "";
      const lang = (tgLang || navLang || "").toLowerCase();
      return lang.startsWith("ar") ? "ar" : "en";
    }}

    const LANG = getLang();

    const I18N = {{
      ar: {{
        pageTitleShare: "مشاركة البطاقة",
        pageTitleSave: "حفظ البطاقة",
        titleShare: "مشاركة البطاقة",
        titleSave: "حفظ البطاقة",
        subtitle: "",
        shareBtn: "📤 مشاركة البطاقة",
        saveBtn: "💾 حفظ البطاقة",
        openBtn: "🖼️ فتح الصورة",
        opening: "جاري فتح المشاركة...",
        saving: "جاري حفظ البطاقة...",
        preparingTelegram: "جاري تجهيز المشاركة داخل تيليجرام...",
        shared: "تمت المشاركة بنجاح.",
        saved: "تم بدء تنزيل البطاقة.",
        shareCancelled: "",
        failed: "تعذر تنفيذ العملية.",
        openOnly: "تم فتح الصورة.",
        telegramFallback: "تعذر فتح مشاركة الملف المباشرة. تم التحويل إلى مشاركة تيليجرام.",
        unsupported: "تعذر تنفيذ العملية مباشرة. يمكنك فتح الصورة والتعامل معها يدويًا.",
        imageAlt: "بطاقة التهنئة"
      }},
      en: {{
        pageTitleShare: "Share Card",
        pageTitleSave: "Save Card",
        titleShare: "Share Card",
        titleSave: "Save Card",
        subtitle: "",
        shareBtn: "📤 Share Card",
        saveBtn: "💾 Save Card",
        openBtn: "🖼️ Open Image",
        opening: "Opening share sheet...",
        saving: "Saving card...",
        preparingTelegram: "Preparing Telegram share...",
        shared: "Shared successfully.",
        saved: "Download started.",
        shareCancelled: "",
        failed: "Could not complete the action.",
        openOnly: "Image opened.",
        telegramFallback: "Direct file sharing is unavailable. Switched to Telegram sharing.",
        unsupported: "This action is unavailable directly. You can open the image manually.",
        imageAlt: "Greeting Card"
      }}
    }};

    const T = I18N[LANG];

    document.documentElement.lang = LANG;
    document.documentElement.dir = LANG === "ar" ? "rtl" : "ltr";

    const titleEl = document.getElementById("title");
    const subtitleEl = document.getElementById("subtitle");
    const cardImage = document.getElementById("cardImage");
    const shareBtn = document.getElementById("shareBtn");
    const openBtn = document.getElementById("openBtn");
    const note = document.getElementById("note");

    openBtn.textContent = T.openBtn;
    cardImage.alt = T.imageAlt;

    if (tg) {{
      try {{
        tg.ready();
        tg.expand();
      }} catch (e) {{
        console.log("tg init error", e);
      }}
    }}

    function isTelegramMiniApp() {{
      return !!(tg && tg.initDataUnsafe);
    }}

    function isIOS() {{
      const platform = (tg?.platform || "").toLowerCase();
      const ua = (navigator.userAgent || "").toLowerCase();
      return platform === "ios" || /iphone|ipad|ipod/.test(ua);
    }}

    function isAndroid() {{
      const platform = (tg?.platform || "").toLowerCase();
      const ua = (navigator.userAgent || "").toLowerCase();
      return platform === "android" || ua.includes("android");
    }}

    const ANDROID_SAVE_MODE = isAndroid() && !isIOS();

    document.title = ANDROID_SAVE_MODE ? T.pageTitleSave : T.pageTitleShare;
    titleEl.textContent = ANDROID_SAVE_MODE ? T.titleSave : T.titleShare;
    subtitleEl.textContent = T.subtitle || "";

    if (!subtitleEl.textContent.trim()) {{
      subtitleEl.style.display = "none";
    }}

    shareBtn.textContent = ANDROID_SAVE_MODE ? T.saveBtn : T.shareBtn;

    let isBusy = false;

    function setBusy(flag) {{
      isBusy = !!flag;
      shareBtn.disabled = isBusy;
      openBtn.disabled = isBusy;
    }}

    function setNote(text) {{
      note.textContent = text || "";
    }}

    function getDownloadFileName() {{
      return LANG === "ar" ? "card.png" : "greeting-card.png";
    }}

    async function buildShareFile() {{
      const res = await fetch(absoluteImageUrl, {{
        cache: "no-store",
        credentials: "omit"
      }});
      if (!res.ok) throw new Error("failed_to_fetch_image");
      const blob = await res.blob();
      return new File([blob], getDownloadFileName(), {{ type: blob.type || "image/png" }});
    }}

    function openImageOnly() {{
      window.open(absoluteImageUrl, "_blank");
      setNote(T.openOnly);
    }}

    function fallbackBrowserDownload() {{
      const a = document.createElement("a");
      a.href = absoluteImageUrl;
      a.download = getDownloadFileName();
      a.rel = "noopener";
      document.body.appendChild(a);
      a.click();
      a.remove();
    }}

    async function requestPreparedMessageId() {{
      const res = await fetch(preparedApiUrl, {{
        method: "POST",
        credentials: "omit",
        headers: {{
          "Content-Type": "application/json"
        }}
      }});

      if (!res.ok) {{
        throw new Error("prepared_message_request_failed");
      }}

      const data = await res.json();
      if (!data || !data.ok || !data.id) {{
        throw new Error("prepared_message_invalid_response");
      }}

      return data.id;
    }}

    function sendPreparedMessage(id) {{
      return new Promise((resolve, reject) => {{
        if (!tg) {{
          reject(new Error("telegram_not_available"));
          return;
        }}

        let done = false;

        function cleanup() {{
          try {{
            tg.offEvent("shareMessageSent", onSent);
            tg.offEvent("shareMessageFailed", onFailed);
            tg.offEvent("prepared_message_sent", onPreparedSent);
            tg.offEvent("prepared_message_failed", onPreparedFailed);
          }} catch (e) {{
            console.log("cleanup event error", e);
          }}
        }}

        function finishOk() {{
          if (done) return;
          done = true;
          cleanup();
          resolve();
        }}

        function finishErr(error) {{
          if (done) return;
          done = true;
          cleanup();
          reject(error instanceof Error ? error : new Error(String(error || "telegram_share_failed")));
        }}

        function onSent() {{
          finishOk();
        }}

        function onFailed(payload) {{
          const msg = payload && payload.error ? payload.error : "shareMessageFailed";
          finishErr(new Error(msg));
        }}

        function onPreparedSent() {{
          finishOk();
        }}

        function onPreparedFailed(payload) {{
          const msg = payload && payload.error ? payload.error : "prepared_message_failed";
          finishErr(new Error(msg));
        }}

        try {{
          tg.onEvent("shareMessageSent", onSent);
          tg.onEvent("shareMessageFailed", onFailed);
          tg.onEvent("prepared_message_sent", onPreparedSent);
          tg.onEvent("prepared_message_failed", onPreparedFailed);
        }} catch (e) {{
          console.log("event bind error", e);
        }}

        try {{
          if (typeof tg.shareMessage === "function") {{
            tg.shareMessage(id);
          }} else if (window.TelegramWebviewProxy && typeof window.TelegramWebviewProxy.postEvent === "function") {{
            window.TelegramWebviewProxy.postEvent(
              "web_app_send_prepared_message",
              JSON.stringify({{ id }})
            );
          }} else if (window.external && typeof window.external.notify === "function") {{
            window.external.notify(
              JSON.stringify({{
                eventType: "web_app_send_prepared_message",
                eventData: {{ id }}
              }})
            );
          }} else {{
            finishErr(new Error("telegram_share_api_not_available"));
            return;
          }}
        }} catch (e) {{
          finishErr(e);
          return;
        }}

        setTimeout(() => {{
          if (!done) {{
            finishErr(new Error("telegram_share_timeout"));
          }}
        }}, 15000);
      }});
    }}

    async function tryNativeShareFirst() {{
      const file = await buildShareFile();
      if (navigator.canShare && navigator.canShare({{ files: [file] }})) {{
        await navigator.share({{ files: [file] }});
        return true;
      }}
      return false;
    }}

    async function tryTelegramPreparedShare() {{
      if (!isTelegramMiniApp()) {{
        return false;
      }}

      setNote(T.preparingTelegram);
      const preparedId = await requestPreparedMessageId();
      await sendPreparedMessage(preparedId);
      setNote(T.shared);
      return true;
    }}

    async function saveOnAndroid() {{
      setNote(T.saving);

      if (tg && typeof tg.downloadFile === "function") {{
        try {{
          tg.downloadFile({{
            url: absoluteImageUrl,
            file_name: getDownloadFileName()
          }});
          setNote(T.saved);
          return true;
        }} catch (e) {{
          console.log("downloadFile failed", e);
        }}
      }}

      try {{
        fallbackBrowserDownload();
        setNote(T.saved);
        return true;
      }} catch (e) {{
        console.log("fallback download failed", e);
      }}

      openImageOnly();
      return false;
    }}

    async function doShare() {{
      if (isBusy) return;

      setBusy(true);

      try {{
        // Android: حفظ فقط
        if (ANDROID_SAVE_MODE) {{
          await saveOnAndroid();
          return;
        }}

        // iOS / غيره: مشاركة كما هي
        setNote(T.opening);

        try {{
          const nativeShared = await tryNativeShareFirst();
          if (nativeShared) {{
            setNote("");
            return;
          }}
        }} catch (err) {{
          const errName = (err && err.name) ? err.name : "";
          if (errName === "AbortError") {{
            setNote(T.shareCancelled);
            return;
          }}
          console.log("native share failed", err);
        }}

        if (isTelegramMiniApp()) {{
          try {{
            const tgShared = await tryTelegramPreparedShare();
            if (tgShared) {{
              return;
            }}
          }} catch (e) {{
            console.log("telegram prepared share failed", e);
          }}
        }}

        openImageOnly();
      }} catch (err) {{
        console.log("share/save failed", err);
        setNote(T.unsupported);
        openImageOnly();
      }} finally {{
        setBusy(false);
      }}
    }}

    shareBtn.addEventListener("click", doShare);
    openBtn.addEventListener("click", openImageOnly);
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html)


# ---------------------------
# Startup + Routes
# ---------------------------
@app.on_event("startup")
async def startup():
    require_env()

    for queue_name in job_queues.keys():
        for i in range(max(1, WORKER_COUNT)):
            asyncio.create_task(worker_loop(queue_name, i + 1))

    log.info(
        "App started (workers_per_queue=%s, max_queue=%s, gen_concurrency_per_queue=%s, gen_rate_limit=%ss, fp_dedup=%ss, sheet=%s/%s, instance=%s, output_folder=%s, public_base_url=%s, share_ttl=%s)",
        WORKER_COUNT,
        MAX_QUEUE_SIZE,
        GEN_CONCURRENCY,
        RATE_LIMIT_SECONDS,
        FP_DEDUP_SECONDS,
        "on" if SHEET_ID else "off",
        SHEET_TAB,
        INSTANCE_NAME,
        OUTPUT_FOLDER_ID or "not-set",
        guess_base_url() or "not-set",
        SHARE_TTL_SECONDS,
    )

    log.info("Active bots on this instance: %s", ", ".join(BOTS.keys()))
    log.info(
        "Queue mapping: %s",
        {
            QUEUE_ARABIA_WARD: ["alarabia", "kounuz_alward"],
            QUEUE_HAFEZ_FALAH: ["alhafez", "alfalah"],
            QUEUE_AMRO: ["amro"],
        },
    )


@app.get("/")
def home():
    return {
        "status": "ok",
        "instance": INSTANCE_NAME,
        "active_bots": list(BOTS.keys()),
        "output_folder_set": bool(OUTPUT_FOLDER_ID),
        "public_base_url": guess_base_url(),
        "share_items": len(SHARE_STORE),
        "queues": {
            QUEUE_ARABIA_WARD: {
                "bots": ["alarabia", "kounuz_alward"],
                "size": job_queues[QUEUE_ARABIA_WARD].qsize(),
            },
            QUEUE_HAFEZ_FALAH: {
                "bots": ["alhafez", "alfalah"],
                "size": job_queues[QUEUE_HAFEZ_FALAH].qsize(),
            },
            QUEUE_AMRO: {
                "bots": ["amro"],
                "size": job_queues[QUEUE_AMRO].qsize(),
            },
        },
    }


@app.post("/webhook/alarabia")
async def webhook_alarabia(req: Request):
    return await handle_webhook(req, "alarabia")


@app.post("/webhook/alhafez")
async def webhook_alhafez(req: Request):
    return await handle_webhook(req, "alhafez")


@app.post("/webhook/alfalah")
async def webhook_alfalah(req: Request):
    return await handle_webhook(req, "alfalah")


@app.post("/webhook/kounuz_alward")
async def webhook_kounuz_alward(req: Request):
    return await handle_webhook(req, "kounuz_alward")


@app.post("/webhook/amro")
async def webhook_amro(req: Request):
    return await handle_webhook(req, "amro")
