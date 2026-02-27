import os
import json
import time
import re
import asyncio
import logging
import random
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Tuple, List, Union

import requests
from fastapi import FastAPI, Request

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
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))  # general requests
EXPORT_TIMEOUT = int(os.getenv("EXPORT_TIMEOUT", "45"))  # export/png can be slower
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "200"))
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "3"))  # 2-4 recommended

# IMPORTANT: rate limit ONLY on generation
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "10"))  # 1 gen request / X sec per chat
PROGRESS_PING_SECONDS = float(os.getenv("PROGRESS_PING_SECONDS", "8"))  # "still working..." after X sec

RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "5"))
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "0.7"))
RETRY_MAX_DELAY = float(os.getenv("RETRY_MAX_DELAY", "8.0"))

# Fingerprint dedupe window (seconds)
FP_DEDUP_SECONDS = int(os.getenv("FP_DEDUP_SECONDS", "60"))

TG_API = "https://api.telegram.org/bot{}/{}"

# Only these are rate-limited + blocked during creating
GEN_COMMANDS = {"GEN", "CONFIRM_GEN"}

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
]

PLACEHOLDER_AR = os.getenv("PLACEHOLDER_AR", "<<Name in Arabic>>")
PLACEHOLDER_EN = os.getenv("PLACEHOLDER_EN", "<<Name in English>>")

# ---------------------------
# Env (Bots) - legacy vars kept
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

# For templates
TemplateField = Union[str, List[str]]

# ---------------------------
# Optional: External config (JSON) to ease adding bots
# ---------------------------
BOTS_CONFIG_JSON = os.getenv("BOTS_CONFIG_JSON", "").strip()


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


BOTS: Dict[str, Dict[str, Any]] = load_bots_config()

# ---------------------------
# App
# ---------------------------
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
                    r.status_code,
                    url,
                    attempt,
                    RETRY_MAX_ATTEMPTS,
                    r.text[:300],
                )
                if attempt < RETRY_MAX_ATTEMPTS:
                    _sleep_backoff(attempt)
                    continue
            return r
        except (requests.Timeout, requests.ConnectionError, requests.RequestException) as e:
            last_exc = e
            log.warning(
                "Network error on %s %s (attempt %s/%s): %s",
                method,
                url,
                attempt,
                RETRY_MAX_ATTEMPTS,
                repr(e),
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
                log.warning("Retryable Google HttpError %s (%s) attempt %s/%s", status, label, attempt, RETRY_MAX_ATTEMPTS)
                _sleep_backoff(attempt)
                continue
            raise
        except Exception as e:
            last_exc = e
            if attempt < RETRY_MAX_ATTEMPTS and isinstance(e, (TimeoutError, ConnectionError)):
                log.warning("Retryable Google error (%s) attempt %s/%s: %s", label, attempt, RETRY_MAX_ATTEMPTS, repr(e))
                _sleep_backoff(attempt)
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("google_execute_with_retry failed unexpectedly")


# ---------------------------
# Telegram helpers (per-bot token) + retry
# ---------------------------
def tg(bot_token: str, method: str, data: Optional[dict] = None, files: Optional[dict] = None) -> requests.Response:
    url = TG_API.format(bot_token, method)
    r = request_with_retry("POST", url, timeout=HTTP_TIMEOUT, data=data, files=files)
    if r.status_code != 200:
        log.warning("TG error %s %s: %s", method, r.status_code, r.text[:800])
    return r


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


def tg_edit_message(bot_token: str, chat_id: str, message_id: int, text: str, reply_markup: Optional[dict] = None) -> None:
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup)
    tg(bot_token, "editMessageText", payload)


def tg_answer_callback(bot_token: str, callback_query_id: str) -> None:
    if callback_query_id:
        tg(bot_token, "answerCallbackQuery", {"callback_query_id": callback_query_id})


def tg_toast(bot_token: str, callback_query_id: str, text: str, show_alert: bool = False) -> None:
    # Toast if show_alert=False
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


def tg_send_photo(bot_token: str, chat_id: str, png_bytes: bytes, caption: str = "", reply_markup: Optional[dict] = None) -> None:
    files = {"photo": ("card.png", png_bytes)}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    tg(bot_token, "sendPhoto", data=data, files=files)


# ---------------------------
# Google clients (cached)
# ---------------------------
_drive = None
_slides = None
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
            if not (isinstance(tsq, list) and len(tsq) >= design_count and all(str(x).strip() for x in tsq[:design_count])):
                raise RuntimeError(f"{bot_key}: template_square list is missing/invalid for design_count={design_count}")
            if supports_vertical:
                if not (isinstance(tv, list) and len(tv) >= design_count and all(str(x).strip() for x in tv[:design_count])):
                    raise RuntimeError(f"{bot_key}: template_vertical list is missing/invalid for design_count={design_count}")

    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN) and not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Provide OAuth vars (GOOGLE_CLIENT_ID/SECRET/REFRESH_TOKEN) or SERVICE_ACCOUNT_JSON")


def build_clients():
    global _drive, _slides, _creds

    if _drive and _slides and _creds:
        try:
            if not _creds.valid or _creds.expired:
                _creds.refresh(GARequest())
        except Exception:
            pass
        return _drive, _slides, _creds

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
        _drive, _slides, _creds = drive, slides, creds
        log.info("Using OAuth user credentials")
        return drive, slides, creds

    info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    slides = build("slides", "v1", credentials=creds, cache_discovery=False)
    _drive, _slides, _creds = drive, slides, creds
    log.info("Using Service Account credentials")
    return drive, slides, creds


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
# Messages / Keyboards (unified)
# ---------------------------
DIV = "\n--------------------\n"


def msg_high_load(ar_only: bool) -> str:
    if ar_only:
        return "الضغط عالي الآن. حاول مرة أخرى بعد قليل."
    return "High load right now. Please try again in a moment." + DIV + "الضغط عالي الآن. حاول بعد قليل."


def msg_rate_limited(ar_only: bool, seconds: float) -> str:
    if ar_only:
        return f"تم استقبال طلب توليد قبل قليل. الرجاء الانتظار {int(seconds)} ثانية ثم حاول مرة أخرى."
    return f"Please wait {int(seconds)} seconds then try again." + DIV + "تم استقبال طلب توليد قبل قليل. حاول بعد قليل."


# --- Branding messages
BRANDING: Dict[str, Dict[str, str]] = {
    "alarabia": {
        "welcome_ar": (
            "مرحباً بكم في بوت توليد بطاقات التهنئة الرقمية بشركة العربية\n\n"
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
            "تطوير: عمرو بن عبدالعزيز العديني"
        )
    },
    "alfalah": {
        "welcome_ar": (
            "مرحبا بكم في بوت إصدار بطاقات التهنئة لمنسوبي مدارس الفلاح\n\n"
            "تطوير: عمرو بن عبدالعزيز اسماعيل"
        )
    },
    "kounuz_alward": {
        "welcome_ar": (
            "مرحبا بكم في بوت إصدار بطاقات التهنئة لمنسوبي كنوز الورد\n\n"
            "تطوير: عمرو العديني"
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


# --- AR/EN bot messages
def ar_msg_welcome(bot_key: str) -> str:
    br = get_branding(bot_key)
    ar = br.get("welcome_ar", "مرحباً بك")
    en = br.get("welcome_en", "Welcome")
    return ar + DIV + en


def ar_msg_need_start() -> str:
    # CHANGED: add guidance to press the button below
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
    ar = f"تأكيد البيانات:\n\nالاسم بالعربية: {name_ar}\nالاسم بالإنجليزية: {name_en}\n\n"
    en = f"Confirm details:\n\nArabic: {name_ar}\nEnglish: {name_en}\n\n"
    return ar + DIV + en


def ar_msg_creating() -> str:
    return "جاري إصدار البطاقة..." + DIV + "Issuing your card..."


def ar_msg_still_working() -> str:
    return "لا يزال جاري إصدار البطاقة..." + DIV + "Still issuing your card..."


def ar_msg_ready() -> str:
    # CHANGED: use إصدار بدل إنشاء
    return "تم إصدار البطاقة بنجاح." + DIV + "Your card has been issued successfully."


def ar_msg_error(err: str) -> str:
    return "خطأ أثناء إصدار البطاقة:\n" + err + DIV + "Error while issuing the card:\n" + err


def ar_kb_start_card() -> dict:
    return {"inline_keyboard": [[{"text": "إصدار بطاقة تهنئة / Issue Card", "callback_data": "START_CARD"}]]}


def ar_kb_start_again() -> dict:
    return {"inline_keyboard": [[{"text": "↩️ Start / ابدأ", "callback_data": "START"}]]}


def ar_kb_wait_en() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "تعديل الاسم العربي / Edit Arabic", "callback_data": "EDIT_AR"}],
        ]
    }


def ar_kb_confirm() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "إصدار البطاقة / Issue", "callback_data": "GEN"}],
            [
                {"text": "تعديل العربي / Edit Arabic", "callback_data": "EDIT_AR"},
                {"text": "تعديل الإنجليزي / Edit English", "callback_data": "EDIT_EN"},
            ],
            [{"text": "❌ إلغاء العملية / Cancel", "callback_data": "CANCEL"}],
        ]
    }


def ar_kb_after_ready() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "إصدار بطاقة أخرى / Issue Another Card", "callback_data": "START_CARD"}],
            [{"text": "↩️ Start / ابدأ", "callback_data": "START"}],
        ]
    }


# --- Arabic-only messages
def hz_msg_welcome(bot_key: str) -> str:
    br = get_branding(bot_key)
    return br.get("welcome_ar", "مرحباً بك")


def hz_msg_need_start() -> str:
    # CHANGED: add guidance to press the button below
    return "للعودة للبداية، الرجاء إرسال /start أو اضغط زر (البداية) أدناه."


def hz_msg_ask_name() -> str:
    return "اكتب اسمك:"


def hz_msg_invalid_ar(reason_ar: str) -> str:
    return f"غير صحيح: {reason_ar}\n\nاكتب الاسم بالعربية فقط."


def hz_msg_review_name(name_ar: str) -> str:
    return f"مراجعة الاسم:\n\nالاسم: {name_ar}"


def hz_msg_choose_size(supports_vertical: bool) -> str:
    if supports_vertical:
        return "اختر مقاس البطاقة"
    return "المقاس المتاح: مربع"


def hz_msg_choose_design(design_count: int) -> str:
    if design_count <= 1:
        return "التصميم الافتراضي"
    return "اختر رقم التصميم"


def hz_msg_preview(name_ar: str, size_label: str, design_number: int) -> str:
    return (
        "ملخص البطاقة قبل الإصدار:\n\n"
        f"الاسم: {name_ar}\n"
        f"المقاس: {size_label}\n"
        f"رقم التصميم: {design_number}\n\n"
        "هل تريد التأكيد؟"
    )


def hz_msg_creating() -> str:
    return "جاري إصدار البطاقة..."


def hz_msg_still_working() -> str:
    return "لا يزال جاري إصدار البطاقة..."


def hz_msg_ready() -> str:
    # CHANGED: use إصدار
    return "تم إصدار البطاقة بنجاح."


def hz_msg_error(err: str) -> str:
    return "خطأ أثناء إصدار البطاقة:\n" + err


# --- Arabic-only keyboards
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
        rows.append([{"text": "تغيير رقم التصميم", "callback_data": "BACK_DESIGN"}])
    rows.append([{"text": "❌ إلغاء العملية", "callback_data": "CANCEL"}])
    return {"inline_keyboard": rows}


def hz_kb_after_ready() -> dict:
    return {"inline_keyboard": [[{"text": "↩️ البداية", "callback_data": "START"}]]}


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
    chosen_size: str = ""      # "SQUARE" or "VERTICAL"
    chosen_design: int = 1     # 1..design_count
    last_name_ar: str = ""     # last used name
    last_gen_ts: float = 0     # rate limit only for generation
    creating_msg_id: int = 0
    recent_fps: Dict[str, float] = field(default_factory=dict)


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
    s.creating_msg_id = 0
    if not keep_last_name:
        s.last_name_ar = ""


def bump_seq(s: Session):
    s.seq += 1


# ---------------------------
# Queue worker + inflight dedupe
# ---------------------------
job_queue: asyncio.Queue = asyncio.Queue()
_inflight_lock = asyncio.Lock()
_inflight: set = set()  # (bot_key, chat_id, seq)


@dataclass
class Job:
    bot_key: str
    chat_id: str
    name_ar: str
    name_en: str
    template_id: str
    requested_at: float
    seq: int


async def worker_loop(worker_id: int):
    require_env()
    log.info("Worker %s started", worker_id)
    while True:
        job: Job = await job_queue.get()
        try:
            await process_job(job)
        except Exception as e:
            log.exception("Job failed: %s", e)
        finally:
            async with _inflight_lock:
                _inflight.discard((job.bot_key, job.chat_id, job.seq))
            job_queue.task_done()


async def _progress_ping(bot_token: str, bot_key: str, chat_id: str, seq: int):
    await asyncio.sleep(PROGRESS_PING_SECONDS)
    s = get_session(bot_key, chat_id)
    async with s.lock:
        if s.seq != seq or s.state != STATE_CREATING:
            return
    bot = BOTS[bot_key]
    if bot.get("lang_mode") == "AR_EN":
        tg_send_message(bot_token, chat_id, ar_msg_still_working())
    else:
        tg_send_message(bot_token, chat_id, hz_msg_still_working())


async def process_job(job: Job):
    bot = BOTS[job.bot_key]
    bot_token = bot["token"]
    s = get_session(job.bot_key, job.chat_id)

    async with s.lock:
        if job.seq != s.seq:
            log.info("Skip stale job for %s", job.chat_id)
            return

    try:
        png_bytes = generate_card_png(
            template_id=job.template_id,
            name_ar=job.name_ar,
            name_en=job.name_en,
            lang_mode=bot["lang_mode"],
        )

        async with s.lock:
            if job.seq != s.seq:
                log.info("Skip stale result for %s", job.chat_id)
                return

        # ✅ الصورة لحالها (بدون كابشن وبدون أزرار)
        tg_send_photo(bot_token, job.chat_id, png_bytes, caption="", reply_markup=None)

        # ✅ بعدها رسالة نجاح + الأزرار (حتى ما تكون الأزرار على نفس البطاقة)
        if bot["lang_mode"] == "AR_EN":
            tg_send_message(bot_token, job.chat_id, ar_msg_ready(), ar_kb_after_ready())
        else:
            tg_send_message(bot_token, job.chat_id, hz_msg_ready(), hz_kb_after_ready())

        async with s.lock:
            s.last_name_ar = job.name_ar or s.last_name_ar
            reset_session(s, keep_last_name=True)

    except Exception as e:
        if bot["lang_mode"] == "AR_EN":
            tg_send_message(bot_token, job.chat_id, ar_msg_error(str(e)), ar_kb_start_again())
        else:
            tg_send_message(bot_token, job.chat_id, hz_msg_error(str(e)), hz_kb_start_again())
        async with s.lock:
            reset_session(s, keep_last_name=True)


# ---------------------------
# Google: generate PNG (no Drive upload)
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
    drive, slides, creds = build_clients()
    pres_id = None
    try:
        copied = google_execute_with_retry(
            lambda: drive.files().copy(
                fileId=template_id,
                body={"name": f"tg_card_{int(time.time())}"},
                supportsAllDrives=True,
            ).execute(),
            label="drive.files.copy",
        )
        pres_id = copied["id"]

        reqs = [{"replaceAllText": {"containsText": {"text": PLACEHOLDER_AR}, "replaceText": name_ar}}]
        if lang_mode == "AR_EN":
            reqs.append({"replaceAllText": {"containsText": {"text": PLACEHOLDER_EN}, "replaceText": name_en}})

        google_execute_with_retry(
            lambda: slides.presentations().batchUpdate(
                presentationId=pres_id,
                body={"requests": reqs},
            ).execute(),
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
# Update parsing
# ---------------------------
def extract_update(data: Dict[str, Any]) -> Tuple[int, Optional[str], Optional[str], Optional[int], Optional[str]]:
    update_id = int(data.get("update_id") or 0)

    if "callback_query" in data:
        cq = data["callback_query"]
        cq_id = str(cq.get("id") or "")
        msg = cq.get("message") or {}
        chat_id = str((msg.get("chat") or {}).get("id") or "")
        message_id = int(msg.get("message_id") or 0)
        text = str(cq.get("data") or "")
        return update_id, chat_id, text, message_id, cq_id

    msg = data.get("message") or {}
    chat_id = str((msg.get("chat") or {}).get("id") or "")
    message_id = int(msg.get("message_id") or 0)
    text = str(msg.get("text") or "")
    return update_id, chat_id, text, message_id, None


def normalize_cmd(text: str) -> str:
    t = clean_text(text).lower()
    if t in ("/start", "start", "ابدأ", "ابدا", "البداية"):
        return "START"
    return ""


# ---------------------------
# Template picking (generalized)
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


def size_label_ar(size_key: str) -> str:
    return "مربع" if size_key == "SQUARE" else "طولي"


# ---------------------------
# Core handler (per bot)
# ---------------------------
async def handle_webhook(req: Request, bot_key: str):
    bot = BOTS[bot_key]
    bot_token = bot["token"]
    lang_mode = bot.get("lang_mode")
    is_ar_only = (lang_mode == "AR_ONLY")
    supports_vertical = bool(bot.get("supports_vertical"))
    design_count = int(bot.get("design_count") or 1)

    data = await req.json()
    update_id, chat_id, text_raw, msg_id, cq_id = extract_update(data)

    if not chat_id:
        return {"ok": True}

    s = get_session(bot_key, chat_id)

    text = clean_text(text_raw)
    cmd = normalize_cmd(text)

    # accept callbacks as commands
    if text in (
        "EDIT_AR",
        "EDIT_EN",
        "GEN",
        "GEN_SQUARE",
        "GEN_VERTICAL",
        "START_CARD",
        "START",
        "CONFIRM_NAME",
        "CANCEL",
        "BACK_SIZE",
        "BACK_DESIGN",
        "CONFIRM_GEN",
    ):
        cmd = text

    if text.startswith("DESIGN_"):
        cmd = text

    # ---- Fingerprint dedupe within 60 seconds
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
        tg_answer_callback(bot_token, cq_id)

    async with s.lock:
        if s.state == STATE_CREATING and cmd in GEN_COMMANDS:
            if cq_id:
                tg_toast(bot_token, cq_id, "⏳ جاري إصدار البطاقة... الرجاء الانتظار", show_alert=False)
            return {"ok": True}

        if cmd in GEN_COMMANDS:
            if (now - s.last_gen_ts) < RATE_LIMIT_SECONDS:
                remaining = RATE_LIMIT_SECONDS - (now - s.last_gen_ts)
                if cq_id:
                    tg_toast(bot_token, cq_id, f"⏳ انتظر {int(remaining)} ثانية ثم حاول", show_alert=False)
                else:
                    tg_send_message(bot_token, s.chat_id, msg_rate_limited(is_ar_only, remaining))
                return {"ok": True}
            s.last_gen_ts = now

        if cmd == "CANCEL":
            bump_seq(s)
            reset_session(s, keep_last_name=True)
            if not is_ar_only:
                tg_send_message(bot_token, s.chat_id, ar_msg_welcome(bot_key), ar_kb_start_card())
            else:
                tg_send_message(bot_token, s.chat_id, hz_msg_welcome(bot_key), hz_kb_start_card())
            s.state = STATE_MENU
            return {"ok": True}

        if cmd == "START":
            bump_seq(s)
            reset_session(s, keep_last_name=True)
            if not is_ar_only:
                tg_send_message(bot_token, s.chat_id, ar_msg_welcome(bot_key), ar_kb_start_card())
            else:
                tg_send_message(bot_token, s.chat_id, hz_msg_welcome(bot_key), hz_kb_start_card())
            s.state = STATE_MENU
            return {"ok": True}

        if cmd == "START_CARD":
            bump_seq(s)
            s.state = STATE_WAIT_AR
            s.name_ar = ""
            s.name_en = ""
            s.chosen_size = ""
            s.chosen_design = 1

            if not is_ar_only:
                tg_send_message(bot_token, s.chat_id, ar_msg_ask_ar())
            else:
                tg_send_message(bot_token, s.chat_id, hz_msg_ask_name())
            return {"ok": True}

        if s.state == STATE_WAIT_AR:
            ok, val = validate_ar(text)
            if not ok:
                if not is_ar_only:
                    tg_send_message(bot_token, s.chat_id, ar_msg_invalid_ar(val))
                else:
                    tg_send_message(bot_token, s.chat_id, hz_msg_invalid_ar(val), hz_kb_start_again())
                return {"ok": True}

            s.name_ar = val

            if not is_ar_only:
                s.state = STATE_WAIT_EN
                tg_send_message(bot_token, s.chat_id, ar_msg_ask_en(), ar_kb_wait_en())
                return {"ok": True}

            s.state = STATE_REVIEW_NAME
            tg_send_message(bot_token, s.chat_id, hz_msg_review_name(s.name_ar), hz_kb_review_name())
            return {"ok": True}

        if s.state == STATE_WAIT_EN and (not is_ar_only):
            if cmd == "EDIT_AR":
                s.state = STATE_WAIT_AR
                tg_send_message(bot_token, s.chat_id, ar_msg_ask_ar())
                return {"ok": True}

            ok, val = validate_en(text)
            if not ok:
                tg_send_message(bot_token, s.chat_id, ar_msg_invalid_en(val), ar_kb_wait_en())
                return {"ok": True}

            s.name_en = val
            s.state = STATE_CONFIRM
            tg_send_message(bot_token, s.chat_id, ar_msg_confirm(s.name_ar, s.name_en), ar_kb_confirm())
            return {"ok": True}

        if s.state == STATE_REVIEW_NAME and is_ar_only:
            if cmd == "EDIT_AR":
                s.state = STATE_WAIT_AR
                tg_send_message(bot_token, s.chat_id, hz_msg_ask_name())
                return {"ok": True}

            if cmd == "CONFIRM_NAME":
                s.state = STATE_CHOOSE_SIZE
                tg_send_message(bot_token, s.chat_id, hz_msg_choose_size(supports_vertical), hz_kb_choose_size(supports_vertical))
                return {"ok": True}

            tg_send_message(bot_token, s.chat_id, hz_msg_review_name(s.name_ar), hz_kb_review_name())
            return {"ok": True}

        if s.state == STATE_CONFIRM and (not is_ar_only):
            if cmd == "EDIT_AR":
                s.state = STATE_WAIT_AR
                tg_send_message(bot_token, s.chat_id, ar_msg_ask_ar())
                return {"ok": True}
            if cmd == "EDIT_EN":
                s.state = STATE_WAIT_EN
                tg_send_message(bot_token, s.chat_id, ar_msg_ask_en(), ar_kb_wait_en())
                return {"ok": True}

            if cmd == "GEN":
                if job_queue.qsize() >= MAX_QUEUE_SIZE:
                    tg_send_message(bot_token, s.chat_id, msg_high_load(ar_only=False), ar_kb_start_again())
                    reset_session(s, keep_last_name=True)
                    return {"ok": True}

                s.state = STATE_CREATING
                tg_send_message(bot_token, s.chat_id, ar_msg_creating())

                async with _inflight_lock:
                    key = (bot_key, s.chat_id, s.seq)
                    if key in _inflight:
                        return {"ok": True}
                    _inflight.add(key)

                asyncio.create_task(_progress_ping(bot_token, bot_key, s.chat_id, s.seq))
                await job_queue.put(
                    Job(
                        bot_key=bot_key,
                        chat_id=s.chat_id,
                        name_ar=s.name_ar,
                        name_en=s.name_en,
                        template_id=pick_template_id(bot, "SQUARE", 1),
                        requested_at=time.time(),
                        seq=s.seq,
                    )
                )
                return {"ok": True}

            tg_send_message(bot_token, s.chat_id, ar_msg_confirm(s.name_ar, s.name_en), ar_kb_confirm())
            return {"ok": True}

        if s.state == STATE_CHOOSE_SIZE and is_ar_only:
            if cmd == "GEN_SQUARE":
                s.chosen_size = "SQUARE"
                if design_count > 1:
                    s.state = STATE_CHOOSE_DESIGN
                    tg_send_message(bot_token, s.chat_id, hz_msg_choose_design(design_count), kb_choose_design("SQUARE", design_count))
                else:
                    s.chosen_design = 1
                    s.state = STATE_PREVIEW_AR
                    tg_send_message(
                        bot_token,
                        s.chat_id,
                        hz_msg_preview(s.name_ar, size_label_ar(s.chosen_size), s.chosen_design),
                        kb_preview_ar(supports_vertical, design_count),
                    )
                return {"ok": True}

            if cmd == "GEN_VERTICAL" and supports_vertical:
                s.chosen_size = "VERTICAL"
                if design_count > 1:
                    s.state = STATE_CHOOSE_DESIGN
                    tg_send_message(bot_token, s.chat_id, hz_msg_choose_design(design_count), kb_choose_design("VERTICAL", design_count))
                else:
                    s.chosen_design = 1
                    s.state = STATE_PREVIEW_AR
                    tg_send_message(
                        bot_token,
                        s.chat_id,
                        hz_msg_preview(s.name_ar, size_label_ar(s.chosen_size), s.chosen_design),
                        kb_preview_ar(supports_vertical, design_count),
                    )
                return {"ok": True}

            tg_send_message(bot_token, s.chat_id, hz_msg_choose_size(supports_vertical), hz_kb_choose_size(supports_vertical))
            return {"ok": True}

        if s.state == STATE_CHOOSE_DESIGN and is_ar_only:
            if cmd.startswith("DESIGN_"):
                parts = cmd.split("_")
                if len(parts) == 3:
                    sv = parts[1]
                    idx = int(parts[2])
                    size_key = "SQUARE" if sv == "S" else "VERTICAL"
                    s.chosen_size = size_key
                    s.chosen_design = max(1, min(design_count, idx))

                    s.state = STATE_PREVIEW_AR
                    tg_send_message(
                        bot_token,
                        s.chat_id,
                        hz_msg_preview(s.name_ar, size_label_ar(s.chosen_size), s.chosen_design),
                        kb_preview_ar(supports_vertical, design_count),
                    )
                    return {"ok": True}

            if not s.chosen_size:
                s.chosen_size = "SQUARE"
            tg_send_message(bot_token, s.chat_id, hz_msg_choose_design(design_count), kb_choose_design(s.chosen_size, design_count))
            return {"ok": True}

        if s.state == STATE_PREVIEW_AR and is_ar_only:
            if cmd == "BACK_SIZE" and supports_vertical:
                s.state = STATE_CHOOSE_SIZE
                tg_send_message(bot_token, s.chat_id, hz_msg_choose_size(supports_vertical), hz_kb_choose_size(supports_vertical))
                return {"ok": True}

            if cmd == "BACK_DESIGN" and design_count > 1:
                s.state = STATE_CHOOSE_DESIGN
                if not s.chosen_size:
                    s.chosen_size = "SQUARE"
                tg_send_message(bot_token, s.chat_id, hz_msg_choose_design(design_count), kb_choose_design(s.chosen_size, design_count))
                return {"ok": True}

            if cmd == "EDIT_AR":
                s.state = STATE_WAIT_AR
                tg_send_message(bot_token, s.chat_id, hz_msg_ask_name())
                return {"ok": True}

            if cmd == "CONFIRM_GEN":
                if job_queue.qsize() >= MAX_QUEUE_SIZE:
                    tg_send_message(bot_token, s.chat_id, msg_high_load(ar_only=True), hz_kb_start_again())
                    reset_session(s, keep_last_name=True)
                    return {"ok": True}

                s.state = STATE_CREATING
                tg_send_message(bot_token, s.chat_id, hz_msg_creating())

                async with _inflight_lock:
                    key = (bot_key, s.chat_id, s.seq)
                    if key in _inflight:
                        return {"ok": True}
                    _inflight.add(key)

                asyncio.create_task(_progress_ping(bot_token, bot_key, s.chat_id, s.seq))

                template_id = pick_template_id(bot, s.chosen_size or "SQUARE", s.chosen_design or 1)
                await job_queue.put(
                    Job(
                        bot_key=bot_key,
                        chat_id=s.chat_id,
                        name_ar=s.name_ar,
                        name_en="",
                        template_id=template_id,
                        requested_at=time.time(),
                        seq=s.seq,
                    )
                )
                return {"ok": True}

            if not s.chosen_size:
                s.chosen_size = "SQUARE"
            tg_send_message(
                bot_token,
                s.chat_id,
                hz_msg_preview(s.name_ar, size_label_ar(s.chosen_size), s.chosen_design),
                kb_preview_ar(supports_vertical, design_count),
            )
            return {"ok": True}

        if s.state == STATE_CREATING:
            return {"ok": True}

        if not is_ar_only:
            tg_send_message(bot_token, s.chat_id, ar_msg_need_start(), ar_kb_start_again())
        else:
            tg_send_message(bot_token, s.chat_id, hz_msg_need_start(), hz_kb_start_again())
        return {"ok": True}


# ---------------------------
# Routes
# ---------------------------
@app.on_event("startup")
async def startup():
    require_env()
    for i in range(max(1, WORKER_COUNT)):
        asyncio.create_task(worker_loop(i + 1))
    log.info(
        "App started (workers=%s, max_queue=%s, gen_rate_limit=%ss, fp_dedup=%ss)",
        WORKER_COUNT,
        MAX_QUEUE_SIZE,
        RATE_LIMIT_SECONDS,
        FP_DEDUP_SECONDS,
    )


@app.get("/")
def home():
    return {"status": "ok"}


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
