import os
import json
import time
import re
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Tuple

import requests
from fastapi import FastAPI, Request

from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.transport.requests import Request as GARequest
from googleapiclient.discovery import build

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("telegram-multi-card-bot")

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

HTTP_TIMEOUT = 20
TG_API = "https://api.telegram.org/bot{}/{}"

# ---------------------------
# Env (Bots)
# ---------------------------
BOT_TOKEN_ALARABIA = os.getenv("BOT_TOKEN_ALARABIA", "").strip()
TEMPLATE_SLIDES_ID_ALARABIA_SQUARE = os.getenv("TEMPLATE_SLIDES_ID_ALARABIA_SQUARE", "").strip()

BOT_TOKEN_ALHAFEZ = os.getenv("BOT_TOKEN_ALHAFEZ", "").strip()
TEMPLATE_SLIDES_ID_ALHAFEZ_SQUARE = os.getenv("TEMPLATE_SLIDES_ID_ALHAFEZ_SQUARE", "").strip()
TEMPLATE_SLIDES_ID_ALHAFEZ_VERTICAL = os.getenv("TEMPLATE_SLIDES_ID_ALHAFEZ_VERTICAL", "").strip()

BOTS = {
    "alarabia": {
        "token": BOT_TOKEN_ALARABIA,
        "template_square": TEMPLATE_SLIDES_ID_ALARABIA_SQUARE,
        "template_vertical": "",  # not used
        "lang_mode": "AR_EN",
    },
    "alhafez": {
        "token": BOT_TOKEN_ALHAFEZ,
        "template_square": TEMPLATE_SLIDES_ID_ALHAFEZ_SQUARE,
        "template_vertical": TEMPLATE_SLIDES_ID_ALHAFEZ_VERTICAL,
        "lang_mode": "AR_ONLY",
    },
}

# ---------------------------
# App
# ---------------------------
app = FastAPI()

# ---------------------------
# Telegram helpers (per-bot token)
# ---------------------------
def tg(bot_token: str, method: str, data: Optional[dict] = None, files: Optional[dict] = None) -> requests.Response:
    url = TG_API.format(bot_token, method)
    r = requests.post(url, data=data, files=files, timeout=HTTP_TIMEOUT)
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

def tg_answer_callback(bot_token: str, callback_query_id: str) -> None:
    if callback_query_id:
        tg(bot_token, "answerCallbackQuery", {"callback_query_id": callback_query_id})

def tg_send_photo(bot_token: str, chat_id: str, png_bytes: bytes, caption: str, reply_markup: Optional[dict] = None) -> None:
    files = {"photo": ("card.png", png_bytes)}
    data = {"chat_id": chat_id, "caption": caption}
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
    # arabia
    if not BOT_TOKEN_ALARABIA:
        raise RuntimeError("BOT_TOKEN_ALARABIA is missing")
    if not TEMPLATE_SLIDES_ID_ALARABIA_SQUARE:
        raise RuntimeError("TEMPLATE_SLIDES_ID_ALARABIA_SQUARE is missing")

    # alhafez
    if not BOT_TOKEN_ALHAFEZ:
        raise RuntimeError("BOT_TOKEN_ALHAFEZ is missing")
    if not TEMPLATE_SLIDES_ID_ALHAFEZ_SQUARE:
        raise RuntimeError("TEMPLATE_SLIDES_ID_ALHAFEZ_SQUARE is missing")
    if not TEMPLATE_SLIDES_ID_ALHAFEZ_VERTICAL:
        raise RuntimeError("TEMPLATE_SLIDES_ID_ALHAFEZ_VERTICAL is missing")

    # google auth
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
        return False, "الاسم العربي فارغ."
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
DIV = "\n--------------------\n"

# --- AlArabia (AR+EN كما هو)
def ar_msg_welcome():
    ar = (
        "مرحباً بكم في بوت توليد بطاقات التهنئة الرقمية بشركة العربية\n\n"
        "يمكن لكل موظف إصدار البطاقة بسرعة وبشكل مستقل – مبادرة شخصية وحل رقمي – تطوير: عمرو إسماعيل"
    )
    en = (
        "Welcome to the Digital Greeting Card Bot at AlArabia Company\n\n"
        "Every employee can issue cards quickly and on their own – "
        "A personal initiative and digital solution – developed by Amro Ismail"
    )
    return ar + DIV + en

def ar_msg_need_start():
    return "الرجاء إرسال /start للبدء من جديد." + DIV + "Please send /start to start again."

def ar_msg_ask_ar():
    return "اكتب اسمك بالعربية:" + DIV + "Enter your name in Arabic:"

def ar_msg_ask_en():
    return "اكتب اسمك بالإنجليزية:" + DIV + "Enter your name in English:"

def ar_msg_invalid_ar(reason_ar: str):
    return f"غير صحيح: {reason_ar}\n\nاكتب الاسم بالعربية فقط." + DIV + "Invalid Arabic name.\n\nPlease type Arabic letters only."

def ar_msg_invalid_en(reason_ar: str):
    return f"غير صحيح: {reason_ar}\n\nاكتب الاسم بالإنجليزية فقط." + DIV + "Invalid English name.\n\nPlease type English letters only."

def ar_msg_confirm(name_ar: str, name_en: str):
    ar = f"تأكيد البيانات:\n\nالاسم بالعربية: {name_ar}\nالاسم بالإنجليزية: {name_en}\n\n"
    en = f"Confirm details:\n\nArabic: {name_ar}\nEnglish: {name_en}\n\n"
    return ar + DIV + en

def ar_msg_creating():
    return "جاري توليد البطاقة..." + DIV + "Generating your card..."

def ar_msg_ready():
    return "تم إنشاء البطاقة." + DIV + "Your card is ready."

def ar_msg_error(err: str):
    return "خطأ أثناء إنشاء البطاقة:\n" + err + DIV + "Error while creating the card:\n" + err

def ar_kb_start_card():
    return {"inline_keyboard": [[{"text": "🎉 إصدار بطاقة تهنئة / Generate Card", "callback_data": "START_CARD"}]]}

def ar_kb_start_again():
    return {"inline_keyboard": [[{"text": "▶️ Start / ابدأ", "callback_data": "START"}]]}

def ar_kb_wait_en():
    return {"inline_keyboard": [[{"text": "إعادة كتابة الاسم العربي / Edit Arabic", "callback_data": "EDIT_AR"}]]}

def ar_kb_confirm():
    return {
        "inline_keyboard": [
            [{"text": "توليد البطاقة / Generate", "callback_data": "GEN"}],
            [
                {"text": "تعديل العربي / Edit Arabic", "callback_data": "EDIT_AR"},
                {"text": "تعديل الإنجليزي / Edit English", "callback_data": "EDIT_EN"},
            ],
        ]
    }

def ar_kb_after_ready():
    return {
        "inline_keyboard": [
            [{"text": "🔁 إصدار بطاقة أخرى / Generate Another Card", "callback_data": "START_CARD"}],
            [{"text": "🏠 البداية / Start", "callback_data": "START"}],
        ]
    }

# --- AlHafez (Arabic only)
def hz_msg_welcome():
    return (
        "يسر جمعية الحافظ لتأهيل حفاظ القرآن الكريم أن تعلن عن إطلاق بوت إصدار بطاقات التهنئة الرقمية،\n"
        "والذي يهدف إلى تمكين منسوبي الجمعية من إصدار بطاقاتهم إلكترونيًا بسهولة واستقلالية، دعمًا لمسيرة التحول الرقمي بالجمعية.\n\n"
        "تطوير: عمرو بن عبدالعزيز العديني."
    )

def hz_msg_need_start():
    return "الرجاء إرسال /start للبدء من جديد."

def hz_msg_ask_ar():
    return "اكتب اسمك بالعربية:"

def hz_msg_invalid_ar(reason_ar: str):
    return f"غير صحيح: {reason_ar}\n\nاكتب الاسم بالعربية فقط."

def hz_msg_confirm(name_ar: str):
    return f"تأكيد البيانات:\n\nالاسم: {name_ar}\n\nاختر نوع البطاقة:"

def hz_msg_creating():
    return "جاري توليد البطاقة..."

def hz_msg_ready():
    return "تم إنشاء البطاقة."

def hz_msg_error(err: str):
    return "خطأ أثناء إنشاء البطاقة:\n" + err

def hz_kb_start_card():
    return {"inline_keyboard": [[{"text": "🎉 إصدار بطاقة تهنئة", "callback_data": "START_CARD"}]]}

def hz_kb_start_again():
    return {"inline_keyboard": [[{"text": "▶️ ابدأ", "callback_data": "START"}]]}

def hz_kb_choose_type():
    # أزرار تحت بعض
    return {
        "inline_keyboard": [
            [{"text": "✅ إصدار بطاقة (مربع)", "callback_data": "GEN_SQUARE"}],
            [{"text": "📐 إصدار بطاقة (طولي)", "callback_data": "GEN_VERTICAL"}],
            [{"text": "✏️ تعديل الاسم", "callback_data": "EDIT_AR"}],
        ]
    }

def hz_kb_after_ready():
    # الأزرار المطلوبة تحت بعض
    return {
        "inline_keyboard": [
            [{"text": "🔁 إعادة الإصدار", "callback_data": "START_CARD"}],
            [{"text": "📐 إصدار بطاقة بمقاس طولي", "callback_data": "START_CARD_VERTICAL"}],
            [{"text": "🏠 البداية", "callback_data": "START"}],
        ]
    }

# ---------------------------
# Session
# ---------------------------
STATE_MENU = "MENU"
STATE_WAIT_AR = "WAIT_AR"
STATE_WAIT_EN = "WAIT_EN"
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
    last_fingerprint: str = ""
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    seq: int = 0
    # only for alhafez quick-start vertical button
    preferred_kind: str = "SQUARE"  # "SQUARE" or "VERTICAL"

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

def reset_session(s: Session):
    s.state = STATE_MENU
    s.name_ar = ""
    s.name_en = ""
    s.preferred_kind = "SQUARE"

def bump_seq(s: Session):
    s.seq += 1

# ---------------------------
# Queue worker
# ---------------------------
job_queue: asyncio.Queue = asyncio.Queue()

@dataclass
class Job:
    bot_key: str
    chat_id: str
    name_ar: str
    name_en: str
    template_id: str
    requested_at: float
    seq: int

async def worker_loop():
    require_env()
    log.info("Worker started")
    while True:
        job: Job = await job_queue.get()
        try:
            await process_job(job)
        except Exception as e:
            log.exception("Job failed: %s", e)
        finally:
            job_queue.task_done()

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

        if job.bot_key == "alarabia":
            tg_send_photo(bot_token, job.chat_id, png_bytes, ar_msg_ready(), ar_kb_after_ready())
        else:
            tg_send_photo(bot_token, job.chat_id, png_bytes, hz_msg_ready(), hz_kb_after_ready())

        async with s.lock:
            reset_session(s)

    except Exception as e:
        if job.bot_key == "alarabia":
            tg_send_message(bot_token, job.chat_id, ar_msg_error(str(e)))
        else:
            tg_send_message(bot_token, job.chat_id, hz_msg_error(str(e)))
        async with s.lock:
            reset_session(s)

# ---------------------------
# Google: generate PNG (no Drive upload)
# ---------------------------
def export_png(pres_id: str, slide_object_id: str, creds) -> bytes:
    if not creds.valid or creds.expired:
        creds.refresh(GARequest())
    url = f"https://docs.google.com/presentation/d/{pres_id}/export/png?pageid={slide_object_id}"
    headers = {"Authorization": f"Bearer {creds.token}"}
    r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Export PNG failed: HTTP {r.status_code} - {r.text[:300]}")
    return r.content

def generate_card_png(template_id: str, name_ar: str, name_en: str, lang_mode: str) -> bytes:
    drive, slides, creds = build_clients()
    pres_id = None
    try:
        copied = drive.files().copy(
            fileId=template_id,
            body={"name": f"tg_card_{int(time.time())}"},
            supportsAllDrives=True,
        ).execute()
        pres_id = copied["id"]

        reqs = [
            {"replaceAllText": {"containsText": {"text": PLACEHOLDER_AR}, "replaceText": name_ar}},
        ]
        if lang_mode == "AR_EN":
            reqs.append({"replaceAllText": {"containsText": {"text": PLACEHOLDER_EN}, "replaceText": name_en}})

        slides.presentations().batchUpdate(
            presentationId=pres_id,
            body={"requests": reqs},
        ).execute()

        pres = slides.presentations().get(presentationId=pres_id).execute()
        slide_id = pres["slides"][0]["objectId"]

        return export_png(pres_id, slide_id, creds)

    finally:
        if pres_id:
            try:
                drive.files().delete(fileId=pres_id, supportsAllDrives=True).execute()
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
    if t in ("/start", "start", "ابدأ", "ابدا"):
        return "START"
    return ""

# ---------------------------
# Core handler (per bot)
# ---------------------------
async def handle_webhook(req: Request, bot_key: str):
    bot = BOTS[bot_key]
    bot_token = bot["token"]

    data = await req.json()
    update_id, chat_id, text_raw, msg_id, cq_id = extract_update(data)

    if not chat_id:
        return {"ok": True}

    s = get_session(bot_key, chat_id)

    # Dedup
    fp = f"{update_id}|{msg_id}|{cq_id or ''}|{text_raw}"
    async with s.lock:
        if update_id and s.last_update_id >= update_id:
            return {"ok": True}
        if s.last_fingerprint == fp:
            return {"ok": True}
        if update_id:
            s.last_update_id = update_id
        s.last_fingerprint = fp

    # Answer callback only once
    if cq_id:
        tg_answer_callback(bot_token, cq_id)

    text = clean_text(text_raw)
    cmd = normalize_cmd(text)

    # callbacks
    if text in (
        "EDIT_AR",
        "EDIT_EN",
        "GEN",
        "GEN_SQUARE",
        "GEN_VERTICAL",
        "START_CARD",
        "START_CARD_VERTICAL",
        "START",
    ):
        cmd = text

    async with s.lock:
        # START
        if cmd == "START":
            bump_seq(s)
            reset_session(s)
            if bot_key == "alarabia":
                tg_send_message(bot_token, s.chat_id, ar_msg_welcome(), ar_kb_start_card())
            else:
                tg_send_message(bot_token, s.chat_id, hz_msg_welcome(), hz_kb_start_card())
            s.state = STATE_MENU
            return {"ok": True}

        # START_CARD / START_CARD_VERTICAL
        if cmd in ("START_CARD", "START_CARD_VERTICAL"):
            bump_seq(s)
            reset_session(s)
            s.state = STATE_WAIT_AR
            if bot_key == "alarabia":
                tg_send_message(bot_token, s.chat_id, ar_msg_ask_ar())
            else:
                s.preferred_kind = "VERTICAL" if cmd == "START_CARD_VERTICAL" else "SQUARE"
                tg_send_message(bot_token, s.chat_id, hz_msg_ask_ar())
            return {"ok": True}

        # WAIT_AR
        if s.state == STATE_WAIT_AR:
            ok, val = validate_ar(text)
            if not ok:
                if bot_key == "alarabia":
                    tg_send_message(bot_token, s.chat_id, ar_msg_invalid_ar(val))
                else:
                    tg_send_message(bot_token, s.chat_id, hz_msg_invalid_ar(val))
                return {"ok": True}

            s.name_ar = val

            if bot_key == "alarabia":
                s.state = STATE_WAIT_EN
                tg_send_message(bot_token, s.chat_id, ar_msg_ask_en(), ar_kb_wait_en())
                return {"ok": True}
            else:
                s.state = STATE_CONFIRM
                # لو جاية من زر "طولي" نخليه يختار النوع هنا برضه (لكن default = preferred_kind)
                tg_send_message(bot_token, s.chat_id, hz_msg_confirm(s.name_ar), hz_kb_choose_type())
                return {"ok": True}

        # WAIT_EN (alarabia only)
        if s.state == STATE_WAIT_EN:
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

        # CONFIRM
        if s.state == STATE_CONFIRM:
            if bot_key == "alarabia":
                if cmd == "EDIT_AR":
                    s.state = STATE_WAIT_AR
                    tg_send_message(bot_token, s.chat_id, ar_msg_ask_ar())
                    return {"ok": True}
                if cmd == "EDIT_EN":
                    s.state = STATE_WAIT_EN
                    tg_send_message(bot_token, s.chat_id, ar_msg_ask_en(), ar_kb_wait_en())
                    return {"ok": True}

                # GEN (keep compatibility)
                if cmd == "GEN":
                    s.state = STATE_CREATING
                    tg_send_message(bot_token, s.chat_id, ar_msg_creating())

                    await job_queue.put(
                        Job(
                            bot_key=bot_key,
                            chat_id=s.chat_id,
                            name_ar=s.name_ar,
                            name_en=s.name_en,
                            template_id=bot["template_square"],
                            requested_at=time.time(),
                            seq=s.seq,
                        )
                    )
                    return {"ok": True}

                tg_send_message(bot_token, s.chat_id, ar_msg_confirm(s.name_ar, s.name_en), ar_kb_confirm())
                return {"ok": True}

            else:
                # AlHafez confirm
                if cmd == "EDIT_AR":
                    s.state = STATE_WAIT_AR
                    tg_send_message(bot_token, s.chat_id, hz_msg_ask_ar())
                    return {"ok": True}

                # If user presses generate square/vertical
                if cmd in ("GEN_SQUARE", "GEN_VERTICAL"):
                    s.state = STATE_CREATING
                    tg_send_message(bot_token, s.chat_id, hz_msg_creating())

                    template_id = bot["template_square"] if cmd == "GEN_SQUARE" else bot["template_vertical"]

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

                # fallback: show chooser
                tg_send_message(bot_token, s.chat_id, hz_msg_confirm(s.name_ar), hz_kb_choose_type())
                return {"ok": True}

        # CREATING
        if s.state == STATE_CREATING:
            return {"ok": True}

        # Default
        if bot_key == "alarabia":
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
    asyncio.create_task(worker_loop())
    log.info("App started")

@app.get("/")
def home():
    return {"status": "ok"}

@app.post("/webhook/alarabia")
async def webhook_alarabia(req: Request):
    return await handle_webhook(req, "alarabia")

@app.post("/webhook/alhafez")
async def webhook_alhafez(req: Request):
    return await handle_webhook(req, "alhafez")
