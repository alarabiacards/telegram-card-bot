
import os
import io
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
log = logging.getLogger("telegram-card-bot")

# ---------------------------
# Env
# ---------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TEMPLATE_SLIDES_ID = os.getenv("TEMPLATE_SLIDES_ID", "").strip()
OUTPUT_FOLDER_ID = os.getenv("OUTPUT_FOLDER_ID", "").strip()  # not used now, kept for compatibility

# OAuth (recommended)
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "").strip()

# fallback Service Account (optional)
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON", "").strip()

PLACEHOLDER_AR = os.getenv("PLACEHOLDER_AR", "<<Name in Arabic>>")
PLACEHOLDER_EN = os.getenv("PLACEHOLDER_EN", "<<Name in English>>")

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/presentations",
]

# ---------------------------
# App
# ---------------------------
app = FastAPI()

# ---------------------------
# Telegram helpers
# ---------------------------
TG_API = "https://api.telegram.org/bot{}/{}"
HTTP_TIMEOUT = 20

def tg(method: str, data: Optional[dict] = None, files: Optional[dict] = None) -> requests.Response:
    url = TG_API.format(BOT_TOKEN, method)
    r = requests.post(url, data=data, files=files, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        log.warning("TG error %s %s: %s", method, r.status_code, r.text[:800])
    return r

def tg_send_message(chat_id: str, text: str, reply_markup: Optional[dict] = None) -> Optional[int]:
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup)
    r = tg("sendMessage", payload)
    try:
        j = r.json()
        return int(j["result"]["message_id"])
    except Exception:
        return None

def tg_edit_message(chat_id: str, message_id: int, text: str, reply_markup: Optional[dict] = None) -> bool:
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup)
    r = tg("editMessageText", payload)
    return r.status_code == 200

def tg_answer_callback(callback_query_id: str) -> None:
    if callback_query_id:
        tg("answerCallbackQuery", {"callback_query_id": callback_query_id})

def tg_send_photo(chat_id: str, png_bytes: bytes, caption: str) -> None:
    files = {"photo": ("card.png", png_bytes)}
    data = {"chat_id": chat_id, "caption": caption}
    tg("sendPhoto", data=data, files=files)

# ---------------------------
# Google clients (cached)
# ---------------------------
_drive = None
_slides = None
_creds = None

def require_env():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing")
    if not TEMPLATE_SLIDES_ID:
        raise RuntimeError("TEMPLATE_SLIDES_ID is missing")

    # OAuth preferred (fix quota problems)
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

    # OAuth user credentials
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

    # Service Account fallback
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
# Messages (Arabic + English)
# ---------------------------
DIV = "\n--------------------\n"

def msg_welcome():
    ar = (
        "مرحباً بمنسوبي الشركة العربية في بوت توليد بطاقات التهنئة.\n"
        
    )
    en = (
        "Welcome to the Greeting Card Generation Bot for AlArabia Company staff.\n"
        
    )
    return ar + DIV + en

def msg_ask_ar():
    ar = "اكتب اسمك بالعربية:"
    en = "Enter your name in Arabic:"
    return ar + DIV + en

def msg_ask_en():
    ar = "اكتب اسمك بالإنجليزية:"
    en = "Enter your name in English:"
    return ar + DIV + en

def msg_invalid_ar(reason_ar: str):
    ar = f"غير صحيح: {reason_ar}\n\nاكتب الاسم بالعربية فقط."
    en = "Invalid Arabic name.\n\nPlease type Arabic letters only."
    return ar + DIV + en

def msg_invalid_en(reason_ar: str):
    ar = f"غير صحيح: {reason_ar}\n\nاكتب الاسم بالإنجليزية فقط."
    en = "Invalid English name.\n\nPlease type English letters only."
    return ar + DIV + en

def msg_confirm(name_ar: str, name_en: str):
    ar = (
        "تأكيد البيانات:\n\n"
        f"الاسم بالعربية: {name_ar}\n"
        f"الاسم بالإنجليزية: {name_en}\n\n"
        "اختر أحد الخيارات:"
    )
    en = (
        "Confirm details:\n\n"
        f"Arabic: {name_ar}\n"
        f"English: {name_en}\n\n"
        "Choose an option:"
    )
    return ar + DIV + en

def msg_creating():
    ar = "جاري توليد البطاقة..."
    en = "Generating your card..."
    return ar + DIV + en

def msg_ready():
    ar = "تم إنشاء البطاقة."
    en = "Your card is ready."
    return ar + DIV + en

def msg_error(err: str):
    ar = "خطأ أثناء إنشاء البطاقة:\n" + err
    en = "Error while creating the card:\n" + err
    return ar + DIV + en

# ---------------------------
# Keyboards (keep other buttons, remove Start entirely)
# ---------------------------
def kb_wait_en():
    return {
        "inline_keyboard": [
            [{"text": "إعادة كتابة الاسم العربي / Edit Arabic", "callback_data": "EDIT_AR"}],
        ]
    }

def kb_confirm():
    return {
        "inline_keyboard": [
            [{"text": "توليد البطاقة / Generate", "callback_data": "GEN"}],
            [
                {"text": "تعديل العربي / Edit Arabic", "callback_data": "EDIT_AR"},
                {"text": "تعديل الإنجليزي / Edit English", "callback_data": "EDIT_EN"},
            ],
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
    state: str = STATE_MENU
    name_ar: str = ""
    name_en: str = ""
    last_update_id: int = 0
    last_fingerprint: str = ""
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

sessions: Dict[str, Session] = {}

def get_session(chat_id: str) -> Session:
    s = sessions.get(chat_id)
    if not s:
        s = Session(chat_id=chat_id)
        sessions[chat_id] = s
    return s

def reset_session(s: Session):
    s.state = STATE_MENU
    s.name_ar = ""
    s.name_en = ""

# ---------------------------
# Queue worker
# ---------------------------
job_queue: asyncio.Queue = asyncio.Queue()

@dataclass
class Job:
    chat_id: str
    name_ar: str
    name_en: str
    requested_at: float

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
    s = get_session(job.chat_id)

    try:
        png_bytes = generate_card_png(job.name_ar, job.name_en)
        tg_send_photo(job.chat_id, png_bytes, msg_ready())
        async with s.lock:
            reset_session(s)

    except Exception as e:
        tg_send_message(job.chat_id, msg_error(str(e)))
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

def generate_card_png(name_ar: str, name_en: str) -> bytes:
    drive, slides, creds = build_clients()
    pres_id = None

    try:
        copied = drive.files().copy(
            fileId=TEMPLATE_SLIDES_ID,
            body={"name": f"tg_card_{int(time.time())}"},
            supportsAllDrives=True,
        ).execute()
        pres_id = copied["id"]

        slides.presentations().batchUpdate(
            presentationId=pres_id,
            body={
                "requests": [
                    {"replaceAllText": {"containsText": {"text": PLACEHOLDER_AR}, "replaceText": name_ar}},
                    {"replaceAllText": {"containsText": {"text": PLACEHOLDER_EN}, "replaceText": name_en}},
                ]
            }
        ).execute()

        pres = slides.presentations().get(presentationId=pres_id).execute()
        slide_id = pres["slides"][0]["objectId"]

        png_bytes = export_png(pres_id, slide_id, creds)
        return png_bytes

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

@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()
    update_id, chat_id, text_raw, msg_id, cq_id = extract_update(data)

    if cq_id:
        tg_answer_callback(cq_id)

    if not chat_id:
        return {"ok": True}

    s = get_session(chat_id)

    # Dedup retries
    fp = f"{update_id}|{msg_id}|{text_raw}"
    if update_id and s.last_update_id >= update_id:
        return {"ok": True}
    if s.last_fingerprint == fp:
        return {"ok": True}
    if update_id:
        s.last_update_id = update_id
    s.last_fingerprint = fp

    text = clean_text(text_raw)
    cmd = normalize_cmd(text)

    # callbacks
    if text in ("EDIT_AR", "EDIT_EN", "GEN"):
        cmd = text

    async with s.lock:
        # START works from any state: send welcome + ask arabic (no buttons)
        if cmd == "START":
            reset_session(s)
            tg_send_message(s.chat_id, msg_welcome())
            tg_send_message(s.chat_id, msg_ask_ar())
            s.state = STATE_WAIT_AR
            return {"ok": True}

        # WAIT_AR
        if s.state == STATE_WAIT_AR:
            ok, val = validate_ar(text)
            if not ok:
                tg_send_message(s.chat_id, msg_invalid_ar(val))
                return {"ok": True}

            s.name_ar = val
            s.state = STATE_WAIT_EN
            tg_send_message(s.chat_id, msg_ask_en(), kb_wait_en())
            return {"ok": True}

        # WAIT_EN
        if s.state == STATE_WAIT_EN:
            if cmd == "EDIT_AR":
                s.state = STATE_WAIT_AR
                tg_send_message(s.chat_id, msg_ask_ar())
                return {"ok": True}

            ok, val = validate_en(text)
            if not ok:
                tg_send_message(s.chat_id, msg_invalid_en(val), kb_wait_en())
                return {"ok": True}

            s.name_en = val
            s.state = STATE_CONFIRM
            tg_send_message(s.chat_id, msg_confirm(s.name_ar, s.name_en), kb_confirm())
            return {"ok": True}

        # CONFIRM
        if s.state == STATE_CONFIRM:
            if cmd == "EDIT_AR":
                s.state = STATE_WAIT_AR
                tg_send_message(s.chat_id, msg_ask_ar())
                return {"ok": True}
            if cmd == "EDIT_EN":
                s.state = STATE_WAIT_EN
                tg_send_message(s.chat_id, msg_ask_en(), kb_wait_en())
                return {"ok": True}

            if cmd == "GEN":
                s.state = STATE_CREATING
                tg_send_message(s.chat_id, msg_creating())
                await job_queue.put(Job(chat_id=s.chat_id, name_ar=s.name_ar, name_en=s.name_en, requested_at=time.time()))
                return {"ok": True}

            tg_send_message(s.chat_id, msg_confirm(s.name_ar, s.name_en), kb_confirm())
            return {"ok": True}

        # CREATING: ignore
        if s.state == STATE_CREATING:
            return {"ok": True}

        # If user writes anything بدون /start: نرشده بدون أزرار
        tg_send_message(s.chat_id, msg_welcome())
        tg_send_message(s.chat_id, "أرسل /start للبدء.\n" + DIV + "Send /start to begin.")
        return {"ok": True}
