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
log = logging.getLogger("telegram-card-bot")

# ---------------------------
# Env (STRICT NAMES)
# ---------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN_ALARABIA", "").strip()
TEMPLATE_SLIDES_ID = os.getenv("TEMPLATE_SLIDES_ID_ALARABIA_SQUARE", "").strip()

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "").strip()
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

def require_env():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN_ALARABIA is missing")
    if not TEMPLATE_SLIDES_ID:
        raise RuntimeError("TEMPLATE_SLIDES_ID_ALARABIA_SQUARE is missing")
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN) and not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Provide Google OAuth or Service Account credentials")

def tg(method: str, data: Optional[dict] = None, files: Optional[dict] = None) -> requests.Response:
    url = TG_API.format(BOT_TOKEN, method)
    return requests.post(url, data=data, files=files, timeout=HTTP_TIMEOUT)

def tg_send_message(chat_id: str, text: str, reply_markup: Optional[dict] = None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    tg("sendMessage", payload)

def tg_send_photo(chat_id: str, png_bytes: bytes, caption: str, reply_markup: Optional[dict] = None):
    files = {"photo": ("card.png", png_bytes)}
    data = {"chat_id": chat_id, "caption": caption}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    tg("sendPhoto", data=data, files=files)

# ---------------------------
# Google Clients
# ---------------------------
_drive = None
_slides = None
_creds = None

def build_clients():
    global _drive, _slides, _creds

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
    else:
        info = json.loads(SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    slides = build("slides", "v1", credentials=creds, cache_discovery=False)

    _drive, _slides, _creds = drive, slides, creds
    return drive, slides, creds

# ---------------------------
# Generate Card
# ---------------------------
def export_png(pres_id: str, slide_object_id: str, creds) -> bytes:
    if not creds.valid or creds.expired:
        creds.refresh(GARequest())

    url = f"https://docs.google.com/presentation/d/{pres_id}/export/png?pageid={slide_object_id}"
    headers = {"Authorization": f"Bearer {creds.token}"}
    r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)

    if r.status_code != 200:
        raise RuntimeError("PNG export failed")

    return r.content

def generate_card_png(name_ar: str, name_en: str) -> bytes:
    drive, slides, creds = build_clients()

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

    drive.files().delete(fileId=pres_id, supportsAllDrives=True).execute()

    return png_bytes

# ---------------------------
# Routes
# ---------------------------
@app.on_event("startup")
async def startup():
    require_env()

@app.get("/")
def home():
    return {"status": "ok"}

@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()

    if "message" not in data:
        return {"ok": True}

    msg = data["message"]
    chat_id = str(msg["chat"]["id"])
    text = msg.get("text", "").strip()

    if text.lower() == "/start":
        tg_send_message(chat_id, "🎉 Send your Arabic name first:")
        return {"ok": True}

    parts = text.split("|")

    if len(parts) == 2:
        name_ar = parts[0].strip()
        name_en = parts[1].strip()

        tg_send_message(chat_id, "Generating your card...")

        png = generate_card_png(name_ar, name_en)
        tg_send_photo(chat_id, png, "Your card is ready 🎉")

    else:
        tg_send_message(chat_id, "Format: ArabicName | EnglishName")

    return {"ok": True}
