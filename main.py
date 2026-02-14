import os
import io
import json
import time
import requests
from fastapi import FastAPI, Request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request as GARequest

app = FastAPI()

BOT_TOKEN = os.getenv("BOT_TOKEN")
TEMPLATE_SLIDES_ID = os.getenv("TEMPLATE_SLIDES_ID")
OUTPUT_FOLDER_ID = os.getenv("OUTPUT_FOLDER_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

PLACEHOLDER_AR = "<<Name in Arabic>>"
PLACEHOLDER_EN = "<<Name in English>>"

def tg(method, data=None, files=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    return requests.post(url, data=data, files=files)

def build_clients():
    info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/presentations",
        ],
    )
    drive = build("drive", "v3", credentials=creds)
    slides = build("slides", "v1", credentials=creds)
    return drive, slides

def parse(text):
    if "|" not in text:
        return None, None
    ar, en = text.split("|", 1)
    return ar.strip(), en.strip()

@app.get("/")
def home():
    return {"status": "Bot is running"}

@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()
    message = data.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    text = message.get("text", "")

    if not chat_id:
        return {"ok": True}

    if text.lower() in ["/start", "start"]:
        tg("sendMessage", {"chat_id": chat_id, "text": "أرسل الاسم هكذا:\nمحمد | Mohammed"})
        return {"ok": True}

    ar, en = parse(text)
    if not ar or not en:
        tg("sendMessage", {"chat_id": chat_id, "text": "صيغة غير صحيحة.\nاستخدم:\nمحمد | Mohammed"})
        return {"ok": True}

    tg("sendMessage", {"chat_id": chat_id, "text": "⏳ جاري إنشاء البطاقة..."})

    drive, slides = build_clients()

    copy = drive.files().copy(
        fileId=TEMPLATE_SLIDES_ID,
        body={"name": f"card_{int(time.time())}"}
    ).execute()

    pres_id = copy["id"]

    slides.presentations().batchUpdate(
        presentationId=pres_id,
        body={
            "requests": [
                {"replaceAllText": {"containsText": {"text": PLACEHOLDER_AR}, "replaceText": ar}},
                {"replaceAllText": {"containsText": {"text": PLACEHOLDER_EN}, "replaceText": en}},
            ]
        }
    ).execute()

    pres = slides.presentations().get(presentationId=pres_id).execute()
    slide_id = pres["slides"][0]["objectId"]

    creds = drive._http.credentials
    if not creds.valid or creds.expired:
        creds.refresh(GARequest())

    url = f"https://docs.google.com/presentation/d/{pres_id}/export/png?pageid={slide_id}"
    headers = {"Authorization": f"Bearer {creds.token}"}
    png = requests.get(url, headers=headers).content

    media = MediaIoBaseUpload(io.BytesIO(png), mimetype="image/png")
    drive.files().create(
        body={"name": "card.png", "parents": [OUTPUT_FOLDER_ID]},
        media_body=media
    ).execute()

    files = {"photo": ("card.png", png)}
    tg("sendPhoto", {"chat_id": chat_id, "caption": "✨ بطاقتك جاهزة ✨"}, files=files)

    drive.files().delete(fileId=pres_id).execute()

    return {"ok": True}
