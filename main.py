import os
import io
import time
import requests
from fastapi import FastAPI, Request

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request as GARequest

app = FastAPI()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TEMPLATE_SLIDES_ID = os.getenv("TEMPLATE_SLIDES_ID", "")
OUTPUT_FOLDER_ID = os.getenv("OUTPUT_FOLDER_ID", "")

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

PLACEHOLDER_AR = "<<Name in Arabic>>"
PLACEHOLDER_EN = "<<Name in English>>"

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/presentations",
]

# ---------- Telegram ----------
def tg(method, data=None, files=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        return requests.post(url, data=data, files=files, timeout=30)
    except Exception as e:
        print("Telegram error:", method, e)
        return None

def send_text(chat_id, text):
    tg("sendMessage", {"chat_id": chat_id, "text": text})

# ---------- Google ----------
def build_clients():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        raise RuntimeError("Missing GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REFRESH_TOKEN")

    if not TEMPLATE_SLIDES_ID:
        raise RuntimeError("TEMPLATE_SLIDES_ID missing")
    if not OUTPUT_FOLDER_ID:
        raise RuntimeError("OUTPUT_FOLDER_ID missing")

    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=SCOPES,
    )

    # force refresh to get access token
    creds.refresh(GARequest())

    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    slides = build("slides", "v1", credentials=creds, cache_discovery=False)
    return drive, slides, creds

def parse(text):
    text = " ".join((text or "").split()).strip()
    if "|" not in text:
        return None, None
    ar, en = text.split("|", 1)
    ar, en = ar.strip(), en.strip()
    if not ar or not en:
        return None, None
    return ar, en

def export_png_with_retry(creds, pres_id, slide_id, tries=4):
    if not creds.valid or creds.expired:
        creds.refresh(GARequest())

    url = f"https://docs.google.com/presentation/d/{pres_id}/export/png?pageid={slide_id}"
    headers = {"Authorization": f"Bearer {creds.token}"}

    last = None
    for i in range(tries):
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code == 200:
            return r.content
        last = (r.status_code, r.text[:400])
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.0 * (i + 1))
            continue
        raise RuntimeError(f"Export failed HTTP {r.status_code}: {r.text[:400]}")
    raise RuntimeError(f"Export failed after retries: {last}")

# ---------- Routes ----------
@app.get("/")
def home():
    return {"status": "Bot is running"}

@app.get("/webhook")
def webhook_check():
    return {"ok": True}

@app.post("/webhook")
async def webhook(req: Request):
    try:
        data = await req.json()
    except Exception:
        return {"ok": True}

    message = data.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()

    if not chat_id:
        return {"ok": True}

    if (text or "").lower() in ["/start", "start", "help"]:
        send_text(chat_id, "أرسل الاسم هكذا:\nمحمد أحمد | Mohammed Ahmed")
        return {"ok": True}

    ar, en = parse(text)
    if not ar or not en:
        send_text(chat_id, "صيغة غير صحيحة.\nاستخدم:\nمحمد أحمد | Mohammed Ahmed")
        return {"ok": True}

    send_text(chat_id, "⏳ جاري إنشاء البطاقة...")

    drive = None
    pres_id = None

    try:
        drive, slides, creds = build_clients()

        # Copy template (now owned by YOUR Gmail via OAuth)
        copy = drive.files().copy(
            fileId=TEMPLATE_SLIDES_ID,
            body={"name": f"card_{int(time.time())}"}
        ).execute()
        pres_id = copy["id"]

        # Replace placeholders
        slides.presentations().batchUpdate(
            presentationId=pres_id,
            body={
                "requests": [
                    {"replaceAllText": {"containsText": {"text": PLACEHOLDER_AR, "matchCase": True}, "replaceText": ar}},
                    {"replaceAllText": {"containsText": {"text": PLACEHOLDER_EN, "matchCase": True}, "replaceText": en}},
                ]
            }
        ).execute()

        # Get first slide id
        pres = slides.presentations().get(presentationId=pres_id).execute()
        slide_id = pres["slides"][0]["objectId"]

        # Export PNG
        png = export_png_with_retry(creds, pres_id, slide_id, tries=4)

        # Save PNG to folder (optional)
        media = MediaIoBaseUpload(io.BytesIO(png), mimetype="image/png", resumable=False)
        drive.files().create(
            body={"name": "card.png", "parents": [OUTPUT_FOLDER_ID]},
            media_body=media
        ).execute()

        # Send photo to Telegram
        files = {"photo": ("card.png", png)}
        tg("sendPhoto", {"chat_id": chat_id, "caption": "✨ بطاقتك جاهزة ✨"}, files=files)

    except Exception as e:
        print("Error:", e)
        send_text(chat_id, f"❌ خطأ أثناء إنشاء البطاقة:\n{e}")

    finally:
        if drive and pres_id:
            try:
                drive.files().delete(fileId=pres_id).execute()
            except Exception:
                pass

    return {"ok": True}
