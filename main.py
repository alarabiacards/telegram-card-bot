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

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TEMPLATE_SLIDES_ID = os.getenv("TEMPLATE_SLIDES_ID", "")
OUTPUT_FOLDER_ID = os.getenv("OUTPUT_FOLDER_ID", "")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON", "")

PLACEHOLDER_AR = "<<Name in Arabic>>"
PLACEHOLDER_EN = "<<Name in English>>"

# ---- Telegram helpers ----
def tg(method, data=None, files=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        return requests.post(url, data=data, files=files, timeout=30)
    except Exception as e:
        print("Telegram request failed:", method, e)
        return None

def send_text(chat_id, text):
    tg("sendMessage", {"chat_id": chat_id, "text": text})

# ---- Google helpers ----
def build_clients():
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("SERVICE_ACCOUNT_JSON missing")
    if not TEMPLATE_SLIDES_ID:
        raise RuntimeError("TEMPLATE_SLIDES_ID missing")
    if not OUTPUT_FOLDER_ID:
        raise RuntimeError("OUTPUT_FOLDER_ID missing")

    info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/presentations",
        ],
    )
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    slides = build("slides", "v1", credentials=creds, cache_discovery=False)
    return drive, slides

def parse(text):
    text = " ".join((text or "").split()).strip()
    if "|" not in text:
        return None, None
    ar, en = text.split("|", 1)
    ar, en = ar.strip(), en.strip()
    if not ar or not en:
        return None, None
    return ar, en

def export_png_with_retry(drive, pres_id, slide_id, tries=4):
    creds = drive._http.credentials
    if not creds.valid or creds.expired:
        creds.refresh(GARequest())

    url = f"https://docs.google.com/presentation/d/{pres_id}/export/png?pageid={slide_id}"
    headers = {"Authorization": f"Bearer {creds.token}"}

    last = None
    for i in range(tries):
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code == 200:
            return r.content
        last = (r.status_code, r.text[:500])
        # transient errors
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.0 * (i + 1))
            continue
        raise RuntimeError(f"Export failed HTTP {r.status_code}: {r.text[:500]}")
    raise RuntimeError(f"Export failed after retries: {last}")

# ---- Routes ----
@app.get("/")
def home():
    return {"status": "Bot is running"}

# IMPORTANT: Telegram sometimes validates the webhook URL using GET/HEAD.
# Having GET /webhook fixes "invalid webhook URL specified".
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

    # Basic help
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
        drive, slides = build_clients()

        # Copy template
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
                    {
                        "replaceAllText": {
                            "containsText": {"text": PLACEHOLDER_AR, "matchCase": True},
                            "replaceText": ar
                        }
                    },
                    {
                        "replaceAllText": {
                            "containsText": {"text": PLACEHOLDER_EN, "matchCase": True},
                            "replaceText": en
                        }
                    },
                ]
            }
        ).execute()

        # Get first slide id
        pres = slides.presentations().get(presentationId=pres_id).execute()
        slide_id = pres["slides"][0]["objectId"]

        # Export PNG
        png = export_png_with_retry(drive, pres_id, slide_id, tries=4)

        # Save to Drive folder
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
        # Delete copied presentation
        if drive and pres_id:
            try:
                drive.files().delete(fileId=pres_id).execute()
            except Exception:
                pass

    return {"ok": True}
