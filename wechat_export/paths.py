from pathlib import Path
import hashlib


EXPORT_ROOT = Path(__file__).resolve().parent
OUTPUT_ROOT = EXPORT_ROOT / "output"
KEYS_FILE = OUTPUT_ROOT / "keys" / "all_keys.json"
DATABASES_DIR = OUTPUT_ROOT / "databases"
DECRYPTED_DIR = DATABASES_DIR / "decrypted"
MESSAGE_DIR = DECRYPTED_DIR / "message"
IMAGES_DIR = OUTPUT_ROOT / "images" / "decoded_images"
EXPORTS_DIR = OUTPUT_ROOT / "exports"
CHAT_EXPORTS_DIR = EXPORTS_DIR / "chat"
MANUAL_EXPORTS_DIR = CHAT_EXPORTS_DIR / "manual"
BATCH_EXPORTS_DIR = CHAT_EXPORTS_DIR / "batch"
CONTACT_EXPORTS_DIR = EXPORTS_DIR / "contacts"
REPORTS_DIR = OUTPUT_ROOT / "reports"


def ensure_output_dirs():
    for path in (
        OUTPUT_ROOT,
        KEYS_FILE.parent,
        DATABASES_DIR,
        DECRYPTED_DIR,
        IMAGES_DIR,
        EXPORTS_DIR,
        CHAT_EXPORTS_DIR,
        MANUAL_EXPORTS_DIR,
        BATCH_EXPORTS_DIR,
        CONTACT_EXPORTS_DIR,
        REPORTS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def target_table_for(username):
    return f"Msg_{hashlib.md5(username.encode()).hexdigest()}"
