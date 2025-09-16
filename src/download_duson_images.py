import os
import json
import mysql.connector
import requests
import unicodedata
import re
from collections import defaultdict
from urllib.parse import urlparse
from typing import Dict, List, Tuple, Set

# -------- Env / Config --------
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "__duson")

# Must end with /
BASE_URL = os.getenv("BASE_URL", "https://duson.am/storage/media/")

OUTPUT_DIR = os.getenv("OUTPUT_DIR") or os.path.join(os.path.dirname(__file__), "..", "downloads")
OUTPUT_DIR = os.path.abspath(OUTPUT_DIR)

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))  # seconds

db_config = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
}

# -------- Helpers --------
def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

def sanitize_filename(filename: str) -> str:
    normalized = unicodedata.normalize('NFKD', filename).encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'[^\w\-_\. ]', '_', normalized)

def ensure_trailing_slash(s: str) -> str:
    return s if s.endswith("/") else s + "/"

# -------- Main --------
def main():
    base_url = ensure_trailing_slash(BASE_URL)

    # Гарантируем существование базовой папки загрузок
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[Init] Images will be saved to: {OUTPUT_DIR}")

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # 1) Fetch all products with media JSON field
        cursor.execute("SELECT id, item_id, media FROM products ORDER BY id ASC")
        product_rows = cursor.fetchall()

        if not product_rows:
            print("No products found.")
            return

        # 2) Extract all media IDs from products
        all_media_ids: Set[str] = set()
        product_media_map: Dict[Tuple[int, str], List[str]] = {}  # (product_id, item_id) -> [media_ids]

        for prod_id, item_id, media_raw in product_rows:
            media_ids: List[str] = []
            if media_raw:
                try:
                    data = json.loads(media_raw)
                    if isinstance(data, list):
                        media_ids = [str(i) for i in data if i]
                    else:
                        media_ids = [str(data)] if data else []
                except json.JSONDecodeError:
                    media_ids = [str(media_raw)] if media_raw else []

            # Remove duplicates and invalid values
            media_ids = list({mid for mid in media_ids if mid and str(mid).lower() != "none"})
            product_media_map[(prod_id, str(item_id))] = media_ids
            all_media_ids.update(media_ids)

        if not all_media_ids:
            print("No valid media IDs found in products.")
            return

        # 3) Fetch id and file_name from media_hub for all media IDs
        format_strings = ','.join(['%s'] * len(all_media_ids))
        query = f"SELECT id, file_name FROM media_hub WHERE id IN ({format_strings})"
        cursor.execute(query, tuple(all_media_ids))
        media_rows = cursor.fetchall()

        media_id_to_file: Dict[str, str] = {str(mid): fname for mid, fname in media_rows}

        # 4) Build grouped data: item_id -> list of (media_id, full URL)
        grouped_data: Dict[str, List[Tuple[str, str]]] = defaultdict(list)

        for (prod_id, item_id), media_ids in product_media_map.items():
            folder_key = str(item_id)
            for mid in media_ids:
                file_name = media_id_to_file.get(mid)
                if not file_name:
                    print(f"[{folder_key}] Warning: media id {mid} not found in media_hub")
                    continue
                url = f"{base_url}{mid}/{file_name}"
                if is_valid_url(url):
                    grouped_data[folder_key].append((mid, url))
                else:
                    print(f"[{folder_key}] Invalid URL constructed: {url}")

        # 5) Download images using original sanitized filename
        for folder_name, media_entries in grouped_data.items():
            out_folder = os.path.join(OUTPUT_DIR, folder_name)
            os.makedirs(out_folder, exist_ok=True)

            for mid, url in media_entries:
                file_name = media_id_to_file.get(mid)
                if not file_name:
                    continue

                file_name = sanitize_filename(file_name)
                filename = os.path.join(out_folder, file_name)

                if os.path.exists(filename):
                    print(f"[{folder_name}] Skipped (already exists): {filename}")
                    continue

                try:
                    headers = {'User-Agent': 'Mozilla/5.0'}
                    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                    response.raise_for_status()

                    with open(filename, 'wb') as f:
                        f.write(response.content)

                    print(f"[{folder_name}] Downloaded: {filename}")
                except Exception as e:
                    print(f"[{folder_name}] Error downloading {url}: {e}")

    except Exception as e:
        print("Error:", e)
    finally:
        try:
            if 'conn' in locals() and conn.is_connected():
                conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
