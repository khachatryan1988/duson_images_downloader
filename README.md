# Duson Images Downloader

Downloads product images from `duson.am` by reading product → media relationships from MySQL and saving files into folders named by `item_id`.

- **Products table**: `products(id, item_id, media)`  
  - `media` may be a JSON array of media IDs, a single value, or a raw string.
- **Media table**: `media_hub(id, file_name)`
- **Result**: Files are downloaded to `./<item_id>/<original_file_name>` and existing files are skipped.

---

## How it works

1. Read all rows from `products` and extract media IDs from the `media` field.
2. Join media IDs with `media_hub` to resolve `file_name`.
3. Build URLs as:  
4. python src/download_duson_images.py
