import os
import json
from bs4 import BeautifulSoup

# ================= CONFIG ================= #

CROP_DIR = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/test_table_outputs"
OUT_DIR = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/table_outputs_converted_to_json"

os.makedirs(OUT_DIR, exist_ok=True)

# ================= HELPERS ================= #

def clean_header(text):
    """
    Light cleanup only:
    - strip spaces
    - collapse multiple spaces
    """
    return " ".join(text.split())

def is_summary_row(text):
    text = text.lower()
    return any(k in text for k in [
        "total", "grand total", "net total",
        "round off", "amount in words", "gst payable"
    ])

def is_valid_item_row(row):
    return any(v and str(v).strip() for v in row.values())

# ================= ITEM EXTRACTION ================= #

def extract_items(md_path):
    soup = BeautifulSoup(open(md_path, encoding="utf-8"), "lxml")
    rows = soup.find_all("tr")

    if len(rows) < 2:
        return []

    # ---- Extract headers dynamically ----
    header_cells = rows[0].find_all(["td", "th"])
    headers = [clean_header(c.get_text(strip=True)) for c in header_cells]

    if not headers:
        return []

    items = []

    for r in rows[1:]:
        cells = [c.get_text(strip=True) for c in r.find_all("td")]
        row_text = " ".join(cells)

        if not cells or is_summary_row(row_text):
            continue

        row = {}

        for idx, header in enumerate(headers):
            row[header] = cells[idx] if idx < len(cells) else None

        if is_valid_item_row(row):
            items.append(row)

    return items

# ================= MAIN PIPELINE ================= #

def process_invoice(image_id):
    md_path = os.path.join(CROP_DIR, image_id, f"{image_id}.md")

    if not os.path.exists(md_path):
        print(f"⚠️ Missing crop file for {image_id}")
        return

    items = extract_items(md_path)

    output = {
        "invoice_id": image_id,
        "items": items,
        "confidence": {
            "item_rows": len(items),
            "columns_detected": list(items[0].keys()) if items else []
        }
    }

    out_path = os.path.join(OUT_DIR, f"{image_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved → {out_path}")

def run_all():
    for image_id in os.listdir(CROP_DIR):
        process_invoice(image_id)

# ================= ENTRY ================= #

if __name__ == "__main__":
    run_all()
