import os
import json
import re
from collections import defaultdict

INPUT_DIR = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/table_outputs_converted_to_json"
OUTPUT_DIR = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/combined_table_outputs_converted_to_json"

os.makedirs(OUTPUT_DIR, exist_ok=True)

page_pattern = re.compile(r"^(.*)_page_(\d+)\.json$")
grouped_files = defaultdict(list)

# Group files
for file in os.listdir(INPUT_DIR):
    if not file.endswith(".json"):
        continue

    match = page_pattern.match(file)
    if match:
        base_name, page_num = match.group(1), int(match.group(2))
        grouped_files[base_name].append((page_num, file))
    else:
        grouped_files[file.replace(".json", "")].append((None, file))

# Merge logic
for base_name, files in grouped_files.items():
    files.sort(key=lambda x: (x[0] is None, x[0]))

    combined = {
        "invoice_id": base_name,
        "items": [],
        "confidence": {
            "item_rows": 0,
            "columns_detected": set()
        }
    }

    has_pages = any(p is not None for p, _ in files)

    # If file has no _page_, just copy it as-is
    if not has_pages and len(files) == 1:
        _, file = files[0]
        with open(os.path.join(INPUT_DIR, file), "r", encoding="utf-8") as f:
            data = json.load(f)

        with open(os.path.join(OUTPUT_DIR, f"{base_name}.json"), "w", encoding="utf-8") as out:
            json.dump(data, out, indent=2, ensure_ascii=False)

        print(f"✅ Kept as-is: {base_name}.json")
        continue

    # Merge paginated files
    for page_num, file in files:
        with open(os.path.join(INPUT_DIR, file), "r", encoding="utf-8") as f:
            data = json.load(f)

        # Merge items
        combined["items"].extend(data.get("items", []))

        # Merge confidence
        conf = data.get("confidence", {})
        combined["confidence"]["item_rows"] += conf.get("item_rows", 0)
        combined["confidence"]["columns_detected"].update(
            conf.get("columns_detected", [])
        )

    # Convert set → list
    combined["confidence"]["columns_detected"] = list(
        combined["confidence"]["columns_detected"]
    )

    # Write output
    with open(os.path.join(OUTPUT_DIR, f"{base_name}.json"), "w", encoding="utf-8") as out:
        json.dump(combined, out, indent=2, ensure_ascii=False)

    print(f"✅ Merged pages → {base_name}.json")
