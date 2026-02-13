import csv
import json
import os
import re

CSV_PATH = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/glm_vs_textract_comparison.csv"
JSON_PATH = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/Invoices_OCR_Data_With_Processed_output.json"
OUTPUT_CSV = "/home/ubuntu22/MyProjects/paddle_ocr_vl/glm_vs_textract_comparison_with_links.csv"

# =============================
# Normalization function
# =============================
def normalize(name):
    name = os.path.splitext(name)[0]
    name = name.replace(" ", "_")
    name = re.sub(r"_+", "_", name)
    return name.strip()

# =============================
# Build mapping
# =============================
with open(JSON_PATH, "r") as f:
    data = json.load(f)

link_map = {}

for item in data:
    output_name = os.path.basename(item["outputFileLink"])
    key = normalize(output_name)

    link_map[key] = {
        "inputFileLink": item["inputFileLink"],
        "outputFileLink": item["outputFileLink"],
        "processedOutputFileLink": item["processedOutputFileLink"]
    }

# =============================
# Update CSV
# =============================
with open(CSV_PATH, "r", newline="") as infile, \
     open(OUTPUT_CSV, "w", newline="") as outfile:

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + [
        "inputFileLink",
        "outputFileLink",
        "processedOutputFileLink"
    ]

    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        file_name = row["file_name"]
        key = normalize(file_name)

        links = link_map.get(key, {})

        # Debug if missing
        if not links:
            print("No match:", key)

        row["inputFileLink"] = links.get("inputFileLink", "")
        row["outputFileLink"] = links.get("outputFileLink", "")
        row["processedOutputFileLink"] = links.get("processedOutputFileLink", "")

        writer.writerow(row)

print("Done!")
