import os
import json
import csv
import re
from rapidfuzz import fuzz

# =============================
# CONFIG
# =============================

GLM_DIR = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/combined_table_outputs_converted_to_json"
TEXTRACT_DIR = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/processed_textract_output"

OUTPUT_COMPARISON_CSV = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/glm_vs_textract_comparison.csv"
OUTPUT_ITEM_SUMMARY_CSV = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/glm_vs_textract_item_summary.csv"
OUTPUT_INVOICE_SUMMARY_CSV = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/glm_vs_textract_invoice_summary.csv"


# =============================
# HELPERS
# =============================

def clean_text(s):
    if s is None:
        return ""

    s = str(s).lower()

    # Remove OCR noise
    s = re.sub(r"old\s*mrp[:\s]*\d*\.?\d*", "", s)
    s = re.sub(r"mrp[:\s]*\d*\.?\d*", "", s)

    # Keep useful characters
    s = re.sub(r"[^a-z0-9\*\./\s]", " ", s)
    s = re.sub(r"\s+", " ", s)

    return s.strip()


def parse_float(v):
    try:
        if v is None or v == "":
            return None
        return float(str(v).replace("%", "").strip())
    except:
        return None


def zero_equivalent(a, b):
    """
    Treat:
    Textract = 0
    GLM = "" or None
    as equal
    """
    if (a == 0 or a == "0") and (b is None or b == "" or b == 0):
        return True
    return False


# =============================
# ALIASES
# =============================

GLM_ALIASES = {
    "name": [
        "product", "product name", "product description",
        "description", "description of goods",
        "description of goods/services",
        "item", "item name", "item description",
        "sku description",
        "sr. product description",
        "product & packing",
        "product name & packing",
        "item name & packing",
        "item name | old mrp",
        "product description | old mrp",
        "item name | mrp",
        "item description | mrp",
        "particulars",
        "products",
        "hsn code & item name",
        "hsn & item name",
        "product description & packing",
        "product name & pack"
    ],

    "batchNo": [
        "batch", "batch no", "batch no.", "batchno",
        "batchno.", "batch.no",
        "batch #", "batch#",
        "mfg batchno", "free batch no.",
        "ch. no."
    ],

    "hsnCode": [
        "hsn", "hsn code", "hsncode",
        "hsn/sac", "hsn / sac", "hsn/sac code",
        "hsn code/sac", "hsn / sac code",
        "hsh", "hisn", "hsv", "shn"
    ],

    "qty": [
        "qty", "qty.", "quantity",
        "q'ty", "units", "uom",
        "qty + free", "qty free", "qty+free",
        "pcs", "pcs.",
        "case", "out"
    ],

    "freeQty": [
        "free", "fr.", "fq",
        "free qty", "free/sch",
        "free /sch", "free / sch",
        "free / sch ant",
        "pcs[fr.", "free pcs"
    ],

    "packaging": [
        "pack", "packing", "pkg", "pkng",
        "box", "units",
        "pack size"
    ],

    "mrp": [
        "mrp", "m.r.p", "m.r.p.",
        "old mrp", "oldmrp",
        "o mrp", "o.mrp",
        "new mrp", "revised mrp",
        "n.mrp", "t.mrp",
        "old", "npaa mrp",
        "mrp ref"
    ],

    "rate": [
        "rate", "ptr", "p.t.r.", "p.t.r",
        "s.rate", "unit price",
        "price", "rate/ doz",
        "net rate",
        "rate]"
    ],

    "discountPercent": [
        "disc", "disc%", "disc %",
        "dis%", "di.%", "d%",
        "dis.", "dis",
        "discount", "discount %"
    ],

    "schemePercent": [
        "sch", "sch %", "sch%",
        "sch dis", "scheme", "scheme %",
        "coupon", "coupon disc"
    ],

    "itemAmount": [
        "amount", "amt", "amt.",
        "amour",
        "net amount", "net amt",
        "netamt.", "netamount",
        "net value", "net",
        "value",
        "gross value",
        "taxable", "taxable amt.",
        "taxable amount",
        "net taxable amt"
    ],

    "gst": [
        "gst", "gst%", "gst %",
        "tax%", "tax rate(%)",
        "igst", "igst (%)",
        "cgst", "cgst %",
        "sgst", "sgst %",
        "gst amt", "gst amount",
        "sgst/utgst",
        "cgst/sgst",
        "cgst(%)", "sgst(%)",
        "tax", "tax type"
    ]
}



def find_by_alias(p, aliases):
    for key in p.keys():
        for alias in aliases:
            if clean_text(key) == clean_text(alias):
                return p.get(key)
    return None


# =============================
# NORMALIZE GLM
# =============================

def normalize_glm_item(p):
    name_raw = find_by_alias(p, GLM_ALIASES["name"]) or ""
    name = clean_text(name_raw)

    if name == "" or name.startswith("old"):
        return None

    return {
        "name": name,
        "batchNo": clean_text(find_by_alias(p, GLM_ALIASES["batchNo"])),
        "hsnCode": clean_text(find_by_alias(p, GLM_ALIASES["hsnCode"])),
        "qty": parse_float(find_by_alias(p, GLM_ALIASES["qty"])),
        "mrp": parse_float(find_by_alias(p, GLM_ALIASES["mrp"])),
        "rate": parse_float(find_by_alias(p, GLM_ALIASES["rate"])),
        "discountPercent": parse_float(find_by_alias(p, GLM_ALIASES["discountPercent"])),
        "schemePercent": parse_float(find_by_alias(p, GLM_ALIASES["schemePercent"])),
        "itemAmount": parse_float(find_by_alias(p, GLM_ALIASES["itemAmount"])),
        "gst": parse_float(find_by_alias(p, GLM_ALIASES["gst"])) or 5
    }


# =============================
# SIMILARITY
# =============================

def numeric_similarity(a, b):
    if zero_equivalent(a, b):
        return 100

    if a is None or b is None:
        return 0

    if a == 0 and b == 0:
        return 100

    return max(0, 100 - abs(a - b) / max(abs(a), abs(b)) * 100)


def field_similarity(a, b):
    if isinstance(a, str) or isinstance(b, str):
        a = clean_text(a)
        b = clean_text(b)
        if zero_equivalent(a, b):
            return 100
        return fuzz.token_sort_ratio(a, b)
    return numeric_similarity(a, b)


# =============================
# MATCHING (NO THRESHOLD)
# One-to-one greedy matching
# =============================

def match_items(textract_items, glm_items):
    matches = []
    used_glm = set()

    for t in textract_items:
        best_idx = None
        best_score = -1

        t_name = clean_text(t.get("name"))

        for i, g in enumerate(glm_items):
            if i in used_glm:
                continue

            name_score = fuzz.token_sort_ratio(
                t_name,
                g.get("name")
            )

            if name_score > best_score:
                best_score = name_score
                best_idx = i

        if best_idx is not None:
            matches.append((t, glm_items[best_idx], best_score))
            used_glm.add(best_idx)
        else:
            # No GLM item left → extra Textract row
            matches.append((t, None, 0))

    # Remaining GLM items → missed by Textract
    for i, g in enumerate(glm_items):
        if i not in used_glm:
            matches.append((None, g, 0))

    return matches


# =============================
# COMPARE INVOICE
# =============================

def compare_invoice(textract_json, glm_json):

    textract_items = textract_json.get("final_output", {}).get("items", [])
    glm_raw = glm_json.get("items", [])

    glm_items = []
    for g in glm_raw:
        n = normalize_glm_item(g)
        if n:
            glm_items.append(n)

    matches = match_items(textract_items, glm_items)

    fields = [
        "name", "batchNo", "hsnCode", "qty",
        "mrp", "rate", "discountPercent",
        "schemePercent", "itemAmount", "gst"
    ]

    comparison_rows = []
    item_summary = []
    item_avgs = []

    for idx, (t, g, match_score) in enumerate(matches, 1):

        scores = []

        for f in fields:
            t_val = t.get(f) if t else None
            g_val = g.get(f) if g else None

            s = field_similarity(t_val, g_val)
            scores.append(s)

            comparison_rows.append([
                idx, f, t_val, g_val, round(s, 2)
            ])

        item_avg = round(sum(scores) / len(scores), 2)
        item_avgs.append(item_avg)

        item_summary.append([
            idx,
            match_score,
            item_avg,
            "EXTRA_TEXTRACT" if g is None else (
                "MISSING_IN_TEXTRACT" if t is None else "MATCHED"
            )
        ])

    invoice_avg = round(sum(item_avgs) / len(item_avgs), 2) if item_avgs else 0

    return comparison_rows, item_summary, invoice_avg


# =============================
# MAIN
# =============================

def main():

    with open(OUTPUT_COMPARISON_CSV, "w", newline="", encoding="utf-8") as comp_f, \
         open(OUTPUT_ITEM_SUMMARY_CSV, "w", newline="", encoding="utf-8") as item_f, \
         open(OUTPUT_INVOICE_SUMMARY_CSV, "w", newline="", encoding="utf-8") as inv_f:

        comp_writer = csv.writer(comp_f)
        item_writer = csv.writer(item_f)
        inv_writer = csv.writer(inv_f)

        comp_writer.writerow([
            "file_name", "item_index", "field",
            "textract_value", "glm_value",
            "field_similarity"
        ])

        item_writer.writerow([
            "file_name", "item_index",
            "name_match_score",
            "item_avg_similarity",
            "status"
        ])

        inv_writer.writerow([
            "file_name",
            "invoice_avg_similarity"
        ])

        for file in sorted(os.listdir(GLM_DIR)):
            try:
                with open(os.path.join(GLM_DIR, file)) as gf:
                    glm_json = json.load(gf)

                with open(os.path.join(TEXTRACT_DIR, file)) as tf:
                    textract_json = json.load(tf)

                comp_rows, item_rows, inv_avg = compare_invoice(textract_json, glm_json)

                for r in comp_rows:
                    comp_writer.writerow([file] + r)

                for r in item_rows:
                    item_writer.writerow([file] + r)

                inv_writer.writerow([file, inv_avg])

                print(f"Processed {file} → {inv_avg}%")

            except Exception as e:
                print(f"Skipped {file} → {e}")

    print("\nDONE")
    print("Comparison:", OUTPUT_COMPARISON_CSV)
    print("Item Summary:", OUTPUT_ITEM_SUMMARY_CSV)
    print("Invoice Summary:", OUTPUT_INVOICE_SUMMARY_CSV)


if __name__ == "__main__":
    main()
