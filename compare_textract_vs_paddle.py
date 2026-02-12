import os
import json
import csv
import re
from rapidfuzz import fuzz

# =============================
# CONFIG
# =============================

PADDLE_DIR = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/combined_table_outputs_converted_to_json"
TEXTRACT_DIR = ""
OUTPUT_CSV = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/paddle_vs_textract_comparison.csv"
OUTPUT_SUMMARY_CSV = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/paddle_vs_textract_summary.csv"


MATCH_THRESHOLD_STRICT = 70
MATCH_THRESHOLD_RELAXED = 55


# =============================
# HELPERS
# =============================

def clean_text(s):
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip().lower()


def parse_float(v):
    try:
        return float(str(v).replace("%", "").strip())
    except:
        return None


def find_by_alias(p, aliases):
    for key in p.keys():
        for alias in aliases:
            if clean_text(key) == clean_text(alias):
                return p.get(key)
    return None


# =============================
# PADDLE ALIAS MAP (EXPANDED)
# =============================

# PADDLE_ALIASES = {
#     "name": [
#         "product", "product name", "product description",
#         "description", "description of goods",
#         "description of goods/services",
#         "item name", "item description",
#         "sku description", "sr. product description",
#         "hsn code & item name", "item name & packing",
#         "product & packing"
#     ],

#     "batchNo": [
#         "batch", "batch no", "batch no.", "batchno",
#         "batchno.", "batch.no", "mfg batchno",
#         "free batch no."
#     ],

#     "hsnCode": [
#         "hsn", "hsn code", "hsncode", "hsn/sac",
#         "hsn/sac code", "hsn / sac", "hsh"
#     ],

#     "qty": [
#         "qty", "qty.", "quantity", "units", "uom"
#     ],

#     "freeQty": [
#         "free", "fr.", "fq", "free qty",
#         "free / sch ant"
#     ],

#     "packaging": [
#         "pack", "packing", "pkng", "pkg"
#     ],

#     "mrp": [
#         "mrp", "m.r.p", "old mrp", "oldmrp",
#         "o mrp", "o.mrp", "new mrp",
#         "revised mrp", "n.mrp", "old"
#     ],

#     "rate": [
#         "rate", "ptr", "unit price",
#         "s.rate", "rate/ doz", "price"
#     ],

#     "discountPercent": [
#         "disc", "disc%", "dis%", "di.%", "d%",
#         "discount", "discount %", "disc %"
#     ],

#     "schemePercent": [
#         "sch", "sch %", "sch dis", "scheme",
#         "coupon", "coupon disc"
#     ],

#     "itemAmount": [
#         "amount", "net amount", "net amt",
#         "netamt.", "net amt.", "total amt.",
#         "amour", "amt", "net value"
#     ],

#     "gst": [
#         "gst%", "gst", "tax rate(%)",
#         "igst (%)", "cgst %", "sgst %",
#         "gst %"
#     ]
# }

# PADDLE_ALIASES = {
#     "name": [
#         "product", "product name", "product description",
#         "description", "description of goods",
#         "description of goods/services",
#         "item", "item name", "item description",
#         "sku description",
#         "sr. product description",
#         "product & packing",
#         "product name & packing",
#         "item name & packing",
#         "item name | old mrp",
#         "product description | old mrp",
#         "particulars",
#         "products"
#     ],

#     "batchNo": [
#         "batch", "batch no", "batch no.", "batchno",
#         "batchno.", "batch.no", "batch #", "batch#",
#         "mfg batchno", "free batch no.", "batch#", "ch. no."
#     ],

#     "hsnCode": [
#         "hsn", "hsn code", "hsncode",
#         "hsn/sac", "hsn / sac", "hsn/sac code",
#         "hsn code/sac", "hsh", "hisn", "hsv", "shn"
#     ],

#     "qty": [
#         "qty", "qty.", "quantity", "q'ty",
#         "qty + free", "qty free",
#         "pcs", "pcs.", "units", "uom",
#         "qty.+free", "qty free batch no."
#     ],

#     "freeQty": [
#         "free", "fr.", "fq",
#         "free qty", "free/sch",
#         "free /sch", "free / sch",
#         "free / sch ant", "fqty/sch%"
#     ],

#     "packaging": [
#         "pack", "packing", "pkg", "pkng",
#         "packing 1*10tab", "pkg", "uom",
#         "box", "pcs", "units"
#     ],

#     "mrp": [
#         "mrp", "m.r.p", "m.r.p.",
#         "old mrp", "oldmrp", "old mrp",
#         "o mrp", "o.mrp", "o.m.r.p",
#         "new mrp", "revised mrp",
#         "n.mrp", "t.mrp", "mrp ref"
#     ],

#     "rate": [
#         "rate", "ptr", "p.t.r.", "s.rate",
#         "unit price", "net rate",
#         "price", "rate/ doz",
#         "b.rate", "s/rate"
#     ],

#     "discountPercent": [
#         "disc", "disc%", "disc %",
#         "dis%", "di.%", "d%",
#         "dis.", "discount", "discount %",
#         "sch/disc", "dis amt", "dis. amt"
#     ],

#     "schemePercent": [
#         "sch", "sch %", "sch dis",
#         "scheme", "scheme %",
#         "coupon", "coupon disc",
#         "sch amt", "sch/discount"
#     ],

#     "itemAmount": [
#         "amount", "amt", "amt.",
#         "amour",
#         "net amount", "net amt", "netamt.",
#         "net value", "value",
#         "taxable", "taxable amt.",
#         "taxable amount", "gross value"
#     ],

#     "gst": [
#         "gst", "gst%", "gst %",
#         "tax%", "tax rate(%)",
#         "igst", "igst (%)",
#         "cgst", "cgst %",
#         "sgst", "sgst %",
#         "gst amt", "gst amount",
#         "sgst/utgst", "cgst/sgst"
#     ]
# }

# PADDLE_ALIASES = {
#     "name": [
#         "product", "product name", "product description",
#         "description", "description of goods",
#         "description of goods/services",
#         "item", "item name", "item description",
#         "sku description",
#         "sr. product description",
#         "product & packing",
#         "product name & packing",
#         "item name & packing",
#         "item name | old mrp",
#         "product description | old mrp",
#         "particulars",
#         "products",
#         "hsn code & item name"          # NEW
#     ],

#     "batchNo": [
#         "batch", "batch no", "batch no.", "batchno",
#         "batchno.", "batch.no", "batch #", "batch#",
#         "mfg batchno", "free batch no.",
#         "ch. no."
#     ],

#     "hsnCode": [
#         "hsn", "hsn code", "hsncode",
#         "hsn/sac", "hsn / sac", "hsn/sac code",
#         "hsn code/sac", "hsh", "hisn", "hsv", "shn"
#     ],

#     "qty": [
#         "qty", "qty.", "quantity",
#         "q'ty", "units", "uom",
#         "qty + free", "qty free",
#         "pcs", "pcs."
#     ],

#     "freeQty": [
#         "free", "fr.", "fq",
#         "free qty", "free/sch",
#         "free /sch", "free / sch",
#         "free / sch ant"
#     ],

#     "packaging": [
#         "pack", "packing", "pkg", "pkng",
#         "box", "units"
#     ],

#     "mrp": [
#         "mrp", "m.r.p", "m.r.p.",
#         "old mrp", "oldmrp",
#         "o mrp", "o.mrp",
#         "new mrp", "revised mrp",
#         "n.mrp", "t.mrp",
#         "old", "npaa mrp"               # NEW
#     ],

#     "rate": [
#         "rate", "ptr", "p.t.r.",
#         "s.rate", "unit price",
#         "price", "rate/ doz",
#         "net rate"
#     ],

#     "discountPercent": [
#         "disc", "disc%", "disc %",
#         "dis%", "di.%", "d%",
#         "dis.", "discount",
#         "discount %"
#     ],

#     "schemePercent": [
#         "sch", "sch %", "sch dis",
#         "scheme", "scheme %",
#         "coupon", "coupon disc"
#     ],

#     "itemAmount": [
#         "amount", "amt", "amt.",
#         "amour",
#         "net amount", "net amt",
#         "netamt.", "net value",
#         "value",
#         "taxable", "taxable amt.",
#         "taxable amount",
#         "net taxable amt"              # NEW
#     ],

#     "gst": [
#         "gst", "gst%", "gst %",
#         "tax%", "tax rate(%)",
#         "igst", "igst (%)",
#         "cgst", "cgst %",
#         "sgst", "sgst %",
#         "gst amt", "gst amount",
#         "sgst/utgst",
#         "tax",                            # NEW
#         "tax type"                        # NEW
#     ]
# }

PADDLE_ALIASES = {
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





# =============================
# NORMALIZE PADDLE ITEM (ROBUST)
# =============================

def normalize_paddle_item(p):
    name_raw = find_by_alias(p, PADDLE_ALIASES["name"]) or ""
    name = re.sub(r"^\d+\s*", "", str(name_raw))

    return {
        "name": clean_text(name),
        "batchNo": clean_text(find_by_alias(p, PADDLE_ALIASES["batchNo"])),
        "hsnCode": clean_text(find_by_alias(p, PADDLE_ALIASES["hsnCode"])),
        "qty": parse_float(find_by_alias(p, PADDLE_ALIASES["qty"])),
        "freeQty": parse_float(find_by_alias(p, PADDLE_ALIASES["freeQty"])),
        "packaging": clean_text(find_by_alias(p, PADDLE_ALIASES["packaging"])),
        "mrp": parse_float(find_by_alias(p, PADDLE_ALIASES["mrp"])),
        "rate": parse_float(find_by_alias(p, PADDLE_ALIASES["rate"])),
        "discountPercent": parse_float(find_by_alias(p, PADDLE_ALIASES["discountPercent"])),
        "schemePercent": parse_float(find_by_alias(p, PADDLE_ALIASES["schemePercent"])),
        "itemAmount": parse_float(find_by_alias(p, PADDLE_ALIASES["itemAmount"])),
        "gst": parse_float(find_by_alias(p, PADDLE_ALIASES["gst"])) or 5
    }


# =============================
# SIMILARITY
# =============================

def numeric_similarity(a, b):
    if a is None or b is None:
        return 0
    if a == 0 and b == 0:
        return 100
    return max(0, 100 - abs(a - b) / max(abs(a), abs(b)) * 100)


def field_similarity(a, b):
    if isinstance(a, str):
        return fuzz.token_sort_ratio(a, b)
    return numeric_similarity(a, b)


# =============================
# MATCH ITEMS
# =============================

def match_items(textract_items, paddle_items):
    matches = []

    for t in textract_items:
        best, best_score = None, 0

        for p in paddle_items:
            name_score = fuzz.token_sort_ratio(
                clean_text(t.get("name")),
                p.get("name")
            )

            batch_score = fuzz.ratio(
                clean_text(t.get("batchNo")),
                p.get("batchNo")
            ) if t.get("batchNo") and p.get("batchNo") else 0

            score = 0.8 * name_score + 0.2 * batch_score

            if score > best_score:
                best, best_score = p, score

        threshold = MATCH_THRESHOLD_STRICT if t.get("batchNo") else MATCH_THRESHOLD_RELAXED

        if best_score >= threshold:
            matches.append((t, best))

    return matches


# =============================
# COMPARE ONE INVOICE
# =============================

def compare_invoice(textract_json, paddle_json):
    if "final_output" not in textract_json or "items" not in textract_json["final_output"]:
        raise ValueError("invalid textract structure")

    if "items" not in paddle_json or not paddle_json["items"]:
        raise ValueError("invalid paddle structure")

    textract_items = textract_json["final_output"]["items"]
    paddle_items = [normalize_paddle_item(p) for p in paddle_json["items"]]

    matches = match_items(textract_items, paddle_items)
    if not matches:
        return [], 0.0

    fields = [
        "name", "batchNo", "hsnCode", "qty",
        "mrp", "rate", "discountPercent",
        "schemePercent", "itemAmount", "gst"
    ]

    rows, item_avgs = [], []

    for idx, (t, p) in enumerate(matches, 1):
        scores = []

        for f in fields:
            s = field_similarity(clean_text(t.get(f)), clean_text(p.get(f)))
            scores.append(s)

            rows.append({
                "item_index": idx,
                "field": f,
                "textract_value": t.get(f),
                "paddle_value": p.get(f),
                "field_similarity": round(s, 2)
            })

        avg = round(sum(scores) / len(scores), 2)
        item_avgs.append(avg)

        rows.append({
            "item_index": idx,
            "field": "ITEM_AVERAGE",
            "textract_value": "",
            "paddle_value": "",
            "field_similarity": "",
            "item_avg_similarity": avg
        })

    return rows, round(sum(item_avgs) / len(item_avgs), 2)


# =============================
# MAIN
# =============================

# def main():
#     bad_invoices = []
#     good_invoices = 0

#     with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
#         writer = csv.writer(f)
#         writer.writerow([
#             "file_name", "item_index", "field",
#             "textract_value", "paddle_value",
#             "field_similarity", "item_avg_similarity",
#             "invoice_avg_similarity"
#         ])

#         for file in sorted(os.listdir(PADDLE_DIR)):
#             try:
#                 with open(os.path.join(PADDLE_DIR, file)) as pf:
#                     paddle_json = json.load(pf)
#                 with open(os.path.join(TEXTRACT_DIR, file)) as tf:
#                     textract_json = json.load(tf)

#                 rows, inv_avg = compare_invoice(textract_json, paddle_json)

#                 for r in rows:
#                     writer.writerow([
#                         file,
#                         r.get("item_index"),
#                         r.get("field"),
#                         r.get("textract_value"),
#                         r.get("paddle_value"),
#                         r.get("field_similarity"),
#                         r.get("item_avg_similarity", ""),
#                         inv_avg
#                     ])
#                 if(inv_avg == 0.0):
#                     print(f"✅ Processed {file} → similarity {inv_avg}%")
#                     bad_invoices.append(file)
#                 if inv_avg > 0:
#                     good_invoices += 1

#             except Exception as e:
#                 print(f"⚠️ Skipped {file} → {e}")

#     print("\n🎉 DONE →", OUTPUT_CSV)
#     print(f"good invoices: {good_invoices}")
#     with open("/home/ubuntu22/MyProjects/paddle_ocr_vl/bad_invoices.json", "w", encoding="utf-8") as f:
#         json.dump(bad_invoices, f, indent=2, ensure_ascii=False)



def main():
    bad_invoices = []
    good_invoices = 0

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as detail_f, \
         open(OUTPUT_SUMMARY_CSV, "w", newline="", encoding="utf-8") as summary_f:

        detail_writer = csv.writer(detail_f)
        summary_writer = csv.writer(summary_f)

        # Detailed CSV header
        detail_writer.writerow([
            "file_name", "item_index", "field",
            "textract_value", "paddle_value",
            "field_similarity", "item_avg_similarity",
            "invoice_avg_similarity"
        ])

        # Summary CSV header
        summary_writer.writerow([
            "file_name", "invoice_avg_similarity"
        ])

        for file in sorted(os.listdir(PADDLE_DIR)):
            try:
                with open(os.path.join(PADDLE_DIR, file)) as pf:
                    paddle_json = json.load(pf)

                with open(os.path.join(TEXTRACT_DIR, file)) as tf:
                    textract_json = json.load(tf)

                rows, inv_avg = compare_invoice(textract_json, paddle_json)

                # -----------------------------
                # WRITE SUMMARY CSV (ALWAYS)
                # -----------------------------
                summary_writer.writerow([file, inv_avg])

                # -----------------------------
                # WRITE DETAILED CSV
                # -----------------------------
                if rows:
                    for r in rows:
                        detail_writer.writerow([
                            file,
                            r.get("item_index"),
                            r.get("field"),
                            r.get("textract_value"),
                            r.get("paddle_value"),
                            r.get("field_similarity"),
                            r.get("item_avg_similarity", ""),
                            inv_avg
                        ])
                else:
                    # Explicit row for zero-match invoices
                    detail_writer.writerow([
                        file,
                        "",
                        "NO_MATCH_FOUND",
                        "",
                        "",
                        "",
                        "",
                        inv_avg
                    ])

                if inv_avg == 0.0:
                    bad_invoices.append(file)
                else:
                    good_invoices += 1

                print(f"✅ Processed {file} → similarity {inv_avg}%")

            except Exception as e:
                print(f"⚠️ Skipped {file} → {e}")

    print("\n🎉 DONE")
    print("Detailed CSV →", OUTPUT_CSV)
    print("Summary CSV  →", OUTPUT_SUMMARY_CSV)
    print(f"Good invoices: {good_invoices}")

    with open("/home/ubuntu22/MyProjects/paddle_ocr_vl/bad_invoices.json", "w", encoding="utf-8") as f:
        json.dump(bad_invoices, f, indent=2, ensure_ascii=False)



if __name__ == "__main__":
    main()
