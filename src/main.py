import os
import time
import csv
import shutil
import subprocess
import psutil
import pynvml

# =============================
# CONFIG
# =============================

MODEL_NAME = "thor-glm-ocr:q8_0" #change

input_folder = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/cropped_oriented_preprocessed_images"
completed_input_folder = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/completed_cropped_oriented_preprocessed_images"
output_root = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/raw_glm_outputs"

csv_path = "/home/thor01/Documents/ab_workspace/ocr/glm_ocr/glm_ocr_test/glm_ocr_benchmark_progress.csv"

valid_exts = {".jpg", ".jpeg", ".png"}

os.makedirs(completed_input_folder, exist_ok=True)
os.makedirs(output_root, exist_ok=True)

# =============================
# GPU Monitoring
# =============================

pynvml.nvmlInit()
handle = pynvml.nvmlDeviceGetHandleByIndex(0)

def get_gpu_stats():
    mem = pynvml.nvmlDeviceGetMemoryInfo(handle)
    util = pynvml.nvmlDeviceGetUtilizationRates(handle)
    return mem.used / 1024**2, util.gpu  # MB, %

# =============================
# CSV Init (append-safe)
# =============================

csv_exists = os.path.exists(csv_path)

with open(csv_path, mode="a", newline="") as f:
    writer = csv.writer(f)
    if not csv_exists:
        writer.writerow([
            "Image Name",
            "Processing Time (sec)",
            "GPU VRAM Before (MB)",
            "GPU VRAM After (MB)",
            "GPU VRAM Used Delta (MB)",
            "GPU Load (%)",
            "CPU Load (%)",
            "RAM Usage (%)",
        ])

# =============================
# PROCESS LOOP
# =============================

for filename in sorted(os.listdir(input_folder)):

    ext = os.path.splitext(filename)[1].lower()
    if ext not in valid_exts:
        continue

    img_path = os.path.join(input_folder, filename)
    completed_path = os.path.join(completed_input_folder, filename)

    # Skip if already done
    if os.path.exists(completed_path):
        continue

    try:
        print(f"\n🔄 Processing: {filename}")

        base_name = os.path.splitext(filename)[0]
        output_dir = os.path.join(output_root, base_name)
        os.makedirs(output_dir, exist_ok=True)

        output_md = os.path.join(output_dir, f"{base_name}.md")

        # =============================
        # SYSTEM STATS BEFORE
        # =============================
        gpu_before, _ = get_gpu_stats()
        cpu_before = psutil.cpu_percent(interval=None)
        ram_before = psutil.virtual_memory().percent

        # =============================
        # RUN GLM OCR
        # =============================
        prompt = f"Table Recognition: ./{img_path}"

        start_time = time.time()

        with open(output_md, "w") as outfile:
            subprocess.run(
                ["ollama", "run", MODEL_NAME, prompt],
                stdout=outfile,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )

        end_time = time.time()
        process_time = end_time - start_time

        # =============================
        # SYSTEM STATS AFTER
        # =============================
        gpu_after, gpu_load = get_gpu_stats()
        cpu_after = psutil.cpu_percent(interval=None)
        ram_after = psutil.virtual_memory().percent

        gpu_delta = gpu_after - gpu_before
        avg_cpu = (cpu_before + cpu_after) / 2
        avg_ram = (ram_before + ram_after) / 2

        # =============================
        # WRITE CSV (append immediately)
        # =============================
        with open(csv_path, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                filename,
                round(process_time, 4),
                round(gpu_before, 2),
                round(gpu_after, 2),
                round(gpu_delta, 2),
                gpu_load,
                round(avg_cpu, 2),
                round(avg_ram, 2),
            ])

        # =============================
        # MOVE IMAGE AFTER SUCCESS
        # =============================
        shutil.move(img_path, completed_path)

        print(f"✅ Done: {filename} ({process_time:.2f}s)")

    except subprocess.CalledProcessError as e:
        print(f"❌ GLM OCR failed for {filename}")
        print(e.stderr)

    except Exception as e:
        print(f"❌ Error processing {filename}: {e}")

print("\n🎉 GLM OCR Batch Finished")
print(f"📊 CSV: {csv_path}")
