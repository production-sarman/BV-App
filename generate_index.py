import os
import json
import re
import time
from collections import defaultdict

# ==========================
# PATHS
# ==========================
vsm_root     = os.path.dirname(os.path.abspath(__file__))
master_docs  = os.path.join(vsm_root, "Master_Documents")
vsm_data     = os.path.join(vsm_root, "vsm_data")
central_json = os.path.join(vsm_root, "central_index.json")

# ==========================
# SUPPORTED EXTENSIONS
# ==========================
SUPPORTED_EXTENSIONS = (
    ".docx", ".pdf", ".txt", ".xls", ".xlsx",
    ".sldprt", ".sldasm", ".step", ".stp"
)

FILE_PRIORITY = [
    ".docx", ".pdf", ".txt", ".xlsx",
    ".xls", ".sldprt", ".sldasm", ".step", ".stp"
]

def get_ext(filename: str) -> str:
    return os.path.splitext(filename)[1].lower()

def priority_of(filename: str) -> int:
    ext = get_ext(filename)
    return FILE_PRIORITY.index(ext) if ext in FILE_PRIORITY else len(FILE_PRIORITY)

# ==========================
# UNIT NORMALIZATION (kA -> A)
# ==========================
def normalize_units(text: str) -> str:
    def ka_repl(match):
        value = float(match.group(1))
        return str(int(value * 1000)) + "A"
    text = re.sub(r"(\d+(?:\.\d+)?)\s*[Kk][Aa]", ka_repl, text)
    text = re.sub(r"(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?[A-Za-z]*)", r"\1~\2", text)
    text = re.sub(r"[\s\-_]+", "_", text.strip())
    text = text.replace("~", "-")
    return text.upper()

# ==========================
# STAGE EXTRACTION FROM FILENAME
# ==========================
def extract_stage_from_filename(filename: str):
    filename_no_ext = os.path.splitext(filename)[0]
    parts = filename_no_ext.split("_")

    last_match = re.match(r"^(\d+\.\d+)$", parts[-1])
    if last_match:
        return last_match

    first_match = re.match(r"^(\d+\.\d+)$", parts[0])
    if first_match:
        return first_match

    return re.search(r"(\d+\.\d+)", filename)

# ==========================
# TIME FORMATTER
# ==========================
def format_time(seconds: float) -> str:
    if seconds < 0:
        return "calculating..."
    if seconds < 60:
        return f"{int(seconds)}s"
    return f"{int(seconds // 60)}m {int(seconds % 60)}s"

# ==========================
# PROGRESS DISPLAY
# ==========================
def print_progress(current: int, total: int, elapsed: float, label: str = ""):
    if total == 0:
        return
    percent = current / total * 100
    filled  = int(percent / 2)
    bar     = "█" * filled + "░" * (50 - filled)
    eta     = (elapsed / current) * (total - current) if current > 0 else 0
    print(
        f"\r  [{bar}] {percent:5.1f}%  "
        f"{current}/{total} files  "
        f"Elapsed: {format_time(elapsed)}  ETA: {format_time(eta)}  "
        f"{label:<40}",
        end="", flush=True
    )

# ==========================
# VALIDATION
# ==========================
if not os.path.exists(master_docs):
    raise FileNotFoundError(f"Master Documents folder not found: {master_docs}")
if not os.path.exists(vsm_data):
    raise FileNotFoundError(f"VSM Data folder not found: {vsm_data}")

print("[INFO] Folder Validation Successful")
print(f"[INFO] VSM Root    : {vsm_root}")
print(f"[INFO] Master Docs : {master_docs}")
print()
print("=" * 70)
print("  IMPORTANT: Paths in central_index.json will be stored as RELATIVE.")
print("  Old location: C:\\Users\\SarmanEngineering\\Documents\\vector_database")
print("  New location: D:\\vector_database")
print("  Future moves: just re-run this script — nothing else to change.")
print("=" * 70)
print()

# ==========================
# PRE-SCAN
# ==========================
print("[INFO] Pre-scanning folders...")
all_file_list = []

for root, dirs, files in os.walk(master_docs):
    if root == master_docs:
        continue
    rel = os.path.relpath(root, master_docs)
    if len(rel.split(os.sep)) < 2:
        continue
    for file in files:
        all_file_list.append((root, file))

total_files = len(all_file_list)
print(f"[INFO] Found {total_files} total files to process\n")

# ==========================
# INDEXING
# ─────────────────────────
# THE KEY FIX:
#   Before (broken after move):
#     "D:\\vector_database\\VSM\\Master_Documents\\EE\\...\\file.docx"
#
#   After (works on any drive):
#     "Master_Documents/EE/.../file.docx"
#
#   The server resolves:  vsm_root + relative_path = correct absolute path
#   So moving from D:\ to E:\ just needs one re-run of this script.
# ==========================
central_index  = defaultdict(lambda: {"variants": {}})
variant_count  = 0
stage_count    = 0
skipped_count  = 0
processed      = 0
seen_variants  = set()
indexing_start = time.time()

print("=" * 80)
print("  INDEXING STARTED")
print("=" * 80)

for root, file in all_file_list:
    processed += 1
    elapsed    = time.time() - indexing_start

    path_parts  = os.path.relpath(root, master_docs).split(os.sep)
    product_type = path_parts[0]
    variant      = path_parts[1]
    variant_key  = normalize_units(variant.upper())

    print_progress(processed, total_files, elapsed, label=f"{variant_key[:35]}")

    variant_uid = f"{product_type}::{variant_key}"
    if variant_uid not in seen_variants:
        seen_variants.add(variant_uid)
        central_index[product_type]["variants"].setdefault(variant_key, {})
        variant_count += 1

    if not file.lower().endswith(SUPPORTED_EXTENSIONS):
        skipped_count += 1
        continue

    stage_match = extract_stage_from_filename(file)
    if not stage_match:
        skipped_count += 1
        continue

    stage = stage_match.group(1)

    # Store path RELATIVE to vsm_root (forward slashes for safety)
    full_path     = os.path.join(root, file)
    relative_path = os.path.relpath(full_path, vsm_root).replace("\\", "/")

    stage_entry = central_index[product_type]["variants"][variant_key].setdefault(
        stage, {"documents": []}
    )

    if relative_path in stage_entry["documents"]:
        continue

    stage_entry["documents"].append(relative_path)
    stage_entry["documents"].sort(key=lambda p: priority_of(os.path.basename(p)))

    if len(stage_entry["documents"]) == 1:
        stage_count += 1

print()

# ==========================
# SAVE
# ==========================
print("\n[INFO] Saving central_index.json...")
save_start = time.time()

with open(central_json, "w", encoding="utf-8") as f:
    json.dump(dict(central_index), f, indent=4, ensure_ascii=False)

save_time  = time.time() - save_start
total_time = time.time() - indexing_start

print("\n" + "=" * 70)
print("  INDEXING COMPLETE")
print("=" * 70)
print(f"  Variants indexed  : {variant_count}")
print(f"  Stages indexed    : {stage_count}")
print(f"  Files skipped     : {skipped_count}")
print(f"  Files processed   : {processed}")
print(f"  Save time         : {format_time(save_time)}")
print(f"  Total time        : {format_time(total_time)}")
print("=" * 70)
print(f"  [SUCCESS] central_index.json saved at: {central_json}")
print()
print("  Paths are RELATIVE to vsm_root.")
print("  Safe to move drives anytime — just re-run this script.")
print("=" * 70)