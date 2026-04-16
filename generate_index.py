import os
import json
import re
import time
from collections import defaultdict

# ==========================
# PATHS
# ==========================
vsm_root = os.path.dirname(os.path.abspath(__file__))
master_docs = os.path.join(vsm_root, "Master_Documents")
vsm_data = os.path.join(vsm_root, "vsm_data")
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
# UNIT NORMALIZATION (kA → A)
# ==========================
def normalize_units(text: str) -> str:
    """
    Normalise a variant name/query into a canonical index key.

    Rules applied in order:
    1. kA/KA -> plain amperes         10kA      -> 10000A
    2. Protect range hyphens          0.5-125A  -> 0.5~125A  (temp placeholder)
    3. Collapse word separators       Variant-1 -> VARIANT_1
    4. Restore range hyphens          0.5~125A  -> 0.5-125A
    5. Uppercase

    Examples:
        TR_FLU_01_6V_7000A_Variant-1  ->  TR_FLU_01_6V_7000A_VARIANT_1
        TR_3PH_CS_01_9V_10kA          ->  TR_3PH_CS_01_9V_10000A
        TR_CYLT_04_0.5-125A           ->  TR_CYLT_04_0.5-125A
    """
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
    """
    Extract stage number from filename.
    Stage is always the LAST or FIRST part of filename (before extension).

    Examples:
        TR_CYLT_04_0.5-125A_1.3.docx   -> stage = "1.3"  (last part)
        1.3_TR_CYLT_04_0.5-125A.docx   -> stage = "1.3"  (first part)
        TR_3PH_CS_01_9V_10000A_2.1.pdf -> stage = "2.1"  (last part)
    """
    filename_no_ext = os.path.splitext(filename)[0]
    parts = filename_no_ext.split("_")

    # Primary: check last part (e.g. "1.3")
    last_part = parts[-1]
    last_match = re.match(r"^(\d+\.\d+)$", last_part)
    if last_match:
        return last_match

    # Secondary: check first part (e.g. "1.3")
    first_part = parts[0]
    first_match = re.match(r"^(\d+\.\d+)$", first_part)
    if first_match:
        return first_match

    # Fallback: scan whole filename — keeps all other products working safely
    return re.search(r"(\d+\.\d+)", filename)


# ==========================
# TIME FORMATTER
# ==========================
def format_time(seconds: float) -> str:
    """Convert seconds to human readable string."""
    if seconds < 0:
        return "calculating..."
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}m {secs}s"


# ==========================
# PROGRESS DISPLAY
# ==========================
def print_progress(current: int, total: int, elapsed: float, label: str = ""):
    """Print a realistic progress bar with time remaining."""
    if total == 0:
        return

    percent = current / total * 100
    filled  = int(percent / 2)       # 50 char wide bar
    bar     = "█" * filled + "░" * (50 - filled)

    if current > 0:
        avg_time_per_item = elapsed / current
        remaining_items   = total - current
        eta               = avg_time_per_item * remaining_items
    else:
        eta = 0

    elapsed_str   = format_time(elapsed)
    eta_str       = format_time(eta)

    print(
        f"\r  [{bar}] {percent:5.1f}%  "
        f"{current}/{total} files  "
        f"Elapsed: {elapsed_str}  "
        f"ETA: {eta_str}  "
        f"{label:<40}",
        end="",
        flush=True
    )


# ==========================
# VALIDATION
# ==========================
if not os.path.exists(master_docs):
    raise FileNotFoundError(f"Master Documents folder not found: {master_docs}")

if not os.path.exists(vsm_data):
    raise FileNotFoundError(f"VSM Data Documents Folder not found: {vsm_data}")

print("[INFO] Folder Validation Successful")

# ==========================
# PRE-SCAN: COUNT TOTAL FILES
# Needed for accurate progress %
# ==========================
print("[INFO] Pre-scanning folders to count files...")
total_files = 0
all_file_list = []  # store (root, file) tuples for main loop

for root, dirs, files in os.walk(master_docs):
    if root == master_docs:
        continue
    relative_path = os.path.relpath(root, master_docs)
    path_parts    = relative_path.split(os.sep)
    if len(path_parts) < 2:
        continue
    for file in files:
        all_file_list.append((root, file))
        total_files += 1

print(f"[INFO] Found {total_files} total files to process\n")

# ==========================
# INDEXING
# ==========================
central_index = defaultdict(lambda: {"variants": {}})

variant_count  = 0
stage_count    = 0
skipped_count  = 0
processed      = 0

indexing_start = time.time()

print("=" * 80)
print("  INDEXING STARTED")
print("=" * 80) 

# Track which variants already counted
seen_variants = set()

for root, file in all_file_list:

    processed += 1
    elapsed    = time.time() - indexing_start

    relative_path = os.path.relpath(root, master_docs)
    path_parts    = relative_path.split(os.sep)

    product_type = path_parts[0]
    variant      = path_parts[1]
    variant_key  = normalize_units(variant.upper())

    # Show progress bar
    print_progress(
        processed, total_files, elapsed,
        label=f"{variant_key[:35]}"
    )

    # Track new variants
    variant_uid = f"{product_type}::{variant_key}"
    if variant_uid not in seen_variants:
        seen_variants.add(variant_uid)
        central_index[product_type]["variants"].setdefault(variant_key, {})
        variant_count += 1

    # Check extension
    if not file.lower().endswith(SUPPORTED_EXTENSIONS):
        skipped_count += 1
        continue

    # ── UPDATED STAGE EXTRACTION ──────────────────
    stage_match = extract_stage_from_filename(file)
    # ─────────────────────────────────────────────

    if not stage_match:
        skipped_count += 1
        continue

    stage         = stage_match.group(1)
    full_doc_path = os.path.abspath(os.path.join(root, file))

    stage_entry = central_index[product_type]["variants"][variant_key].setdefault(
        stage, {"documents": []}
    )

    if full_doc_path in stage_entry["documents"]:
        continue

    stage_entry["documents"].append(full_doc_path)
    stage_entry["documents"].sort(
        key=lambda p: priority_of(os.path.basename(p))
    )

    if len(stage_entry["documents"]) == 1:
        stage_count += 1

# Move to next line after progress bar
print()

# ==========================
# SAVE
# ==========================
print("\n[INFO] Saving central_index.json...")
save_start = time.time()

with open(central_json, "w", encoding="utf-8") as f:
    json.dump(dict(central_index), f, indent=4, ensure_ascii=False)

save_time = time.time() - save_start
total_time = time.time() - indexing_start

# ==========================
# SUMMARY
# ==========================
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
print(f"  [SUCCESS] central_index.json saved at:")
print(f"  {central_json}")
print("=" * 70)