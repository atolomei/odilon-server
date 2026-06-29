#!/usr/bin/env bash
# =============================================================================
#  check-raid6.sh  –  Odilon RAID 6 drive consistency checker
# =============================================================================
#
#  Two modes:
#
#  1. Auto-detect  (original behaviour)
#     ./check-raid6.sh [base_dir]
#     Scans base_dir for subdirs that contain .odilon.sys.
#
#  2. Explicit drive list  (multi-volume installations)
#     ./check-raid6.sh "drive0,drive1,drive2"
#     ./check-raid6.sh "/data/vol1/d0,/data/vol1/d1,/data/vol1/d2"
#     Comma-separated paths; absolute or relative.
#
#  Compatible with bash 3.2 (macOS default). No declare -A used.
# =============================================================================

if [[ -t 1 ]]; then
    RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
    BOLD='\033[1m';   RESET='\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; BOLD=''; RESET=''
fi

count_files() {
    local dir="$1"
    [[ -d "$dir" ]] || { echo 0; return; }
    find "$dir" -maxdepth 1 -type f 2>/dev/null | wc -l | tr -d ' \t'
}
count_subdirs() {
    local dir="$1"
    [[ -d "$dir" ]] || { echo 0; return; }
    find "$dir" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | wc -l | tr -d ' \t'
}
du_kib() {
    local dir="$1"
    [[ -d "$dir" ]] || { echo 0; return; }
    du -sk "$dir" 2>/dev/null | awk '{print $1}'
}

# ── Mode selection ─────────────────────────────────────────────────────────────
# $1 contains a comma → explicit drive-list mode; otherwise auto-detect.
DRIVES=()

if [[ "${1:-}" == *","* ]]; then
    # Explicit mode: split on comma via temp file (bash 3.2 safe, no declare -A)
    _TMP=$(mktemp)
    echo "${1}" | tr ',' '\n' > "$_TMP"
    while IFS= read -r _e; do
        _e="${_e#"${_e%%[![:space:]]*}"}"   # ltrim
        _e="${_e%"${_e##*[![:space:]]}"}"   # rtrim
        [[ -n "$_e" ]] && DRIVES+=("$_e")
    done < "$_TMP"
    rm -f "$_TMP"
    MODE_DESC="Explicit drive list"
else
    # Auto-detect mode
    BASE_DIR="${1:-.}"
    _TMP=$(mktemp)
    for _d in "$BASE_DIR"/*/; do
        _d="${_d%/}"
        [[ -d "$_d/.odilon.sys" ]] && echo "$_d"
    done | sort > "$_TMP"
    while IFS= read -r _d; do
        [[ -n "$_d" ]] && DRIVES+=("$_d")
    done < "$_TMP"
    rm -f "$_TMP"
    MODE_DESC="Auto-detected from: ${BASE_DIR}"
fi

NUM_DRIVES=${#DRIVES[@]}

if (( NUM_DRIVES < 2 )); then
    echo -e "${RED}ERROR${RESET}: fewer than 2 drives found/specified." >&2
    echo "  Auto-detect : ./check-raid6.sh /path/to/data/dir" >&2
    echo "  Drive list  : ./check-raid6.sh \"drive0,drive1,drive2\"" >&2
    exit 1
fi

echo -e "${BOLD}Odilon RAID 6 — consistency check${RESET}"
echo "Mode     : $MODE_DESC"
echo "Drives   : $NUM_DRIVES"
for _d in "${DRIVES[@]}"; do
    printf "  %-20s  %s\n" "$(basename "$_d")" "$_d"
done
echo

# ── Collect unique bucket IDs across all drives (temp file, no declare -A) ────
_TMP=$(mktemp)
for _drive in "${DRIVES[@]}"; do
    for _bdir in "$_drive"/*/; do
        _bid=$(basename "${_bdir%/}")
        [[ "$_bid" != ".odilon.sys" ]] && [[ -d "$_bdir" ]] && echo "$_bid"
    done
done | sort -nu > "$_TMP"

BUCKETS=()
while IFS= read -r _bid; do
    [[ -n "$_bid" ]] && BUCKETS+=("$_bid")
done < "$_TMP"
rm -f "$_TMP"

if (( ${#BUCKETS[@]} == 0 )); then
    echo "No bucket data directories found."; exit 0
fi
echo "Buckets  : ${#BUCKETS[@]}  (ids: ${BUCKETS[*]})"
echo

TOTAL_ISSUES=0

# ── Per-bucket comparison ──────────────────────────────────────────────────────
for _bucket in "${BUCKETS[@]}"; do

    echo -e "${BOLD}─── Bucket ${_bucket} ─────────────────────────────────────────────────────${RESET}"

    HEAD_FILES=(); VER_FILES=(); META_DIRS=(); DATA_KIB=()

    for _drive in "${DRIVES[@]}"; do
        HEAD_FILES+=( "$(count_files   "$_drive/$_bucket")" )
        VER_FILES+=(  "$(count_files   "$_drive/$_bucket/version")" )
        META_DIRS+=(  "$(count_subdirs "$_drive/.odilon.sys/buckets/$_bucket")" )
        DATA_KIB+=(   "$(du_kib        "$_drive/$_bucket")" )
    done

    printf "  %-24s  %11s  %11s  %11s  %10s\n" \
        "Drive" "HeadShards" "VerShards" "MetaDirs" "DataKiB"
    printf "  %-24s  %11s  %11s  %11s  %10s\n" \
        "────────────────────" "──────────" "─────────" "────────" "───────"
    for _i in "${!DRIVES[@]}"; do
        printf "  %-24s  %11s  %11s  %11s  %10s\n" \
            "$(basename "${DRIVES[$_i]}")" \
            "${HEAD_FILES[$_i]}" "${VER_FILES[$_i]}" \
            "${META_DIRS[$_i]}"  "${DATA_KIB[$_i]}"
    done
    echo

    _bucket_issues=0
    _ref=$(basename "${DRIVES[0]}")

    for _i in "${!DRIVES[@]}"; do
        (( _i == 0 )) && continue
        _d=$(basename "${DRIVES[$_i]}")
        if [[ "${HEAD_FILES[$_i]}" != "${HEAD_FILES[0]}" ]]; then
            echo -e "  ${RED}FAIL${RESET}  HeadShards : ${_ref}=${HEAD_FILES[0]}  ${_d}=${HEAD_FILES[$_i]}"
            (( _bucket_issues++ )) || true
        fi
        if [[ "${VER_FILES[$_i]}" != "${VER_FILES[0]}" ]]; then
            echo -e "  ${RED}FAIL${RESET}  VerShards  : ${_ref}=${VER_FILES[0]}  ${_d}=${VER_FILES[$_i]}"
            (( _bucket_issues++ )) || true
        fi
        if [[ "${META_DIRS[$_i]}" != "${META_DIRS[0]}" ]]; then
            echo -e "  ${RED}FAIL${RESET}  MetaDirs   : ${_ref}=${META_DIRS[0]}  ${_d}=${META_DIRS[$_i]}"
            (( _bucket_issues++ )) || true
        fi
        _r="${DATA_KIB[0]}"; _t="${DATA_KIB[$_i]}"
        _diff=$(( _r > _t ? _r - _t : _t - _r ))
        if (( _diff > 10 )); then
            echo -e "  ${RED}FAIL${RESET}  DataKiB    : ${_ref}=${_r}  ${_d}=${_t}  (diff=${_diff} KiB)"
            (( _bucket_issues++ )) || true
        fi
    done

    if (( _bucket_issues == 0 )); then
        echo -e "  ${GREEN}OK${RESET}  Bucket ${_bucket} — all ${NUM_DRIVES} drives in sync"
    else
        (( TOTAL_ISSUES += _bucket_issues )) || true
    fi
    echo
done

# ── Metadata bucket-set cross-check ───────────────────────────────────────────
echo -e "${BOLD}─── Metadata bucket-set consistency ─────────────────────────────────────${RESET}"
META_BUCKET_COUNTS=()
for _drive in "${DRIVES[@]}"; do
    META_BUCKET_COUNTS+=( "$(count_subdirs "$_drive/.odilon.sys/buckets")" )
done
printf "  %-24s  %s\n" "Drive" "BucketMetaDirs"
printf "  %-24s  %s\n" "────────────────────" "──────────────"
for _i in "${!DRIVES[@]}"; do
    printf "  %-24s  %s\n" "$(basename "${DRIVES[$_i]}")" "${META_BUCKET_COUNTS[$_i]}"
done
echo

_ref=$(basename "${DRIVES[0]}")
_meta_issues=0
for _i in "${!DRIVES[@]}"; do
    (( _i == 0 )) && continue
    _d=$(basename "${DRIVES[$_i]}")
    if [[ "${META_BUCKET_COUNTS[$_i]}" != "${META_BUCKET_COUNTS[0]}" ]]; then
        echo -e "  ${RED}FAIL${RESET}  BucketMetaDirs : ${_ref}=${META_BUCKET_COUNTS[0]}  ${_d}=${META_BUCKET_COUNTS[$_i]}"
        (( _meta_issues++ )) || true
    fi
done
if (( _meta_issues == 0 )); then
    echo -e "  ${GREEN}OK${RESET}  Bucket metadata dirs consistent across all ${NUM_DRIVES} drives"
else
    (( TOTAL_ISSUES += _meta_issues )) || true
fi
echo

# ── Summary ────────────────────────────────────────────────────────────────────
echo -e "${BOLD}════════════════════════════════════════════════════════════════════════${RESET}"
if (( TOTAL_ISSUES == 0 )); then
    echo -e "${GREEN}${BOLD}ALL DRIVES IN SYNC${RESET}  — ${#BUCKETS[@]} bucket(s), ${NUM_DRIVES} drives"
else
    echo -e "${RED}${BOLD}OUT OF SYNC — ${TOTAL_ISSUES} issue(s) found${RESET}  (${#BUCKETS[@]} bucket(s), ${NUM_DRIVES} drives)"
    echo -e "${YELLOW}Tip: restart Odilon to re-trigger RAIDSixDriveSync, or check the log for encoding errors.${RESET}"
fi
echo
exit $(( TOTAL_ISSUES > 0 ? 1 : 0 ))
#!/usr/bin/env bash
# raid6_check.sh — Check RAID 6 volume directories
# Usage:
#   ./raid6_check.sh                        (interactive prompt)
#   ./raid6_check.sh "/data/d0,/data/d1,/data/d2"

set -euo pipefail

# ── Input ─────────────────────────────────────────────────────────────────────
if [[ $# -ge 1 && -n "$1" ]]; then
    RAW_DIRS="$1"
else
    echo ""
    echo "Enter the data directories for the volume (comma-separated):"
    echo "  Example: /data/d0,/data/d1,/data/d2"
    echo ""
    read -rp "Directories: " RAW_DIRS
fi

# ── Parse comma-separated string into array ────────────────────────────────────
IFS=',' read -ra DIRS_RAW <<< "$RAW_DIRS"
DIRS=()
for d in "${DIRS_RAW[@]}"; do
    # Trim leading/trailing whitespace
    d="${d#"${d%%[![:space:]]*}"}"
    d="${d%"${d##*[![:space:]]}"}"
    [[ -n "$d" ]] && DIRS+=("$d")
done

N=${#DIRS[@]}

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  RAID 6 Volume Check"
echo "  Drives (N=$N)"
echo "══════════════════════════════════════════════════════════"

if [[ $N -eq 0 ]]; then
    echo "ERROR: No directories provided."
    exit 1
fi

# Validate supported N values: 3, 6, 12, 24, 48
case $N in
    3|6|12|24|48) ;;
    *)
        echo "WARNING: N=$N is not a standard RAID 6 volume size (3/6/12/24/48)."
        ;;
esac

# ── Per-drive checks ──────────────────────────────────────────────────────────
ALL_OK=true
declare -A BUCKET_SET

for i in "${!DIRS[@]}"; do
    dir="${DIRS[$i]}"
    echo ""
    echo "  Drive $i → $dir"
    echo "  ──────────────────────────────────────────────────"

    # Existence
    if [[ ! -d "$dir" ]]; then
        echo "  [ERROR] Directory does not exist."
        ALL_OK=false
        continue
    fi

    # Readable
    if [[ ! -r "$dir" ]]; then
        echo "  [ERROR] Directory is not readable."
        ALL_OK=false
        continue
    fi

    # Free space
    AVAIL=$(df -h "$dir" 2>/dev/null | awk 'NR==2{print $4}')
    USED=$(df -h  "$dir" 2>/dev/null | awk 'NR==2{print $3}')
    TOTAL=$(df -h "$dir" 2>/dev/null | awk 'NR==2{print $2}')
    echo "  [OK]    Directory exists and is readable."
    echo "          Disk usage: used=$USED / total=$TOTAL  (available=$AVAIL)"

    # Bucket directories (first-level subdirs = buckets)
    BUCKETS=( $(find "$dir" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | sort) )
    echo "          Buckets found: ${#BUCKETS[@]}"
    for b in "${BUCKETS[@]}"; do
        bname=$(basename "$b")
        BUCKET_SET["$bname"]=1

        # Count shard files (head)
        SHARD_COUNT=$(find "$b" -maxdepth 1 -type f 2>/dev/null | wc -l | tr -d ' ')
        # Count versioned shards
        VER_COUNT=0
        if [[ -d "$b/version" ]]; then
            VER_COUNT=$(find "$b/version" -type f 2>/dev/null | wc -l | tr -d ' ')
        fi
        # Count metadata files
        META_DIR="$b/meta"
        META_COUNT=0
        if [[ -d "$META_DIR" ]]; then
            META_COUNT=$(find "$META_DIR" -type f 2>/dev/null | wc -l | tr -d ' ')
        fi

        echo "          └─ bucket: $bname  | shards(head)=$SHARD_COUNT  versions=$VER_COUNT  metadata=$META_COUNT"
    done
done

# ── Cross-drive consistency ────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════"
echo "  Cross-drive summary"
echo "══════════════════════════════════════════════════════════"
echo "  Unique buckets seen across all drives: ${#BUCKET_SET[@]}"
for bname in "${!BUCKET_SET[@]}"; do
    PRESENT=0
    MISSING_ON=()
    for dir in "${DIRS[@]}"; do
        if [[ -d "$dir/$bname" ]]; then
            (( PRESENT++ )) || true
        else
            MISSING_ON+=("$dir")
        fi
    done
    if [[ $PRESENT -eq $N ]]; then
        echo "  [OK]    Bucket '$bname' present on all $N drives."
    else
        echo "  [WARN]  Bucket '$bname' missing on: ${MISSING_ON[*]}"
        ALL_OK=false
    fi
done

echo ""
if $ALL_OK; then
    echo "  Result: ALL CHECKS PASSED ✓"
else
    echo "  Result: ONE OR MORE WARNINGS/ERRORS DETECTED ✗"
fi
echo "══════════════════════════════════════════════════════════"
echo ""
#!/usr/bin/env bash
# =============================================================================
#  raid6_check.sh  –  Odilon RAID 6 drive consistency checker
# =============================================================================
#  Usage:
#    ./raid6_check.sh [base_dir]
#
#  base_dir : directory containing the driveN folders (default: current dir)
#  Example:
#    ./raid6_check.sh /Volumes/odilon-test/odilon-test-data-raid6
#
#  Checks per bucket (cache / work / tmp excluded — they legitimately differ):
#    1. HeadShards  – files directly in  driveN/<bucketId>/
#    2. VerShards   – files in           driveN/<bucketId>/version/
#    3. MetaDirs    – object subdirs in  driveN/.odilon.sys/buckets/<bucketId>/
#    4. DataKiB     – disk usage of      driveN/<bucketId>/
#
#  Exit 0 = in sync, exit 1 = mismatch found.
# =============================================================================

BASE_DIR="${1:-.}"

# colour codes (auto-disabled when stdout is not a terminal)
if [[ -t 1 ]]; then
    RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
    BOLD='\033[1m';   RESET='\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; BOLD=''; RESET=''
fi

# ── helpers ───────────────────────────────────────────────────────────────────
count_files() {                         # regular files directly in dir (depth 1)
    local dir="$1"
    [[ -d "$dir" ]] || { echo 0; return; }
    find "$dir" -maxdepth 1 -type f 2>/dev/null | wc -l | tr -d ' \t'
}
count_subdirs() {                       # immediate subdirectories (depth 1)
    local dir="$1"
    [[ -d "$dir" ]] || { echo 0; return; }
    find "$dir" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | wc -l | tr -d ' \t'
}
du_kib() {                              # total KiB (macOS + Linux)
    local dir="$1"
    [[ -d "$dir" ]] || { echo 0; return; }
    du -sk "$dir" 2>/dev/null | awk '{print $1}'
}

# ── detect drives (any immediate subdir that contains .odilon.sys) ────────────
# Avoids process substitution (done < <()) for sh/POSIX compatibility.
_TMP_DRIVES=$(mktemp)
for _d in "$BASE_DIR"/*/; do
    _d="${_d%/}"
    [[ -d "$_d/.odilon.sys" ]] && echo "$_d"
done | sort > "$_TMP_DRIVES"

DRIVES=()
while IFS= read -r _d; do
    [[ -n "$_d" ]] && DRIVES+=("$_d")
done < "$_TMP_DRIVES"
rm -f "$_TMP_DRIVES"

NUM_DRIVES=${#DRIVES[@]}
if (( NUM_DRIVES < 2 )); then
    echo "ERROR: fewer than 2 Odilon drives found under '$BASE_DIR'." >&2
    echo "       Run from (or pass) the directory that contains the driveN folders." >&2
    exit 1
fi

echo -e "${BOLD}Odilon RAID 6 — consistency check${RESET}"
echo "Base dir : $BASE_DIR"
echo "Drives   : $NUM_DRIVES"
for _d in "${DRIVES[@]}"; do printf "  %-16s  %s\n" "$(basename "$_d")" "$_d"; done
echo

# ── collect unique bucket IDs (union across all drives) ───────────────────────
_TMP_BUCKETS=$(mktemp)
for _drive in "${DRIVES[@]}"; do
    for _bdir in "$_drive"/*/; do
        _bid=$(basename "${_bdir%/}")
        [[ "$_bid" != ".odilon.sys" ]] && [[ -d "$_bdir" ]] && echo "$_bid"
    done
done | sort -nu > "$_TMP_BUCKETS"

BUCKETS=()
while IFS= read -r _bid; do
    [[ -n "$_bid" ]] && BUCKETS+=("$_bid")
done < "$_TMP_BUCKETS"
rm -f "$_TMP_BUCKETS"

if (( ${#BUCKETS[@]} == 0 )); then
    echo "No bucket data directories found."; exit 0
fi
echo "Buckets  : ${#BUCKETS[@]}  (ids: ${BUCKETS[*]})"
echo

TOTAL_ISSUES=0

# ── per-bucket comparison ─────────────────────────────────────────────────────
for _bucket in "${BUCKETS[@]}"; do

    echo -e "${BOLD}─── Bucket ${_bucket} ─────────────────────────────────────────────────────${RESET}"

    HEAD_FILES=(); VER_FILES=(); META_DIRS=(); DATA_KIB=()

    for _drive in "${DRIVES[@]}"; do
        HEAD_FILES+=( "$(count_files   "$_drive/$_bucket")" )
        VER_FILES+=(  "$(count_files   "$_drive/$_bucket/version")" )
        META_DIRS+=(  "$(count_subdirs "$_drive/.odilon.sys/buckets/$_bucket")" )
        DATA_KIB+=(   "$(du_kib        "$_drive/$_bucket")" )
    done

    # table
    printf "  %-24s  %11s  %11s  %11s  %10s\n" \
        "Drive" "HeadShards" "VerShards" "MetaDirs" "DataKiB"
    printf "  %-24s  %11s  %11s  %11s  %10s\n" \
        "────────────────────" "──────────" "─────────" "────────" "───────"
    for _i in "${!DRIVES[@]}"; do
        printf "  %-24s  %11s  %11s  %11s  %10s\n" \
            "$(basename "${DRIVES[$_i]}")" \
            "${HEAD_FILES[$_i]}" "${VER_FILES[$_i]}" \
            "${META_DIRS[$_i]}"  "${DATA_KIB[$_i]}"
    done
    echo

    # compare every drive against drive[0]
    _bucket_issues=0
    _ref_name=$(basename "${DRIVES[0]}")

    for _i in "${!DRIVES[@]}"; do
        (( _i == 0 )) && continue
        _dname=$(basename "${DRIVES[$_i]}")

        if [[ "${HEAD_FILES[$_i]}" != "${HEAD_FILES[0]}" ]]; then
            echo -e "  ${RED}FAIL${RESET}  HeadShards : ${_ref_name}=${HEAD_FILES[0]}  ${_dname}=${HEAD_FILES[$_i]}"
            (( _bucket_issues++ )) || true
        fi
        if [[ "${VER_FILES[$_i]}" != "${VER_FILES[0]}" ]]; then
            echo -e "  ${RED}FAIL${RESET}  VerShards  : ${_ref_name}=${VER_FILES[0]}  ${_dname}=${VER_FILES[$_i]}"
            (( _bucket_issues++ )) || true
        fi
        if [[ "${META_DIRS[$_i]}" != "${META_DIRS[0]}" ]]; then
            echo -e "  ${RED}FAIL${RESET}  MetaDirs   : ${_ref_name}=${META_DIRS[0]}  ${_dname}=${META_DIRS[$_i]}"
            (( _bucket_issues++ )) || true
        fi

        # ±10 KiB tolerance for filesystem block rounding
        _r="${DATA_KIB[0]}"; _t="${DATA_KIB[$_i]}"
        _diff=$(( _r > _t ? _r - _t : _t - _r ))
        if (( _diff > 10 )); then
            echo -e "  ${RED}FAIL${RESET}  DataKiB    : ${_ref_name}=${_r}  ${_dname}=${_t}  (diff=${_diff} KiB)"
            (( _bucket_issues++ )) || true
        fi
    done

    if (( _bucket_issues == 0 )); then
        echo -e "  ${GREEN}OK${RESET}  Bucket ${_bucket} — all ${NUM_DRIVES} drives in sync"
    else
        (( TOTAL_ISSUES += _bucket_issues )) || true
    fi
    echo
done

# ── cross-drive: same number of bucket metadata dirs on every drive ───────────
echo -e "${BOLD}─── Metadata bucket-set consistency ─────────────────────────────────────${RESET}"
META_BUCKET_COUNTS=()
for _drive in "${DRIVES[@]}"; do
    META_BUCKET_COUNTS+=( "$(count_subdirs "$_drive/.odilon.sys/buckets")" )
done
printf "  %-24s  %s\n" "Drive" "BucketMetaDirs"
printf "  %-24s  %s\n" "────────────────────" "──────────────"
for _i in "${!DRIVES[@]}"; do
    printf "  %-24s  %s\n" "$(basename "${DRIVES[$_i]}")" "${META_BUCKET_COUNTS[$_i]}"
done
echo

_ref_name=$(basename "${DRIVES[0]}")
_meta_issues=0
for _i in "${!DRIVES[@]}"; do
    (( _i == 0 )) && continue
    _dname=$(basename "${DRIVES[$_i]}")
    if [[ "${META_BUCKET_COUNTS[$_i]}" != "${META_BUCKET_COUNTS[0]}" ]]; then
        echo -e "  ${RED}FAIL${RESET}  BucketMetaDirs : ${_ref_name}=${META_BUCKET_COUNTS[0]}  ${_dname}=${META_BUCKET_COUNTS[$_i]}"
        (( _meta_issues++ )) || true
    fi
done
if (( _meta_issues == 0 )); then
    echo -e "  ${GREEN}OK${RESET}  Bucket metadata dirs consistent across all ${NUM_DRIVES} drives"
else
    (( TOTAL_ISSUES += _meta_issues )) || true
fi
echo

# ── summary ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}════════════════════════════════════════════════════════════════════════${RESET}"
if (( TOTAL_ISSUES == 0 )); then
    echo -e "${GREEN}${BOLD}ALL DRIVES IN SYNC${RESET}  — ${#BUCKETS[@]} bucket(s), ${NUM_DRIVES} drives"
else
    echo -e "${RED}${BOLD}OUT OF SYNC — ${TOTAL_ISSUES} issue(s) found${RESET}  (${#BUCKETS[@]} bucket(s), ${NUM_DRIVES} drives)"
    echo -e "${YELLOW}Tip: restart Odilon to re-trigger RAIDSixDriveSync, or check the log for encoding errors.${RESET}"
fi
echo
exit $(( TOTAL_ISSUES > 0 ? 1 : 0 ))

