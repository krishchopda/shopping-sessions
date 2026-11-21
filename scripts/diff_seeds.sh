#!/bin/sh
# Differential test: solver vs scripts/reference.py on several generator seeds.
# Usage: scripts/diff_seeds.sh GENERATOR SOLVER COMPARE_PY [SEEDS...]
set -eu
GEN=$1; SOL=$2; CMP=$3; shift 3
for s in "$@"; do
  D=$(mktemp -d); R=$(mktemp -d); N=$(mktemp -d)
  "$GEN" --seed "$s" --events 200000 --clicks 50000 --output "$D/in" >/dev/null
  python3 "$(dirname "$0")/reference.py" "$D/in" "$R"
  DATA_DIR="$D/in" OUTPUT_DIR="$N" "$SOL" 2>/dev/null
  echo "seed $s: $(python3 "$CMP" "$N" "$R" | tail -1)"
  rm -rf "$D" "$R" "$N"
done
