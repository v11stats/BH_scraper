#!/bin/bash
# update_data.sh
# Runs the full data pipeline daily at 6pm Pacific time.
# Scheduled via cron — see cron entry at bottom of this file.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="/home/sambauser/shared_folder/bh_modeling"
LOG_FILE="$SCRIPT_DIR/update_data_log.txt"

# Redirect all output (including setup below) to the log file so cron failures
# are never silently dropped (this server has no local mail transport, so
# cron's default "mail unredirected output" behavior goes nowhere).
exec >> "$LOG_FILE" 2>&1

echo "=== Run started: $(date) ==="

# Ensure local pip binaries are on PATH
export PATH="$HOME/.local/bin:$PATH"

# Set JAVA_HOME for tabula-py
export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))"
export PATH="$JAVA_HOME/bin:$PATH"

# Load email credentials if present
ENV_FILE="$SCRIPT_DIR/.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

python3 "$SCRIPT_DIR/process_a_a_data.py" \
    --admit-discharge-dir "$DATA_DIR/OSH AandA Admit Discharge" \
    --census-dir "$DATA_DIR/OSH AandA Census" \
    --docket-dir "$DATA_DIR/Court_Appearance_Documents"

echo "=== Run finished: $(date) ==="

# -------------------------------------------------------
# To install the cron job, run:
#   crontab -e
# Then add this line (6pm Pacific = UTC-7 in summer, UTC-8 in winter):
#   0 1 * * * /home/miki/git/mental_health_data/build_model_data/update_data.sh
# (1am UTC = 6pm PDT; adjust to 2am UTC in winter for PST)
# -------------------------------------------------------
