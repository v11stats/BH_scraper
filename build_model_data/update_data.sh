#!/bin/bash
# update_data.sh
# Runs the full data pipeline on Mon, Tue, Wed at 6pm Pacific time.
# Scheduled via cron — see cron entry at bottom of this file.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="/home/sambauser/shared_folder/bh_modeling"
LOG_FILE="$SCRIPT_DIR/update_data_log.txt"

echo "=== Run started: $(date) ===" >> "$LOG_FILE"

python3 "$SCRIPT_DIR/process_a_a_data.py" \
    --admit-discharge-dir "$DATA_DIR/OSH AandA Admit Discharge" \
    --census-dir "$DATA_DIR/OSH AandA Census" \
    --docket-dir "$DATA_DIR/Court_Appearance_Documents" \
    >> "$LOG_FILE" 2>&1

echo "=== Run finished: $(date) ===" >> "$LOG_FILE"

# -------------------------------------------------------
# To install the cron job, run:
#   crontab -e
# Then add this line (6pm Pacific = UTC-7 in summer, UTC-8 in winter):
#   0 1 * * 2,3,4 /home/miki/git/mental_health_data/build_model_data/update_data.sh
# (1am UTC = 6pm PDT; adjust to 2am UTC in winter for PST)
# -------------------------------------------------------
