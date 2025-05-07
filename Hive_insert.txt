#!/bin/bash

# Usage: ./script.sh <start_year> <start_month> <end_year> <end_month>
if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <start_year> <start_month> <end_year> <end_month>"
  exit 1
fi

START_YEAR=$1
START_MONTH=$2
END_YEAR=$3
END_MONTH=$4
FINAL_TABLE="your_db.final_table"
BASE_STAGE_TABLE="tablename"

# Create log directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="./partition_load_logs_${TIMESTAMP}"
mkdir -p "$LOG_DIR"
SUCCESS_LOG="${LOG_DIR}/success.log"
FAIL_LOG="${LOG_DIR}/failures.log"

# Convert dates to YYYYMM
start_date=$(date -d "${START_YEAR}-${START_MONTH}-01" +"%Y%m")
end_date=$(date -d "${END_YEAR}-${END_MONTH}-01" +"%Y%m")
current_date=$start_date

while [ "$current_date" -le "$end_date" ]; do
  YEAR=${current_date:0:4}
  MONTH=${current_date:4:2}
  STAGE_TABLE="${BASE_STAGE_TABLE}${YEAR}${MONTH}"

  echo "Processing ${STAGE_TABLE}..."

  TABLE_EXISTS=$(impala-shell -B -q "SHOW TABLES LIKE '${STAGE_TABLE}';" | grep -w "${STAGE_TABLE}")
  if [[ -z "$TABLE_EXISTS" ]]; then
    echo "Table ${STAGE_TABLE} not found. Skipping." | tee -a "$FAIL_LOG"
    current_date=$(date -d "${YEAR}-${MONTH}-01 +1 month" +"%Y%m")
    continue
  fi

  MONTH_END=$(date -d "${YEAR}-${MONTH}-01 +1 month -1 day" +"%Y-%m-%d")

  columns=$(impala-shell -B --output_delimiter='|' -q "DESCRIBE ${STAGE_TABLE};" | awk -F'|' 'NF >= 2 { gsub(/^ +| +$/, "", $1); print $1 }')
  select_list=""
  while read -r col; do
    select_list+="${col}, "
  done <<< "$columns"
  select_list=${select_list%, }

  QUERY="INSERT INTO TABLE ${FINAL_TABLE} PARTITION (month_end_date='${MONTH_END}') SELECT ${select_list} FROM ${STAGE_TABLE};"

  echo "Executing query for ${STAGE_TABLE}..."

  impala-shell -q "$QUERY" &> "${LOG_DIR}/${STAGE_TABLE}.log"
  if [ $? -eq 0 ]; then
    echo "SUCCESS: ${STAGE_TABLE}" | tee -a "$SUCCESS_LOG"
  else
    echo "FAILURE: ${STAGE_TABLE}" | tee -a "$FAIL_LOG"
    echo "See log: ${LOG_DIR}/${STAGE_TABLE}.log"
  fi

  current_date=$(date -d "${YEAR}-${MONTH}-01 +1 month" +"%Y%m")
done

echo ""
echo "Process completed."
echo "Success log: $SUCCESS_LOG"
echo "Failure log: $FAIL_LOG"
