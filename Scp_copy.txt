#!/bin/bash
set -e

CONFIG_FILE=$1
if [ -z "$CONFIG_FILE" ]; then
    echo "Usage: ./scp_cluster_sync.sh scp_sync_config.properties"
    exit 1
fi

source "$CONFIG_FILE"

# --- Rule 7: Safe Path Validation Function ---
validate_and_remove() {
    local target_path=$1
    local is_local=$2

    if [ -z "$target_path" ] || [ "$target_path" = "/" ] || [ "$target_path" = "/root" ] || [ "$target_path" = "/home" ] || [ "$target_path" = "/hdfs" ]; then
        echo "ERROR: Unsafe path rejection for deletion: $target_path"
        exit 1
    }

    if [ "$is_local" = "true" ]; then
        if [ -e "$target_path" ]; then
            echo "Path validation passed. Safely removing local path: $target_path"
            rm -rf "$target_path"
        fi
    else
        if hdfs dfs -test -d "$target_path"; then
            echo "Path validation passed. Safely removing HDFS path: $target_path"
            hdfs dfs -rm -r -skipTrash "$target_path"
        fi
    fi
}

# Function to dynamically fetch table or partition location from Hive Metastore
get_hive_table_location() {
    local db=$1
    local tbl=$2
    local part_filter=$3 # Optional: e.g., "txn_date='2026-06-01'"

    local desc_query="DESCRIBE FORMATTED $db.$tbl;"
    if [ -n "$part_filter" ]; then
        desc_query="DESCRIBE FORMATTED $db.$tbl PARTITION ($part_filter);"
    fi

    # Extract the 'Location:' field from DESCRIBE FORMATTED output, clean up whitespace and carriage returns
    local loc
    loc=$(beeline -u "jdbc:hive2://localhost:10000/default;auth=noSasl" --silent=true -e "$desc_query" 2>/dev/null | grep -i "location:" | head -n 1 | awk '{print $2}' | tr -d '\r')
    
    echo "$loc"
}

# Read Config and Process Jobs
grep -v "^#" "$CONFIG_FILE" | grep -v "^$" | while IFS='|' read -r job_name mode prod_db prod_tbl dev_db dev_tbl part_col start_val end_val query_file delim target_loc; do
    
    [[ "$job_name" == "job_name" ]] && continue

    echo "=================================================="
    echo "Starting Job: $job_name | Mode: $mode"
    echo "=================================================="

    TEMP_LOCAL_STAGING="/tmp/sync_staging_${job_name}_$(date +%s)"
    mkdir -p "$TEMP_LOCAL_STAGING"
    DEV_WAREHOUSE_DIR="${DEV_BASE_PATH}/${dev_db}.db/${dev_tbl}"

    if [ "$mode" = "FULL" ]; then
        # --- Rule 5: Master Table Full Copy with Dynamic Path Lookup ---
        PROD_HDFS_PATH=$(get_hive_table_location "$prod_db" "$prod_tbl")
        
        if [ -z "$PROD_HDFS_PATH" ]; then
            echo "ERROR: Could not dynamically resolve HDFS path for production table $prod_db.$prod_tbl"
            exit 1
        }
        echo "Dynamically resolved Production table path: $PROD_HDFS_PATH"

        echo "Extracting full table from Production HDFS to local staging..."
        hdfs dfs -get "$PROD_HDFS_PATH/*" "$TEMP_LOCAL_STAGING/"

        echo "Transferring files via SCP to Development server..."
        ssh "${DEV_USER}@${DEV_HOST}" "mkdir -p ${DEV_WAREHOUSE_DIR}"
        scp -r "$TEMP_LOCAL_STAGING"/* "${DEV_USER}@${DEV_HOST}:${DEV_WAREHOUSE_DIR}/"

        # Dynamically create table on Dev using production schema definition and repair metastore
        ssh "${DEV_USER}@${DEV_HOST}" "beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -e 'CREATE TABLE IF NOT EXISTS ${dev_db}.${dev_tbl} LIKE ${prod_db}.${prod_tbl} STORED AS PARQUET; MSCK REPAIR TABLE ${dev_db}.${dev_tbl};'"

    elif [ "$mode" = "PARTITION" ] || [ "$mode" = "RANGE" ]; then
        # --- Rule 3: Date Partition / Range Copy with Dynamic Path Lookup ---
        current_date="$start_val"
        if [ "$mode" = "PARTITION" ]; then
            end_val="$start_val"
        fi

        while [ "$current_date" \<= "$end_val" ]; do
            echo "Processing partition: $part_col=$current_date"
            
            # Dynamically fetch partition-specific HDFS path from metastore
            PROD_PART_HDFS=$(get_hive_table_location "$prod_db" "$prod_tbl" "$part_col='$current_date'")
            
            PART_STAGING="${TEMP_LOCAL_STAGING}/$part_col=$current_date"
            mkdir -p "$PART_STAGING"

            if [ -n "$PROD_PART_HDFS" ] && hdfs dfs -test -d "$PROD_PART_HDFS"; then
                echo "Resolved partition path: $PROD_PART_HDFS"
                hdfs dfs -get "$PROD_PART_HDFS/*" "$PART_STAGING/"

                ssh "${DEV_USER}@${DEV_HOST}" "mkdir -p ${DEV_WAREHOUSE_DIR}/$part_col=$current_date"
                scp -r "$PART_STAGING"/* "${DEV_USER}@${DEV_HOST}:${DEV_WAREHOUSE_DIR}/$part_col=$current_date/"

                ssh "${DEV_USER}@${DEV_HOST}" "beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -e 'CREATE TABLE IF NOT EXISTS ${dev_db}.${dev_tbl} LIKE ${prod_db}.${prod_tbl} STORED AS PARQUET PARTITIONED BY (${part_col} STRING); ALTER TABLE ${dev_db}.${dev_tbl} ADD IF NOT EXISTS PARTITION (${part_col}=\x27$current_date\x27);'"
            else
                echo "Warning: Production partition path could not be resolved or does not exist for date: $current_date"
            fi

            current_date=$(date -I -d "$current_date + 1 day")
        done

    elif [ "$mode" = "TEXT_EXPORT" ]; then
        # --- Rule 4: Text File Export via External Query File ---
        if [ ! -f "$query_file" ]; then
            echo "ERROR: Query file not found at path: $query_file"
            exit 1
        fi

        EXPORT_FILE="/tmp/export_${job_name}.txt"
        validate_and_remove "$EXPORT_FILE" true

        SQL_QUERY=$(cat "$query_file")
        echo "Executing query loaded from file: $query_file"

        beeline -u "jdbc:hive2://localhost:10000/default;auth=noSasl" --outputformat=csv2 --delimiterValue="$delim" -e "$SQL_QUERY" > "$EXPORT_FILE"

        if [ "$target_loc" = "local_dev" ]; then
            echo "Copying exported text file to Development local path via SCP..."
            scp "$EXPORT_FILE" "${DEV_USER}@${DEV_HOST}:/data/dev/local_path/${job_name}_output.txt"
        elif [ "$target_loc" = "local_prod" ]; then
            echo "Development environment not configured. Saving file locally on Production server node..."
            mkdir -p /tmp/prod_fallback_exports/
            cp "$EXPORT_FILE" "/tmp/prod_fallback_exports/${job_name}_output.txt"
        fi

        validate_and_remove "$EXPORT_FILE" true
    fi

    validate_and_remove "$TEMP_LOCAL_STAGING" true
    echo "Job $job_name completed successfully."
done
