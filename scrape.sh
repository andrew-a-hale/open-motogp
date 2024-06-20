#!/bin/bash
# TODO: Cloud Hosting

# DATA LAKE PATHS
export START_TIMESTAMP=$(date +%s%N)
export DATA_LAKE=./data-lake
export LOGS=$DATA_LAKE/logs/$START_TIMESTAMP
export BRONZE=$DATA_LAKE/bronze
export SILVER=$DATA_LAKE/silver
export GOLD=$DATA_LAKE/gold

# LOG FILES
export LOG_FILE=process.log
export METRIC_FILE=metrics.log
export DQ_FILE=data-quality.log

# LOG ARCHIVE
export LOG_ARCHIVE=$LOGS/process.log
export METRIC_ARCHIVE=$LOGS/metrics.ndjson
export DQ_ARCHIVE=$LOGS/data-quality.ndjson

# MESSAGE QUEUE
export QUEUE_DB=queue.db
export COMPLETED_STATUS=2
export ERRORED_STATUS=4

# CREATE DIRECTORIES
[ ! -e "$LOGS" ] && mkdir -p "$LOGS"
[ ! -e $BRONZE/seasons ] && mkdir -p $BRONZE/seasons
[ ! -e $BRONZE/events ] && mkdir -p $BRONZE/events
[ ! -e $BRONZE/categories ] && mkdir -p $BRONZE/categories
[ ! -e $BRONZE/sessions ] && mkdir -p $BRONZE/sessions
[ ! -e $BRONZE/classifications ] && mkdir -p $BRONZE/classifications
[ ! -e $SILVER ] && mkdir -p $SILVER
[ ! -e $GOLD ] && mkdir -p $GOLD

# HELPERS
get_seasons() {
    START=$(date +%s%N)
    printf "$(date --iso-8601=ns) [INFO] getting seasons\n" >> $LOG_FILE

    URL=https://api.pulselive.motogp.com/motogp/v1/results/seasons
    SEASONS=$( \
        curl -s "$URL" \
        | jq -s -c '.[] | sort_by(-.year | tonumber)' \
        | jq -c '.[] | {year: .year, id: .id}'
    )

    if [[ $LOAD_TYPE == "INCREMENTAL" ]]
    then
        SEASONS=$(head -n 1 <<< $SEASONS)
    fi

    if [[ $(printf "$SEASONS" | wc -l) -eq 1 ]]
    then
        printf "$(date --iso-8601=ns) [ERROR] found no seasons\n" >> $LOG_FILE
        exit 1
    fi

    DURATION=$((($(date +%s%N) - START) / 1000000))
    jq -cn --arg DURATION "$DURATION" '{"event": "GET_SEASONS", "duration_ms": $DURATION | tonumber}' >> $METRIC_FILE
    printf "$SEASONS" | duckdb -c "COPY (SELECT * FROM read_json_auto('/dev/stdin')) TO '$BRONZE/seasons/seasons.csv'"
    printf "$SEASONS"
}

get_events() {
    START=$(date +%s%N)
    printf "$(date --iso-8601=ns) [INFO] getting events for season: $1\n" >> $LOG_FILE

    EVENTS=$( \
        curl -s "https://api.pulselive.motogp.com/motogp/v1/results/events?seasonUuid=$1&isFinished=true" \
        | jq --arg SEASON_ID "$1" -c '.[] | {name: .name, sname: .short_name, id: .id, season_id: $SEASON_ID}' 
    )

    if [[ $(printf "$EVENTS" | wc -l) -eq 1 ]]
    then
        printf "$(date --iso-8601=ns) [ERROR] found no events for season: $1\n" >> $LOG_FILE
        exit 1
    fi

    DURATION=$((($(date +%s%N) - START) / 1000000))
    jq -cn --arg DURATION "$DURATION" --arg SEASON "$1" '{"event": "GET_EVENTS", "duration_ms": $DURATION | tonumber, "season_id": $SEASON}' >> $METRIC_FILE
    printf "$EVENTS" | duckdb -c "COPY (SELECT * FROM read_json_auto('/dev/stdin')) TO '$BRONZE/events' (FORMAT CSV, PARTITION_BY (season_id), OVERWRITE_OR_IGNORE)"
    printf "$EVENTS" 
}

get_categories() {
    START=$(date +%s%N)
    printf "$(date --iso-8601=ns) [INFO] getting category for season: $1, event: $2\n" >> $LOG_FILE

    CATEGORIES=$( \
        curl -s "https://api.pulselive.motogp.com/motogp/v1/results/categories?eventUuid=$2" \
        | jq --arg SEASON_ID "$1" --arg EVENT_ID "$2" -c \
            '.[] | {name: (.name | match("^[[:alnum:]]+"; "g").string), id: .id, season_id: $SEASON_ID, event_id: $EVENT_ID}' 
    )

    if [[ $(printf "$CATEGORIES" | wc -l) -eq 1 ]]
    then
        printf "$(date --iso-8601=ns) [ERROR] found no categories for event: $2\n" >> $LOG_FILE
        exit 1
    fi

    DURATION=$((($(date +%s%N) - START) / 1000000))
    jq -cn --arg DURATION "$DURATION" --arg EVENT "$2" '{"event": "GET_CATEGORIES", "duration_ms": $DURATION | tonumber, "event_id": $EVENT}' >> $METRIC_FILE
    printf "$CATEGORIES" | duckdb -c "COPY (SELECT * FROM read_json_auto('/dev/stdin')) TO '$BRONZE/categories' (FORMAT CSV, PARTITION_BY (season_id, event_id), OVERWRITE_OR_IGNORE)"
    printf "$CATEGORIES"
}

get_sessions() {
    START=$(date +%s%N)
    printf "$(date --iso-8601=ns) [INFO] getting session for season: $1, event: $2, category: $3\n" >> $LOG_FILE

    SESSIONS=$( \
        curl -s "https://api.pulselive.motogp.com/motogp/v1/results/sessions?eventUuid=$2&categoryUuid=$3" \
        | jq -c --arg SEASON_ID "$1" --arg EVENT_ID "$2" --arg CATEGORY_ID "$3" \
            '.[] | {name: (.type + (.number // "" | tostring)), id: .id, season_id: $SEASON_ID, event_id: $EVENT_ID, category_id: $CATEGORY_ID}'
    )

    if [[ $(printf "$SESSIONS" | wc -l) -eq 1 ]]
    then
        printf "$(date --iso-8601=ns) [ERROR] found no sessions for event: $2, category: $3\n" >> $LOG_FILE
        exit 1
    fi

    DURATION=$((($(date +%s%N) - START) / 1000000))
    jq -cn --arg DURATION "$DURATION" --arg EVENT "$2" --arg CATEGORY "$3" \
        '{"event": "GET_SESSIONS", "duration_ms": $DURATION | tonumber, "event_id": $EVENT, "category_id": $CATEGORY}' >> $METRIC_FILE
    printf "$SESSIONS" | duckdb -c "COPY (SELECT * FROM read_json_auto('/dev/stdin')) TO '$BRONZE/sessions' (FORMAT CSV, PARTITION_BY (season_id, event_id, category_id), OVERWRITE_OR_IGNORE)"
    printf "$SESSIONS"
}

get_classification() {
    START=$(date +%s%N)
    printf "$(date --iso-8601=ns) [INFO] getting classification for season: $1, event: $2, category: $3, session: $4\n" >> $LOG_FILE

    CLASSIFICATION=$( \
        curl -s "https://api.pulselive.motogp.com/motogp/v1/results/session/$4/classification?test=false" \
        | jq -c --arg SEASON_ID "$1" --arg EVENT_ID "$2" --arg CATEGORY_ID "$3" --arg SESSION_ID "$4" \
            '.classification | .[] | {season_id: $SEASON_ID, event_id: $EVENT_ID, category_id: $CATEGORY_ID, session_id: $SESSION_ID, name: .rider.full_name, number: .rider.number, pos: .position, pts: (.points // 0)}' 2> /dev/null \
        | jq -r '"\(.season_id),\(.event_id),\(.category_id),\(.session_id),\(.name),\(.number),\(.pos),\(.pts)"'
    )

    if [[ $(printf "$CLASSIFICATION" | wc -l) -lt 2 ]]
    then
        printf "$(date --iso-8601=ns) [ERROR] found no classification data for session: $4\n" >> $LOG_FILE
        exit 1
    fi

    DURATION=$((($(date +%s%N) - START) / 1000000))
    jq -cn --arg DURATION "$DURATION" --arg SESSION "$4" '{"event": "GET_CLASSIFICATION", "duration_ms": $DURATION | tonumber, "session_id": $SESSION}' >> $METRIC_FILE
    printf "$CLASSIFICATION" | duckdb -c "COPY (SELECT column0 AS season_id, column1 AS event_id, column2 AS category_id, column3 AS session_id, column4 AS name, column5 AS number, column6 AS pos, column7 AS pts FROM read_csv('/dev/stdin')) TO '$BRONZE/classifications' (FORMAT CSV, PARTITION_BY (season_id, event_id, category_id, session_id), OVERWRITE_OR_IGNORE)"
}

upsert_queue() {
    START=$(date +%s%N)

    FILE=$1
    create="\
CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR PRIMARY KEY,
    season_id VARCHAR,
    event_id VARCHAR,
    category_id VARCHAR,
    session_id VARCHAR,
    status INTEGER DEFAULT 0,
    attempt INTEGER DEFAULT 0,
    added_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP 
);"
    insert="\
INSERT OR IGNORE INTO tasks (id, season_id, event_id, category_id, session_id)
SELECT last_run.session_id, last_run.season_id, last_run.event_id, last_run.category_id, last_run.session_id
FROM read_csv_auto($FILE) AS last_run
LEFT JOIN tasks on last_run.session_id = tasks.id
WHERE tasks.id IS NULL;"
    
    duckdb queue.db "$create";
    duckdb queue.db "$insert";

    DURATION=$((($(date +%s%N) - START) / 1000000))
    jq -cn --arg DURATION "$DURATION" '{"event": "UPSERT_QUEUE", "duration_ms": $DURATION | tonumber}' >> $METRIC_FILE
}

get_tasks() {
    START=$(date +%s%N)
    duckdb -csv queue.db "SELECT season_id, event_id, category_id, session_id FROM tasks WHERE status < $COMPLETED_STATUS LIMIT $1;"

    DURATION=$((($(date +%s%N) - START) / 1000000))
    jq -cn --arg DURATION "$DURATION" '{"event": "GET_TASKS", "duration_ms": $DURATION | tonumber}' >> $METRIC_FILE
}

update_tasks() {
    START=$(date +%s%N)
    TASK_FILE=tasks.csv

    # COMPLETED
    echo id > $TASK_FILE
    ls $DATA_LAKE_PATH | cut -d '.' -f1 >> $TASK_FILE
    update="\
UPDATE tasks
SET status = $COMPLETED_STATUS, updated_timestamp = get_current_timestamp()
FROM read_csv_auto($TASK_FILE) AS last_run
WHERE last_run.id = tasks.id AND tasks.status <> 2;"

    duckdb queue.db "$update"

    # ERRORED 
    echo season_id > bad-seasons.csv
    echo event_id > bad-events.csv
    echo event_id,category_id > bad-event-category.csv
    echo session_id > bad-sessions.csv

    UUID_PATTERN="[[:alnum:]]{8}-[[:alnum:]]{4}-[[:alnum:]]{4}-[[:alnum:]]{4}-[[:alnum:]]{12}"
    cat $LOG_FILE | grep "ERROR.* no events" | grep -oE $UUID_PATTERN >> bad-seasons.csv
    cat $LOG_FILE | grep "ERROR.* no categories" | grep -oE $UUID_PATTERN >> bad-events.csv
    cat $LOG_FILE | grep "ERROR.* no sessions" | grep -oE $UUID_PATTERN | xargs -l2 | tr ' ' ',' >> bad-event-category.csv
    cat $LOG_FILE | grep "ERROR.* no classification" | grep -oE $UUID_PATTERN >> bad-sessions.csv

    update="\
UPDATE tasks
SET status = $ERRORED_STATUS, updated_timestamp = get_current_timestamp()
FROM read_csv_auto('bad-seasons.csv') AS bad_seasons
WHERE bad_seasons.season_id = tasks.season_id;"
    duckdb queue.db "$update";

    update="\
UPDATE tasks
SET status = $ERRORED_STATUS, updated_timestamp = get_current_timestamp()
FROM read_csv_auto('bad-events.csv') AS bad_events
WHERE bad_events.event_id = tasks.event_id;"
    duckdb queue.db "$update";

    update="\
UPDATE tasks
SET status = $ERRORED_STATUS, updated_timestamp = get_current_timestamp()
FROM read_csv_auto('bad-event-category.csv') AS bad_evt_cats
WHERE bad_evt_cats.event_id = tasks.event_id AND bad_evt_cats.category_id = tasks.category_id;"
    duckdb queue.db "$update";

    update="\
UPDATE tasks
SET status = $ERRORED_STATUS, updated_timestamp = get_current_timestamp()
FROM read_csv_auto('bad-sessions.csv') AS bad_sessions
WHERE bad_sessions.session_id = tasks.session_id;"
    duckdb queue.db "$update";

    DURATION=$((($(date +%s%N) - START) / 1000000))
    jq -cn --arg DURATION "$DURATION" '{"event": "UPDATE_TASKS", "duration_ms": $DURATION | tonumber}' >> $METRIC_FILE
}

# EXPORTS FOR PARALLEL
export -f get_events
export -f get_categories
export -f get_sessions
export -f get_classification

# LOAD TYPE
if [[ $1 -gt 0 ]]
then
    LOAD_TYPE="INCREMENTAL"
    TASK_COUNT=$1
else
    LOAD_TYPE="FULL"
    TASK_COUNT=$((2**16))
fi

# START PROCESS
printf "STARTING $LOAD_TYPE LOAD DOING UPTO $TASK_COUNT TASKS\n"
OVERALL_START=$(date +%s%N)

# GET TASK LIST
START=$(date +%s%N)
TASK_FILE=tasks.csv
echo "season_id,event_id,category_id,session_id" > $TASK_FILE

get_seasons | jq -r '"\(.id)"' \
| parallel get_events | jq -r '"\(.season_id) \(.id)"' \
| parallel --colsep ' ' get_categories | jq -r '"\(.season_id) \(.event_id) \(.id)"' \
| parallel --colsep ' ' get_sessions | jq -r '"\(.season_id),\(.event_id),\(.category_id),\(.id)"' >> $TASK_FILE

DURATION=$((($(date +%s%N) - START) / 1000000))
jq -cn --arg DURATION "$DURATION" '{"event": "GET_TASKS_LIST", "duration_ms": $DURATION | tonumber}' >> $METRIC_FILE

upsert_queue $TASK_FILE
rm $TASK_FILE

# DO TASKS
get_tasks $TASK_COUNT | parallel --colsep ',' get_classification
update_tasks

# SILVER
START=$(date +%s%N)
duckdb -s "COPY (SELECT * FROM read_csv('$BRONZE/seasons/**/*.csv', filename = true)) TO '$SILVER/seasons.parquet'"
duckdb -s "COPY (SELECT * FROM read_csv('$BRONZE/events/**/*.csv', filename = true)) TO '$SILVER/events.parquet'"
duckdb -s "COPY (SELECT * FROM read_csv('$BRONZE/categories/**/*.csv', filename = true)) TO '$SILVER/categories.parquet'"
duckdb -s "COPY (SELECT * FROM read_csv('$BRONZE/sessions/**/*.csv', filename = true)) TO '$SILVER/sessions.parquet'"

schema="\
{
    'season_id': 'VARCHAR',
    'event_id': 'VARCHAR',
    'category_id': 'VARCHAR',
    'session_id': 'VARCHAR',
    'name': 'VARCHAR',
    'number': 'INTEGER',
    'position': 'INTEGER',
    'points': 'INTEGER'
}"
duckdb -s "COPY (SELECT * FROM read_csv('$BRONZE/classifications/**/*.csv', nullstr='null', columns=$schema, filename = true)) TO '$SILVER/classifications.parquet'"

DURATION=$((($(date +%s%N) - START) / 1000000))
jq -cn --arg DURATION "$DURATION" '{"event": "LOAD_SILVER", "duration_ms": $DURATION | tonumber}' >> $METRIC_FILE

# DATA QUALITY
## CHECK NULL POSITION
START=$(date +%s%N)
NULL_POSITION=$(duckdb -json -s "SELECT filename, COUNT(*) AS NULL_POSITION_COUNT FROM '$SILVER/classifications.parquet' WHERE position IS NULL GROUP BY filename")
if [[ $(wc -l <<< $NULL_POSITION) -gt 0 ]]
then
    jq -c '.[]' <<< $NULL_POSITION >> $DQ_FILE
fi

[[ -e $DQ_FILE && $(wc -l <<< $DQ_FILE) -gt 0 ]] && printf "FOUND DATA QUALITY ISSUES\\n"

DURATION=$((($(date +%s%N) - START) / 1000000))
jq -cn --arg DURATION "$DURATION" '{"event": "CHECK_DQ", "duration_ms": $DURATION | tonumber}' >> $METRIC_FILE

# GOLD
START=$(date +%s%N)
duckdb -s "\
COPY (
    SELECT
        seasons.year,
        events.name AS event_name,
        events.sname AS event_short_name,
        categories.name AS category,
        sessions.name AS session,
        classification.name AS rider_name,
        classification.number AS rider_number,
        classification.position,
        classification.points
    FROM read_parquet('$SILVER/classifications.parquet') AS classification
    LEFT JOIN read_parquet('$SILVER/seasons.parquet') AS seasons ON seasons.id = classification.season_id
    LEFT JOIN read_parquet('$SILVER/events.parquet') AS events ON events.id = classification.event_id
    LEFT JOIN read_parquet('$SILVER/categories.parquet') AS categories
    ON categories.id = classification.category_id AND categories.event_id = classification.event_id
    LEFT JOIN read_parquet('$SILVER/sessions.parquet') AS sessions ON sessions.id = classification.session_id
) TO '$GOLD/mgp.parquet';"

DURATION=$((($(date +%s%N) - START) / 1000000))
jq -cn --arg DURATION "$DURATION" '{"event": "LOAD_GOLD", "duration_ms": $DURATION | tonumber}' >> $METRIC_FILE

# TIMING OVERALL
DURATION=$((($(date +%s%N) - OVERALL_START) / 1000000))
REQUESTS=$(cat $LOG_FILE | grep INFO | wc -l)
RPS=$((REQUESTS/(DURATION/1000)))

jq -cn --arg DURATION "$DURATION" --arg REQUESTS "$REQUESTS" --arg RPS "$RPS" \
    '{"event": "SCRAPE", "duration_ms": $DURATION | tonumber, "requests": $REQUESTS | tonumber, "rps": $RPS | tonumber}' >> $METRIC_FILE
printf "$REQUESTS requests made\n"
printf "$DURATION duration in ms\n"
printf "$RPS RPS\n"
printf "COMPLETED\n"

# CLEANUP
[ -e $LOG_FILE ] && mv $LOG_FILE $LOG_ARCHIVE
[ -e $METRIC_FILE ] && mv $METRIC_FILE $METRIC_ARCHIVE
[ -e $DQ_FILE ] && mv $DQ_FILE $DQ_ARCHIVE
rm bad-* $TASK_FILE