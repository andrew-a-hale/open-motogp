#!/bin/bash
export DATA_LAKE_PATH=./data-lake/$(date +%s%N)
export RESULTS_FILE=results.csv
export TASK_FILE=tasks.csv
export LOG_FILE=scrape.log
export QUEUE_DB=queue.db
export COMPLETED_STATUS=2
export ERRORED_STATUS=4

[ -e $LOG_FILE ] && rm $LOG_FILE 
[ -e $RESULTS_FILE ] && rm $RESULTS_FILE
[ -e out ] && rm out 
[ ! -e out ] && mkdir out 
[ ! -e $DATA_LAKE_PATH ] && mkdir -p $DATA_LAKE_PATH
[ ! -e $DATA_LAKE_PATH/logs ] && mkdir $DATA_LAKE_PATH/logs
[ ! -e $DATA_LAKE_PATH/seasons ] && mkdir $DATA_LAKE_PATH/seasons
[ ! -e $DATA_LAKE_PATH/events ] && mkdir $DATA_LAKE_PATH/events
[ ! -e $DATA_LAKE_PATH/categories ] && mkdir $DATA_LAKE_PATH/categories
[ ! -e $DATA_LAKE_PATH/sessions ] && mkdir $DATA_LAKE_PATH/sessions
[ ! -e $DATA_LAKE_PATH/classifications ] && mkdir $DATA_LAKE_PATH/classifications

if [[ $1 -gt 0 ]]
then
    LOAD_TYPE="INCREMENTAL"
    TASK_COUNT=$1
else
    LOAD_TYPE="FULL"
    TASK_COUNT=$((2**16))
fi

printf "STARTING $LOAD_TYPE LOAD DOING UPTO $TASK_COUNT TASKS\n"

get_seasons() {
    printf "$(date --iso-8601=ns) [INFO] getting seasons\n" >> $LOG_FILE

    SEASONS=$( \
        curl -s "https://api.pulselive.motogp.com/motogp/v1/results/seasons" \
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

    printf "$SEASONS" | tee "$DATA_LAKE_PATH/seasons/seasons.ndjson"
}

get_events() {
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

    printf "$EVENTS" | tee "$DATA_LAKE_PATH/events/$1.ndjson"
}

get_categories() {
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

    printf "$CATEGORIES" | tee "$DATA_LAKE_PATH/categories/$2.ndjson"
}

get_sessions() {
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

    printf "$SESSIONS" | tee "$DATA_LAKE_PATH/sessions/$2-$3.ndjson"
}

get_classification() {
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

    printf "$CLASSIFICATION" > "$DATA_LAKE_PATH/classifications/$4.csv"
}

upsert_queue() {
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
}

get_tasks() {
    duckdb -csv queue.db "SELECT season_id, event_id, category_id, session_id FROM tasks WHERE status < $COMPLETED_STATUS LIMIT $1;"
}

update_tasks() {
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
}

export -f get_events
export -f get_categories
export -f get_sessions
export -f get_classification
export -f upsert_queue
export -f get_tasks

START=$(date +%s)
echo "season_id,event_id,category_id,session_id" > $TASK_FILE

get_seasons | jq -r '"\(.id)"' \
| parallel get_events | jq -r '"\(.season_id) \(.id)"' \
| parallel --colsep ' ' get_categories | jq -r '"\(.season_id) \(.event_id) \(.id)"' \
| parallel --colsep ' ' get_sessions | jq -r '"\(.season_id),\(.event_id),\(.category_id),\(.id)"' >> $TASK_FILE

upsert_queue $TASK_FILE
rm $TASK_FILE

get_tasks $TASK_COUNT | parallel --colsep ',' get_classification
update_tasks

DURATION=$(($(date +%s) - START))
REQUESTS=$(cat $LOG_FILE | grep INFO | wc -l)
RPS=$((REQUESTS/DURATION))

# EXPORT
duckdb -s "COPY (SELECT * FROM read_json_auto('$DATA_LAKE_PATH/seasons/*.ndjson')) TO 'out/seasons.parquet'"
duckdb -s "COPY (SELECT * FROM read_json_auto('$DATA_LAKE_PATH/events/*.ndjson')) TO 'out/events.parquet'"
duckdb -s "COPY (SELECT * FROM read_json_auto('$DATA_LAKE_PATH/categories/*.ndjson')) TO 'out/categories.parquet'"
duckdb -s "COPY (SELECT * FROM read_json_auto('$DATA_LAKE_PATH/sessions/*.ndjson')) TO 'out/sessions.parquet'"

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
duckdb -s "COPY (SELECT * FROM read_csv('$DATA_LAKE_PATH/classifications/*.csv', nullstr='null', columns=$schema)) TO 'out/classifications.parquet'"

duckdb -s "\
COPY (
    SELECT seasons.year, events.name AS event_name, events.sname AS event_short_name, categories.name AS category, sessions.name AS session, classification.*
    FROM read_parquet('out/classifications.parquet') AS classification
    LEFT JOIN read_parquet('out/seasons.parquet') AS seasons ON seasons.id = classification.season_id
    LEFT JOIN read_parquet('out/events.parquet') AS events ON events.id = classification.event_id
    LEFT JOIN read_parquet('out/categories.parquet') AS categories ON categories.id = classification.category_id AND categories.event_id = classification.event_id
    LEFT JOIN read_parquet('out/sessions.parquet') AS sessions ON sessions.id = classification.session_id
) TO 'out/mgp.parquet';"

# CLEANUP
mv $LOG_FILE $DATA_LAKE_PATH/logs/$(date +%s%N).log
rm bad-* $TASK_FILE

printf "$REQUESTS requests made\n"
printf "$DURATION duration in seconds\n"
printf "$RPS RPS\n"
printf "COMPLETED\n"