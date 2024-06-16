#!/bin/bash

export DATA_LAKE_PATH=./data-lake
export RESULTS_FILE=results.csv
export TASK_FILE=tasks.csv
export LOG_FILE=scrape.log
export QUEUE_DB=queue.db
export COMPLETED_STATUS=2
export ERRORED_STATUS=4

[ -e $LOG_FILE ] && rm $LOG_FILE 
[ -e $RESULTS_FILE ] && rm $RESULTS_FILE
[ ! -e $DATA_LAKE_PATH ] && mkdir $DATA_LAKE_PATH
[ ! -e $DATA_LAKE_PATH/logs ] && mkdir $DATA_LAKE_PATH/logs

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

    printf "$SEASONS"
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

    printf "$EVENTS"
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

    printf "$CATEGORIES"
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

    printf "$SESSIONS"
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

    printf "$CLASSIFICATION" > "$DATA_LAKE_PATH/$4.csv"
}

upsert_queue() {
    FILE=$1
    setup="DROP TABLE IF EXISTS staging;"
    create="\
PRAGMA journal_mode=WAL2;
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    season_id TEXT,
    event_id TEXT,
    category_id TEXT,
    session_id TEXT,
    status INTEGER DEFAULT 0,
    attempt INTEGER DEFAULT 0,
    added_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp INTEGER
);"
    load=".import --csv $FILE staging"
    insert="\
INSERT OR IGNORE INTO tasks (id, season_id, event_id, category_id, session_id)
SELECT session_id, season_id, event_id, category_id, session_id FROM staging;"
    
    sqlite3 queue.db "$setup";
    sqlite3 queue.db "$create";
    sqlite3 queue.db "$load";
    sqlite3 queue.db "$insert";
}

get_tasks() {
    sqlite3 -csv queue.db "SELECT season_id, event_id, category_id, session_id FROM tasks WHERE STATUS < '$COMPLETED_STATUS' LIMIT $1;"
}

update_tasks() {
    # COMPLETED
    echo id > $TASK_FILE
    ls $DATA_LAKE_PATH | cut -d '.' -f1 >> $TASK_FILE
    setup="DROP TABLE IF EXISTS last_run"
    load=".import --csv $TASK_FILE last_run"
    update="UPDATE tasks SET status = $COMPLETED_STATUS FROM last_run WHERE last_run.id = tasks.id;"

    sqlite3 queue.db "$setup"
    sqlite3 queue.db "$load"
    sqlite3 queue.db "$update"

    # ERRORED 
    echo season_id > bad-seasons.csv
    echo event_id > bad-events.csv
    echo event_id,category_id > bad-event-category.csv
    echo session_id > bad-sessions.csv

    # TODO: parallel these for bigger log files
    UUID_PATTERN="[[:alnum:]]{8}-[[:alnum:]]{4}-[[:alnum:]]{4}-[[:alnum:]]{4}-[[:alnum:]]{12}"
    cat $LOG_FILE | grep "ERROR.* no events" | grep -oE $UUID_PATTERN >> bad-seasons.csv
    cat $LOG_FILE | grep "ERROR.* no categories" | grep -oE $UUID_PATTERN >> bad-events.csv
    cat $LOG_FILE | grep "ERROR.* no sessions" | grep -oE $UUID_PATTERN | xargs -l2 | tr ' ' ',' >> bad-event-category.csv
    cat $LOG_FILE | grep "ERROR.* no classification" | grep -oE $UUID_PATTERN >> bad-sessions.csv

    load_seasons=".import --csv bad-seasons.csv bad_seasons"
    update="UPDATE tasks SET status = $ERRORED_STATUS FROM bad_seasons WHERE bad_seasons.season_id = tasks.season_id;"
    sqlite3 queue.db "$load_seasons";
    sqlite3 queue.db "$update";

    load_events=".import --csv bad-events.csv bad_events"
    update="UPDATE tasks SET status = $ERRORED_STATUS FROM bad_events WHERE bad_events.event_id = tasks.event_id;"
    sqlite3 queue.db "$load_events";
    sqlite3 queue.db "$update";

    load_evt_cats=".import --csv bad-event-category.csv bad_evt_cats"
    update="UPDATE tasks SET status = $ERRORED_STATUS FROM bad_evt_cats WHERE bad_evt_cats.event_id = tasks.event_id AND bad_evt_cats.category_id = tasks.category_id;"
    sqlite3 queue.db "$load_evt_cats";
    sqlite3 queue.db "$update";

    load_sessions=".import --csv bad-sessions.csv bad_sessions"
    update="UPDATE tasks SET status = $ERRORED_STATUS FROM bad_sessions WHERE bad_sessions.session_id = tasks.session_id;"
    sqlite3 queue.db "$load_sessions";
    sqlite3 queue.db "$update";

    # CLEANUP
    sqlite3 queue.db 'DROP TABLE IF EXISTS bad_seasons';
    sqlite3 queue.db 'DROP TABLE IF EXISTS bad_events';
    sqlite3 queue.db 'DROP TABLE IF EXISTS bad_evt_cats';
    sqlite3 queue.db 'DROP TABLE IF EXISTS bad_sessions';
}

export -f get_events
export -f get_categories
export -f get_sessions
export -f get_classification
export -f upsert_queue
export -f get_tasks

START=$(date +%s)
echo "season_id,event_id,category_id,session_id,rider_name,rider_number,position,pts" > $RESULTS_FILE
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
# TODO

# CLEANUP
mv $LOG_FILE $DATA_LAKE_PATH/logs/$(date +%s%N).log
rm bad-* $RESULTS_FILE $TASK_FILE

printf "$REQUESTS requests made\n"
printf "$DURATION duration in seconds\n"
printf "$RPS RPS\n"
printf "COMPLETED\n"