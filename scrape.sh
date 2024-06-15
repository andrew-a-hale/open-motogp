#!/bin/bash
set -e
[ -e scrape.log ] && rm scrape.log 
[ -e results.csv ] && rm results.csv

get_seasons() {
    printf "INFO: getting seasons\n" >> scrape.log
    SEASONS=$( \
        curl -s "https://api.pulselive.motogp.com/motogp/v1/results/seasons" \
        | jq -c '.[] | {year: .year, id: .id}' \
    )
    if [[ $(echo $SEASONS | wc -w) -eq 0 ]]
    then
        echo "ERROR: Found no seasons!"
        exit 1
    fi

    printf "$SEASONS"
}

get_events() {
    printf "INFO: getting events for season: $1\n" >> scrape.log
    EVENTS=$( \
        curl -s "https://api.pulselive.motogp.com/motogp/v1/results/events?seasonUuid=$1&isFinished=true" \
        | jq --arg SEASON_ID "$1" -c '.[] | {name: .name, sname: .short_name, id: .id, season_id: $SEASON_ID}'
    )
    if [[ $(echo $EVENTS | wc -w) -eq 0 ]]
    then
        echo "ERROR: Found no events!"
        exit 1
    fi

    printf "$EVENTS"
}

get_categories() {
    printf "INFO: getting category for season: $1, event: $2\n" >> scrape.log
    CATEGORIES=$( \
        curl -s "https://api.pulselive.motogp.com/motogp/v1/results/categories?eventUuid=$2" \
        | jq --arg SEASON_ID "$1" --arg EVENT_ID "$2" -c \
            '.[] | {name: (.name | match("^[[:alnum:]]+"; "g").string), id: .id, season_id: $SEASON_ID, event_id: $EVENT_ID}'
    )
    if [[ $(echo $CATEGORIES | wc -w) -eq 0 ]]
    then
        echo "ERROR: Found no categories!"
        exit 1
    fi

    printf "$CATEGORIES"
}

get_sessions() {
    printf "INFO: getting session for season: $1, event: $2, category: $3\n" >> scrape.log
    SESSIONS=$( \
        curl -s "https://api.pulselive.motogp.com/motogp/v1/results/sessions?eventUuid=$2&categoryUuid=$3" \
        | jq -c --arg SEASON_ID "$1" --arg EVENT_ID "$2" --arg CATEGORY_ID "$3" \
            '.[] | {name: (.type + (.number // "" | tostring)), id: .id, season_id: $SEASON_ID, event_id: $EVENT_ID, category_id: $CATEGORY_ID}'
    )
    if [[ $(echo $SESSIONS | wc -w) -eq 0 ]]
    then
        echo "ERROR: Found no sessions!"
        exit 1
    fi

    printf "$SESSIONS"
}

get_classification() {
    printf "INFO: getting classification for season: $1, event: $2, category: $3, session: $4\n" >> scrape.log
    CLASSIFICATION=$( \
        curl -s "https://api.pulselive.motogp.com/motogp/v1/results/session/$4/classification?test=false" \
        | jq --arg SEASON_ID "$1" --arg EVENT_ID "$2" --arg CATEGORY_ID "$3" --arg SESSION_ID "$4" '.classification | .[] | {season_id: $SEASON_ID, event_id: $EVENT_ID, category_id: $CATEGORY_ID, session_id: $SESSION_ID, name: .rider.full_name, number: .rider.number, pos: .position, pts: (.points // 0)}' \
        | jq -r '"\(.season_id),\(.event_id),\(.category_id),\(.session_id),\(.name),\(.number),\(.pos),\(.pts)"'
    )
    if [[ $(echo $CLASSIFICATION | wc -w) -eq 0 ]]
    then
        echo "ERROR: Found no classification data!"
        exit 1
    fi

    printf "$CLASSIFICATION"
}

export -f get_seasons
export -f get_events
export -f get_categories
export -f get_sessions
export -f get_classification

get_seasons | jq -r '.id' | head -n 1 \
| parallel get_events | jq -r '"\(.season_id) \(.id)"' | head -n 2 \
| parallel --colsep ' ' get_categories | jq -r '"\(.season_id) \(.event_id) \(.id)"' \
| parallel --colsep ' ' get_sessions | jq -r '"\(.season_id) \(.event_id) \(.category_id) \(.id)"' \
| parallel --colsep ' ' get_classification \
| tee -a results.csv