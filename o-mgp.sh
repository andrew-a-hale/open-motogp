#!/bin/bash
set -e

# Season: https://api.pulselive.motogp.com/motogp/v1/results/seasons
# Event: https://api.pulselive.motogp.com/motogp/v1/results/events?seasonUuid={season}&isFinished=true
# Category: https://api.pulselive.motogp.com/motogp/v1/results/categories?eventUuid={event}
# Session: https://api.pulselive.motogp.com/motogp/v1/results/sessions?eventUuid={event}&categoryUuid={category}
# Classification: https://api.pulselive.motogp.com/motogp/v1/results/session/{session}/classification?test=false

# STYLING
export GUM_FILTER_SELECTED_PREFIX_FOREGROUND="124"
export GUM_FILTER_PROMPT_FOREGROUND="124"
export GUM_FILTER_INDICATOR_FOREGROUND="124"
export GUM_FILTER_MATCH_FOREGROUND="124"
export GUM_SPIN_SPINNER_FOREGROUND="124"
export GUM_CONFIRM_PROMPT_FOREGROUND="124"
export GUM_CONFIRM_SELECTED_BACKGROUND="124"

# Welcome Splash
clear
gum style --border normal --margin "1" --padding "1 2" --border-foreground 124 \
"Welcome to $(gum style --foreground 124 'Open MotoGP')."

sleep 1

while true 
do
    # Season Selection
    get_years() {
        YEARS=$(curl -s "https://api.pulselive.motogp.com/motogp/v1/results/seasons" | jq -c '.[] | {year: .year, id: .id}')
        if [[ $(wc -w <<< $YEARS) -eq 0 ]]
        then
            gum style --border normal --margin "1" --padding "1 2" --border-foreground 124 \
            "ERROR: Found no seasons!"
            exit 1
        fi

        export YEARS
    }

    echo Please select a year:
    gum spin --spinner dot --title "Getting Years..." -- sleep 1 ; get_years
    YEAR=$(echo $YEARS | jq -s '.[] | .year' | gum filter --height 10 --placeholder "start typing...")
    YEAR_ID=$(echo $YEARS | jq -s --argjson Y "$YEAR" '.[] | select(.year==$Y) | .id' | tr -d \")
    gum style --foreground 124 "$YEAR"

    # Event Selection
    get_events() {
        EVENTS=$(curl -s "https://api.pulselive.motogp.com/motogp/v1/results/events?seasonUuid=$YEAR_ID&isFinished=true" | jq -c '.[] | {name: .name, sname: .short_name, id: .id}')
        if [[ $(wc -w <<< $EVENTS) -eq 0 ]]
        then
            gum style --border normal --margin "1" --padding "1 2" --border-foreground 124 \
            "ERROR: Found no events!"
            exit 1
        fi

        export EVENTS
    }

    echo Please select an event:
    gum spin --spinner dot --title "Getting Events..." -- sleep 1 ; get_events
    EVENT=$(echo $EVENTS | jq -s '.[] | .name' | tr -d \" | gum filter --height 10 --placeholder "start typing...")
    EVENT_ID=$(echo $EVENTS | jq -s --arg E "$EVENT" '.[] | select(.name==$E) | .id' | tr -d \")
    EVENT_SHORT=$(echo $EVENTS | jq -s --arg E "$EVENT" '.[] | select(.name==$E) | .sname' | tr -d \")
    gum style --foreground 124 "$EVENT"

    # Category Selection
    get_categories() {
        CATEGORIES=$(curl -s "https://api.pulselive.motogp.com/motogp/v1/results/categories?eventUuid=$EVENT_ID" | jq -c '.[] | {name: (.name | match("^[[:alnum:]]+"; "g").string), id: .id}')
        if [[ $(wc -w <<< $CATEGORIES) -eq 0 ]]
        then
            gum style --border normal --margin "1" --padding "1 2" --border-foreground 124 \
            "ERROR: Found no categories!"
            exit 1
        fi

        export CATEGORIES
    }

    echo Please select a category:
    gum spin --spinner dot --title "Getting Categories..." -- sleep 1 ; get_categories
    CATEGORY=$(echo $CATEGORIES | jq -s '.[] | .name' | tr -d \" | gum filter --height 10 --placeholder "start typing...")
    CATEGORY_ID=$(echo $CATEGORIES | jq -s --arg C "$CATEGORY" '.[] | select(.name==$C) | .id' | tr -d \")
    gum style --foreground 124 "$CATEGORY"

    # Session Selection
    get_sessions() {
        SESSIONS=$(curl -s "https://api.pulselive.motogp.com/motogp/v1/results/sessions?eventUuid=$EVENT_ID&categoryUuid=$CATEGORY_ID" | jq -c '.[] | {name: (.type + (.number // "" | tostring)), id: .id}')
        if [[ $(wc -w <<< $SESSIONS) -eq 0 ]]
        then
            gum style --border normal --margin "1" --padding "1 2" --border-foreground 124 \
            "ERROR: Found no sessions!"
            exit 1
        fi

        export SESSIONS
    }

    echo Please select a session:
    gum spin --spinner dot --title "Getting Sessions..." -- sleep 1 ; get_sessions
    SESSION=$(echo $SESSIONS | jq -s '.[] | .name' | tr -d \" | gum filter --height 10 --placeholder "start typing...")
    SESSION_ID=$(echo $SESSIONS | jq -s --arg S "$SESSION" '.[] | select(.name==$S) | .id' | tr -d \")
    gum style --foreground 124 "$SESSION"

    # Export Classification Data
    FILENAME="$YEAR"_"$EVENT_SHORT"_"$CATEGORY"_"$SESSION".csv
    export_classification() {
        echo "session_id,name,pos,pts" > $FILENAME
        curl -s "https://api.pulselive.motogp.com/motogp/v1/results/session/$SESSION_ID/classification?test=false" \
        | jq '.classification | .[] | {name: .rider.full_name, pos: .position, pts: .points}' \
        | jq -r --arg S "$SESSION_ID" '"\($S),\(.name),\(.pos),\(.pts)"' >> $FILENAME

        if [[ $(wc -l < $FILENAME) -eq 1 ]]
        then
            gum style --border normal --margin "1" --padding "1 2" --border-foreground 124 \
            "ERROR: Found no classification data!"
            rm $FILENAME
            exit 1
        fi
    }

    gum style --border normal --margin "1" --padding "1 2" --border-foreground 124 \
    "Exporting Classification to $FILENAME"
    gum spin --spinner dot --title "Getting Classification..." -- sleep 1 ; export_classification

    if gum confirm "Would you like to make another request?" ; then
        continue
    elif test $? -eq 1; then
        gum style --border normal --margin "1" --padding "1 2" --border-foreground 124 \
        "Thank you for using $(gum style --foreground 124 'Open MotoGP')."
        exit 1
    else
        exit 130
    fi
done