#!/bin/bash

# Script to get some attributes from twitter data
# Dependencies:
# - gzcat
# - jq

# file as input. Ex: georef-tweets-20140304.json.gz
input=$1
# file to save the results. Ex: output.csv
output=$2

# Example of running
# chmod +x twitter_attributes.sh
# ./twitter_attributes.sh georef-tweets-20140304.json.gz ouput.csv

gzcat $input | jq '[.text,.geo.coordinates[0],.geo.coordinates[1], .retweeted, .id, .id_str, .created_at, .in_reply_to_user_id, .lang, .place.name, .place.id, .place.bouding_box.coordinates[0], .place.bouding_box.coordinates[1], .place.bouding_box.coordinates[2], .place.bouding_box.coordinates[3], .place.place_type, .place.name, .place.country_code, .place.country, .place.full_name,.user.name, .user.id, .user.id_str, .user.location, .user.friends_count, .user.created_at, .user.screen_name, .user.screen_name, .user.created_at] | @csv' | head -1 > $output
