#!/bin/bash

# Script to get some attributes from twitter data
# Dependencies:
# - gzcat
# - jq

# file as input. Ex: georef-tweets-20140304.json.gz
input=rojo.isti.cnr.it/tweets/georef-tweets/georef-tweets-20140*
# file to save the results. Ex: output.csv
#output=$2

# Example of running
# chmod +x twitter_attributes.sh
# ./twitter_attributes.sh georef-tweets-20140304.json.gz ouput.csv

gzcat rojo.isti.cnr.it/tweets/georef-tweets/georef-tweets-20140* >  jq -r '[.text,.geo.coordinates[0],.geo.coordinates[1], .retweeted, .id, .id_str, .created_at, .in_reply_to_user_id, .lang, .place.name, .place.id, .place.bounding_box.coordinates[0][0][0], .place.bounding_box.coordinates[0][0][1],.place.bounding_box.coordinates[0][1][0], .place.bounding_box.coordinates[0][1][1],.place.bounding_box.coordinates[0][2][0], .place.bounding_box.coordinates[0][2][1],.place.bounding_box.coordinates[0][3][0], .place.bounding_box.coordinates[0][3][1], .place.place_type, .place.country_code, .place.country, .place.full_name,.user.name, .user.id, .user.id_str, .user.friends_count, .user.created_at, .user.screen_name] | @csv' > all_tweets.json
