#!/bin/bash

BBOX="-73.817,-33.733,-28.850,16.800"
TABLE="twitter.brasil_world_cup_geo_ref_tweets"

# Primeiros tweets para criar a tabela
gzcat ~/brasil-georef-tweets/brasil-georef-tweets-20140610.json.gz | python filter_tweets_by_bounding_box.py -c True -b $BBOX $TABLE
gzcat ~/brasil-georef-tweets/brasil-georef-tweets-2014061[1-9].json.gz | python filter_tweets_by_bounding_box.py -b $BBOX $TABLE
gzcat ~/brasil-georef-tweets/brasil-georef-tweets-2014062[0-9].json.gz | python filter_tweets_by_bounding_box.py -b $BBOX $TABLE
gzcat ~/brasil-georef-tweets/brasil-georef-tweets-20140630.json.gz | python filter_tweets_by_bounding_box.py -b $BBOX $TABLE

gzcat ~/brasil-georef-tweets/brasil-georef-tweets-201407*.json.gz | python filter_tweets_by_bounding_box.py -b $BBOX $TABLE



