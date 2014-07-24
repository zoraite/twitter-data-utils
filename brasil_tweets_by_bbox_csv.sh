#!/bin/bash

EXEC="filter_tweets_by_bbox_csv.py"
BBOX="-73.817,-33.733,-28.850,16.800"
TABLE="brasil_world_cup_geo_ref_tweets.csv"

# Primeiros tweets para criar a tabela
# gzcat ~/brasil-georef-tweets/brasil-georef-tweets-20140610.json.gz | python $EXEC -m "w" -b $BBOX $TABLE
# gzcat ~/brasil-georef-tweets/brasil-georef-tweets-2014061[1-9].json.gz | python $EXEC -b $BBOX $TABLE
# gzcat ~/brasil-georef-tweets/brasil-georef-tweets-2014062[1-9].json.gz | python $EXEC -b $BBOX $TABLE
cat dia20.json | head -n 10 | python $EXEC -b $BBOX sample.csv
# gzcat ~/brasil-georef-tweets/brasil-georef-tweets-20140630.json.gz | python $EXEC -b $BBOX $TABLE

# gzcat ~/brasil-georef-tweets/brasil-georef-tweets-201407*.json.gz | python $EXEC -b $BBOX $TABLE



