#!/bin/bash

gzcat ./georef-tweets-2014061*.json.gz | python filter_tweets_by_bounding_box.py "twitter.hpc_brasil_geo_ref_tweets"
