#!/bin/bash

gzcat rojo.isti.cnr.it/tweets/georef-tweets/georef-tweets-* | python filter_tweets_by_bounding_box.py 
