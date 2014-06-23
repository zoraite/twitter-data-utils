# -*- coding: UTF-8 -*-

__author__ = 'igobrilhante'

import psycopg2


import json
import sys
import time

DSN = "dbname=igo"
# TABLE = "twitter.brasil_geo_ref_tweets_all"

# '''
#     Bounding Box for Pisa and nearby cities from OSM
# '''
# UPPER_LAT = 43.9320
# BOTTOM_LAT = 43.4813
# LEFT_LNG = 10.0271
# RIGHT_LNG = 11.1031

'''
    Bounding Box for Brasil
'''
UPPER_LAT = 4.124546
BOTTOM_LAT = -33.267398
LEFT_LNG = -74.047852
RIGHT_LNG = -34.752502


def escape(t):
    return str(t.replace("\\", "").replace("\"",""))


def create_or_replace_table(table, attributes):
    conn = psycopg2.connect(DSN)
    curs = conn.cursor()

    curs.execute("DROP TABLE IF EXISTS " + table)
    curs.execute("CREATE TABLE " + table + "(" + attributes + ")")
    conn.commit()


def create_tweets_table(table):
    create_or_replace_table(table, "id bigserial NOT NULL,\
                                      text text,\
                                      latitude double precision,\
                                      longitude double precision,\
                                      retweeted boolean,\
                                      id_tweet bigint,\
                                      id_tweet_str text,\
                                      created_at timestamp without time zone,\
                                      in_reply_to_user_id text,\
                                      lang text,\
                                      place_name text,\
                                      place_id text,\
                                      place_bounding_box_00 double precision,\
                                      place_bounding_box_01 double precision,\
                                      place_bounding_box_10 double precision,\
                                      place_bounding_box_11 double precision,\
                                      place_bounding_box_20 double precision,\
                                      place_bounding_box_21 double precision,\
                                      place_bounding_box_30 double precision,\
                                      place_bounding_box_31 double precision,\
                                      place_type text,\
                                      place_country_code text,\
                                      place_country text,\
                                      place_full_name text,\
                                      user_name text,\
                                      user_id bigint,\
                                      user_id_str text,\
                                      user_friends_count integer,\
                                      user_created_at timestamp without time zone,\
                                      user_screen_name text\
                                ")


def get_int_from_string(t):
    a = -1
    try:
        a = int(t)
    except Exception, e:
        e
    return a


def get_string(t):
    a = ""
    try:
        a = int(t)
    except Exception, e:
        e
    return a


def get_float_from_string(t):
    a = 0.0
    try:
        a = float(t)
    except Exception, e:
        e
    return a


def get_attributes(json_data):

    content = json_data["text"]
    coordinate_0 = json_data["geo"]["coordinates"][0]
    coordinate_1 = json_data["geo"]["coordinates"][1]
    retweeted = json_data["retweeted"]
    id = json_data["id"]
    id_str = json_data["id_str"]
    created_at = json_data["created_at"]
    lang = json_data["lang"]
    in_reply_to_user_id = json_data["in_reply_to_user_id"]

    try:
        place_id = json_data["place"]["id"]
    except Exception, e:
        place_id =  ""

    try:
        place_name = json_data["place"]["name"]
    except Exception, e:
        place_name = ""

    try:
        place_bb_00 = json_data["place"]["bounding_box"][0][0]
        place_bb_01 = json_data["place"]["bounding_box"][0][1]
        place_bb_10 = json_data["place"]["bounding_box"][1][0]
        place_bb_11 = json_data["place"]["bounding_box"][1][1]
        place_bb_20 = json_data["place"]["bounding_box"][2][0]
        place_bb_21 = json_data["place"]["bounding_box"][2][1]
        place_bb_30 = json_data["place"]["bounding_box"][3][0]
        place_bb_31 = json_data["place"]["bounding_box"][3][1]
    except Exception, e:
        place_bb_00 = 0.0
        place_bb_01 = 0.0
        place_bb_10 = 0.0
        place_bb_11 = 0.0
        place_bb_20 = 0.0
        place_bb_21 = 0.0
        place_bb_30 = 0.0
        place_bb_31 = 0.0

    try:
        place_type = json_data["place"]["type"]
        place_country_code = json_data["place"]["country_code"]
        place_country = json_data["place"]["country"]
        place_full_name = json_data["place"]["full_name"]
    except Exception, e:
        place_type = ""
        place_country_code = ""
        place_country = ""
        place_full_name = ""

    user_name = json_data["user"]["name"]
    user_id = json_data["user"]["id"]
    user_id_str = json_data["user"]["id_str"]
    user_friend_count = json_data["user"]["friends_count"]
    user_created_at = json_data["user"]["created_at"]
    user_screen_name = json_data["user"]["screen_name"]

    return (
        content,
        coordinate_0,
        coordinate_1,
        retweeted,
        id,
        id_str,
        created_at,
        in_reply_to_user_id,
        lang,
        place_name,
        place_id,
        place_bb_00,
        place_bb_01,
        place_bb_10,
        place_bb_11,
        place_bb_20,
        place_bb_21,
        place_bb_30,
        place_bb_31,
        place_type,
        place_country_code,
        place_country,
        place_full_name,
        user_name,
        user_id,
        user_id_str,
        user_friend_count,
        user_created_at,
        user_screen_name
    )


def insert_tweet(row, table):
    # print("Tweet: ({})".format(time.time()))

    conn = psycopg2.connect(DSN)
    curs = conn.cursor()
    query = "INSERT INTO " + table + "(\
                                      text,\
                                      latitude,\
                                      longitude,\
                                      retweeted,\
                                      id_tweet,\
                                      id_tweet_str,\
                                      created_at,\
                                      in_reply_to_user_id,\
                                      lang,\
                                      place_name ,\
                                      place_id,\
                                      place_bounding_box_00,\
                                      place_bounding_box_01,\
                                      place_bounding_box_10,\
                                      place_bounding_box_11,\
                                      place_bounding_box_20,\
                                      place_bounding_box_21,\
                                      place_bounding_box_30,\
                                      place_bounding_box_31,\
                                      place_type,\
                                      place_country_code,\
                                      place_country,\
                                      place_full_name,\
                                      user_name,\
                                      user_id,\
                                      user_id_str,\
                                      user_friends_count,\
                                      user_created_at,\
                                      user_screen_name \
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    curs.execute(query, row)
    conn.commit()


def filter_out(row):

    lat = row[1]
    lng = row[2]

    if UPPER_LAT >= lat >= BOTTOM_LAT:
        if RIGHT_LNG >= lng >= LEFT_LNG:
            return True
    return False

if __name__ == '__main__':

    table = sys.argv[1]
    print table

    create_tweets_table(table)
    for row in iter(sys.stdin.readline, ''):
        json_data = json.loads(row)

        if type(json_data) is unicode:
            json_data = json.loads(json_data)

        if json_data["geo"] is not None:
            t = get_attributes(json_data)
            if filter_out(t):
                insert_tweet(t, table)
