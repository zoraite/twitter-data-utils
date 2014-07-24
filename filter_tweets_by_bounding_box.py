# -*- coding: UTF-8 -*-

__author__ = 'igobrilhante'

import psycopg2
import logging
import json
import sys
import time
from optparse import OptionParser

DSN = "dbname=igo"
# TABLE = "twitter.brasil_geo_ref_tweets_all"

# '''
#     Bounding Box for Pisa and nearby cities from OSM
# '''
# UPPER_LAT = 43.9320
# BOTTOM_LAT = 43.4813
# LEFT_LNG = 10.0271
# RIGHT_LNG = 11.1031


class FilterTweetsDB:

    def __init__(self, coordinates=None):
        self.conn = psycopg2.connect(DSN)
        self.create_at_str = None
        self.logger = logging.getLogger(__name__)
        self.init_logger("FilterTweets")
        '''
            Bounding Box for Brasil
        '''
        self.ne_lat = 4.124546
        self.sw_lat = -33.267398
        self.sw_lng = -74.047852
        self.ne_lng = -34.752502

        if coordinates is not None:
            self.set_bbox(coordinates)


    @staticmethod
    def escape(t):
        return str(t.replace("\\", "").replace("\"", ""))

    def create_or_replace_table(self, table, attributes):
        self.logger.info("Create or Replace Table %s" % table)

        curs = self.conn.cursor()

        curs.execute("DROP TABLE IF EXISTS " + table)
        curs.execute("CREATE TABLE " + table + "(" + attributes + ")")
        curs.close()

    def create_tweets_table(self, table):
        self.create_or_replace_table(table,
                                     "id bigserial NOT NULL,\
                                          text text,\
                                          latitude double precision,\
                                          longitude double precision,\
                                          geom geometry,\
                                          retweeted boolean,\
                                          id_tweet bigint,\
                                          id_tweet_str text,\
                                          created_at timestamp without time zone,\
                                          created_at_str text,\
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

    @staticmethod
    def get_int_from_string(t):
        a = -1
        try:
            a = int(t)
        except Exception, e:
            e
        return a

    @staticmethod
    def get_string(t):
        a = ""
        try:
            a = int(t)
        except Exception, e:
            e
        return a

    @staticmethod
    def get_float_from_string(t):
        a = 0.0
        try:
            a = float(t)
        except Exception, e:
            e
        return a

    @staticmethod
    def convert_tweet_time(tweet):
        return time.strftime('%Y-%m-%d', time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))

    def get_attributes(self, tweet):

        content = tweet["text"]
        coordinate_0 = tweet["geo"]["coordinates"][0]
        coordinate_1 = tweet["geo"]["coordinates"][1]
        geom = "POINT(%s %s)" % (coordinate_0, coordinate_1)
        retweeted = tweet["retweeted"]
        id = tweet["id"]
        id_str = tweet["id_str"]
        created_at = tweet["created_at"]
        lang = tweet["lang"]
        in_reply_to_user_id = tweet["in_reply_to_user_id"]

        create_at_str = str(self.convert_tweet_time(tweet))

        if create_at_str != self.create_at_str:
            self.create_at_str = create_at_str;
            self.logger.info("Current Date: " + str(self.create_at_str))

        try:
            place_id = tweet["place"]["id"]
        except Exception, e:
            place_id = ""

        try:
            place_name = tweet["place"]["name"]
        except Exception, e:
            place_name = ""

        try:
            place_bb_00 = tweet["place"]["bounding_box"][0][0]
            place_bb_01 = tweet["place"]["bounding_box"][0][1]
            place_bb_10 = tweet["place"]["bounding_box"][1][0]
            place_bb_11 = tweet["place"]["bounding_box"][1][1]
            place_bb_20 = tweet["place"]["bounding_box"][2][0]
            place_bb_21 = tweet["place"]["bounding_box"][2][1]
            place_bb_30 = tweet["place"]["bounding_box"][3][0]
            place_bb_31 = tweet["place"]["bounding_box"][3][1]
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
            place_type = tweet["place"]["type"]
            place_country_code = tweet["place"]["country_code"]
            place_country = tweet["place"]["country"]
            place_full_name = tweet["place"]["full_name"]
        except Exception, e:
            place_type = ""
            place_country_code = ""
            place_country = ""
            place_full_name = ""

        user_name = tweet["user"]["name"]
        user_id = tweet["user"]["id"]
        user_id_str = tweet["user"]["id_str"]
        user_friend_count = tweet["user"]["friends_count"]
        user_created_at = tweet["user"]["created_at"]
        user_screen_name = tweet["user"]["screen_name"]

        return (
            content,
            coordinate_0,
            coordinate_1,
            geom,
            retweeted,
            id,
            id_str,
            created_at,
            create_at_str,
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

    def insert_tweet(self, row, table):
        # print("Tweet: ({})".format(time.time()))

        conn = self.conn
        curs = conn.cursor()
        query = "INSERT INTO " + table + "(\
                                          text,\
                                          latitude,\
                                          longitude, \
                                          geom,\
                                          retweeted,\
                                          id_tweet,\
                                          id_tweet_str,\
                                          created_at,\
                                          created_at_str,\
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
                                    ) VALUES (" \
                                         "%s, %s, %s, ST_GeomFromText(%s,4326), %s, %s, %s, " \
                                         "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                         "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" \
                                         ")"

        curs.execute(query, row)
        curs.close()

    def filter_out(self, row):

        lat = row[1]
        lng = row[2]

        if self.ne_lat >= lat >= self.sw_lat:
            if self.ne_lng >= lng >= self.sw_lng:
                return True
        return False

    def set_bbox(self, coordinates):
        coord = coordinates.split(",")
        self.sw_lng = float(coord[0])
        self.sw_lat = float(coord[1])
        self.ne_lng = float(coord[2])
        self.ne_lat = float(coord[3])

    def init_logger(self, prefix):
        # Logging set up

        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        file_handler = logging.FileHandler(prefix + ".log")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(console)
        self.logger.addHandler(file_handler)
        self.logger.setLevel(logging.DEBUG)

if __name__ == '__main__':
    usage = "usage: %prog [options] table_name"
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--create", dest="create",
                      help="Create new table")
    parser.add_option("-b", "--bbox", dest="bbox",
                      help="Bounding box", default="")

    (options, args) = parser.parse_args()

    table = args[0]

    filter_tweets = FilterTweetsDB(coordinates=options.bbox) if options.bbox is not "" else FilterTweetsDB()

    filter_tweets.logger.info("Table: " + str(table))

    if options.create:
        filter_tweets.create_tweets_table(table)

    try:
        for row in iter(sys.stdin.readline, ''):
            json_data = json.loads(row)

            if type(json_data) is unicode:
                json_data = json.loads(json_data)

            if "geo" in json_data and json_data["geo"] is not None:
                t = filter_tweets.get_attributes(json_data)
                if filter_tweets.filter_out(t):
                    filter_tweets.insert_tweet(t, table)
    except Exception, e:
        filter_tweets.logger.warn(str(e))
    filter_tweets.conn.commit()
    filter_tweets.conn.close()
