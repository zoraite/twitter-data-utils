from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import datetime
import json
import ConfigParser
import gzip


class StdOutListener(StreamListener):

    """
    A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """

    def __init__(self):
        super(StdOutListener, self).__init__()
        self.prefix = "brasil-georef-tweets"
        self.today = None
        self.current_file = None

    def on_data(self, data):

        if self.is_today():
            self.write_data(data)
        else:
            self.close_file()
            self.create_file()
            self.write_data(data)

        print data
        return True

    def on_error(self, status):
        print status

    def create_file(self):
        self.today = datetime.date.today()
        file_name = self.format_file_name()
        self.current_file = gzip.open(file_name, "a")

    def write_data(self, data):
        self.current_file.write(json.dumps(data) + "\n")

    def close_file(self):
        if self.current_file is not None:
            self.current_file.close()

    def is_today(self):
        today = datetime.date.today()
        return today == self.today

    def format_file_name(self):
        return self.prefix + "-" + str(self.today.year) + str(self.today.month) + str(self.today.day) + ".json.gz"


if __name__ == '__main__':

    section = "API"

    config = ConfigParser.RawConfigParser()
    config.read('keys.api')

    consumer_key = config.get(section, "consumer_key")
    consumer_secret = config.get(section, "consumer_secret")

    access_token = config.get(section, "access_token")
    access_token_secret = config.get(section, "access_token_secret")

    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(locations=[-73.817,  -33.733,  -28.850,   16.800 ] )