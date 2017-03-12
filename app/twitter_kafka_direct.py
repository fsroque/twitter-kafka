#!/usr/bin/env python
from twython import TwythonStreamer
from kafka import KafkaProducer
import json
import os
import config

producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

######################################################################
# Create a handler for the streaming data that stays open...
######################################################################


class MyStreamer(TwythonStreamer):
    # Handler
    ''' Handles data received from the stream. '''

    ######################################################################
    # Include a counter in the class
    ######################################################################
    def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret, timeout=300, retry_count=None, retry_in=10, client_args=None, handlers=None, chunk_size=1):
        super(MyStreamer, self).__init__(app_key, app_secret, oauth_token, oauth_token_secret, timeout, retry_count, retry_in, client_args, handlers, chunk_size)
        self.num_tweets = 0

    ######################################################################
    # For each status event
    ######################################################################

    def on_success(self, data):
        ##################################################################
        # Structure of the twython status object
        # {'possibly_sensitive': False, 'in_reply_to_screen_name': None, 'in_reply_to_status_id': None, 'quoted_status_id': 790971946754150400, 'text': 'Yesss🙌 https://t.co/y8FlhU41c4', 'id': 790997352991580160, 'truncated': False, 'is_quote_status': True, 'coordinates': None, 'favorite_count': 0, 'user': {'name': 'bri', 'utc_offset': None, 'profile_image_url': 'http://pbs.twimg.com/profile_images/753408765806964737/ZsT5WcFD_normal.jpg', 'profile_banner_url': 'https://pbs.twimg.com/profile_banners/2314503667/1469320751', 'profile_image_url_https': 'https://pbs.twimg.com/profile_images/753408765806964737/ZsT5WcFD_normal.jpg', 'profile_background_image_url_https': 'https://abs.twimg.com/images/themes/theme1/bg.png', 'profile_link_color': '0084B4', 'id': 2314503667, 'listed_count': 0, 'profile_use_background_image': True, 'description': '|| Mercy ||', 'default_profile': True, 'favourites_count': 9974, 'profile_sidebar_border_color': 'C0DEED', 'followers_count': 565, 'notifications': None, 'contributors_enabled': False, 'id_str': '2314503667', 'verified': False, 'is_translator': False, 'screen_name': 'briannacynai', 'profile_background_image_url': 'http://abs.twimg.com/images/themes/theme1/bg.png', 'location': None, 'url': None, 'follow_request_sent': None, 'default_profile_image': False, 'time_zone': None, 'protected': False, 'geo_enabled': False, 'lang': 'en', 'created_at': 'Mon Jan 27 23:34:29 +0000 2014', 'statuses_count': 1369, 'profile_text_color': '333333', 'profile_background_color': 'C0DEED', 'profile_background_tile': False, 'following': None, 'profile_sidebar_fill_color': 'DDEEF6', 'friends_count': 547}, 'in_reply_to_user_id_str': None, 'filter_level': 'low', 'geo': None, 'in_reply_to_user_id': None, 'display_text_range': [0, 6], 'id_str': '790997352991580160', 'in_reply_to_status_id_str': None, 'quoted_status': {'possibly_sensitive': False, 'in_reply_to_screen_name': 'briannacynai', 'in_reply_to_status_id': None, 'quoted_status_id': 790966982556418048, 'text': '@briannacynai https://t.co/POWj2iRVZ7', 'id': 790971946754150400, 'truncated': False, 'is_quote_status': True, 'coordinates': None, 'favorite_count': 1, 'user': {'name': 'chyyy🉐', 'utc_offset': None, 'profile_image_url': 'http://pbs.twimg.com/profile_images/789941252435374080/b4baKFaU_normal.jpg', 'profile_banner_url': 'https://pbs.twimg.com/profile_banners/1405257536/1477171606', 'profile_image_url_https': 'https://pbs.twimg.com/profile_images/789941252435374080/b4baKFaU_normal.jpg', 'profile_background_image_url_https': 'https://abs.twimg.com/images/themes/theme1/bg.png', 'profile_link_color': '0084B4', 'id': 1405257536, 'listed_count': 2, 'profile_use_background_image': True, 'description': 'Not the replaceable type... |Gabby❤️|', 'default_profile': True, 'favourites_count': 1758, 'profile_sidebar_border_color': 'C0DEED', 'followers_count': 912, 'notifications': None, 'contributors_enabled': False, 'id_str': '1405257536', 'verified': False, 'is_translator': False, 'screen_name': 'chynnaxx__', 'profile_background_image_url': 'http://abs.twimg.com/images/themes/theme1/bg.png', 'location': 'Detroit, MI', 'url': None, 'follow_request_sent': None, 'default_profile_image': False, 'time_zone': None, 'protected': False, 'geo_enabled': True, 'lang': 'en', 'created_at': 'Sun May 05 14:53:28 +0000 2013', 'statuses_count': 3156, 'profile_text_color': '333333', 'profile_background_color': 'C0DEED', 'profile_background_tile': False, 'following': None, 'profile_sidebar_fill_color': 'DDEEF6', 'friends_count': 568}, 'in_reply_to_user_id_str': '2314503667', 'filter_level': 'low', 'geo': None, 'in_reply_to_user_id': 2314503667, 'display_text_range': [0, 13], 'id_str': '790971946754150400', 'in_reply_to_status_id_str': None, 'retweeted': False, 'quoted_status_id_str': '790966982556418048', 'entities': {'user_mentions': [{'name': 'bri', 'indices': [0, 13], 'id_str': '2314503667', 'screen_name': 'briannacynai', 'id': 2314503667}], 'hashtags': [], 'symbols': [], 'urls': [{'expanded_url': 'https://twitter.com/itsdanielbtw/status/790966982556418048', 'indices': [14, 37], 'url': 'https://t.co/POWj2iRVZ7', 'display_url': 'twitter.com/itsdanielbtw/s…'}]}, 'retweet_count': 0, 'lang': 'und', 'created_at': 'Tue Oct 25 17:43:02 +0000 2016', 'favorited': False, 'source': '<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>', 'place': None, 'contributors': None}, 'retweeted': False, 'quoted_status_id_str': '790971946754150400', 'entities': {'user_mentions': [], 'hashtags': [], 'symbols': [], 'urls': [{'expanded_url': 'https://twitter.com/chynnaxx__/status/790971946754150400', 'indices': [7, 30], 'url': 'https://t.co/y8FlhU41c4', 'display_url': 'twitter.com/chynnaxx__/sta…'}]}, 'retweet_count': 0, 'lang': 'und', 'created_at': 'Tue Oct 25 19:23:59 +0000 2016', 'favorited': False, 'timestamp_ms': '1477423439630', 'source': '<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>', 'place': None, 'contributors': None}
        ##################################################################

        if data.get('lang', None) not in ['en', 'no']:
            return True

        # publishes the tweet to the kafka topic
        producer.send(config.KAFKA_TOPIC, data)
        self.num_tweets += 1
        if self.num_tweets % 10 == 0:
            print('Published {} tweets'.format(self.num_tweets))

        return True

    ######################################################################
    # Supress Failure to keep demo running... In a production situation
    # Handle with seperate handler
    ######################################################################

    def on_error(self, status_code, data):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            self.disconnect()
        print('Got an error with status code: ' + str(status_code))
        return True  # To continue listening


######################################################################
# Main Loop Init
######################################################################


if __name__ == '__main__':

    CONSUMER_KEY = os.environ.get("CONSUMER_KEY", config.CONSUMER_KEY)
    CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET", config.CONSUMER_SECRET)
    ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", config.ACCESS_TOKEN)
    ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET", config.ACCESS_TOKEN_SECRET)
    WORDS_TO_TRACK = os.environ.get("WORDS_TO_TRACK", config.WORDS_TO_TRACK).split(',')

    stream = MyStreamer(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    ######################################################################
    # Custom Filter rules pull all traffic for those filters in real time.
    ######################################################################
    # Filter stream by tweets containing the WORDS_TO_TRACK
    print('Filtering tweets by: {}'.format(', '.join(WORDS_TO_TRACK)))
    stream.statuses.filter(track=WORDS_TO_TRACK)
