#!/usr/bin/env python
import tweepy
from kafka import KafkaProducer
import json
import config

producer = KafkaProducer(bootstrap_servers='kafkadocker_kafka_1:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

######################################################################
# Create a handler for the streaming data that stays open...
######################################################################


class StdOutListener(tweepy.StreamListener):
    # Handler
    ''' Handles data received from the stream. '''

    ######################################################################
    # Include a counter in the class
    ######################################################################
    def __init__(self, api=None):
        super(StdOutListener, self).__init__()
        self.num_tweets = 0

    ######################################################################
    # For each status event
    ######################################################################

    def on_status(self, status):
        ##################################################################
        # Structure of the tweepy status object
        # {
        #  'contributors': None,
        #  'truncated': False,
        #  'text': 'My Top Followers in 2010: @tkang1 @serin23 @uhrunland @aliassculptor @kor0307 @yunki62. Find yours @ http://mytopfollowersin2010.com',
        #  'in_reply_to_status_id': None,
        #  'id': 21041793667694593,
        #  '_api': <tweepy.api.api object="" at="" 0x6bebc50="">,
        #  'author': <tweepy.models.user object="" at="" 0x6c16610="">,
        #  'retweeted': False,
        #  'coordinates': None,
        #  'source': 'My Top Followers in 2010',
        #  'in_reply_to_screen_name': None,
        #  'id_str': '21041793667694593',
        #  'retweet_count': 0,
        #  'in_reply_to_user_id': None,
        #  'favorited': False,
        #  'retweeted_status': <tweepy.models.status object="" at="" 0xb2b5190="">,
        #  'source_url': 'http://mytopfollowersin2010.com',
        #  'user': <tweepy.models.user object="" at="" 0x6c16610="">,
        #  'geo': None,
        #  'in_reply_to_user_id_str': None,
        #  'created_at': datetime.datetime(2011, 1, 1, 3, 15, 29),
        #  'in_reply_to_status_id_str': None,
        #  'place': None
        # }
        # </tweepy.models.user></tweepy.models.status></tweepy.models.user></tweepy.api.api>
        ##################################################################

        if status.lang not in ['en', 'no']:
            return True
        message = {}
        message['language'] = status.lang
        message['author'] = status.user.screen_name
        message['followers_count'] = status.user.followers_count
        message['friends_count'] = status.user.friends_count
        message['text'] = status.text
        message['created_at'] = status.created_at.isoformat()
        message['coordinates'] = None
        if status.coordinates:
            message['coordinates'] = status.coordinates.coordinates

        # Prints the text of the tweet
        # print ('%d,%d,%d,%s,%s' % (status.user.followers_count, status.user.friends_count,status.user.statuses_count, status.user.id_str, status.user.screen_name))
        # return False

        # publishes the message to the kafka topic
        producer.send(config.KAFKA_TOPIC, message)
        self.num_tweets += 1
        if self.num_tweets % 10 == 0:
            print('Published {} tweets'.format(self.num_tweets))

        return True

    ######################################################################
    # Supress Failure to keep demo running... In a production situation
    # Handle with seperate handler
    ######################################################################

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False
        print('Got an error with status code: ' + str(status_code))
        return True  # To continue listening

    def on_timeout(self):

        print('Timeout...')
        return True  # To continue listening

######################################################################
# Main Loop Init
######################################################################


if __name__ == '__main__':

    listener = StdOutListener()

    # sign oath cert

    auth = tweepy.OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)

    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

    # uncomment to use api in stream for data send/retrieve algorythms
    # api = tweepy.API(auth)

    stream = tweepy.Stream(auth, listener)

    ######################################################################
    # Sample delivers a stream of 1% (random selection) of all tweets
    ######################################################################
    # stream.sample()

    ######################################################################
    # Custom Filter rules pull all traffic for those filters in real time.
    ######################################################################
    # Filter stream by tweets containing the WORDS_TO_TRACK
    print('Filtering tweets by: {}'.format(', '.join(config.WORDS_TO_TRACK)))
    stream.filter(track=config.WORDS_TO_TRACK)
