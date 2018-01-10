import json

import tweepy


# override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):
    def __init__(self):
        super().__init__()

        self.max_status = 100
        self.nb_status = 0

    def keep_alive(self):
        """Called when a keep-alive arrived"""
        return

    def on_exception(self, exception):
        """Called when an unhandled exception occurs."""
        print("MyStreamListener.on_exception()")
        print(exception)

    def on_limit(self, track):
        """Called when a limitation notice arrives"""
        print("MyStreamListener.on_limit({})".format(track))

    def on_timeout(self):
        """Called when stream connection times out"""
        print("MyStreamListener.on_timeout()")

    def on_disconnect(self, notice):
        """Called when twitter sends a disconnect notice

        Disconnect codes are listed here:
        https://dev.twitter.com/docs/streaming-apis/messages#Disconnect_messages_disconnect
        """
        print("MyStreamListener.on_disconnect({})".format(notice))

    def on_warning(self, notice):
        """Called when a disconnection warning message arrives"""
        print("MyStreamListener.on_warning({})".format(notice))

    def on_status(self, status):
        self.nb_status += 1
        print('{}/{} : {}'.format(self.nb_status, self.max_status, status.created_at))

        with open('tweets_filter.txt', 'a') as tweet_file:
            s = json.dumps(status._json)
            print(s, file=tweet_file)

        if self.nb_status >= self.max_status:
            return False
        else:
            return True

    def on_error(self, status_code):
        print("MyStreamListener.on_error({})".format(status_code))
        # Enhance Your Calm
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

        return False


def get_tweepy_api():
    consumer_key = 'YMvYTBA9MM8RssmqaFMOfTsKL'
    consumer_secret = '95vqPAXq5WZjL6BESarw7DqS8NyaOsiZ7hBTmMseT2feeHj3eP'

    access_token = '939044482632253441-J87ueSVpbWhHxzRT0ATgjV3VvZHS70A'
    access_token_secret = 'qJRoXfVggjOghBhuZDDugic2yQ5TyudJH0zzvgmAaJNxe'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth_handler=auth, wait_on_rate_limit=False, wait_on_rate_limit_notify=False)

    return api


def main():
    api = get_tweepy_api()

    my_stream_listener = MyStreamListener()
    my_stream = tweepy.Stream(auth=api.auth, listener=my_stream_listener)

    # empty file
    with open('tweets_filter.txt', 'w') as tweet_file:
        pass

    # https: // developer.twitter.com / en / docs / tweets / filter - realtime / guides / basic - stream - parameters
    # locations = [ -122.75, 36.8, -121.75, 37.8 ] # San Francisco
    # locations = [ -74,40,-73,41 ] # New York City
    # locations = [ 5.28, 43.22, 5.55, 43.38 ]
    locations = [4.5, 43.0, 7.6, 45]

    # my_stream.filter(track = ['twitter'])
    # my_stream.filter(track=['travail'], languages=['fr'], locations=locations)
    # my_stream.filter(track=['travail'], languages=['fr'])
    # myStream.filter(locations = [ -6.38,49.87,1.77,55.81 ])
    # my_stream.filter(locations=locations)
    my_stream.filter(languages=['fr'], track=["a"])


if __name__ == '__main__':
    main()
