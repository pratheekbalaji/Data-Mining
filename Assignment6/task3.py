import tweepy
import random
import sys


class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        global sequence_no
        global tweets
        global tags
        hashtag = status.entities['hashtags']

        if len(hashtag) > 0:

            if sequence_no < 101:
                tweets.append(status)

            else:
                random_index = random.randint(0, sequence_no)

                if random_index < 100:
                    tweets[random_index] = status
            for tweet in tweets:
                hashtag = tweet.entities['hashtags']
                for h in hashtag:
                    t = h['text']
                    t = t.lower()
                    if t not in tags:
                        tags[t] = 1
                    if t in tags:
                        tags[t] += 1

            highest_counts = sorted(tags.items(), key=lambda x: (-x[1], x[0]))

            max_val = -1
            r = 0
            f = open(sys.argv[2], 'a')
            f.write("The number of tweets with tags from the beginning:" + " " + str(sequence_no))
            f.write("\n")

            for h in highest_counts:
                val, freq = h[0], h[1]
                if (r == 0):
                    max_val = freq
                    r += 1
                    f.write(str(val) + "\t" + ":" + str(freq))
                    f.write("\n")
                elif (r != 3) and max_val == freq:
                    f.write(str(val) + "\t" + ":" + str(freq))
                    f.write("\n")
                elif (r != 3) and max_val != freq:
                    f.write(str(val) + "\t" + ":" + str(freq))
                    f.write("\n")
                    max_val = freq
                    r += 1
                if (r == 3):
                    f.close()
                    break
        sequence_no += 1

    def on_error(self, status_code):
        if status_code == 420:
            return False


consumer_key = '9iBGenwZsZSLTErimZ4UYfK7Y'
consumer_secret_key = 'fz2bTPaDs6z48Xf1yS4QKr96iB7bcc8a8xw19mJYJNrGmjSh7G'
access_token = '523021974-iMzHZ0jUOn0lYmdjPpct49DKYxiQC08M804MHePu'
access_token_secret = 'MQpaQZkgKPWXHZqr5CFPV5mN4F3vZPP7bZ4LL8X3OVRAY'
auth = tweepy.OAuthHandler(consumer_key=consumer_key, consumer_secret=consumer_secret_key)
auth.set_access_token(key=access_token, secret=access_token_secret)
myStreamListener = MyStreamListener()
tweets = []
tags = {}
sequence_no = 1
api = tweepy.API(auth)
myStream = tweepy.Stream(auth=auth, listener=myStreamListener)

myStream.filter(track=["Cricket", "Football", "Sachin", "Thala", "CR7", "MSD", "python", "corona", "quarantine"])
