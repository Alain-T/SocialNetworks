from urllib2 import urlopen, Request
import time
import datetime

# https://github.com/minimaxir/facebook-page-post-scraper/
# https://www.norconex.com/how-to-crawl-facebook/
# https://developers.facebook.com/tools-and-support/

def request_until_succeed(url):
    req = Request(url)
    success = False
    while success is False:
        try:
            response = urlopen(req)
            if response.getcode() == 200:
                success = True
        except Exception as e:
            print(e)
            time.sleep(5)

            print("Error for URL {}: {}".format(url, datetime.datetime.now()))
            print("Retrying.")

    return response.read()

access_token = ""

url = "https://graph.facebook.com/v2.11/me?fields=id,name&access_token=" + access_token

print(request_until_succeed(url))
