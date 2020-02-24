import urllib
import json
import time

webFD = urllib.request.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")
wrtFile = open('Jubin_Kothari_Final_Tweets_500K.txt', 'w')

start = time.time()
tweet = webFD.readline()
StaticMaxTweetRecords = 500500										#Dumping more data to avoid shortfall due to duplicate records
TweetRecords = 0
newLine = True
while (newLine):
    try:
       uData = json.loads(tweet.decode('utf8'))
    except ValueError:
       uData = None
	   
    if uData != None:
       json.dump(uData, wrtFile)
       wrtFile.write('\n')
       TweetRecords += 1
	   
    tweet = webFD.readline()
    if len(tweet) < 1:
         newLine = False
    elif TweetRecords == StaticMaxTweetRecords:
         newLine = False
    else:
         newLine = True
end = time.time()

print('Part 1b Processing Time for ', StaticMaxTweetRecords, ' records: ', (end - start), ' seconds')

wrtFile.close()
webFD.close()
