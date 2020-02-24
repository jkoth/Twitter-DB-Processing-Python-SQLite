import sqlite3
import urllib
import json
import time

webFD = urllib.request.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")
conn = sqlite3.connect('csc455_tweets.db')
c = conn.cursor()

start = time.time()
tweet = webFD.readline()
StaticMaxTweetRecords = 500000
VariableMaxTweetRecords = StaticMaxTweetRecords						#Initially matches Static max value but will modify within loop until Tweet Table counts match Static value. Needed to make sure final table contains as many records req. 
TweetRecords = 0
newLine = True
while (newLine):
    try:
       uData = json.loads(tweet.decode('utf8'))
    except ValueError:
       uData = None
	   
    if uData != None:
       if uData['geo'] != None and uData['place'] != None:			#Geo Table Update
           g_id = uData['place']['id']
           g_type = uData['geo']['type']
           lat = uData['geo']['coordinates'][0]
           long = uData['geo']['coordinates'][1]
           
           geo_insert_sql = 'INSERT OR IGNORE INTO TwtGeo VALUES(?,?,?,?)'                    #INSERT STATEMENT FOR GEO TABLE
           c.execute(geo_insert_sql, (g_id, g_type, lat, long))
       else:
           g_id = None
           g_type = None
           lat = None
           long = None  
		   
       u_nm = uData['user']['name']									#User Table Update
       s_nm = uData['user']['screen_name']
       desc = uData['user']['description']
       frdCt = uData['user']['friends_count']
       u_id = uData['user']['id']    
		
																	#Tweets Table Update
       rtwtst = uData.get('retweeted_status')  						#checking for retweeted_status dict
       if rtwtst != None:
          rtwtct = uData['retweeted_status']['retweet_count']
       else:
          rtwtct = uData['retweet_count']
	
       crdt = uData['created_at']
       t_id = uData['id_str']
       txt = uData['text']
       src = uData['source']
       rui = uData['in_reply_to_user_id']
       rsn = uData['in_reply_to_screen_name']
       rsi = uData['in_reply_to_status_id']
       cont = uData['contributors']
       
       user_insert_sql = 'INSERT OR IGNORE INTO TwtUsers VALUES(?,?,?,?,?)'               #INSERT STATEMENT FOR USER TABLE
       c.execute(user_insert_sql, (u_id, u_nm, s_nm, desc, frdCt))
       tweet_insert_sql = 'INSERT OR IGNORE INTO Tweets VALUES(?,?,?,?,?,?,?,?,?,?,?)'    #INSERT STATEMENT FOR TWEET TABLE
       c.execute(tweet_insert_sql, (crdt, t_id, txt, src, rui, rsn, rsi, rtwtct, cont, u_id, g_id))
       
       TweetRecords += 1
   
    tweet = webFD.readline()
    if len(tweet) < 1:
         newLine = False
    elif TweetRecords == VariableMaxTweetRecords:
         Counts = c.execute(''' SELECT COUNT(*) FROM Tweets;''').fetchall()
         if Counts[0][0] == StaticMaxTweetRecords:
            newLine = False
         else:
            newLine = True
            VariableMaxTweetRecords = VariableMaxTweetRecords + (StaticMaxTweetRecords - Counts[0][0])			  #Adding diff in MaxTweetRecords. eg. if tweet table is short by 20 records, MaxTweetRecords will be initial value plus 20 records and will continue until table reaches max req.
    else:
         newLine = True
end = time.time()

print('Part 1c Processing Time = ', (end - start), ' seconds')
Counts = c.execute(''' SELECT COUNT(*) FROM Tweets;''').fetchall()
print('Tweet Table Counts: ', Counts[0][0])
Counts = c.execute(''' SELECT COUNT(*) FROM TwtUsers;''').fetchall()
print('Users Table Counts: ', Counts[0][0])
Counts = c.execute(''' SELECT COUNT(*) FROM TwtGeo;''').fetchall()
print('Geo Table Counts: ', Counts[0][0])

webFD.close()
conn.commit()
c.close()
conn.close()
