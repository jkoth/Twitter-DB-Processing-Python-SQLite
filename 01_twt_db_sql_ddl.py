DrpGTbl = 'DROP TABLE IF EXISTS TwtGeo;'
DrpUTbl = 'DROP TABLE IF EXISTS TwtUsers;'
DrpTTbl = 'DROP TABLE IF EXISTS Tweets;'

GTbl = '''CREATE TABLE TwtGeo
(
  g_id VARCHAR(16) CONSTRAINT TwtGeoPK PRIMARY KEY,
  g_type VARCHAR(20),
  latitude REAL,
  longitude REAL
);'''

UTbl = '''CREATE TABLE TwtUsers
(
  id INTEGER CONSTRAINT TwtUsersPK PRIMARY KEY, 
  name VARCHAR(50),
  screen_name VARCHAR(75),
  description VARCHAR(250),
  friends_count INTEGER
);'''

TTbl = '''CREATE TABLE Tweets
(
  created_at DATE,
  id_str VARCHAR(20) CONSTRAINT TweetsPK PRIMARY KEY,
  text VARCHAR(140),
  source VARCHAR(250),
  in_reply_to_user_id INTEGER,
  in_reply_to_screen_name VARCHAR(75),
  in_reply_to_status_id INTEGER,
  retweet_count INTEGER,
  contributors VARCHAR(200),
  user_id VARCHAR(30)
    CONSTRAINT TweetsFK_UserID
    REFERENCES TwtUsers(id),
  geo_id VARCHAR(16)
    CONSTRAINT TweetsFK_GeoID
    REFERENCES TwtGeo(g_id)
);'''

import sqlite3

conn = sqlite3.connect('csc455_tweets.db')
c = conn.cursor()
c.execute(DrpGTbl)
c.execute(GTbl)
c.execute(DrpUTbl)
c.execute(UTbl)
c.execute(DrpTTbl)
c.execute(TTbl)

conn.commit()
c.close()
conn.close()
