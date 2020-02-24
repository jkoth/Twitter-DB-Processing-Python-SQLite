import sqlite3
import csv

conn = sqlite3.connect('csc455_tweets.db')
c = conn.cursor()

#Part 4 - a
InsertSQL = '''INSERT INTO TwtGeo VALUES('Unknown',NULL,0,0);'''

QRY = '''UPDATE TwtGeo
         SET latitude = ROUND(latitude, 4), longitude = ROUND(longitude,4);'''		 

c.execute(QRY)
c.execute(InsertSQL)

OutputQRY = '''SELECT *
               FROM TwtGeo;'''

output = c.execute(OutputQRY).fetchall()

outFile = open('Jubin_Kothari_Final_Part_4_GeoTable.csv','w',newline='', encoding='utf-8')       #opening in 'utf-8' format to allow certain values to be written properly
wrt = csv.writer(outFile,delimiter = '|')
for row in output:
    wrt.writerow(row)
outFile.close()

#Part 4 - b
QRY = '''UPDATE Tweets
		 SET geo_id = 'Unknown'
		 WHERE geo_id IS NULL;'''
		 
OutputQRY = '''SELECT *
               FROM Tweets;'''

output = c.execute(OutputQRY).fetchall()

outFile = open('Jubin_Kothari_Final_Part_4_TweetTable.csv','w',newline='', encoding='utf-8')      #opening in 'utf-8' format to allow certain values to be written properly
wrt = csv.writer(outFile,delimiter = '|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
wrtRow = []
for row in output:
    for val in row:
        if val != None:
           val = str(val).replace('\n','')
           val = str(val).replace('\r','')
        wrtRow.append(val)
    wrt.writerow(wrtRow)
    wrtRow = []
outFile.close()

UnknownLocQry = '''SELECT COUNT(*)
                 FROM Tweets
                 WHERE geo_id = 'Unknown'; '''

LocQry = '''SELECT COUNT(*)
            FROM Tweets; '''

c.execute(QRY)
Total_Loc_Ct = c.execute(LocQry).fetchall()
Unknown_Loc_Ct = c.execute(UnknownLocQry).fetchall()

print((Total_Loc_Ct[0][0] - Unknown_Loc_Ct[0][0]), 'known,', (Unknown_Loc_Ct[0][0]), 'unknown,', round((Unknown_Loc_Ct[0][0]/Total_Loc_Ct[0][0]),2), '% locations are available')


#Part 4 - c
OutputQRY = '''SELECT TwtUsers.id, TwtUsers.name, TwtUsers.screen_name, TwtUsers.description, TwtUsers.friends_count,
               CASE subqry.TF
                 WHEN 'TRUE' THEN 'TRUE'
                 ELSE 'FALSE'
               END AS 'true/false'
               FROM TwtUsers LEFT JOIN (SELECT id, 'TRUE' as TF
                                        FROM TwtUsers
                                        WHERE screen_name LIKE '%' || name || '%'
                                           OR description LIKE '%' || name || '%') subqry
                             ON (TwtUsers.id = subqry.id);'''

output = c.execute(OutputQRY).fetchall()

outFile = open('Jubin_Kothari_Final_Part_4_UserTable.csv','w',newline='', encoding='utf-8')     #opening in 'utf-8' format to allow certain values to be written properly
wrt = csv.writer(outFile,delimiter = '|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
wrtRow = []
for row in output:
    for val in row:
        if val != None:
           val = str(val).replace('\n','')
           val = str(val).replace('\r','')
        wrtRow.append(val)
    wrt.writerow(wrtRow)
    wrtRow = []
outFile.close()

conn.commit()
c.close()
conn.close()
