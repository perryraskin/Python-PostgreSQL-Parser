import psycopg2
import re  
text = '2017-01-01.01:00:21' 
print (re.findall('\\d+', text))

user = "ctp_user";
pw = "";

conn = psycopg2.connect("dbname='log_to_db' user='" + user + "' host='localhost' password='" + pw + "'")

cur = conn.cursor()
cur.execute("select * from logs where id=302")
print (cur.fetchall())
conn.close()