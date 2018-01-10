import psycopg2
import datetime
import re
import sys

user = "";
pw = "";

def setUserAndPw(u, p):
  user = u;
  pw = p;

def tableIsEmpty():
  try:
    conn = psycopg2.connect("dbname='log_to_db' user='" + user + "' host='localhost' password='" + pw + "'")

    cur = conn.cursor()
    cur.execute("SELECT * from logs")
    rows = cur.rowcount

    if rows == 0:
      return True
    else:
      return False
  except:
    print ("Unable to connect to the database")
  finally:
    conn.close()

def insertIntoDB(count, date, ip, request, status, user_agent):
  try:
    conn = psycopg2.connect("dbname='log_to_db' user='" + user + "' host='localhost' password='" + pw + "'")

    cur = conn.cursor()
    cur.execute("INSERT INTO logs values (%s, %s, %s, %s, %s, %s)", (count, date, ip, request, status, user_agent))
    conn.commit()
  except:
    print ("Unable to connect to the database")
  finally:
    conn.close()

def findQuery(startDate, duration, threshold):
  startDate = startDate.replace(".", " ")
  num = re.findall('\\d+', startDate)
  # check startDate format
  try: 
    endDate = datetime.datetime(int(num[0]), int(num[1]), int(num[2]), int(num[3]), int(num[4]), int(num[5]))
  except:
    print ("Invalid date format! Example: 2017-01-01.12:00:00")
    sys.exit()
  if duration == 'hourly':
    endDate = endDate + datetime.timedelta(hours=1)
  else:
    endDate = endDate + datetime.timedelta(days=1)

  try:
    conn = psycopg2.connect("dbname='log_to_db' user='" + user + "' host='localhost' password='" + pw + "'")
    cur = conn.cursor()
    cur.execute("""
        SELECT * 
        FROM (SELECT DISTINCT(ip), count(ip) AS count 
              FROM logs 
              WHERE date >= %s and date <= %s 
              GROUP BY ip) as new_table 
        WHERE new_table.count >= %s""", (startDate, endDate, threshold))

    print ("The following IP Addresses match your query:\n")
    rows = cur.fetchall()
    print ("| IP ADDRESS     ", "| REQUESTS |")
    print ("-----------------------------")
    for row in rows:
       print (" ", row[0], "    ", row[1])
  except (Exception, psycopg2.DatabaseError) as error:
    print (error)
  finally:
    conn.close()