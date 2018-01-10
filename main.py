#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# The goal is to write a parser in Python that parses web server access log file, 
# loads the log to PostgreSQL and checks if a given IP makes more than a certain 
# number of requests for the given duration.
#
# Perry Raskin
#
from PgAccess import setUserAndPw, tableIsEmpty, insertIntoDB, findQuery
import argparse

def main():
    """ IMPORT LOGS TO DATABASE """
    count=0;
    print ("Enter your PostgreSQL username: ")
    sqlUser = input()
    print("Enter your PostgreSQL password: ")
    sqlPw = input();
    setUserAndPw(sqlUser, sqlPw)

    if tableIsEmpty():
      try:
         inFile = open("access.log", "r")
         for line in inFile:
            count = count + 1
            part = line.split('|')
            date = part[0]
            ip = part[1]
            request = part[2]
            status = part[3]
            user_agent = part[4]
            insertIntoDB(count, date, ip, request, status, user_agent)
            print ("Inserting log #", count, "...")

         inFile.close()
         print ("LOGS table has been loaded.")

      except Exception as e:
         raise e
    else: 
      print ("[LOGS table is already loaded.]\n")

    """ COMMAND LINE PARSING """
    parser = argparse.ArgumentParser(description='Provide startDate, duration, and threshold')
    parser.add_argument('--startDate', action="store", required=True)
    parser.add_argument('--duration', action="store", required=True)
    parser.add_argument('--threshold', action="store", type=int, required=True)

    # generate regex for date format
    correctFormat = True


    try:
      args = parser.parse_args()
      startDate = args.startDate
      # check if duration is either 'hourly' or 'daily'
      if (args.duration == 'hourly' or args.duration == 'daily'):
         duration = args.duration
         threshold = args.threshold
         findQuery(startDate, duration, threshold)
      else:
         print ('Invalid duration choice. Please try `hourly` or `daily`.')

    except:
      print ('Invalid input. Be sure your format is: `python main.py --startDate=2017-01-01.00:00:00 --duration=hourly --threshold=100`')


    
    return 0

if __name__ == "__main__":
    main()