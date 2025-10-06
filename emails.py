#!/bin/python3

import csv
import sys

if len(sys.argv) < 2:
    print("""This script pulls emails out of the orders-legacy-DATE.csv files
and prints them in the lname,fname<email> format, one per line
will convert the square legacy orders export csv into useful csvs

    usage: parse.py orders-legacy-DATE-DATE.csv

    output: lname, fname <email>
""")
    exit()
filename=sys.argv[1]
print(filename)


filename=sys.argv[1]
print(filename)

with open(filename,'r') as infile:
    csv=csv.reader(infile)
    email=""
    fname=""
    lname=""
    for row in csv:
        if row[16] != "":
            fname=row[14]
            lname=row[15]
            email=row[16]
        else:
            print(lname+",",fname,"<"+email+">")


