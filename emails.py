#!/bin/python3

import csv
import sys

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


