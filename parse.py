#!/bin/python3

import csv
import sys

if len(sys.argv) < 2:
    print("""This script will parse the square legacy orders export csv
and parse it into useful useful csv's for workshops, tshirts, and reg
    
    usage: parse.py orders-legacy-DATE-DATE.csv

    output: TYPE, fname, lname, email, detail

    where TYPE and detail may be:
        "Registration" -> donor tier
        "Shirt" -> shirt size
        "Workshop" -> specific workshop

    recommend "| grep TYPE > year-type.csv", until someone fixes it to spit out separate files.
    """)
    exit()
filename=sys.argv[1]

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
            #if itemid is workshop - not yet updated for 2025
            if row[31]=="22":
                #foreach registration
                for i in range(int(row[35])):
                    #get the workshop time and print it
                    workshop=row[34][row[34].find(":")+2:row[34].find(":",14)]
                    print(",".join(["workshop",fname,lname,email,workshop]))
            else:
                #foreach con registration
                for i in range(int(row[35])):
                    item=row[34]
                    desc="other"
                    size="None"
                    level=""
                    #2025 looks like "Regular, Supporter : 1, Fitted Cut L : 1"
                    if (item.count(",")==2):
                        desc, level, size = item.split(", ")
                        level=level[:-4]
                        size=size[:-4]
                        if (level=="None") or (level.count("Cut")>0):
                            level,size=size,level
                    if size != "None":
                        print(",".join(["Shirt",lname,fname,email,size]))
                    print(",".join(["Registration",lname,fname,email,level]))



