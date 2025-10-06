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
            address=",".join(row[14:24])
        else:
            #if itemid is workshop
            if row[31]=="22":
                #foreach registration
                for i in range(int(row[35])):
                    #get the workshop time and print it
                    workshop=row[34][row[34].find(":")+2:row[34].find(":",14)]
                    print(",".join(["workshop",workshop,fname,lname,email]))
            else:
                #foreach con registration
                for i in range(int(row[35])):
                    item=row[34]
                    size="None"
                    level="None"
                    #if before,during tshirt sales:
                    if len(item)>312:
                        delimiter=item.rfind(":")
                        size=item[delimiter+2:]
                        if size=="Mens 4XL": print("***:"+address)
                        if size=="None": size=""
                        level=item[273:delimiter-71]
                    else:
                        level=item[273:]
                    print(",".join(["Registration",lname,fname,email,level,size]))


