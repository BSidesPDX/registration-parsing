## parse.py
This script will parse the square legacy orders export csv
and parse it into useful useful csv's for workshops, tshirts, and reg
    
#### usage:
parse.py orders-legacy-DATE-DATE.csv

#### output:
TYPE, fname, lname, email, detail

where TYPE and detail may be:
* "Registration" -> donor tier
* "Shirt" -> shirt size
* "Workshop" -> specific workshop

recommend "| grep TYPE > year-type.csv", until someone fixes it to spit out separate files.

## emails.py
This script pulls emails out of the orders-legacy-DATE.csv files
and prints them in the lname,fname<email> format, one per line
will convert the square legacy orders export csv into useful csvs

#### usage:
parse.py orders-legacy-DATE-DATE.csv

#### output:
lname, fname <email>

