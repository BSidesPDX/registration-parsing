## parse.py
This script will parse the square legacy orders export csv
and parse it into useful useful csv's for workshops, tshirts, and reg

1. Go to https://app.squareup.com/dashboard/orders/overview
2. make sure the complete date range you want is selected
2. In the upper right choose Export -> Legacy export
3. it'll take a minute, then pop up a 'download' button. If you miss it, it'll show up if you click 'export' again, and it'll email you
    
The legacy export is the format weebly used and what i generated the scripts for at first. The standard one might be easier - and not need such a mess of parsing - but i haven't figured that out yet.

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

