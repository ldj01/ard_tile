#!/usr/bin/python
#
#  ard_tile_stats.py
#
# This script gathers and computes stats from the ARD tile database.
#
import sys
import os
import re
from datetime import datetime
from datetime import timedelta
from argparse import ArgumentParser
import cx_Oracle

MAX_TARGET_TIME = 60    # number of minutes to use for identifying
                        # long-running tasks

PROC_BEGIN_DATE = '16-11-2018'

# Read the command line arguments.
parser = ArgumentParser(description='Get stats from the ARD tile database.')
parser.add_argument('dbcon_str', help='Database connection string')
parser.add_argument('start_date', help='Start date (dd-mm-yyyy)')
parser.add_argument('end_date', help='End date (dd-mm-yyyy)')
parser.add_argument('timestep', type=int,
                    help='Histogram time step (minutes)')
parser.add_argument('out_file', help='Output filename')
parser.add_argument('--ignore_dupes', dest='ignore_dupes',
                    action='store_true', default=False,
                    help='Ignore duplicates flag')
args = parser.parse_args()

# Connect to the database.
try:
    dbcon = cx_Oracle.connect(args.dbcon_str)
    curs = dbcon.cursor()
except:
    print("Error:  Unable to connect to the database.")
    sys.exit(1)

# Open the output file.
of = open(args.out_file, 'w')

# Extract task information for each time step.
numtasks = []
start_time = datetime.strptime(args.start_date, '%d-%m-%Y')
end_time = datetime.strptime(args.end_date, '%d-%m-%Y')
delta_time = timedelta(0, 0, 0, 0, args.timestep)
of.write('Date,  Tiles_completed')
of.write(os.linesep)
total = 0
while start_time < end_time and start_time < datetime.now():
    stop_time = start_time + delta_time
    if (args.ignore_dupes):
        curs.execute("select count(*) from "
                     "(select tile_id, date_completed, row_number() "
                     "over(partition by substr(tile_id, 1, 23) "
                     "order by date_completed asc) num "
                     "from ard_completed_tiles) "
                     "where num = 1 and date_completed >= :s "
                     "and date_completed < :e",
                     {'s': start_time, 'e': stop_time})
    else:
        curs.execute("select count(*) from ard_completed_tiles where "
                     "date_completed >= :s and date_completed < :e",
                     {'s': start_time, 'e': stop_time})
    count = curs.fetchone()[0]
    record = ' {},    {:5d}'.format(str(start_time), count)
    of.write(record)
    of.write(os.linesep)
    print(record)
    total += count
    start_time = stop_time

# Get the total number of products generated.
prod_begin_time = datetime.strptime(PROC_BEGIN_DATE, '%d-%m-%Y')
if (args.ignore_dupes):
    curs.execute("select count(distinct substr(tile_id, 1, 23)) "
                 "from ard_completed_tiles where "
                 "date_completed >= :s", {'s': prod_begin_time})
else:
    curs.execute("select count(*) from ard_completed_tiles where "
                 "date_completed >= :s", {'s': prod_begin_time})
total_overall = curs.fetchone()[0]

# Close the database connection.
curs.close()
dbcon.close()

# Close the output file.
of.close()

print '\nTotal tiles for time range: %d' % (total)
print '\nTotal tiles overall: %d' % (total_overall)

sys.exit(0)
