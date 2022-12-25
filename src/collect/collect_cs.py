#
# collect_cs.py Copyright (C) 2021 Dennis Risen, Case Western Reserve University
#
"""
Polls the CPI ClientSessions API in real-time, writing its most significant fields
to time-stamped NNNNNNNNNN_ClientSessionsv4.csv files in the collect_cs sub-directory.
It starts a new file every N hours, and keeps a collection status in collect_cs.json
to facilitate appending only the new polls.
"""
from argparse import ArgumentParser
import csv
import os
import json
import time

import cpiapi
from mylib import credentials, logErr
""" To Do
- Fix the poller. After an initial read through 8 x 1000001 records, it repeatedly
sleeps 299 seconds and reads nothing
"""


def write_state(file_name: str, tables: dict):
    """Writes all table's dynamic state variables to a JSON file

    Parameters:
        file_name (str):	file_name to write JSON-encoded state
        tables (dict):		{tableName:table, ...}

    """
    state = dict()
    for key in tables:				# collect state of each table_name in production
        t = tables[key][-1]				# (last) table with this table_name
        state[key] = {'lastId': t.lastId, 'minSec': t.minSec,
            'maxTime': t.maxTime, 'nextPoll': t.nextPoll,
            'recordsPerHour': t.recordsPerHour, 'startPoll': t.startPoll}
    with open(file_name, 'w') as json_file:
        json.dump(state, json_file) 	# dump as JSON to file


def read_state(file_name: str, tables: dict):
    """Sets the dynamic state attributes in table from a json file

    Parameters:
        file_name (str):		file_name of json-encoded state
        tables (dict):		{tableName: table, ...}
    """
    try:
        with open(file_name, 'r') as json_file:
            states = json.load(json_file)  # read saved state of each table
    except FileNotFoundError:			# file doesn't exist
        print(f"read_state({file_name}) FileNotFoundError. Continuing as if --reset.")
        return
    no_state = []
    fields = ('lastId', 'minSec', 'lastSec', 'maxTime', 'nextPoll', 'recordsPerHour', 'startPoll')
    for key in tables:					# loop through all table_names
        tables_w_key = tables[key]
        table = tables_w_key[0]			# will restore only the first
        state = states.get(key, None)
        if state is not None:			# table_name is in the saved state?
            for attr in fields:			# Yes. import state values
                # allow a file with deprecated or missing attributes
                try:
                    setattr(table, attr, state[attr])
                except KeyError:
                    if attr == 'maxTime':
                        setattr(table, attr, state['maxSec'])  # ***** remove after transition from maxSec to maxTime
        else:							# No. Note missing state in log
            no_state.append(f"{table.tableName} not in file.")
        tables_w_key = tables_w_key[1:]
        for table in tables_w_key: 		# log table(s) that won't be updated
            no_state.append(f"{table.tablename} {table.version} not updated.")
    if len(no_state) > 0:
        logErr(f"read_state: {' '.join(no_state)}")


change_period = 4 * 60 * 60				# time period for changing output files
# change_period = 15 * 60				# time period for changing output files
poll_period = 5*60
outputPath = 'collect_cs'				# directory path to output file
parser = ArgumentParser(description='Write real-time ClientSessions polls to ' + outputPath)
parser.add_argument('--user', action='store', default=None,
                    help='CPI username')
args = parser.parse_args()
# setup Cpi server instance
server = 'ncs01.case.edu'
user, password = credentials.credentials(server, username=args.user)
my_Cpi = cpiapi.Cpi(user, password)
field_names = ['polledTime', '@id', 'apMacAddress', 'macAddress', 'protocol',
               'ssid', 'sessionEndTime', 'sessionStartTime', 'userName']

tbl = cpiapi.find_table(table_name='ClientSessions', version=4, dicts=cpiapi.all_table_dicts)
# customize to get every record, not just the changes in
# tbl.set_time_field(None)  # supported in future version
tbl.timeField = None
tbl.set_pager('hist_pager')

# load table state (reset if no json to read)
read_state('collect_cs.json', {'ClientSessions': [tbl]})

while True:								# loop forever
    # start a new output file each change_period
    file_name = os.path.join(
        outputPath,
        f"{str(int(1000 * time.time()))}_{tbl.tableName}{tbl.version}.csv")
    # start each file with a csv header
    with open(file_name, mode='w', newline='') as outfile:
        writer = csv.DictWriter(outfile, field_names, extrasaction='ignore')
        writer.writeheader()
        poll_time = poll_period * (1 + int(time.time() / poll_period))
        change_time = change_period * (1 + int(poll_time / change_period))
        rec_cnt = 0
        while poll_time < change_time:
            sleep_time = poll_time - time.time()
            if sleep_time > 0:
                print(f"sleeping {sleep_time:2.0f} seconds")
                time.sleep(sleep_time)
            # Every 5 minutes, append a batch of new records to outfile
            tbl.polledTime = poll_time
            for rec in tbl.generator(server=my_Cpi, table=tbl):
                rec_cnt += 1
                rec['macAddress'] = rec['macAddress']['octets']
                rec['apMacAddress'] = rec['apMacAddress']['octets']
                rec['polledTime'] = poll_time
                writer.writerow(rec)
                if rec_cnt > 1000000:
                    print(f"{rec_cnt} records")
                    rec_cnt = 0
                    break
            outfile.flush()
            write_state('collect_cs.json', {'ClientSessions': [tbl]})
            poll_time = poll_period * (1 + int(time.time() / poll_period))
