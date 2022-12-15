#! /usr/bin/python3
# collect_cd.py Copyright (C) 2021 Dennis Risen, Case Western Reserve University
#
"""
Polls the CPI ClientDetails API in real-time, writing its most significant fields
to time-stamped NNNNNNNNNN_ClientBriefv4.csv files in the ./collect_cd directory.
Every N hours, close and move the file to ./files and keeps a collection status in collect_cd.json
to facilitate appending only the new polls.
"""

from argparse import ArgumentParser
import csv
import os
import json
import re
import time

import cpiapi
from credentials import credentials
"""To Do

"""

mac_state = dict() 			# {mac: most recent record, ...}


def write_state(file_name: str, tables: dict):
    """Writes all tables' dynamic state variables to a JSON file

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
        cpiapi.logErr(f"read_state: {' '.join(no_state)}")


change_period = 4 * 60 * 60				# time period for changing output files
poll_period = 4*60
outputPath = 'collect_cd'				# directory path to output file
parser = ArgumentParser(description='Write real-time ClientDetails polls to ' + outputPath)
parser.add_argument('--user', action='store', default=None,
                    help='CPI username')
parser.add_argument('--verbose', action='count', default=0,
                    help='diagnostic messages level')
args = parser.parse_args()

# setup Cpi server instance
server = 'ncs01.case.edu'
user, password = credentials.credentials(server, username=args.user)
my_Cpi = cpiapi.Cpi(user, password)
field_names = ['polledTime', '@id', 'apMacAddress', 'apSlotId', 'associationTime',
               'deviceType', 'macAddress', 'mobilityStatus', 'protocol',
               'ssid', 'status', 'updateTime', 'userName']

tbl = cpiapi.find_table(table_name='ClientDetails', version=4, dicts=cpiapi.all_table_dicts)
# customize to get just the ASSOCIATED records
tbl.set_query_options({'.full': 'true', '.nocount': 'true', 'status': 'ASSOCIATED'})

# load table state (reset if no json to read)
read_state('collect_cd.json', {'ClientDetails': [tbl]})
# move any previous stranded 'ClientBrief' files from ./{outputPath} to ./files
brief_pat = r'([0-9]+_)(Client.+).csv'
for base_name in os.listdir(outputPath):
    m = re.fullmatch(brief_pat, base_name)
    if m:
        try:
            os.rename(os.path.join(outputPath, base_name),
                      os.path.join('files', m.group(1) + 'ClientBriefv4.csv'))
        except Exception as e:
            cpiapi.logErr(f"{e} while renaming {base_name} to ./files")

while True:								# loop forever
    # start a new output file each change_period
    base_name = f"{str(int(1000 * time.time()))}_{'ClientBrief'}{tbl.version}.csv"
    file_name = os.path.join(outputPath, base_name)
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
            # Every n minutes, append a batch of new records to outfile
            tbl.polledTime = poll_time
            dupl_cnt = rec_cnt = 0
            try:
                for rec in tbl.generator(server=my_Cpi, table=tbl, verbose=args.verbose):
                    rec_cnt += 1
                    rec['macAddress'] = mac = rec['macAddress']['octets']
                    rec['apMacAddress'] = rec['apMacAddress']['octets']
                    rec['polledTime'] = poll_time
                    prev_rec = mac_state.get(mac, None)
                    if prev_rec is not None and rec['updateTime'] == prev_rec['updateTime']:
                        dupl_cnt += 1
                        continue			# ignore oversampled data
                    rec_cnt += 1
                    mac_state[mac] = rec
                    writer.writerow(rec)
            except ConnectionAbortedError as ce:
                # new CPI version returns ConnectionAbortedError when there are no records?
                print(f"ConnectionAbortedError {ce}")
            except ConnectionError as ce:
                # Haven't been able to connect for the last (timeout+240)*(1+2+4+8+16) seconds
                print(f"ConnectionError {ce}")
                # just keep waiting for CPI to become available
            outfile.flush()
            print(f"{cpiapi.strfTime(float(poll_time))} {dupl_cnt} duplicate and {rec_cnt} new records")
            write_state('collect_cd.json', {'ClientDetails': [tbl]})
            poll_time = poll_period * (1 + int(time.time() / poll_period))
    # Move the csv file to ./files
    try:
        os.rename(file_name, os.path.join('files', base_name))
    except Exception as e:
        cpiapi.logErr(f"{e} renaming file to ./files")
