#!/usr/bin/python3
#
# collect.py Copyright (C) 2019 Dennis Risen, Case Western Reserve University
#
"""
Continuously collect statistics from CPI APIs that are listed in
cpiapi.production and write each sample to ./files
"""
from argparse import ArgumentParser
from collections import defaultdict
import json
import os
import pprint
import os
import sys
import threading
from time import sleep, time
from typing import Dict, Union

from cpiapi import add_table, all_table_dicts, allTypes, Cpi, date_bad, \
    find_table, Pager, production, real_time, SubTable, Table, to_enum
from mylib import anyToSecs, credentials, strfTime, logErr, printIf, verbose_1

TAU = 20 			# time-constant for recordsPerHour learning. Samples or Days
sampling = [-1, 20, 1]  # initialize down-counter to sample [no, nth, every] record
""" To do
- implement ``--exclude`` option to exclude selected table from collection
- in HistoricalRF*, the collectionTime is corrected and output in csv with +0400 offset.
    Sample done at 2019-10-01-T20:32 contains data for up to 2019-10-02-T04:20:30+0400.
    This is not impossible since it is almost 60 minutes before sample time, because EDT is -0500.
    However allToSec(maxTime) is 2019-10-01-22:00:14, which doesn't even appear in the collectionTime data
    2+ hours ahead of actual EDT local time of the sample
    histPager(record) is called on raw record. It stores final collectionTime text w/o flatten's time correction
- find and fix all lines with *****
- When Cisco has fixed UTC zone formatting, remove DateBad correction in flatten() 
- change dar5 password on ncs01
- install certificate(s)
- Write report for correct data in op/group/sites
- when ingesting into the data lake
    create username table (UID, username) and allow access only to the UID
    when creating the AWS tables: define roles; GRANT SELECT (almost *) to PUBLIC;
    use available DB-native types such as JSON, INET, MACADDR, ...

Not to do
- report fields with total count != recCnt
- csPager add polling in the danger zone
- Write a test daemon, then configure this as a daemon


"""


def check_fields(table: SubTable, record: dict):
    """Sample [no | every n'th | every] record based on table.check_[enums|fields].
    For each enum field, count the instances by value.
    for each field, count the instances by types.

    :param table:       the [Sub]Table
    :param record:      a flattened record
    """
    table.check_enums -= 1
    table.check_fields -= 1
    if table.check_enums == 0:          # Counter underflow?
        for key, val in record.items():  # Yes -- sample. Count values by ENUM field
            ftk = table.fieldTypes.get(key, {'values': None})  # field's type definition
            if isinstance(ftk['values'], dict):  # defined as an ENUM?
                table.field_values[key][val] += 1  # count by enum value
        table.check_enums = table.sample_enums
    if table.check_fields == 0:         # Counter  underflow?
        for key, val in record.items():  # Yes -- sample. Count types by field
            table.field_counts[key][type(val)] += 1  # Number of instances of each type
        table.check_fields = table.sample_fields


def check_fields_init(sub_table: SubTable, fields: int = 0, enums: int = 0):
    """Initialize table for check_fields and field_report.

    :param sub_table:   Table or SubTable
    :param fields:  count field value type(s) in {0=no, 1=sample, 2=all) records
    :param enums:   count ENUM values in {0=no, 1=sample, 2=all) records
    """
    sub_table.field_counts = defaultdict(lambda: defaultdict(int))  # count of instances of each field
    sub_table.field_values = defaultdict(lambda: defaultdict(int))  # count of each value for each enum field
    sub_table.check_enums = sub_table.sample_enums = sampling[min(enums, len(sampling))]
    sub_table.check_fields = sub_table.sample_fields = sampling[min(fields, len(sampling))]


def collect(tables: dict, real_time: bool = False, semaphore: threading.Semaphore = None):
    """Collect the tables in ``tables`` dictionary

    :param tables:      tables to collect {table_name: table, ...}
    :param real_time:   True for real-time priority collector
    :param semaphore:   semaphore (threading.Semaphore): the low-priority collector's Semaphore
    :return:
    """
    my_name = '+' if real_time else '-'
    print(my_name)
    myCpi = Cpi(args.username, args.password,
                baseURL=f"https://{args.server}/webacs/api/",
                semaphore=None if real_time else semaphore)  # CPI server instance
    # Obtain CPI's rate limiting parameters
    tableURL = 'v4/op/rateService/rateLimits'
    cpiReader = Cpi.Reader(myCpi, tableURL, verbose=verbose_1(args.verbose))
    for rec in cpiReader:
        Cpi.maxResults = rec['limitPageSize']
        Cpi.rateWait = Cpi.windowSize = rec['windowSize'] / 1000.0
        Cpi.segmentSize = Cpi.windowSize / rec['windowSegments']
        Cpi.perUserThreshold = rec['perUserThreshold']
        break  							# only expecting the single record
    else:  								# there was no record
        logErr(f"Could not read CPI's {tableURL}")
        sys.exit(1)
    # ClientSessions API frequently times-out with TIMEOUT set to 120
    myCpi.TIMEOUT = 180.0

    if not args.reset:  				# user didn't ask to reset?
        # Restore state from previous run
        read_state('realtime.json' if real_time else 'collect.json', tables)

    # On start-up, collection might be well behind schedule.
    # Reduce batch sizes to reduce the maximum time until each table's oldest
    # records are collected.
    if not real_time:
        Pager.timeScaleSave = Pager.timeScale
        Pager.timeScale = Pager.timeScale / 2  # 1/2-size batches until 1st sleep

    loopForever = True
    cycle = 1                           # report immediately
    while loopForever:
        cycle -= 1
        loopForever = not args.single  	# Loop forever, except for a single poll
        if args.verbose > 0 and cycle == 0:  # Every n'th job retrieval ...
            # ... Report the current scheduling information for each table
            cycle = 10
            print(f"\n{my_name}{'lastId':>12},{'minSec':>20},{'maxTime':>20},"
            + f"{'nextPoll':>20},{'startPoll':>20}, recs/Hr, Hrs, tableName")
            # Produce report of polled tables sorted by ascending nextPoll
            by_nextPoll = [(tbl_lst[0].nextPoll, tbl_lst[0], tbl_name) for tbl_name, tbl_lst in tables.items()]
            by_nextPoll.sort(key=lambda x: [x[0], x[2]])
            for nxt_poll, tbl, tbl_name in by_nextPoll:
                print(f"{my_name}{tbl.lastId:12}, ", end='')
                print(','.join("{:>20}".format(strfTime(t) if isinstance(t, str) and t != '0' or t > 0 else '-')
                    for t in (tbl.minSec, tbl.maxTime, tbl.nextPoll, tbl.startPoll)), end='')
                secs = anyToSecs(tbl.maxTime)
                hours = int((time() - secs) / 3600) if secs is not None and secs != 0.0 else ''
                print(f",{int(tbl.recordsPerHour):8},{hours:4}, {tbl.tableName}")

        # Check for pending shutdown here, if daemon were implemented

        # Select highest priority table by (1)overdue poll then (2)min nextPoll time
        tbl: Union[Table, None] = None
        nextPoll = sys.float_info.max
        for tbl_name in tables:  		# over all names in the tables dict
            if len(args.table_name) > 0 and tbl_name not in args.table_name:
                continue				# ignore this table_name
            tbls = tables[tbl_name]  	# [table, ...] with shared table_name
            for t in tbls:  			# for all versions of a table_name
                tblnp = t.nextPoll
                if t.polled and tblnp < time():  # overdue polled?
                    tbl = t  			# Yes, any overdue poll is 1st priority
                    break  			# break out of nested loops w/ answer is table
                if tblnp < nextPoll:  	# earlier nextPoll needed?
                    tbl, nextPoll = t, tblnp  # Yes, update w/ better 2nd priority
            else:  						# inner loop didn't find a 1st priority
                continue  				# so continue looking for best 2nd priority
            break  						# inner loop found a 1st priority
        if tbl is None:					# no table to find
            print(f"No {'real time' if real_time else 'production'} tables to poll")
            return
        delta = tbl.nextPoll - time()
        if delta > 0 and loopForever:  	# sleep except for single table
            # Update daemon status here, if daemon were implemented
            if args.verbose > 0:
                print(f"{my_name} Sleeping {int(delta)} seconds", end=' ', flush=True)
            sys.stdout.flush()
            sleep(delta)  			# Sleep until requested poll time
            if not real_time:
                Pager.timeScale = Pager.timeScaleSave  # Caught-up. Restore batch size
        # Update daemon status, if daemon were implemented

        # Collect requested records and write to csv file stamped with epochMillis
        tbl.polledTime = time()
        version = tbl.version if tbl.version != "v1" else ""
        file_name = os.path.join(outputPath,
            f"{str(int(1000 * tbl.polledTime))}_{tbl.tableName}{version}")
        printIf(True, f"{my_name}Collecting {file_name} ", end='')
        # check table's fields and enums if not checked in the last 24 hours
        occasional = 1 if tbl.polledTime - tbl.checked_time > 24*60*60 else 0
        if occasional and tbl.tableName == 'ClientDetails':
            tbl.minSec = 0 	# occasionally get all, not just the updated, records

        # initialize for check_fields and field_report

        check_fields_init(tbl, 2 if args.fields == 2 else occasional*args.fields,
                          2 if args.enums == 2 else occasional*args.enums)

        tbl.open_writer(file_name)  	# open table DictWriter; write header record
        for name in tbl.subTables:  	# for each sub_table ...
            sub_table = tbl.subTables[name]
            sub_table.recCnt = 0
            if len(sub_table.select) == 0:  # ... any fields defined to be output?
                continue  				# No. So don't open an output file
            sub_table.open_writer(tbl.file_name + '_' + name)  # Open its DictWriter
            check_fields_init(sub_table,
                              2 if args.fields == 2 else occasional*args.fields,
                              2 if args.enums == 2 else occasional*args.enums)
        first_rec = True
        first_time = lastTime = None  	# init for case that table has <2 records
        if real_time:					# Am I the priority collector?
            semaphore.acquire()			# Yes. Pause the low-priority collector
            sleep(Cpi.windowSize) 		# age-out other Cpi's activity.
        success = True					# Assume successful collection
        try:
            for rec in tbl.generator(server=myCpi, table=tbl, verbose=verbose_1(args.verbose)):
                flat = dict()
                # Flatten tree into a single level dict with hierarchical field names.
                # Recursively output sub_table records, not incl. in flattened results
                flatten(rec, flat, tbl, '')
                if tbl.timeField is not None:  # is there a timeField?
                    if first_rec: 		# Yes
                        lastTime = first_time = flat[tbl.timeField]  # remember 1st value
                        first_rec = False
                    else:
                        lastTime = flat[tbl.timeField]  # remember last value
                try:
                    tbl.writer.writerow(flat)
                except (UnicodeError, UnicodeEncodeError): 	# csv's strict decode to ASCII failed
                    for fld in flat: 	# convert str to ascii w/ backslash where necessary
                        if isinstance(flat[fld], str):
                            flat[fld] = flat[fld].encode('utf-8').decode('ascii', 'backslashreplace')
                    tbl.writer.writerow(flat)
                check_fields(tbl, flat)
        except (ConnectionAbortedError, ConnectionError, ConnectionRefusedError) as e:
            success = False  # collection failed. Will close, but not rename output
            logErr(f"{my_name}{e} reading {tbl.tableName}")
            tbl.nextPoll = time() + 4*60*60  # wait 4 hours before trying again
        if real_time:					# Am I the priority collector?
            sleep(Cpi.windowSize) 		# Yes. age-out my Cpi's activity.
            semaphore.release()			# release the low-priority collector
        # When reading has completed: report, close, and rename the output
        if tbl.sample_fields > 0 or tbl.sample_enums > 0:
            an_err, results = field_report(tbl, args.check_verbose)
            if an_err:
                logErr(results)
            elif len(results) > 0:
                print(my_name+results)

        errorCnt = getattr(tbl, 'errorCnt', 0)
        if args.verbose > 0 and errorCnt > 0:  # report unresponsive APs?
            print(f"{my_name}{errorCnt} errors reading {tbl.tableName}")
        # Adjust the recordsPerHour
        if tbl.polled:  				# is table polled? (state is all records)
            new_RPH = ((TAU - 1) * tbl.recordsPerHour + tbl.recCnt)/TAU
        elif tbl.recCnt == 0 or not success:  # incomplete collection?
            new_RPH = tbl.recordsPerHour  # Yes. don't attempt to compute new RPH
            # 0 records could indicate a problem
            if success:
                logErr(f"No records collected for {tbl.tableName}. Multiple Historical tables reporting\n"
                + "this infers the CPI database was reset. If so, run me once with --reset option.\n"
                + "If only some/one CPI Historical table was reset, kill collect and zero\n"
                + "its 'lastId' entry in collect.json; and restart collect.")
        else:  						# table is not polled (batch is a time series)
            now = time()
            try:  						# in case of bad time data
                first_time = anyToSecs(first_time)
                lastTime = anyToSecs(lastTime)
                if (now - first_time) > tbl.rollup or (now - lastTime) > tbl.rollup:
                    # Some records might be missing. Can't learn from this
                    new_RPH = tbl.recordsPerHour
                else:  					# time range is good
                    hours = (lastTime - first_time)/(60 * 60)
                    # weighted by number of hours of data
                    new_RPH = ((TAU - hours/24) * tbl.recordsPerHour + (hours/24) * (tbl.recCnt/hours))/TAU
            except TypeError:  			# ignore bad time data
                new_RPH = tbl.recordsPerHour  # default bad timeFields
                logErr(f"{tbl.tableName} bad batch timeFields: {first_time} - {lastTime}")
            except ZeroDivisionError:  # change in time was zero
                new_RPH = tbl.recordsPerHour  # default if we no change in timeFields
        printIf(args.verbose > 0,
                f"{my_name}{tbl.recCnt:,} records processed. lId={tbl.lastId}, lS={strfTime(tbl.minSec)},",
                f"mS={strfTime(tbl.maxTime)}, nP={strfTime(tbl.nextPoll)}", end=' ')
        if args.verbose > 0 and new_RPH != tbl.recordsPerHour:
            print(f"rph: {int(tbl.recordsPerHour)}-->{int(new_RPH)}")
        elif args.verbose > 0:
            print('')
        else:
            pass
        tbl.recordsPerHour = new_RPH

        tbl.close_writer('part' if real_time else 'csv', rename=success)  # close & rename file extn
        if occasional:				# This the periodic checking of fields and enums?
            tbl.checked_time = tbl.polledTime  # Yes. Done for another day

        # report, close, and rename each of the subTables' output files
        for name in tbl.subTables:
            sub_table: SubTable = tbl.subTables[name]
            if len(sub_table.select) == 0:  # any fields Selected for output?
                continue  				# No. So no file was opened.
            if sub_table.sample_fields > 0 or sub_table.sample_enums > 0:
                an_err, results = field_report(sub_table, args.check_verbose)
                if an_err:
                    logErr(results)
                elif len(results) > 0:
                    print(my_name+results)
            sub_table.close_writer('part' if real_time else 'csv', rename=success)  # close & rename extn
        # A collection completed.
        if not args.single:  			# not --single
            write_state('realtime.json' if real_time else 'collect.json', tables)  # save state
        sys.stdout.flush()


def field_report(sub_table: SubTable, verbose: int) -> tuple:
    """Report each field that is extra, missing, undefined or type/value mis-matched

    :param sub_table:   table to report
    :param verbose:     True to report all counts
    :return: (error, str,)	(True if report found error(s), report string)
    """
    def details(enum: bool, defined: object, findings: dict) -> tuple:
        """Report details for a single attribute

        :param enum:        True if this is an ENUM; otherwise False
        :param defined:     {name:index, ...} if ENUM else type(expected data)
        :param findings:    {(value if ENUM else class):count, ...}
        :return: (error, str):	(True iff error(s), details string,)
        """
        error = False
        v = []
        for found in findings:			# for each value found
            err = (not enum) and found != defined or enum and found not in defined
            error = error or err
            if err or verbose > 0: 		# unexpected or verbose listing?
                v.append((findings[found], found if enum else str(found)[8:-2]))
        if len(v) == 0:					# nothing found?
            return error, '' 			# return (False, empty string)
        return error, ', '.join(f"{x} {y}" for x, y in v)  # (flag, formatted list)

    # Report field data which is not typed as defined
    error, result = False, ''
    field_types = sub_table.fieldTypes 	# {tableFieldName:definition, ...}
    field_counts = sub_table.field_counts  # {dataFieldName:{type:count, ...}, ...}
    field_values = sub_table.field_values  # (dataFieldName:{value:count, ...}, ...}
    select = sub_table.select			# [fieldName, ...]
    if len(select) == len(sub_table.key_defs):  # no data defined for output?
        return error, result			# Yes, nothing to report
    # Each Selected field must be present and correct enum values
    for key in select:					# for each Selected field
        ft = field_types[key]
        if key in field_counts:			# Any data found?
            err, s = details(False, ft['type'], field_counts[key])
            error = error or err
            if len(s) > 0:				# any detail to report?
                result += f"{sub_table.tableName}.{key} has {s}\n"  # report it
            del field_counts[key] 		# delete each matched field_counts entry
        else:							# no data found
            result += f"{sub_table.tableName}.{key} has no data\n"
        if key in field_values:			# values found for an ENUM field?
            err, s = details(True, ft['values'], field_values[key])
            error = error or err
            if len(s) > 0:				# any detail to report?
                result += f"{sub_table.tableName}.{key} has {s}\n"  # report it
    # field_counts contains only fields that are not Selected (i.e. expected)
    for key in field_counts:			# for each unexpected field
        if key in field_types:			# known field?
            check = field_types[key].get('check', True)  # field should be checked?
        else:
            check = True
        if verbose > 0 or check:
            err, s = details(False, None, field_counts[key])
            error = error or (check and err)
            result += (sub_table.tableName
                       + (' unSELECTed' if key in field_types else ' unknown')
                       + f" field {key} has {s}\n")
    return error, result


def flatten(tree: dict, result: dict, table: Table, path: str = ''):
    """Flattens a tree of dicts to a single level dict with pathname keys.
    Process sub_table: On the way down, replace xxxs:{xxx:list} with xxxs:list
    On the way back up, write each record in the list as a table_things record
    and do not include in the flattened results


    :param tree:    input tree of dict
    :param result:  output flattened dict
    :param table:   Table defining the fields and sub_tables
    :param path:    pathname to top of the tree
    """
    # first, process each simple element, because it might be a name
    for key in tree:
        new_path = key if path == '' else (path+'_'+key)  # append name to pathname
        val = tree[key]
        ft = table.fieldTypes.get(new_path, None)  # defined field type dict()
        if table.subTables.get(new_path, None) is not None or isinstance(val, dict):
            continue 	# defer processing of sub_table or compound until 2nd pass
        elif isinstance(val, list): 	# a table not defined as a subTable?
            ft = table.fieldTypes.get(path, None)  # parent's type...
            if ft is not None and ft['name'] == 'array':  # ... is an array?
                # Each sub-table with name 'xxxs', is {'xxx':[...]}
                result[path] = val 		# Yes, return the JSON named as parent
            else:						# No, error
                logErr(f"Undefined array of {path} in {table.tableName}")
                table.field('List', path, False)  # define it to silence msgs
                result[path] = val
        elif ft is None:				# undefined simple field?
            result[new_path] = val 		# pass through to output
        elif ft['name'] == 'DateBad' and isinstance(val, str):  # Text with bad UTC
            result[new_path] = date_bad(val)
        else:
            result[new_path] = val
    # then process sub_table and compound elements that may reference the name(s)
    for key in tree:
        new_path = key if path == '' else (path+'_'+key)  # append name to pathname
        val = tree[key]
        sub_table = table.subTables.get(new_path, None)
        if sub_table is not None: 		# sub_table
            if len(sub_table.select) == len(sub_table.key_defs):
                continue				# no Selected fields to output
            try:						# navigate to the list
                lst = val[key[:-1]]
            except KeyError:
                logErr(f"sub-table {key} does not have {key[:-1]} element")
                raise TypeError
            if not isinstance(lst, list):
                logErr(f"sub-table {key}[{key[:-1]}] is not a list")
                raise TypeError
            for rec in lst:
                # copy primary name:values into each sub-table record
                for t, k in sub_table.key_defs:
                    try:
                        rec[k] = result[k]
                    except KeyError:
                        logErr(f"Error copying results[{k}] to record[{k}] in record {table.recCnt}")
                        raise KeyError
                rec_dict = dict()
                flatten(rec, rec_dict, table, '')  # recurse w/ results to rec_dict
                check_fields(sub_table, rec_dict)
                empty = True
                for k in rec_dict:
                    if k in sub_table.select:
                        empty = False 	# at least one field is selected for output
                        break
                if not empty:			# any field(s) present to output?
                    try:
                        sub_table.writer.writerow(rec_dict)  # yes, write to output
                    except (UnicodeError, UnicodeEncodeError):  # csv's strict convert to ascii failed
                        for fld in rec_dict:  # convert str to ascii w/ backslash where necessary
                            if isinstance(rec_dict[fld], str):
                                rec_dict[fld] = rec_dict[fld].encode('utf-8').decode('ascii', 'backslashreplace')
                        sub_table.writer.writerow(rec_dict)  # yes, write to output
                    sub_table.recCnt += 1
            # Note that subTable does not return anything into parent results
        elif isinstance(val, dict): 	# compound structure
            flatten(val, result, table, new_path)
        else:
            continue					# primitive was processed during first pass


def write_state(file_name: str, tables: dict):
    """Write all tables' dynamic state variables to a JSON file

    :param file_name:   file_name to write JSON-encoded state
    :param tables:      {tableName:table, ...}
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
    """Sets all tables dynamic state variables from a json file

    :param file_name:   file_name of json-encoded state
    :param tables:      {tableName: table, ...}
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


# Parse command line for arguments
parser = ArgumentParser(description='Write statistics from CPI as csv files to directory')
parser.add_argument('directory')
parser.add_argument('--checkVerbose', action='store_true', dest='check_verbose',
    default=False, help="turn on detailed field checking reporting")
parser.add_argument('--enum', action='store_true', default=False,
                    help='output all ENUM DDL')
parser.add_argument('--enums', action='count', default=0,
                    help="Count of each ENUM value: {never, once daily every n'th record, always}")
parser.add_argument('--exclude', action='append', default=[],
                    help="exclude table. Use multiple --exclude to exclude multiple tables")
parser.add_argument('--fields', action='count', default=0,
                    help="Count missing, unknown or mis-typed field {never, once daily every n'th record, always}.'")
parser.add_argument('--hive', action='store_true', default=False,
                    help='output Hive data definition for selected table')
parser.add_argument('--password', action='store', default=None,
                    help="password to use to login to CPI")
parser.add_argument('--prefix', action='store', default='data',
                    help="API prefix. Default = data")
parser.add_argument('--realtime', action='store_true', default=False,
                    help='Run real-time collection in a separate thread')
parser.add_argument('--reset', action='store_true', default=False,
    help='Skip loading saved state. I.e. start from nothing saved.')
parser.add_argument('--server', action='store', default="ncs01.case.edu",
                    help='CPI server name. default=ncs01.case.edu')
parser.add_argument('--single', action='store_true', default='',
    help="Do single poll (of highest priority table) and don't update saved state")
parser.add_argument('--SQL', action='store_true', default=False,
                    help='output SQL data definition for specified table')
parser.add_argument('--table', action='append', dest='table_name', default=[],
    help='Operate only on listed tables. Use multiple --table for multiple tables.')
parser.add_argument('--username', action='store', default=None,
                    help="user name to use to login to CPI")
parser.add_argument('--ver', action='store', default=None,
                    help="API version. Default matches any version")
parser.add_argument('--verbose', action='count', default=0,
                    help="diagnostic messaging level")
args = parser.parse_args()

modules = ['charset-normalizer', 'filelock', 'idna', 'jmespath', \
           'platformdirs', 'python-dateutil', 'pytz', 'requests', 'six', \
           'type-extensions', 'typing_extensions', 'urllib3']
"""
lst = [(mn, module) for mn, module in sys.modules.items()]
lst.sort()
for mn, module in lst:
    ver = getattr(module, '__version__', 'unknown')
    print(mn, ver)
sys.exit()
"""
len_production = len(production)
for tn in args.table_name:              # add each table explicitly requested
    if tn not in production:		    # not normally a production table?
        tbl = find_table(tn, all_table_dicts, args.ver)
        if tbl is not None:			    # However, have a definition for the table?
            print(f"{tn} definition added for this job")  # Yes
            add_table(production, tbl)  # add table to production for this job
        else:
            print(f"table {tn} is not defined")
for tn in args.exclude:         # remove each table to be excluded from collection
    if tn in production:
        del production[tn]
    if tn in args.table_name:
        del args.table_name[tn]
to_ignore = [tn for tn in production if tn not in args.table_name]  # Yes.
# Reduce wait times when catching-up on fewer tables
# Fewer files in parallel * less total impact
Pager.catchup = Pager.catchup*(1+len(production))/len_production**2

if args.SQL or args.hive:				# output table definitions?
    for table_name in args.table_name:
        tbl = find_table(table_name, all_table_dicts, args.ver)
        if tbl is None:
            print(f"Unknown table {table_name}")
            continue
        if args.SQL:
            print(tbl.to_sql())
        if args.hive:
            # print(table.toHive())
            print(pprint.pformat(tbl.table_columns(), indent=2, width=120))
    sys.exit(0)

if args.enum:
    # print all ENUM definitions
    print(to_enum(allTypes))
    sys.exit(0)

outputPath = args.directory
if not os.path.isdir(outputPath):       # Destination directory is missing?
    os.mkdir(outputPath)                # Yes. Create it
if args.password is None:				# No password provided?
    try:
        cred = credentials(args.server, args.username)
    except KeyError:
        print(f"No username/password credentials found for {args.server}")
        sys.exit(1)
    args.username, args.password = cred
# The real_time and low_priority collect threads each have their own Cpi instance.
# This allows the real_time to block the low_priority for the duration of each poll.
semaphore = threading.Semaphore()		# for realtime to be able to block myCpi
if args.realtime and len(real_time) > 0:  # start real-time thread too?
    print('Starting realtime collect')
    rt = threading.Thread(name='realtime', target=collect,
            kwargs={'tables': real_time, 'real_time': True, 'semaphore': semaphore})
    rt.start()
if len(production) > 0:
    collect(tables=production, real_time=False, semaphore=semaphore)

if args.realtime and len(real_time) > 0:	 # Real-time collection started?
    rt.join()							# Yes. Join before exiting
