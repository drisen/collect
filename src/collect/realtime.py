#!/usr/bin/python3
# realtime.py Copyright (C) 2019 Dennis Risen, Case Western Reserve University
#
import sys
import os.path
from argparse import ArgumentParser
import csv
import json
import re
import urllib3
import time
import math
import statistics

from cpiapi import Cpi, logErr, printIf
from credentials import credentials
"""
The service-provider layer for application(s) that provide realtime
advice based on the number of people (actually client devices) in a Wi-Fi
cell zone.
Currently broken, in transition from a working proof-of-concept.
"""

''' To do
Save current sample for each group.
If the current sample is identical to the previous, drop it w/o outputting it.
Recover if error when writing. Either create new file with version number,
or drop this sample w/ logging to log.

'''

star = None				# expansion of "*", the list of all all attribute names
groupHeaders = None		# dict of csv header rows for each group
aggFunctions = {"average", "count", "min", "max", "sum"}  # valid functions


def aggregate(op: list, keyVals: set, records: dict) -> float:
    """Aggregate one attribute across all the retrieved records.
    Numeric functions ignore missing or non-numeric values.
    Parameters:
        op		(list):		[function, attribute]
            function (str):	function: "average", "count", "min", "max", or "sum"
            attribute (str): name of field to aggregate
        keyVals (set):		set of key values to include in aggregation
        records	(dict):		{key:{'time':float, 'record':dict}}
    Returns:
        (float) for "average" or when some values are float; otherwise (int)
    """

    funct = op[0]				# the aggregation function
    attr = op[1]				# the attribute name
    minMax = None				# result if there are no record values
    total = None				# ditto
    count = 0					# for "average" or "count"
    for key in records:			# consider all retrieved records
        if key not in keyVals:
            continue			# ignore record
        rec = records[key]['record']  # a record
        if funct == "count":
            try:
                if len(rec[attr].rstrip()) > 0:
                    count += 1
            except:
                pass
            continue
        try:					        # convert string to numeric
            x = float(rec[attr])        # get floating value
            if x == int(x):		        # if value is integral
                x = int(x)		        # then cast as int
        except (KeyError, ValueError):  # attribute is missing or not numeric
            continue			        # ignore this record
        if funct in {"average", "sum"}:
            if total is None:
                total = x
            else:
                total += x
            count += 1
        elif funct == "max":
            if minMax is None or x > minMax:
                minMax = x
        elif funct == "min":
            if minMax is None or x < minMax:
                minMax = x
        else:
            logErr(f"Unknown aggregate function: {funct}")
            sys.exit(1)
    try:						# return appropriate result
        if funct == "average":
            return float(total)/count
        elif funct == "count":
            return count
        elif funct in {"min", "max"}:
            return minMax
        elif funct == "sum":
            return total
        else:
            return None
    except (ValueError, ZeroDivisionError):
        return None				# divide by 0 --> no average


def simplySample(seconds: int):
    """Sample forever the table every 'seconds' seconds. Output with writeGroups()
    Parameters:
        seconds (int):	sample period in seconds
    """
    while True:
        time.sleep(time.time() % seconds)
        sample, i = tableGet(args.table, args.attrs, args.where, args.key, args.timestamp)
        writeGroups(sample, args.select, args.groups)


def tableGet(tableURL: str, attrs: set, filters: dict,
             keyAttr: str, timeAttr: str) -> tuple:
    """GET the contents of a table and return as a list of records, each a dict;
    or sys.exit(1) on unrecoverable error
    Parameters:
        tableURL (str):		API URL relative to baseURL
        attrs	(set):		attribute names to return, or None to return all attributes
        filters	(dict):		additional filtering to apply in the URL
        keyAttr (str):		entity's primary key attribute
        timeAttr (str):		record timestamp in seconds/options.timescale
    Side-effect may change the value of global options.attrs
    Returns:
        (list):	{key:{'time':float, 'record':dict}}
        (int):	time (in seconds) spent sleeping
    """
    global args		# when options.attrs is None, may update it to full set of attribute names
    printIf(args.verbose,
            f"tableGet(tableURL={tableURL}, attrs={attrs}, keyAttr={keyAttr}, timeAttr={timeAttr},")
    printIf(args.verbose, f"  filters={filters})")
    records = dict()	            # result starts as empty dict
    recCnt = 0			            # number of records retrieved so far
    tableReader = Cpi.Reader(myCpi, tableURL, filters)
    for row in tableReader:
        if tableReader.recCnt == 1:
            # setup the requested attribute list
            if attrs is None:
                # caller did not specify specific attributes, return all of the attributes
                attrs = list(row.keys())
                attrs.sort()
                args.attrs = attrs
            # ensure that the table includes required key and timestamp attributes
            if not(keyAttr in row):
                logErr(tableURL, f"1st record is missing the key:{keyAttr}")
                print(row)
                sys.exit(1)
            if not(timeAttr in row):
                logErr(tableURL, f"1st record is missing the timestamp:{timeAttr}")
                sys.exit(1)
        record = dict()
        errs = False
        for att in attrs:   	    # ... copy each requested attribute to record
            try:
                record[att] = row[att]
            except KeyError:
                logErr(f"Record number {tableReader.recCnt} is missing attribute {att}")
                errs = True
        if errs:
            print(str(row)[:2000])
            sys.exit(1)
        records[row[keyAttr]] = {'time': float(row[timeAttr])/args.timescale, 'record': record}

    # fall-through to here when all rows have been received and processed
    return records, tableReader.sleepingSeconds


def writeGroups(tableRecs: dict, selects: list, groups: dict):
    """For each group, format the selects fields into a csv record appended to that group's file.
    Parameters:
    tableRecs	(dict):	{key:{'time':float, 'record':dict}}
    selects		(list):	list of: attribute | "*" | [function,attribute]
    groups		(dict):	{groupName:{keyval1, ..., keyvaln}}
    """

    global groupHeaders			# (dict):	{groupName:["time", col2, ..., coln]}
    global star					# [attr1, attr2, ..., attrn]
    if len(tableRecs) == 0:		# no records this sample?
        printIf(args.verbose, 'WriteGroups called with 0 records in sample')
        return					# Ignore this sample
    if groupHeaders is None:
        # For each group, set its headers and write header to the group output file
        # define the expansion for "*" from the first retrieved record
        groupHeaders = dict()
        if len(tableRecs) == 0:
            print('Could not initialize headers, because the first sample returned no records')
            sys.exit(1)
        # calculate "*", the list of all record attributes in the first record
        star = list()
        for key in tableRecs:
            rec = tableRecs[key]['record']
            for attr in rec:
                star.append(attr)
            break
        printIf(args.verbose, f"*={star}")
        for groupName in groups: 	# build header; and output header
            keyVals = groups[groupName]
            header = ["Date"]
            firstVal = True
            for keyVal in keyVals:
                keyName = re.sub(r'\W', '_', keyVal)  # map each non-word char to "_"
                i = 0
                while i < len(selects):
                    select = selects[i]
                    if isinstance(select, list):  # aggregate function
                        if not select[1] in star:
                            print(f"{select[1]} is not a table attribute")
                            sys.exit(1)
                        if firstVal:
                            header.append(select[1] + "_" + select[0])
                    elif select == "*":  # replace "*" with all attributes
                        selects.remove(i)
                        for attr in star:
                            selects.insert(i, attr)
                            header.append(keyName + "_" + attr)
                            i += 1
                        i -= 1
                    else:				# simple attribute
                        if select not in star:
                            print(f"{select} is not a table attribute")
                            sys.exit(1)
                        header.append(keyName + "_" + select)
                    i += 1
                firstVal = False
            if not os.path.isfile(os.path.join(args.path, groupName) + '.csv'):
                # Initialize the output file for this group with its header row
                printIf(args.verbose, f"header={header}")
                with open(os.path.join(args.path, groupName) + '.csv', "w", newline='') as csvFile:
                    csvWriter = csv.writer(csvFile)
                    csvWriter.writerow(header)
            groupHeaders[groupName] = header
        printIf(args.verbose, f"groupHeaders={groupHeaders}")

    for groupName in groups:		# for each group that we are monitoring
        keyVals = groups[groupName]
        if len(tableRecs) == 0:
            break					# nothing to do if there are no records
        for key in tableRecs:
            # initialize csv record w/ time as first field
            csvRec = [time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(tableRecs[key]['time']))]
            break					# value from first record is sufficient
        firstVal = True				# generate aggregates only with 1st attrs
        for keyVal in keyVals:
            for select in selects: 	# for each element in the --select clause
                if isinstance(select, list):  # aggregate(attribute)
                    if firstVal:
                        csvRec.append(aggregate(select, keyVals, tableRecs))
                else:				# simple attribute
                    if keyVal in tableRecs:
                        csvRec.append(tableRecs[keyVal]["record"][select])
                    else:
                        csvRec.append(None)
            firstVal = False
        # append the record to the csv file for this group
        with open(os.path.join(args.path, groupName) + '.csv', 'a', newline='') as csvFile:
            csvWriter = csv.writer(csvFile)
            csvWriter.writerow(csvRec)


parser = ArgumentParser(description=('''Sample selected entities in a table at CPI's sample rate.
Write to [multiple] groupname.csv files
groups is a JSON-formatted dict of {"groupname":{"keyval1", ..., "keyvaln"}}'''))
# parser.add_argument('groups', nargs='*')
parser.add_argument('--select', action='store',
    default='[["sum", "authCount"], ["sum", "count"], "authCount", "count"]',
    help='''JSON-formatted list of attributes to return. e.g. ["name", "mapLocation"].
"*" as an item represents all attributes.
A list, [function, attr], returns the aggregate function to all instances of attr.
Aggregate functions are: average, count, min, max, sum.''')
parser.add_argument('--where', action='store',
    default='{"type":"ACCESSPOINT", "subkey":"All"}',
    help='json-formatted dict of attribute:value pairs for filtering. E.g. {"name":"value", "type":"value"}')
# parser.add_option('--groups', action='store',
# 	default='''{"Tomlinson":["84:b8:02:ad:a3:e0","84:b8:02:b6:a9:50","84:b8:02:bf:45:30"],
# 	    "Veale":["34:a8:4e:3b:c6:20","34:a8:4e:1e:c5:80","34:a8:4e:1e:c8:80",
# 	    "34:a8:4e:3b:be:80","34:a8:4e:d3:31:b0","34:a8:4e:d3:28:10",
# 	    "34:a8:4e:3b:bd:d0","34:a8:4e:d3:1d:90","34:a8:4e:6b:ab:30",
# 	    "34:a8:4e:3b:c4:90","34:a8:4e:d2:ee:30","34:a8:4e:d3:1a:e0"]}''',
# 	help='''JSON-formatted dict of {groupName:[keyval1, ..., keyvaln]} defines the entities in each group.
# 	Writes a separate output file, named path\group.csv, for each group.
# 	dict==None outputs all entities to table.csv''')
parser.add_argument('--groups', action='store',
    default='{"Tomlinson":["84:b8:02:ad:a3:e0","84:b8:02:b6:a9:50","84:b8:02:bf:45:30"]}',
    help='''JSON-formatted dict of {groupName:[keyval1, ..., keyvaln]} defines the entities in each group.
Writes a separate output file, named path\\group.csv, for each group.
dict==None outputs all entities to table.csv''')
parser.add_argument('--key', action='store', default='key',
                    help="name of the entity's key field")
parser.add_argument('--path', action='store', default='',
                    help='directory path for output file(s)')
parser.add_argument('--password', action='store', default=None,
                    help="password to use to login to CPI")
parser.add_argument('--seconds', action='store', type=int, default='0',
                    help='Simply sample every nnn seconds. 120<=nnn')
parser.add_argument('--table', action='store', default='v4/data/ClientCounts',
                    help='relative URL to table. default=v4/data/ClientCounts')
parser.add_argument('--timescale', action='store', type=int, default='1000',
                    help="units for timestamp, in resolution/second")
parser.add_argument('--timestamp', action='store', default='collectionTime',
                    help="name of the table's timestamp attribute")
parser.add_argument('--username', action='store', default='nbiread',
                    help="user name to use to login to CPI")
parser.add_argument('--verbose', action='count', default=False,
                    help="diagnostic message level")
args = parser.parse_args()

# validate --select clause syntax; and calculate set of attributes to retrieve
try:
    args.select = json.loads(args.select)
except:
    print(f"--select={args.select} is not valid JSON")
    sys.exit(1)
args.attrs = set([args.key])		    # always retrieve key
for item in args.select:
    if isinstance(item, str) and item == "*":
        args.attrs = None			    # retrieve all attributes
        break
    elif isinstance(item, str):
        args.attrs.add(item)			# add this attribute to the set
    elif isinstance(item, list) and len(item) == 2:
        if not item[0] in aggFunctions:
            print(f"--select {item[0]} is not a valid aggregation function")
            sys.exit(1)
        args.attrs.add(item[1])		    # add this attribute to the set
    else:
        print(f"--select item {item} is not a string or 2-element list")
        sys.exit(1)

# validate --where clause syntax
try:
    args.where = json.loads(args.where)
except:
    print(f"--where={args.where} is not valid JSON")
    sys.exit(1)
if not isinstance(args.where, dict):
    print(f"--where={args.where} must be a dict")
    sys.exit(1)

# validate --groups clause syntax
# re-cast value-list as a set (which, JSON can't represent)
# and integrate key values with --where clause
if len(args.groups) == 0:				# when no groups were specified ...
    tableName = re.split(r'/', args.table)
    tableName = tableName[len(tableName)-1]
    args.groups = {tableName: None}     # use single group for specified table
else:
    # parse the JSON groups specification
    try:
        args.groups = json.loads(args.groups)
    except:
        print(f"--groups={args.groups} is not valid JSON")
        sys.exit(1)
# if --where clause does not have a term for key, then
# key values to retrieve is union of the key values from all groups
if args.key not in args.where:
    allVals = set()
    for grpName in args.groups:
        if args.groups[grpName] is None or len(args.groups[grpName]) == 0:
            allVals = set()			    # none or empty set means All
            break
        args.groups[grpName] = set(args.groups[grpName])
        allVals |= args.groups[grpName]

    # construct key=in("val1", ..., "valn") filter term
    valFilter = '","'.join(allVals)
    if len(valFilter) > 0:
        valFilter = 'in("' + valFilter + '")'
        if args.key in args.where and len(args.where[args.key]) > 0:
            pass
        else:
            args.where[args.key] = valFilter

if args.password is None:		        # No password provided?
    cred = credentials.credentials('ncs01.case.edu', args.username)
    if cred is None:				    # credentials() couldn't find either?
        print(f"No username/password credentials found for ncs01.case.edu")
        sys.exit(1)
    args.username, args.password = cred
myCpi = Cpi(args.username, args.password)  # CPI server instance
myCpi.TIMEOUT = 90.0    	    # 30 second default and 60 second often timeout

if args.verbose:					    # list the options that will be used
    print(f"--select={args.select}")
    print(f"attrs={args.attrs}")
    print(f"--where={args.where}")
    print(f"--groups={args.groups}")
    print(f"--key={args.key}; --path={args.path}; --seconds={args.seconds}; --timestamp={args.timestamp}")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 	# cpi has local certificates

if args.seconds > 0:		            # output a sample every 'seconds' seconds
    if args.seconds < 120:
        print('Sampling period may not be less then 120 seconds')
        sys.exit(1)
    simplySample(args.seconds)
    sys.exit()

'''
Assumptions:
  The jitter and drift between the system's clocks is insignificant.
  CPI's polling period is stable.
  Each cycle of polling completes within the polling period.
  There is a stable portion of each polling cycle in which none of the requested entities are sampled. Either:
    CPI completes each polling in significantly less time than the polling period, or
    CPI samples in essentially the same order each polling cycle,
    and sampled entities' sample times are not uniformly distributed
  
Goals
- Minimize the maximum delay in delivering samples through to output.
- Deliver samples to the output on a regular schedule, with little jitter.
- During normal operation, sample no more than twice per polling period.
- The minimum time between samples is 1/5 actual polling period.
'''

alpha = 0.25			    # gain
expectedPeriod = 5.0*60     # expected CPI sampling period
period = -1.0			    # CPI sampling period in seconds; >0 --> not discovered
periodStd = -1			    # Standard deviation in CPI sampling period
# loop forever
while True:
    if period < 0:
        # CPI's sampling period is not yet discovered. Discover it.
        # Collect samples every expectedPeriod/5 until each timestamp has changed.
        # There is some offset between the two system's clocks, and there is some delay
        # between sample being posted in CPI and is available in a database search.
        # Represent the sum of these as offsetT.
        # A sample initiated at time T+offsetT will, on average,
        # just retrieve a sample posted at time=timestamp/timescale.
        # For each sample, refine offsetT.
        # 	offsetT = min(offsetT, T - min(time[i])
        # 	for successive samples, offsetT = max(offsetT, T - max(previous.time[i]))
        # Calculated period = avg(each)(Tn[i]-T1[i])
        # Calculated periodStd = 2*sqrt(sum(m){(Tn[i]-T1[]-period)^2}/m)
        # the doubling is because we're interested in the STD of the latter samples in the polling cycle.

        # Collect the 1st sample
        sampleNum = 1		# this is the 1st sample.
        t1 = time.time()
        sample1, i = tableGet(args.table, args.attrs, args.where, args.key, args.timestamp)
        if i > 0:
            continue		# errors caused sleeping and retry(s). Restart.
        # ***** should offsetT be offsetTmin and offsetTmax? *****
        offsetT = t1 - min(rec['time'] for rec in sample1)
        # consprntIftruct a dict of the 'key': 'time' pairs in sample1
        dict1 = {rec['key']: rec['time'] for rec in sample1}
        # initialize a set of 'time'+'key' samples
        sampleDict = {str(rec['time'])+str(rec['key']): {rec['key'], rec['time']} for rec in sample1}
        printIf(args.verbose, 'offsetT=', offsetT, 'seconds')
        continueMain = False
        while time.time() < t1+6*expectedPeriod:
            sampleNum += 1
            time.sleep(t1+sampleNum*60-time.time())
            tn = time.time()
            samplen, i = tableGet(args.table, args.attrs, args.where, args.key, args.timestamp)
            if i > 0:
                # errors caused sleeping and retry(s).
                logErr('Sleeping upset 1st sample')
                continueMain = True 	# Restart
                break
            if len(sample1) != len(sampleN):
                # unusual ... different number of items
                logErr('Sample', sampleNum, 'has different number of items')
                continueMain = True
                break
            offsetT = min(offsetT, tn - min(rec['time'] for rec in samplen))
            printIf(args.verbose, 'minimized offsetT=', offsetT, 'seconds')
            offsetT = max(offsetT, tn - max(rec['time'] for rec in samplen))
            .(args.verbose, 'maximized offsetT=', offsetT, 'seconds')
            # add samples into the 'time'+'key' dictionary
            for rec in samplen:
                sampleDict[str(rec['time'])+str(rec['key'])] = {rec['key'], rec['time']}
            # Are all of the times changed from sample1?
            for rec in samplen:
                if rec['key'] in dict1:
                    dict['key']['timeN'] = rec['time']
                    if rec['time'] != dict1[rec['key']]['time']:
                        allequal = False
                        break
                else:
                    # the new sample has an id that is not in sample1 !
                    allequal = False
                    continueMain = True
                    break
            else:
                allequal = True
            if continueMain:
                break
            if not allequal:
                continue 	            # get another sample
            # every timestamp has changed
            period = statistics.mean(rec['timeN']-rec['time'] for rec in dict1)
            periodStd = statistics.sdev((rec['timeN']-rec['time'] for rec in dict1), mu=period)
            sampleList = list(v for k, v in sampleDict)
            sampleList.sort(key=lambda x: x['time'])
            sampleList = sampleList[:2*len(samplen)] 	# 2 polling cycles of samples sorted by timestamp
            # find the starting time of the longest time gap between timestamps
            gapMax = -1
            gapStart = -1
            for rec in sampleList:
                if gapMax < 0:		# first element in list
                    gapMax = 0
                    gapStart = rec['time']
                    timePrev = gapStart
                else:				# every other element in the list
                    if rec['time']-timePrev >= gapMax:
                        gapMax = rec['time'] - timePrev
                        timePrev = rec['time']

            printIf(args.verbose, 'Found', gapMax, 'second gap in', period,
                    'second period with', periodStd, 'standard deviation')
            # output sampleN
            summary = tableSummary(args.attrs, samplen)
            summarySaveCsv(basefilename + str(tn), summary, args.csv)

            # During steady-state, poll twice per CPI period in order to phase-lock
            # sampling to the gap in CPI's sampling.
            # The 1st sample is polled gapMax - deltaT before the estimated beginning
            # of the gap, and the 2nd sample is polled deltaT after the estimated
            # beginning of the gap.
            # DeltaT is chosen to be significantly greater than sampling jitter
            # and significantly less than the length of the gap.
            # In the 1st sample, initiated at time t1, the end of the previous gap
            # is marked by the minimum timestamp > t1-period+deltaT
            # In the 2nd sample, initiated at time t2=t1+2*deltaT, the start of
            # the next gap is marked by the maximum timestamp
            # The two samples facilitate keeping the sampling synchronized to the gap.
            # pick a deltaT between such that std<<delta and delta<<period

            gapStartPrevious = nnn
            break
        else:
            # dropped through loop without collecting at least 2 periods of samples
            continueMain = True
            mylib.logErr('Could not collect 2 periods of samples. Sleeping and trying again.')
            time.sleep(10*60)

    if continueMain:
        continue		# inner loop requested to continue the main loop
    # If last successful sample was so far in the past that we might have drifted beyond the +/-deltaT capture band
    if (((time.time()+offsetT)-sometime)/period)*periodStd > deltaT/2:
        # ???
        continue
    # steady-state processing
    toSleep = (gapStartPrevious + period - deltaT) - (time.time()+offsetT)
    if toSleep < 0:
        # We've somehow missed the intended polling time. Wait for the next opportunity.
        toSleep = period*math.ceil(-toSleep/period)
        mylib.logErr('Missed a 1st poll time. Trying again at next cycle in', toSleep, 'seconds')
    time.sleep(toSleep)
    t1 = time.time()
    sample1, i = tableGet(args.table, args.attrs, args.where, args.key, args.timestamp)
    if i > deltaT/2:
        # sampling was delayed enough to possibly mislocate the gap
        mylib.logErr('Errors in 1st poll delayed the poll by', i, 'seconds. Will wait until next cycle.')
        # ???
        continue
    toSleep = t1+2*deltaT-time.time()
    time.sleep(toSleep)
    t2 = time.time()
    sample2, i = tableGet(args.table, args.attrs, args.where, args.key, args.timestamp)
    # output the sample
    summary = tableSummary(args.attrs, sample2)
    summarySave(basefilename + str(t2), summary, args.csv)
    if i > deltaT/2:
        # sampling was delayed enough to possibly mislocate the gap.
        mylib.logErr('Errors in 2st poll delayed the poll by', i, 'seconds. Will wait until next cycle.')
        # ???
        continue
    prevGapEnd = min(rec['time'] if rec['time'] > nnn else float("inf") for rec in sample1)
    thisGapSart = max(rec['time'] for rec in sample2)
    # ???
    # Calculate time offset from perfectly in phase. If beyond deltaT/2, cause resynchronization.
    # ???
    if abs(nnn) > deltaT/2:
        # ???
        continue
    # Assign
    toSleep = (previousTime + period) - time.time()
