#
# sessions.py Copyright (C) 2020 by Dennis Risen, Case Western Reserve University
#

"""
Report the clientSessions for client associations with APs during time window
Not fully debugged due to persistent failure of the required CPI APIs.
"""
from argparse import ArgumentParser
from collections.abc import Sequence
import csv
import os.path
import sys
import time
from typing import Dict, Union

from cpiapi import Cpi
from mylib import credentials, anyToSecs, millisToSecs, printIf, strfTime, verbose_1
"""
"""

'''
To Do
Use startTime that is 10 minutes earlier
Update baddate handling immediately after time change
Apply ' --> " change to table.py
Why does HistoricalRFStats return just a few records?  w/o filtering on mac it returns ~58 records per sample time
'''


def get_vals(filename: str) -> set:
    """Read filename csv file and return the set of values in the 1st column

    :param filename:    file name of a csv file
    """
    with open(filename, 'r', newline='') as val_file:
        val_reader = csv.reader(val_file)
        vals = set()
        for rec in val_reader:
            vals.add(rec[0])
    return vals


def collect(tablename: str, filters: dict, cumulatives: Sequence,
        selected: Sequence,	msecTime: bool, transforms, macFunc, sortFunc):
    """Collect table, filtered by 'filters' and 'macfunc', transform 'cumulatives' attributes to incremental
    apply specific 'transforms', sort by sortFunc, and output the 'selected'

    :param tablename:   name of the table (i.e. CPI API)
    :param filters:
    :param cumulatives:
    :param selected:
    :param msecTime:
    :param transforms:
    :param macFunc:     function(record) to retrieve the MAC address
    :param sortFunc:    sortFunc(x) -> sorting key for x
    :return:
    """
    filename = tablename + args.endTime.replace(':', '') + '.csv'
    if os.path.exists(filename): 		# does the file already exist?
        while True:
            response = input(f"{filename} exists. Overwrite? y/n: ")
            if response.upper() == 'Y':
                break
            elif response.upper() == 'N':
                return
            else:
                pass
    printIf(args.verbose, f"reading {tablename}")
    f = {'.full': 'true', '.nocount': 'true'}  # standard filters
    if msecTime:
        f['collectionTime'] = f"between({str(start_msec)},{str(end_msec)})"
    else:
        f['collectionTime'] = f'between("{start_time_bad}","{end_time_bad}")'
    f.update(filters)					# update with any specific filters
    reader = Cpi.Reader(myCpi, 'v4/data/' + tablename, filters=f, verbose=verbose_1(args.verbose))
    buf = []							# records accumulated here
    prev_recs = {}						# initially, no previous records
    for rec in reader:
        try:
            mac = macFunc(rec)			# mac OK
        except (KeyError, ValueError):  # No
            continue					# Ignore this record
        rec['macAddress'] = mac
        if len(cumulatives) > 0:		# Any cumulative to transform to incremental?
            mac_slot_id = mac+str(rec['slotId']) if 'slotId' in selected else mac
            try:						# Yes, manage prevrec for this mac[+slotId]
                prev_rec = prev_recs[mac_slot_id]
            except KeyError:						# no previous --> 1st record for mac[+slotId]
                prev_recs[mac_slot_id] = rec.copy()
                continue				# drop record w/cumulatives from output
            prev_recs[mac_slot_id] = rec.copy()
        # Convert collectionTime to correct string form
        if msecTime:					# in integer milliseconds?
            rec['collectionTime'] = strfTime(millisToSecs(rec['collectionTime']))
        else:							# No, in ISO text that is 8 hours ahead
            rec['collectionTime'] = strfTime(anyToSecs(rec['collectionTime'])+2*3600)
        for attr in cumulatives: 		# transform each cumulative to incremental
            rec[attr] = rec[attr]-prev_rec[attr]  # from previous
        if transforms is not None:
            transforms(rec)				# execute specific field transforms
        buf.append(rec)					# accumulate record for output
    if sortFunc is not None:			# specified sort order?
        buf.sort(key=sortFunc)
    with open(filename, 'w', newline='') as write_file:
        writer = csv.DictWriter(write_file, selected, extrasaction='ignore')
        writer.writeheader()			# write the csv header line
        for rec in buf:
            writer.writerow(rec)


def clientMac(rec: Dict[str, Union[str, Dict[str, str]]]) -> str:
    """Return the client MAC address from a [Historical]ClientStats record

    :param rec:     record from [Historical]ClientStats
    :return:        MAC address [w/o colons]
    :raises         KeyError iff the MAC is not in clients
    """
    mac = rec['macAddress']['octets']
    if mac in clients:
        return mac
    raise KeyError


def keyApMac(rec: Dict[str, Union[str, Dict[str, str]]]) -> str:
    """Obtain the AP MAC address from a [Historical]Client[Counts|Traffics] record.
    Copy AP's name from ap_mac to rec['apName']

    :param rec:     record from [Historical]Client[Counts|Traffics]
    :return:        MAC address w/o colons
    :raises         KeyError iff the MAC is not in ap_macs
    """
    mac = rec['key'].replace(':', '')  	# key has colons, but APDMAC does not
    if mac in ap_macs:
        apdmac = APDMAC[mac]
        rec['apName'] = apdmac['name']
        return mac
    raise KeyError


def apMac(rec) -> str:
    """Obtain the AP MAC address from a [Historical]RF[Counters|LoadStats|Stats] record.
    Copy AP's name from ap_mac to rec['apName']

    :param rec:     record from [Historical]RF[Counters|LoadStats|Stats]
    :return:        MAC
    :raises         KeyError iff the MAC is not in ap_macs
    """
    mac = rec['macAddress']['octets'] 	# macAddress_octets does not have punctuation
    if mac in ap_macs:
        apdmac = APDMAC[mac]
        rec['apName'] = apdmac['name']
        return mac
    raise KeyError


parser = ArgumentParser(description='Report the clientSessions for client associations with APs during time window')
parser.add_argument('--APfile', action='store', dest='apFilename', default=None,
    help='file name of csv file of the AP names to collect')
parser.add_argument('--clientFile', action='store', dest='clientFilename', default=None,
    help='File name of csv file of client mac addresses')
parser.add_argument('--end', action='store', dest='endTime', default=None,
    help='minimum session start date-time. E.g. 2019-01-01T12:12')
parser.add_argument('--start', action='store', dest='startTime', default=None,
    help='maximum session end date-time. E.g. 2019-01-01T14:12')
parser.add_argument('--password', action='store', default=None,
    help='password')
parser.add_argument('--user', action='store', dest='username', default='dar5',
    help='username, if other than dar5')
parser.add_argument('--verbose', action='count', default=0,
    help='print additional messages')
parser.add_argument('--attribute', action='store', default='macAddress_octets',
    help='attribute name for values. Default = macAddress_octets')
args = parser.parse_args()

# define the function, attr(record) that returns the specified attribute of a record
lst = args.attribute.split('_') 		# list of accessors
s = "def attr(record):\n return record['" + "']['".join(lst) + "']"
printIf(args.verbose, f"attribute function={s}")
exec(s)									# define the function

if args.apFilename is None:			    # Default set of APs?
    coreaps = {'samson-p6-w252', 'samson-p6-w254', 'samson-p6-w255', 'samson-p6-w257',
               'samson-p6-w258', 'samson-p6-w260',
            'samson-p9-w253', 'samson-p9-w256', 'samson-p9-w259'}
    neighboraps = {'samson-p6-w268', 'samson-p6-w261', 'samson-p6-w262', 'samson-p6-w263', 'samson-p6-w245',
    'samson-p9-w267', 'samson-p9-w250', 'samson-p9-w251', 'samson-p9-w244'}
    apnames = coreaps | neighboraps
else:									# No. Read AP names from file
    apnames = get_vals(args.apFilename)

# calculate startsWith filter as longest common prefex of apnames
startsWith = None
for name in apnames:
    if startsWith is None:
        startsWith = name
    else:
        i = min(len(startsWith), len(name))
        j = -1
        for j in range(i):
            if name[j] != startsWith[j]:
                break					# found the first character that is different
        else:							# did not find a difference
            j += 1
        startsWith = startsWith[:j]
printIf(args.verbose, f"startsWith={startsWith}")

# calculate startTime and endTime values in milliseconds and ISO date text
time_offset = 60 * 60 * 8				# Seconds that Cisco ISO are ahead of local time
start_msec = int(1000*time.mktime(time.strptime(args.startTime, '%Y-%m-%dT%H:%M')))
start_time_bad = time.strftime('%Y-%m-%dT%H:%M:%S',
                               time.localtime(start_msec / 1000.0 + time_offset)) + ".000+0500"
end_msec = int(1000*time.mktime(time.strptime(args.endTime, '%Y-%m-%dT%H:%M')))
end_time_bad = time.strftime('%Y-%m-%dT%H:%M:%S',
                             time.localtime(end_msec / 1000.0 + time_offset)) + ".000+0500"
printIf(args.verbose,
        f"session startime={strfTime(start_msec/1000.0)},"
        f"endTime={strfTime(end_msec/1000.0)}")
printIf(args.verbose, f"startTimebad={start_time_bad}, endTimebad={end_time_bad}")

# establish login credentials
server = 'ncs01.case.edu'
if args.password is None:			    # No password provided?
    try:
        cred = credentials(server, args.username)
    except KeyError:					# Couldn't find
        print(f"No username/password credentials found for {server}")
        sys.exit(1)
    args.username, args.password = cred

# create CPI server instance
myCpi = Cpi(args.username, args.password, baseURL='https://' + server + '/webacs/api/')

printIf(args.verbose, "Reading AccessPointDetails")
filters = {".full": "true", ".nocount": "true"}
if len(startsWith) > 0:
    filters['name'] = f"startsWith({startsWith})"
reader = Cpi.Reader(myCpi, 'v4/data/AccessPointDetails', filters=filters, verbose=verbose_1(args.verbose))
APDNames = {}							# Indexed by Name
APDMAC = {}								# Indexed by apMAC
for record in reader:
    APDNames[record['name']] = record
    APDMAC[record['macAddress']['octets']] = record

# Check for valid apnames set, and create ap_macs set
ap_macs = set()
apIPs = set()
for name in apnames:
    try:
        ap_macs.add(APDNames[name]['macAddress']['octets'])
        apIPs.add(APDNames[name]['ipAddress']['address'])
    except KeyError:
        print(f"unknown AP name {name} ignored")

# read in client macAddresses
client_raw = get_vals(args.clientFilename)
clients = set()
for mac in client_raw:
    clients.add(mac.replace(':', '')) 	# remove MAC punctuation
del client_raw
printIf(args.verbose, f"clients=\n{clients}")

# define some key functions for list.sort


def apCT(r): return f"{r['apName']} {r['collectionTime']}"


def apSlotCT(r): return f"{r['apName']} {r['slotId']} {r['collectionTime']}"


def macCT(r): return f"{r['macAddress']} {r['collectionTime']}"


# report AP Client Counts from HistoricalClientCounts
filters = {'type': 'ACCESSPOINT', 'subkey': 'All'}
selected = ('collectionTime', 'apName', '2_4Count', '5_0Count')


def hccTransform(rec):
    rec['2_4Count'] = sum(rec[a] for a in ('dot11bCount', 'dot11ax2_4Count',
                                           'dot11gCount', 'dot11n2_4Count'))
    rec['5_0Count'] = sum(rec[a] for a in ('dot11aCount', 'dot11acCount',
                                           'dot11ax5Count', 'dot11n5Count'))


collect('HistoricalClientCounts', filters, [], selected, True, hccTransform, keyApMac, apCT)

# Report Client HistoricalClientStats
cumulatives = ('bytesReceived', 'bytesSent', 'dataRetries', 'packetsReceived',
    'packetsSent', 'raPacketsDropped', 'rtsRetries', 'rxBytesDropped',
    'rxPacketsDropped', 'txBytesDropped', 'txPacketsDropped')
selected = ('collectionTime', 'macAddress', 'dataRate', 'rssi', 'snr') + cumulatives
collect('HistoricalClientStats', {}, cumulatives, selected, True, None, clientMac, macCT)

# report AP HistoricalClientTraffics
filters = {'type': 'ACCESSPOINT', 'subkey': 'All'}
selected = ('collectionTime', 'apName', '2_4Received', '2_4Sent', '5_0Received', '5_0Sent')


def hctTransform(rec):
    rec['2_4Received'] = sum(rec[a] for a in ('dot11ax2_4Received', 'dot11bReceived',
                                              'dot11gReceived', 'dot11n2_4Received'))
    rec['2_4Sent'] = sum(rec[a] for a in ('dot11ax2_4Sent', 'dot11bSent', 'dot11gSent', 'dot11n2_4Sent'))
    rec['5_0Received'] = sum(rec[a] for a in ('dot11aReceived', 'dot11acReceived',
                                              'dot11ax5Received', 'dot11n5Received'))
    rec['5_0Sent'] = sum(rec[a] for a in ('dot11aSent', 'dot11acSent', 'dot11ax5Sent', 'dot11n5Sent'))


collect('HistoricalClientTraffics', filters, [], selected, True, hctTransform, keyApMac, apCT)

# report AP HistoricalRFCounters
cumulatives = ('ackFailureCount', 'failedCount', 'fcsErrorCount',
    'frameDuplicateCount', 'multipleRetryCount', 'retryCount', 'rtsFailureCount',
    'rtsSuccessCount', 'rxFragmentCount', 'rxMulticastFrameCount',
    'txFragmentCount', 'txFrameCount', 'txMulticastFrameCount',
    'wepUndecryptableCount')
selected = ('collectionTime', 'apName', 'slotId') + cumulatives
collect('HistoricalRFCounters', {}, cumulatives, selected, False, None, apMac, apSlotCT)

# report AP HistoricalRFLoadStats
selected = ('collectionTime', 'apName', 'slotId', 'channelUtilization',
    'clientCount', 'poorCoverageClients', 'rxUtilization', 'txUtilization')
collect('HistoricalRFLoadStats', {}, [], selected, False, None, apMac, apSlotCT)

# report AP HistoricalRFStats
selected = ('collectionTime', 'apName', 'slotId', 'channelNumber', 'clientCount',
    'coverageProfile', 'interferenceProfile', 'loadProfile', 'noiseProfile',
    'operStatus', 'powerLevel')
collect('HistoricalRFStats', {}, [], selected, False, None, apMac, apSlotCT)

# report ClientSessions
printIf(args.verbose, "reading ClientSessions")
filters = {'.full': 'true', '.nocount': 'true',
    'sessionStartTime': f"between({str(start_msec)},{str(end_msec)})"}
reader = Cpi.Reader(myCpi, 'v4/data/ClientSessions', filters=filters, verbose=verbose_1(args.verbose))
selected = ('macAddress', 'sessionStartTime', 'sessionEndTime', 'apName',
    'bytesReceived', 'bytesSent', 'connectionType', 'packetsReceived',
    'packetsSent', 'roamReason', 'rssi', 'snr', 'ssid',
    'throughput', 'userName')
buf = []
for record in reader:
    mac = record['macAddress'] = record['macAddress']['octets']
    if mac not in clients:
        continue
    record['sessionStartTime'] = strfTime(millisToSecs(record['sessionStartTime']))
    if record['sessionEndTime'] > end_msec + 1000*60*60*24*365:
        record['sessionEndTime'] = 'associated'
    else:
        record['sessionEndTime'] = strfTime(millisToSecs(record['sessionEndTime']))
    apdmac = APDMAC.get(record['apMacAddress']['octets'], None)
    if apdmac is not None:		# Known AP?
        record['apName'] = apdmac['name']  # output its name
    else:						# No, unknown
        record['apName'] = record['apMacAddress']['octets']  # output its macAddress
    buf.append(record)
buf.sort(key=lambda r: f"{r['macAddress']} {r['sessionStartTime']}")
with open('ClientSessions' + args.endTime.replace(':', '') + '.csv', 'w', newline='') as writefile:
    writer = csv.DictWriter(writefile, selected, extrasaction='ignore')
    writer.writeheader()
    for record in buf:
        writer.writerow(record)
