#!/usr/bin/python3
# Copyright (C) 2020 Dennis Risen, Case Western Reserve University
#
"""
Uses ClientSessions join ClientDetails API real-time data collection of client
device associations to APs stored in AWS S3 to calculate estimated
[infection] exposure risks by AP cell and individual user.
Writes a csv report of the top risks by location and user.
Assists exposure tracking by producing a report of an individual users
location over time.
"""

from argparse import ArgumentParser
from collections import defaultdict
from contextlib import ExitStack
from csv import DictReader
import gzip
from itertools import combinations
from math import log2, sqrt
import os
from queue import Queue
import re
from threading import Thread
from typing import DefaultDict, Union

from awslib import key_split, listRangeObjects, print_selection
import boto3
from mylib import strpTime, strfTime, verbose_1
from timeMachine import TimeMachine

""" To Do
Desk audit w comments of values of each field

histo_report: verify formula
Input advice on device ownership and classification
Obtain device details from ClientDevices
Organize to re-use module as an input source to the wirelessMap application
Update to use the newer ServiceDomain API, instead of the sites API
Measure and tune performance to facilitate use as a Source
"""
do_not_backup = r'C:\Users\dar5\Downloads\DoNotBackup'
tm_path = do_not_backup+r'\awsstuff\prod'  # path to timeMachines
day_secs = 24*60*60			# number of seconds in a day
infra_mac = {}			# {Client_mac: seconds, ...} classified as infrastructure
period = 5*60.0				# default sampling period

infra_secs = 0.0
visited_report = ''
anonymous = defaultdict(float)  # Dict(1, 0.0)  # {client_mac: seconds, ...} of anonymous non-infra client
auth_macs = 0				# count of macs that always authorize
auth_secs = 0.0				# Total seconds of authorized association
non_macs = 0				# count of macs that never authenticated
non_secs = 0.0				# total seconds by macs that didn't authenticate
multi_macs = 0			# count of macs with non-auth time pro-rated to multi users
multi_secs = 0.0			# seconds on non-auth-time prorated to multi users
single_macs = 0				# count of macs authenticated by one user
single_secs = 0.0			# seconds of non-auth time assigned to single user
client_report = ''			# text report from client_user()

track_out = defaultdict(list)  # Dict(1, [])					# {client_mac: [Timeline entries], ...}
ASSOC = 1								# Entry is an Association
DISSOC = 2								# Entry is a Dissociation

# Each client mac address to track, with p=1.0 when mac to be tracked,
# or p(user|mac) for each mac used by user being tracked.
# When multiple tracked users share mac, separate into separate runs.
track_macs = defaultdict(float)  # dict(1, 0.0) 	# {client_mac: p} for each client_mac to track
# PASS 1 FUNCTIONS


class L1r:
    """
    Left to Right Look-ahead 1 iterable
    """
    def __init__(self, reader: callable, **kwargs):
        """Look-ahead 1 iterable.

        :param reader: 	reader(self) in thread. Puts to self.queue until self.stop or EOF --> put(None)
        :param kwargs: 	additional parameters passed to reader
        """
        self.look_ahead = {} 			# the next record to yield, or None
        self.polled_time: float = 0.0 	# epoch-seconds from the look-ahead record
        self._reader = reader
        self.queue = Queue(5000)
        self.stop = False				# reader to continue while stop is False
        kwargs['parent'] = self			# add required parameter
        self._kwargs = kwargs

    def __iter__(self):
        self.stop = False				# reader to continue while stop is False
        self._thread = Thread(target=self._reader, kwargs=self._kwargs)  # pass through kwargs to reader
        self._thread.start()
        self.__next__()					# initial look_ahead
        return self

    def __next__(self):
        result = self.look_ahead		# will return the current look_ahead
        if result is None:				# Received EOF from reader?
            raise StopIteration			# Yes
        entry = self.queue.get()
        if entry is None:
            self.look_ahead = None 	# EOF
        else:
            self.look_ahead = entry
            self.polled_time = float(entry['polledTime'])
        return result

    def close(self):
        """Close self and the reader

        """
        self.stop = True				# stop reader from reading more
        while self.look_ahead is not None:  # empty the Q so that reader can exit
            self.__next__()
        self._thread.join()


def out(start: float, end: float, tup: tuple, user_name: str):
    """Output a client-to-AP association interval. Sum user_seconds by client and client_seconds by AP.

    Appends to bye and hi.
    Updates client_user[client_mac][user_name][0] and visited[client_mac][ap_mac].

    :param start:       epoch-seconds at association
    :param end:         epoch-seconds at dissociation
    :param tup:         (ap_mac, client_mac)
    :param user_name:   userName
    :return:
    """
    global hi, bye, client_user, visited
    dt = end-start+period
    hi.append((start, tup))				# list of associations
    bye.append((end, tup))				# list of dis-associations
    client_mac = tup[1]
    client_user[client_mac][user_name][0] += dt
    visited[client_mac][tup[0]] += dt 	# tot seconds client_mac visited ap_mac


def range_reader(parent: L1r, selection: list, range_start: float, verbose: int = 0):
    """Read, unzip, and DictReader csv.gz objects from S3. Put each record dict to queue.

    :param parent:		while (not parent.stop or EOF), put record to parent.queue. then put None
    :param selection:	list of S3 objects to read
    :param range_start: ignore records prior to this epoch seconds
    :param verbose: 	diagnostic message level
    """
    queue = parent.queue
    for source in selection:			# for each file
        time_stamp = int(key_split(source['Key'])['msec'])
        if int(time_stamp)/1000.0 < range_start:  # collecting started < start of the day?
            if verbose > 0:
                print(f"{source['key']} before start of report")
        if verbose > 0:
            print(f"reading {source['Key']}")
        try:  # catch any unexpected error
            aws_object = s3.Object(bucket_name=bucket, key=source['Key'])
        except Exception as e:
            print(f"s3.Object({bucket}, {source['Key']} causes {e}")
            continue
        else:
            with ExitStack() as stack: 		# when exiting this block, automatically...
                bucket_stream = aws_object.get()['Body']  # aws_object reader
                stack.callback(bucket_stream.close)  # ... bucket_stream.close()
                unzipped_stream = gzip.open(bucket_stream, mode='rt')  # unzip(aws_object)
                stack.enter_context(unzipped_stream)  # ... unzipped_stream.close()
                csv_reader = DictReader(unzipped_stream)  # csv(unzip(aws_object))
                for rec in csv_reader:
                    # yield rec, time_stamp  # record_dict, poll time_stamp
                    if parent.stop:
                        break
                    queue.put(rec)
    queue.put(None) 					# put an EOF, and exit


def siteName2locH(name: str) -> str:
    """Converts sites.name to AccessPointDetails.locationHierarchy

    :param name: 	Location/All Locations/campus(/outdoorArea|(/building(/floor)?)?
    :return: 		campus( > outdoorArea|( > building( > floor)?)?
    """
    # 23 == len('Location/All Locations/')
    return name[23:].replace('/', ' > ')


# FUNCTIONS THAT PRE-PROCESS PASS 1 DATA IN PREPARATION FOR PASS 2
def client_user_process():
    """process client_user to calc p(user|mac); identify macs to track

    For each client_mac in client_user: if user '' and other users, delete user ''
    and proportion its seconds to the other users. map {user:seconds, ...} to {user:p}
    if only user '', record its seconds in anonymous[client_mac]

    Iterates ``args.mac``.
    Updates ``track_macs``.
    """
    global client_report, client_user, track_macs, auth_macs, auth_secs
    global multi_macs, multi_secs, single_macs, single_secs

    # calculate the Bayesian p(user|mac)
    multi_auth = False			# not (yet) any MACs authenticated by multiple users
    for client_mac, user_d in client_user.items():
        if '' in user_d:				# Some non-authenticated time?
            secs = user_d[''][0]		# Yes. seconds of time
            if len(user_d) == 1:		# Only non-authenticated time?
                anonymous[client_mac] = secs  # Yes. client is always anonymous
                user_d[''][1] = 1.0		# p(''|client_mac) = 1.0
                continue
            del user_d['']				# No. Have authenticated user(s) of this mac.
        else:
            secs = 0.0
        # Assign non-authenticated time proportionately to the authenticated user(s)
        sum_secs = sum([t[0] for t in user_d.values()])
        ratio = (1+secs/sum_secs)/(secs+sum_secs)
        for user in user_d:
            auth_macs += 1				# count of macs that authorized
            a_secs = user_d[user][0]
            auth_secs += a_secs 		# Total number of seconds of authorized association
            user_d[user][0] = a_secs + secs*a_secs/sum_secs
            user_d[user][1] = a_secs * ratio
        if len(user_d) > 1:				# more than 1 user?
            # Yes. Sort descending by p so that greatest user is 1st in reporting
            user_d = [(u, lst) for u, lst in user_d.items()]  # extract dict to list
            user_d.sort(key=lambda x: -x[1][1])  # sort by descending p
            client_user[client_mac]: DefaultDict[str, list] = defaultdict(str, user_d)
            multi_macs += 1
            multi_secs += secs
            if args.verbose:			# Report MAC that authenticated as multiple users
                if not multi_auth:		# 1st MAC with multiple users?
                    multi_auth = True 	# Yes. Print sub-report header
                    client_report += '\nMACs that authenticated as multiple users mac(% user, ...)\n'
                client_report += mac_str(client_mac) + '\n'
        else:
            single_macs += 1
            single_secs += secs

    # verify for each client_mac that each p <= 1 and sum(p) == 1
    for client_mac, user_d in client_user.items():
        tot_p = 0.0
        for user, lst in user_d.items():
            p = lst[1]
            tot_p += p
            if p > 1.00001:
                print(f"p={p} {client_mac} {user_d}")
        if not 0.9999 < tot_p < 1.0001:
            print(f"sum(p)={tot_p} for {client_mac}")


def sort_hi_bye():
    """sorts global ``hi`` and ``bye``"""
    global hi, bye
    hi.sort(key=lambda x: x[0])			# sort entrances by ascending association time
    bye.sort(key=lambda x: x[0])		# sort exits by ascending disassociation time


def visited_process():
    """Process visited. Classify infrastructure clients as such..

    Iterates and updates ``visited``.
    Builds ``infra_mac``.
    """
    global visited, infra_mac, visited_report
    # Classify client_mac as infrastructure if it didn't move and is associated most of the time
    histo = defaultdict(int)  # Dict(1, 0)
    for client_mac, d in visited.items():
        if len(d) == 1:					# associated with only 1 AP ?
            ap_mac, tot_secs = d.popitem()  # Yes. get info
            d[ap_mac] = tot_secs		# and put it back
            histo[int(20*tot_secs/domain_secs)] += 1
            if tot_secs < domain_secs*0.75:
                continue
            infra_mac[client_mac] = tot_secs  # client_mac classified as infrastructure
        else:							# sort associated APs by descending association time
            d_lst = list(d.items())
            d_lst.sort(key=lambda x: -x[1])
            visited[client_mac] = defaultdict(DefaultDict[str, float], d_lst)
    if args.verbose:
        histo = [(cnt, secs) for cnt, secs in histo.items()]
        histo.sort()
        for buc, count in histo:
            visited_report += f"{count:5} clients associated {5*buc:3}+% of the wall clock time.\n"


def check_visited():
    cu_set = set([mac for mac in client_user])
    v_set = set([mac for mac in visited])
    common = cu_set & v_set
    print(f"cu_set - {cu_set -common} = v_set - {v_set - common}")
    for mac, user_d in client_user.items():
        cu_sum = sum(t[0] for t in user_d.values())
        if mac in visited:
            v = visited[mac]
            v_sum = sum(s for s in v.values())
            if abs(cu_sum - v_sum) > 0.01:
                print(f"for mac={mac}: cu_sum={cu_sum} != {v_sum}=v_sum")
                print(f"{user_d} !~ {v}")
        else:
            print(f"{mac} not in visited")


# PASS 2 FUNCTIONS
def aggregate(ap_mac: str, end_time: float):
    """Aggregate statistics at AP from its start time through this end_time

    Iterates ``by_ap[mac]['clients']``

    :param ap_mac:		AP to aggregate
    :param end_time: 	aggregate through this time and update start<-end_time
    """
    global by_ap, risk
    ap = by_ap[ap_mac] 	# {'start': float, 'clients':[[client_mac, weight, {mac_b: secs, ...}], ...], ...}
    start = ap.get('start', None)
    ap['start'] = end_time  			# update start time
    if start is None:					# new AP?
        ap['clients'] = []  # Yes. initialize for [[client_mac, weight, {mac_b: secs, ...}], ...}
        return							# no aggregation to do
    if start == end_time:				# statistics are up-to-date?
        return							# Yes. return
    clients: list = ap['clients']
    if len(clients) < 2:
        return							# no exposure w/o 2 or more clients
    # aggregate statistics for the interval from start to end_time
    dt = end_time - start
    client_macs = [client[0] for client in clients]  # independent list for concurrent iteration
    client_copy = client_macs.copy() 	# independent list for concurrent iteration
    pairs_q.put((client_copy, dt))		# aggregate global client_pair risk in a separate thread
    risk[ap_mac] += dt * len(clients) * (len(clients)-1)/2  # sum the risk at this AP
    # calculate local risk between each tracked mac and other macs
    for my_rec in clients:
        my_mac = my_rec[0]
        if my_mac not in track_macs: 	# not tracking this mac?
            continue					# not tracking
        weight = dt*my_rec[1]
        exposed = my_rec[2]
        for b_mac in client_macs:
            if my_mac == b_mac:			# self?
                continue				# Yes. Don't track exposure from me to myself
            other_exposed = exposed.get(b_mac, None)
            if other_exposed is None:
                exposed[b_mac] = weight
            else:
                exposed[b_mac] += weight


def apMac(row) -> str:
    """getter for the mac_address_octets field

    :param row:     flattened AccessPointDetails record
    :return:        MAC without colons
    """
    return row['macAddress_octets'].replace(':', '')  # without the colons


def ap_str(ap_mac: str) -> str:
    """format AP macAddress as AP name and location string

    :param ap_mac: client device MAC
    :return: 	formatted AP macAddress as AP name and location str
    """
    ap = apd_mac.get(ap_mac, None) 	# {'name': apName, 'building': site['building'],
    # 'floor': site['floor'], 'mapLocation': map_loc, ...}
    try:
        return f"{ap['name']}: {ap['building']} {ap['floor']} {ap['mapLocation']}"
    except KeyError:
        return f"No location information for {ap['name']} mac={ap_mac}"
    except TypeError:
        return f"Unknown AP w/MAC={ap_mac}"


def histo_report(histo: defaultdict, scale: callable, title: str, f: str = '8,'):
    """Print the title and histogram of histo with value scaled to scale(i)

    :param histo: 		Dict(1, 0) of value, count
    :param scale: 		function to scale value for printing
    :param title: 		Title header
    :param f:			format for scaled value
    """
    print(title)
    mx = max(x for x in histo)
    mn = min(x for x in histo)
    for i in range(mn+1, mx-1):
        histo[i] += 0					# ensure that there is a value for each i
    hist = list(histo.items())
    hist.sort()							# sort ascending
    for i, j in hist:
        print(format(int(scale(i)), f), '--', format(int(scale(i+1)), f), format(j, '6,'))


def mac_str(client_mac: str) -> str:
    """Format client device as MAC(percent user, ...)

    :param client_mac:  client device MAC
    :return: MAC and user(s)
    """
    # iterates client_user[mac]
    client = client_user[client_mac] 	# {userName: [tot seconds, p], ...}
    lst = [('' if t[1] > 0.999 else str(int(100*t[1]))+'%')+u for u, t in client.items()]
    return client_mac + ('' if len(lst) == 0 or len(lst) == 1 and lst[0] == '' else f"({', '.join(lst)})")


def pairs_thread():
    """While ``pairs_q` is not null, get (clients, dt) and accumulate dt for all combinations of clients.

    Iterates each list passed through ``pairs_q``
    Updates ``client_pairs``, ``sym_pairs``
    """
    global client_pairs, sym_pairs
    while True:
        entry = pairs_q.get()			# wait for and get the next command
        if entry is None:				# command to exit?
            break						# yes
        client_macs, dt = entry
        # global risk between client a and client b
        for a_mac, b_mac in combinations(client_macs, 2):
            client_pairs[a_mac][b_mac] += dt  # sum time a is near b
    # duplicate client_pairs entries to include [mac_b][mac_a] as well as [mac_a][mac_b] where mac_a < mac_b
    sym_pairs = defaultdict(lambda: defaultdict(float))  # Dict(2, 0.0)
    for mac_a, data in client_pairs.items():
        for mac_b, secs in data.items():
            sym_pairs[mac_a][mac_b] = secs
            sym_pairs[mac_b][mac_a] = secs


# Parse command line for opts
parser = ArgumentParser(description='Analyse ClientSessionsDetails csv files. Filter by day. Report Covid risks')
parser.add_argument('--dateindex', action='store', type=int, default=5,
                    help='index of the 1st field of the date range in the Key. Default=5. Original was 4.')
parser.add_argument('--email', action='append', default=[],
                    help='Each --email adds an email address to the receive the reports')
parser.add_argument('--filtermin', action='store', type=float, default=0.0,
                    help='Report only APs with range of risk >=')
parser.add_argument('--infer_home', action='store', type=float, default=3,
                    help="display an anonymous client's majority AP when hours/day at the AP exceeds: default=3.")
parser.add_argument('--infer_user', action='store', type=float, default=0.8,
                    help="infer an anonymous client's user when its time near a user exceeds: default=0.8")
parser.add_argument('--mac', action='append', dest='macs', default=[],
                    help='one --mac=XXXXXXXX for each mac to track')
parser.add_argument('--mindate', action='store', default=None,
                    help='track starts at 00:00:00 on this yyyy/mm/dd')
parser.add_argument('--maxdate', action='store', default=None,
                    help='tracking stops at 00:00:00 on this yyyy/mm/dd')
parser.add_argument('--prefix', action='store',
                    default='cwru-data/network/wifi/ncsdata/dar5/ClientSessionsDetails/',
                    help='bucket + initial prefix. Default=cwru-data/network/wifi/ncsdata/dar5/')
parser.add_argument('--user', action='append', dest='users', default=[],
                    help='one --user=caseId or user@domain to track')
parser.add_argument('--verbose', action='count', default=0,
                    help="turn on diagnostic messages")
args = parser.parse_args()

# Remove punctuation from requested MAC. Change each to lower-case only
for i in range(len(args.users)):
    args.users[i] = args.users[i].lower()
for i in range(len(args.macs)):
    args.macs[i] = args.macs[i].replace(':', '').replace('-', '').lower()
fileRE = table_name = 'ClientSessionsDetails'  # the table to read

# regular expressions for mapLocation cleanup
faceplate_re = r'[0-9]{2,3}-[0-9]{2}-[sSbB]{0,1}[0-9]{1,3}b?-[a-zA-Z0-9][0-9]*'
nth = r'sub-basement|basement|ground|\?|1st|2nd|3rd|4th|5th|6th|7th|8th|9th|10th|11th|12th|13th'
nameth = r'first|second|third|fourth|fifth|sixth|seventh|eight|ninth|tenth|eleventh|twelfth|thirteenth'
nth_floor = re.compile('('+nth+'|'+nameth+')[ _]floor', flags=re.IGNORECASE)
named = re.compile(r'basement|sub-basement|ground|mezz[a-z]*|attic|penthouse|rooftop|top floor', flags=re.IGNORECASE)
thingy = r'elevator[s]|cafeteria|hallway|SER room|stairs|stairwell|'
floor_n = re.compile(r'floor[ _][0-9]+', flags=re.IGNORECASE)
inside = re.compile(f'inside of |inside |in ', flags=re.IGNORECASE)
good = re.compile(r'(by |near )?(('+thingy+r')|((room |rm )?[a-z]?[0-9]+-?[a-z]?))', flags=re.IGNORECASE)

range_start = strpTime(args.mindate, '%Y/%m/%d')
range_end = strpTime(args.maxdate, '%Y/%m/%d')
domain_secs = range_end - range_start

# Define and populate TimeMachines for AccessPointDetails and sites
# Build sites_LH, a mapping from an Access Point's locationHierarchy --> building and floor
# Read in sites table to map from locationHierarchy to building and floor
apd = TimeMachine('AccessPointDetails', key_source="lambda x: x['@id']")
apd.load_gz(filename=os.path.join(tm_path, 'AccessPointDetails.json.gz'))
sites = TimeMachine('sites', key_source="lambda x: x['groupId']")
sites.loose = 1							# accept data for time >= to epoch_msec
sites.load_gz(filename=os.path.join(tm_path, 'sites.json.gz'))

# Get the list of AWS objects to read
s3 = boto3.resource('s3')
selection = [x for x in listRangeObjects(args.prefix, args.mindate,
        args.maxdate, args.dateindex, fileRE, verbose=verbose_1(args.verbose))]
bucket, s, prefix = args.prefix.partition('/')
selection.sort(key=lambda x: x['Key'])
print_selection(selection, lambda x: x['Key'], verbose=verbose_1(args.verbose))
print(f"Reading {len(selection)} files in date range {args.mindate} -- {args.maxdate}")

pat_file_name = r'([0-9]+)_' + table_name + r'(v[1-9])?\.csv.*'  # regex for file to read
# Algorithm
# Each client state observed at t[i] actually occurred between t[i-1] and t[i].
# For simplicity with no change to results, simulate as if the change occurred at exactly t[i-1]
# Read a poll.
# Update associations. For each client mac
# 	if it's still associated with the same AP, extend the client's end_time
# 	else enter a new CSD rec into by_ap[client_mac] with start_time = t[i-1] and end_time = t[i]
# For each AP
# 	update statistics
# 	delete client_macs with end_time < t[i]
#
"""
Will perform analysis:
    Simulate the set of clients associated with each AP for each 5 minutes over the date range so that it can
        Integrate clients*clients*time for each AP (squared is close to n*(n-1)/2)
            Might cubed be a better metric, since more people in the space infers that they are closer?
        Merge statistics for each unauthenticated MAC, times Bayesian, into statistics for the predicted users
            Interesting: proximity between user and (himself or device never authenticated)
                could approximate the number of devices the user carries.
And report:
    Overall metric of risk given time spent on campus:
        sum_for_APs_of(clients*clients*time)/sum_for_all_APs_of(clients*time)
Histogram individual’s risk: the of number of users by by risk=sum_over_all_other_users(time_with other)
"""
# Data structures
# apd_mac = Dict(2) 				# {'name': apName, 'building': site['building'],
# 	'floor': site['floor'], 'mapLocation': map_loc, }
# Record for each client_MAC, the time that it is associated with each AP
visited = defaultdict(lambda: defaultdict(float))  # {client_mac: {ap_mac: total_seconds ...}, ...}
visited: DefaultDict[str, DefaultDict[str, float]]
apd.epoch_msec = int(1000*range_start)  # Set the TimeMachines to epoch msec
sites.epoch_msec = int(1000*range_start)
apd_reader = apd.values()				# fresh generator
sites_reader = sites.values() 			# fresh generator
sites_LH = {'Root Area': {'building': 'not defined', 'floor': 'not defined', 'siteType': 'Floor Area'}}
for row in sites_reader:
    name = row['name']				# Location/All Locations/campus(/outdoorArea|(/building(/floor)?)?
    locationHierarchy = siteName2locH(name)  # ->
    siteType = row.get('siteType', None)
    if siteType is None:				# undefined siteType to ignore?
        if name != 'Location/All Locations':  # and not the root 'Location/All Locations'?
            print(f"No siteType for name={name}, groupName={row.get('groupName', None)}")
    elif siteType == 'Floor Area': 		# floor area?
        row['building'] = name.split('/')[-2]
        row['floor'] = name.split('/')[-1]
    elif siteType == 'Outdoor Area':  	# outdoor area
        row['building'] = name.split('/')[-1]  # outdoor area name used for building
        row['floor'] = ''				# has no floor
    elif siteType == 'Building': 		# building
        pass							# not needed for this mapping
    elif siteType == 'Campus':
        pass
    else:
        print(f"unknown siteType {siteType} for name={name}, groupName={row.get('groupName', None)}")
    sites_LH[locationHierarchy] = row

# build apd_mac, a mapping from AP's mac
# to {'name':ap_name, 'building':building_name, 'floor': floor_name, 'mapLocation: mapLocation}
apd_mac = defaultdict(dict)  # Dict(1, {})
for row in apd_reader:
    apName = row['name']
    orig = row['mapLocation'].split('|')[-1]
    map_loc: str = orig					# provide a hint
    # clean up the mapLocation data somewhat
    if map_loc == 'default location':
        map_loc = ''
    map_loc = re.sub(r'\s', ' ', map_loc)  # whitespace-->' '
    map_loc = re.sub('[ ]?' + faceplate_re, '', map_loc)
    map_loc.lstrip(' ?-')
    map_loc = re.sub(nth_floor, '', map_loc)
    map_loc = re.sub(floor_n, '', map_loc)
    map_loc = map_loc.lstrip(' ?-')
    map_loc = re.sub(named, '', map_loc)
    map_loc = re.sub('  ', '', map_loc)
    map_loc = re.sub(inside, '', map_loc)
    map_loc = map_loc.lstrip()
    map_loc = map_loc.rstrip(' ?.')
    mac_address = apMac(row) 	# get row['macAddress_octets'] or row['macAddress']['octets']
    try:
        site = sites_LH[row['locationHierarchy']]
    except KeyError:
        lh = row['locationHierarchy']
        print(f"locationHierarchy={lh} is not in the sites table")
        apd_mac[mac_address] = {'name': apName, 'building': lh[14:],
                                'floor': '', 'mapLocation': ''}  # best we can do
        continue						# drop this AP from mapping
    apd_mac[mac_address] = {'name': apName, 'building': site['building'],
                            'floor': site['floor'], 'mapLocation': map_loc}

# current collection time
polled_time = 0							# less than any real collectionTime
# in pass 1
by_ap = defaultdict(lambda: defaultdict(int))  # Dict(2)			# {ap_mac: {client_mac: CSD_rec with extra fields, ...}, ...}
by_ap: DefaultDict[str, DefaultDict[str, Union[float, list]]]
# In pass 1, the the AP that each client_mac is associated with
by_client = {}							# {client_mac: ap_mac, ...}
# The total risk at each AP, sum(dt*clients*(clients-1)/2)
risk = defaultdict(int)  # Dict(1)							# {ap_mac: sum(), ...}
# In the 2nd pass, the sum of the time that maca and macb are nearby. maca < macb
client_pairs = defaultdict(lambda: defaultdict(float))  # {maca: {macb: sum(dt), ...}, ...) maca < macb
client_pairs: DefaultDict[str, DefaultDict[str, float]]
sym_pairs = defaultdict(lambda: defaultdict(str, float))  # {maca: {macb: sum(dt), ...}, ...)
sym_pairs: DefaultDict[str, ]

# In the 1st pass, build the client_user Dict to infer p(user|mac)
client_user = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))  # {mac: {user: [sum of time, p]}, ...}, ...}
client_user: DefaultDict[str, DefaultDict[str, list]]
# Convert the poll samples into a compact form for linear-time client insert/remove
# in the 2nd pass. For each association-to-dissociation time interval, insert a
# (ap_mac, client_mac, user) entry into the hi and goodbye lists:
hi = []							# [(start_time, (ap_mac, client_mac, user)), ...]
bye = []						# [(end_time, (ap_mac, client_mac, user)), ...]

l1r = L1r(range_reader, selection=selection, range_start=range_start,
        verbose=verbose_1(args.verbose))
reader = l1r.__iter__()					# will use iter for __next__
reader.__next__()
if l1r.look_ahead is not None:			# __next__() read a record?
    polled_time = l1r.polled_time - period  # Yes. Simulate a previous poll
while l1r.look_ahead is not None and l1r.polled_time <= range_end:
    prev_polled_time = polled_time		# time of previous poll
    polled_time = l1r.polled_time 		# poll time of this poll
    if polled_time < range_start:		# next record is prior to start of the range?
        reader.__next__()				# Yes. Advance to next record
        continue						# ignoring the before-range record
    # Process all of the records for a polled_time into new_by_ap and new-by_client
    new_by_ap = defaultdict(lambda: defaultdict(int))  # Dict(2)  				# {ap_mac: {client_mac: CSD_rec, ...}, ...}
    new_by_client = defaultdict(1)  # Dict(1)  			# {client_mac: ap_mac, ...}
    while l1r.look_ahead is not None and l1r.polled_time == polled_time:
        in_rec = l1r.look_ahead			# will process the look_ahead record
        reader.__next__()				# and advance to always have a look-ahead
        # obtain the data that we need
        client_mac = in_rec['macAddress_octets']
        ap_mac = in_rec['apMacAddress_octets']
        userName: str = in_rec['userName'].lower()  # lower case for matching
        if re.fullmatch(r'[0-9a-f]{2}(-[0-9a-f]{2}){5}', userName):  # MAC in userName?
            in_rec['userName'] = ''			# Yes, clear userName when not a user
        else:							# No
            m = re.fullmatch(r'([a-z]+[0-9]*)@(case|cwru)\.edu', userName)
            if m:						# user@case.edu ?
                in_rec['userName'] = m.group(1)  # Yes. change to just 'user'
            elif userName.startswith('ads\\'):
                in_rec['userName'] = userName[4:]
            else:
                in_rec['userName'] = userName
        other_ap = new_by_client.get(client_mac, None)
        if other_ap is not None: 	# already received an association for this client?
            del new_by_ap[other_ap][client_mac]  # Yes. only 1 location/client
        new_by_client[client_mac] = ap_mac
        new_by_ap[ap_mac][client_mac] = in_rec
    # while exits after processing final record for polled_time

    # Update by_ap and by_client from poll in new_by_ap and new_by_client
    for client_mac, ap_mac in new_by_client.items():  # for each client in the poll
        csd_rec = new_by_ap[ap_mac][client_mac]
        prev_ap_mac = by_client.get(client_mac, None)
        if prev_ap_mac is not None:		# client in the simulation?
            if prev_ap_mac == ap_mac: 	# Yes. At the same AP?
                by_ap[ap_mac][client_mac]['seen'] = polled_time  # Yes. update end_time
                continue
            else:						# No. Roamed from a different AP
                rec = by_ap[prev_ap_mac][client_mac]  # output the old association
                out(rec['start_time'], prev_polled_time, (prev_ap_mac, client_mac),
                    rec['userName'])
                del by_ap[prev_ap_mac][client_mac] 	# remove it from previous AP
        # enter new association
        csd_rec['start_time'] = prev_polled_time
        csd_rec['seen'] = polled_time  # add start & end to new association
        by_ap[ap_mac][client_mac] = csd_rec  # add to the new AP
        by_client[client_mac] = ap_mac

    # Identify, remove and output clients that are no longer associated
    to_del = []							# [client_mac, ...] to delete after iteration
    for client_mac, ap_mac in by_client.items():
        rec = by_ap[ap_mac][client_mac]
        if rec['seen'] > prev_polled_time:  # client is still associated?
            continue					# Yes
        out(rec['start_time'], prev_polled_time, (ap_mac, client_mac), rec['userName'])
        del by_ap[ap_mac][client_mac]
        to_del.append(client_mac)
    for client_mac in to_del:
        del by_client[client_mac]
    del to_del

l1r.close()
# complete phase 1 simulation by disassociating all remaining clients
print(f"Pass 1 final poll at {strfTime(polled_time)}")
for client_mac, ap_mac in by_client.items():
    rec = by_ap[ap_mac][client_mac]
    out(rec['start_time'], polled_time, (ap_mac, client_mac), rec['userName'])

threads = [] 	# pre-process phase 1 outputs for phase 2
for target in [client_user_process, sort_hi_bye, visited_process]:
    t = Thread(target=target)
    t.start()
    threads.append(t)
for t in threads:
    t.join()

print("\nHours associated       Notes")
non_secs = sum(anonymous[m] for m in anonymous)
tot_hours = int((auth_secs+single_secs+multi_secs+non_secs)/3600)
clock_secs = bye[-1][0] - hi[0][0]
print(f"{tot_hours:8,} total in {len(hi):,} sessions during {clock_secs/3600:3.1f} clock hours")
print(f"{int(auth_secs/3600):8,} authenticated by {auth_macs:6,} clients")
print(f"{int(single_secs/3600):8,} anonymous     by {single_macs:6,} clients " +
    f"reassigned to the single user who authenticated on that client")
print(f"{int(multi_secs/3600):8,} anonymous     by {multi_macs:6,} clients " +
    f"prorated to the multiple users who authenticated on that client")
s = ' Including:' if args.verbose else ''
print(f"\n{int(non_secs/3600):8,} anonymous     by {len(anonymous):6,} clients that never authenticated.{s}")
if args.verbose:
    print(visited_report)

i_auth_macs = 0
i_anon_macs = 0
i_auth_secs = 0.0
i_anon_secs = 0.0
for client_mac, secs in infra_mac.items():  # for each infrastructure client
    client = client_user[client_mac]
    if len(client) == 1 and '' in client:  # anonymous only?
        i_anon_macs += 1				# Yes
        i_anon_secs += secs
    else:								# No
        i_auth_macs += 1
        i_auth_secs += secs
print(f"{int(i_anon_secs/3600):8,} anonymous     by {i_anon_macs:6,} stationary infrastructure. Ignoring.")

# Note extent of clients included that didn't authenticate, but are not infrastructure
anonymous = dict([(mac, t) for mac, t in anonymous.items() if mac not in infra_mac])
secs = sum(t for t in anonymous.values())
if len(anonymous) > 0:
    print(f"{int(secs/3600):8,} anonymous     by {len(anonymous):6,} non-infrastructure clients")
print(f"\n{int(i_auth_secs/3600):8,} authenticated by {i_auth_macs:6,} stationary infrastructure. Ignoring.")
print("""
Each client device is identified by its MAC, displayed in one of the following forms:
    a1b2c3d4e5f6                        always anonymous"
    a1b2c3d4e5f6(user)                  authenticated only as user
    a1b2c3d4e5f6(nn%usera, mm%userb, ...) authenticated nn% as usera, mm% as userb, ...
    a1b2c3d4e5f6(user?)                 never authenticated. Maybe this user
    a1b2c3d4e5f6(bldg-ser-Wnn?)         never authenticated. Mostly near this AP-name
""")

# Few client_macs are used by more than 1 user. Simplify calculation per
# tracked mac by only tracking the user with the most usage of this mac.
# move this user to the 1st user in the MAC's list in client_user
track_l = []							# for reporting later, after global statistics
for client_mac, user_d in client_user.items():
    users = []
    max_user = None						# tracked user of this MAC with greatest p
    max_weight = 0						# max(p) of tracked users of this MAC
    for user, lst in user_d.items():
        if user in args.users: 			# request to track this user?
            p = lst[1]
            if p > max_weight:
                max_weight = p
                max_user = user
            users.append(user)
    if len(users) == 0:					# no users of this mac to track?
        continue						# Yes, none
    if len(users) > 1:					# More than 1 users?
        print(f"{client_mac} used by {', '.join(u for u in users)}."
            + f"Only {max_user} will be tracked on this MAC")
    if client_mac in infra_mac and client_mac not in args.macs:
        track_l.append(f"Classified {mac_str(client_mac)} as infrastructure. Will not track.")
        continue
    track_macs[client_mac] = max_weight
for mac in args.macs:					# include requests for macs too
    track_macs[mac] = 1.0				# with weight=1.0, that might override partial weight

# Clarify that some macs will not be tracked
for client_mac in track_macs:
    if client_mac not in infra_mac:
        continue
    if client_mac in args.macs:
        track_l.append(f"Classified {mac_str(client_mac)} as infrastructure, but will track because requested")
        del infra_mac[client_mac]
for mac in track_macs:
    track_l.append(mac_str(mac))

hi_len = len(hi)
bye_len = len(bye)
future = (2500-1970)*365*24*60*60.0		# epoch seconds far in the future
hi.append((future, ('', '')))			# easier boundary testing
bye.append((future, ('', '')))			# easier boundary testing

del by_ap, by_client					# aren't used in the 2nd pass

# 2nd pass: aggregate statistics and track users
# while simulating classified client_macs associating and dis-associating with APs
# {ap_mac: {'start': float, 'clients':[[client_mac[, p, {mac_b:secs, ...}}] ], ...], ...}, ...}
by_ap = defaultdict(lambda: defaultdict(list))  # Dict(2, [])

pairs_q = Queue(1000) 			# entry is None to exit, else (client_a, client_b
threads = []
t = Thread(target=pairs_thread)		# client_pairs[combinations(clients,2)] += dt
t.start()
threads.append(t)

hi_dex = 0
bye_dex = 0
hi_rec = hi[hi_dex]
bye_rec = bye[bye_dex]
hi_t = hi_rec[0]
bye_t = bye_rec[0]
polled_time = min(hi_t, bye_t, future-1)  # current simulation time
# 1. Process all bye for a polled_time as a batch
# Form set of AP's impacted. Queue this for aggregate workers
# when done, process all of the bye's for this polled time
# 2. Process all of the hi for a polled time as a batch
# perform all of the associations while accumulating the set of impacted ap_mac
# Queue aggregate on the set of APs.
# wait for 'done'
# In each phase, count the number of queued commands, then wait for that many queued acks
# Assumption is that there multiple APs

while bye_dex < bye_len and hi_dex < hi_len:
    while bye_t <= polled_time and bye_dex < bye_len:
        tup = bye_rec[1]
        ap_mac = tup[0]
        client_mac = tup[1]
        if client_mac in infra_mac: 	# Infrastructure client?
            bye_dex += 1
            bye_rec = bye[bye_dex]
            bye_t = bye_rec[0]
            continue					# Yes, ignore it
        aggregate(ap_mac, bye_t)  		# process AP's clients through t=bye_t
        # dissociate client_mac from ap_mac
        clients: list = by_ap[ap_mac]['clients']  # [[client_mac, p, {other_mac: {mac_b: secs, ...}, ...}], ...]
        for i in range(len(clients)): 	# find client_mac in AP's associated clients list
            if clients[i][0] == client_mac:
                client = clients.pop(i)  # remove client from the associated clients list
                if len(client) > 1:  	# tracking this client?
                    track_out[client_mac].append([DISSOC, ap_mac, polled_time, client])
                break
        else:							# client_mac is not associated at ap_mac
            print(f"{strfTime(polled_time)} dissociate could not find client_mac "
                + f"{client_mac} at ap_mac={ap_mac}")
        bye_dex += 1
        bye_rec = bye[bye_dex]
        bye_t = bye_rec[0]
    while hi_t <= polled_time and hi_dex < hi_len:
        tup = hi_rec[1]
        ap_mac = tup[0]
        client_mac = tup[1]
        if client_mac in infra_mac:		# Infrastructure client?
            hi_dex += 1
            hi_rec = hi[hi_dex]
            hi_t = hi_rec[0]
            continue					# Yes. Ignore it
        aggregate(ap_mac, hi_t) 		# process AP's clients through t=hi_t
        # associate client_mac with ap_mac
        clients = by_ap[ap_mac]['clients']  # [[client_mac, p, {other_mac: secs, ...}], ...]
        if client_mac in track_macs:  	# tracking this MAC?
            clients.append([client_mac, track_macs[client_mac], {}])  # Yes. Include exposure dict
            track_out[client_mac].append([ASSOC, ap_mac, polled_time])
        else:
            clients.append([client_mac])
        clients.sort()					# so that client_a < client_b in client_a x client_b
        hi_dex += 1
        hi_rec = hi[hi_dex]
        hi_t = hi_rec[0]
    polled_time = min(hi_t, bye_t)
    if polled_time == future:
        break							# exhausted hi and bye lists
pairs_q.put(None)						# command pairs_thread to exit

for t in threads:						# wait for pair_thread to complete
    t.join()							# before accessing client_pairs

# For anonymous client, mac_a, with a_secs = associated secs >9 hours
# for authenticated client, mac_b, that was near mac_a: sum the seconds that each mac_b.user
# was near mac_a. I.e. sym_pairs[mac_a][mac_b]*client_user[mac_b].p
# max_user = user with maximum seconds, max_secs.
# if max_secs > args.infer_user*a_secs, then label with that user
resolved_user = []						# anonymous client macs with inferred user
resolved_hist = defaultdict(int)  # Dict(1, 0)				# Histogram of max_secs/a_secs
home_loc = []							# anonymous macs with home AP inferred
home_hist = defaultdict(int)  # Dict(1, 0)					# Histogram of home_secs/total_secs
for mac_a, user_a in client_user.items():
    if mac_a in infra_mac or '' not in user_a:  # Infrastructure MAC or authenticated client?
        continue  						# Yes. Have complete information
    for ap_max, ap_secs in visited[mac_a].items():
        break
    else:								# internal error
        print(f"client_user[{mac_a}]={client_user[mac_a]}. Yet visited[{mac_a}] does not exist.")
        print(f"infra_mac[{mac_a}]={infra_mac.get(mac_a,'non-existent')}")
        continue
    # get 1st, which is AP with client's maximum association time
    a_secs = sum(lst[0] for lst in user_a.values())  # total seconds client was associated
    if a_secs < 9*3600:  	# associated for < 9 hours. e.g. 3 hours per day for 3 days?
        continue						# Yes. Insufficient data
    users = defaultdict(float)  # Dict(1, 0.0)
    d = sym_pairs[mac_a]				# {other_mac: secs, ...}
    for mac_b, b_secs in d.items():
        user_b = client_user[mac_b]		# {user_name: [seconds, p], ...}
        if '' in user_b:				# mac_b is an anonymous client too?
            continue					# Yes. Can't provide a user inference
        for user, lst in user_b.items():  # for each user of mac_b
            users[user] += b_secs*lst[1]  # + seconds that mac_a and mac_b are near * p(user|mac_b)
    max_user = None						# mac_b's user with most time near this mac_a
    max_secs = 0.0						# max_user's seconds near mac_a
    for user, secs in users.items(): 	# ID mac_b's user with max(seconds) near mac_a
        if secs > max_secs:
            max_secs = secs
            max_user = user
    if max_user is not None:
        fraction = max_secs/a_secs
        resolved_hist[int(10*fraction)] += 1
        if fraction > args.infer_user:  # mac_a near user > threshhold of the time?
            resolved_user.append((mac_a, max_user))  # Yes. Infer user of mac_a
            continue
    # mac_a is an anonymous user without close association with a specific user
    # infer the mac's home base, if it has one ***** finish below
    if a_secs > 0:
        if ap_secs > a_secs:
            print(f"mac_a={mac_a}, ap_secs={ap_secs} > {a_secs}=a_secs")
            mac_a = mac_a
        home_hist[int(10*ap_secs/a_secs)] += 1
    if ap_secs < 0.5*a_secs or a_secs < args.infer_home*clock_secs/24:
        continue
    home_loc.append((mac_a, ap_max))


print("""
Campus-wide statistics on the number of clients which are deemed 'nearby',
because they are associated with the same Access Point.
Each identifiable user has a risk metric = hours/day * number of nearby clients.
When a person carries multiple authenticated clients, reported exposures and risks
are as if the person were multiply present.
For each range of the risk metric, the number of clients:""")
# exposure by user
exposure = defaultdict(float)  # Dict(1, 0.0)
for mac_a, macs in sym_pairs.items():
    mac_a_user_d = client_user[mac_a] 	# {user: [secs,p], ...}
    for user_a, lst_a in mac_a_user_d.items():
        weight_a = lst_a[1]
        if user_a == '' or re.match('-.*-', user_a):  # not_auth or access point?
            continue
        for mac_b, t in macs.items():
            mac_b_user_d = client_user[mac_b]  # {user: [secs, p], ...}
            for user_b, lst_b in mac_b_user_d.items():
                weight_b = lst_b[1]
                if user_b == '' or re.match('-.*-', user_b):  # not_auth or AP?
                    continue
                if user_a == user_b:
                    continue
                exp = weight_a*weight_b*t  # time user_a is exposed to user_b
                exposure[user_a] += exp  # user_a's total exposure to other users
                exposure[user_b] += exp  # user_b's total exposure to other users

sr2 = sqrt(2.0)
# histogram of log(2) of userName exposure
exp = list(exposure.items())
histo = defaultdict(int)  # Dict(1, 0)
for user, t in exp:
    histo[int(2*log2(t/3600+1))] += 1		# hours
histo_report(histo, (lambda x: sr2**x-1), '    hours*nearbys     # users', '8.1f')

# top 10 users for exposure
exp.sort(key=lambda x: -x[1]) 			# sort descending by exposure
top10 = exp[:10]
print(f"\nTop 10 hours*nearbys by user")
for user, t in top10:
    if user == '':
        user = 'unknown because MAC never authenticated'
    print(f"{int(t/3600):10,} {user}")

print("\nOne person's exposure is measured as hours * number of nearby people.")
print("The exposure of n nearby people is hours*n*(n-1)/2.")
# distribution of exposure
histo = defaultdict(int)  # Dict(1, 0)
for x in risk.values():
    histo[int(2*log2(x/clock_secs+1))] += 1 	# also convert seconds to hours
histo_report(histo, (lambda x: sr2**x-1), ' avg(n*(n-1)/2    count of APs', '6.1f')

# top 10 APs for exposure
print("\nAccess Points with the highest average heat metric: hours*n*(n-1)/2")
print("AP Name        avg(n*(n-1)/2")
rsk = list(risk.items())
rsk.sort(key=lambda x: -x[1])			# sort descending by exposure
top10 = rsk[:10]
for ap_mac, x in top10:
    try:
        name = apd_mac[ap_mac]['name']
    except KeyError:
        name = ap_mac
    print(f"{name:16} {x/domain_secs:6.1f}")

# Assignment of inferred user or home AP is deferred until after processing all mac
resolved_secs = 0.0 					# secs of resolved_user association time
for mac_a, max_user in resolved_user: 	# assign the inferred users
    user_a = client_user[mac_a]
    tup = user_a['']
    resolved_secs += tup[0]
    user_a[max_user + '?'] = tup
    del user_a['']

home_secs = 0.0 						# secs of resolved home AP
for mac_a, ap_max in home_loc: 			# assign the inferred home APs
    user_a = client_user[mac_a]
    tup = user_a['']
    home_secs += tup[0]
    try:
        ap_name = apd_mac[ap_max]['name']
    except KeyError:
        ap_name = ap_max
    user_a[ap_name + '?'] = tup
    del user_a['']

print("\nExamining the amount of time that each anonymous client spends near")
print(f"each authenticated user, as a percent of the client's association time.")
print(f"When this percent exceeds {args.infer_user:2.0%} for a user, label")
print(f"the MAC with (user?)")
if args.verbose:
    histo_report(resolved_hist, (lambda x: 10.0*x), 'max user %   count of anon clients', '4')
print(f"{int(resolved_secs/3600):8,} anonymous     by {len(resolved_user):6,}"
    + f" clients near one user >{args.infer_user}")
print(f"\nLabel an anonymous client's home AP when it spends most of its time, and")
print(f"averages more than {args.infer_home} hours/day at that AP. Label the MAC with (AP-name?)")
if args.verbose:
    histo_report(home_hist, (lambda x: 10.0*x), '% time at max-AP  count of anon clients', '4')
print(f"{int(home_secs/3600):8,} anonymous     by {len(home_loc):6,} clients marked with home location")

if len(track_l) > 0:					# tracking messages to report?
    print(f"Tracking the following client MAC:")
    print('\n'.join(track_l))
# output the timeline for each tracked mac
for client_mac, entries in track_out.items():
    summary = defaultdict(float)  # Dict(1, 0.0)				# {user_name: tot_secs, ...}
    print(f"\nTimeline for MAC={mac_str(client_mac)}")
    for lst in entries:
        cmd = lst[0]
        ap_mac = lst[1]
        polled_time = lst[2]
        if cmd == ASSOC:
            print(f"{strfTime(polled_time)} associated to {ap_str(ap_mac)}")
        elif cmd == DISSOC:
            for other_mac, exp in lst[3][2].items():
                print(f"{round(exp/60.0, 1):8.1f} minutes {mac_str(other_mac)}")
                for user in client_user[other_mac]:
                    if user == '' or re.fullmatch(r'.*-.*-[wW][0-9]{2}.*\?', user):
                        continue		# no known user or home AP
                    summary[user] += exp
            print(f"{strfTime(polled_time)} dissociated")
    print(f"\nSummary of total minutes nearby {mac_str(client_mac)}")
    sum_l = [(secs, user) for user, secs in summary.items()]
    sum_l.sort(reverse=True)
    for secs, user in sum_l:
        print(f"{int(round(secs/60,1)):6,} {user}")

"""
message = f"The Access Point {'total' if field == 0 else 'authenticated only'} client counts report is attached"
if platform.system() == 'Linux' and len(args.email) > 0:  # on a linux system and addressees?
    try:
        subprocess.run([r'/usr/bin/mailx', '-s', f"Access Point Client Counts report",
            '-a', out_name] + args.email, check=True, input=message.encode())
    except subprocess.CalledProcessError as e:
        print(f"mailx failed: {e}")
if args.verbose:
    print(f"Reached a record with collection time > day end. Done reading.")
"""
