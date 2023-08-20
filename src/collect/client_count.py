#!/usr/bin/python3
# client_count.py Copyright (C) 2020 Dennis Risen, Case Western Reserve University
#
"""
Produce csv reports, apClientCountsAuthYYYY-MM-DD and apClientCountsTotYYYY-MM-DD
of the number of wireless clients associated with each AP
in the listed buildings in each 30 minute interval.
Range of dates is either the previous day (from CPI server)
or the specified range of dates (from AWS S3).

"""
import cpiapi

from awslib import key_split, listRangeObjects, print_selection
import boto3
from contextlib import ExitStack
from cpiapi import Cpi
import csv
import gzip
from mylib import credentials, fromTimeStamp, strfTime, strpTime, verbose_1
from argparse import ArgumentParser
import os
import platform
import re
import sys
import subprocess
from time import time
use_time_machines = False  # use TimeMachine to obtain old data
if use_time_machines:
    pass  # from timeMachine import TimeMachine
from typing import Union

""" To Do
Verify that it is using Eastern time
"""

csv_path = 'files'  # directory for collect's output files from after noon yesterday
gz_path = 'files/copied'  # directory for files copied and compressed noon yesterday
do_not_backup = 'C:/Users/dar5/Downloads/DoNotBackup/'
tm_path = do_not_backup + 'awsstuff/prod'  # path to timeMachines
day_secs = 24 * 60 * 60  	        # number of seconds in a day
tol = 5  					# allowable time offset between collectionTimes in a sample
polled_pat = r'([^/]+/)+([0-9]+)_[^/]+'  # group(2) is polledTime from filename


class TimedTable:
    """Replacement for, or shim to, the TimeMachine

    """
    my_cpi: cpiapi.Cpi = None
    expire_age = 5 * day_secs*1000      #

    def __init__(self, name: str, key_field: str, api_path: str, aws_objects: list, loose: int = 0):
        """Replacement for, or shim to, the TimeMachine class

        :param name:            TimeMachine name
        :param key_field:       table's primary key field
        :param api_path:        server-relative API to read
        :param aws_objects:     list of available AWS objects to try
        :param loose:           TM data from time {-1: <=, 0: ==, 1: >=} epoch_msec
        """
        self._name = name
        self._key_field = key_field
        self._aws_objects = aws_objects
        self._epoch_msec = None
        if use_time_machines:
            # load the TimeMachine indexed by key_field
            pass  # self.tm = TimeMachine(name, key_source=f"lambda x: x['{key_field}']")
            # self.tm.loose = loose
            # self.tm.load_gz(filename=os.path.join(tm_path, f"{name}.json.gz"))
        elif len(aws_objects) > 0:
            # initialize with oldest AWS data from the supplied range
            for obj in aws_objects:
                # extract polledTime epoch_msec from .+/.+/.../NNNNNNN_apiname.csv.gz
                m = re.fullmatch(polled_pat, obj['Key'])
                obj['polledTime'] = int(m.group(2))
            aws_objects.sort(key=lambda x: x['polledTime'])  # sort by polledTime
            self._epoch_msec = aws_objects[0]['polledTime']
            # initialize with oldest data
            self.tm = {row[key_field]: row for row, ts in range_reader(aws_objects[0:1], 0.0)}
            # if args.verbose > 0:
            #    print(f"Read {aws_objects[0]['Key']}")
        else:
            # load dict indexed by key_field from API
            if TimedTable.my_cpi is None:
                # initialize Cpi
                cred = credentials('ncs01.case.edu')  # get default login credentials
                TimedTable.my_cpi = Cpi(cred[0], cred[1])  # for CPI server instance
            self.tm = {row[key_field]: row for row in Cpi.Reader(TimedTable.my_cpi, api_path)}

    def set_epoch_msec(self, epoch_msec: int):
        """Update TimedTable to contents at 1st poll after epoch_msec

        :param epoch_msec:  integer epoch milliseconds
        :return:
        """
        if use_time_machines:
            pass  # self.tm.epoch_msec = epoch_msec  # adjusts values retrieved from TimeMachine
            # self._epoch_msec = epoch_msec
        else:
            if epoch_msec <= self._epoch_msec:
                return                  # don't try to go back in time
            # as needed, GET newer table values from AWS
            for obj in self._aws_objects:
                polledTime = obj['polledTime']
                if polledTime <= self._epoch_msec:
                    continue            # already loaded
                elif polledTime <= epoch_msec:
                    self.tm.update({row[self._key_field]: row for row, ts in range_reader([obj], 0.0)})
                    # if args.verbose > 0:
                    #    print(f"Read {obj['Key']}")
                    self._epoch_msec = polledTime
                else:
                    break   # No more updates to find, since _aws_objects is sorted
            # and expire older records
            del_count = 0
            for key in list(self.tm):
                if 1000*float(self.tm[key]['polledTime']) < self._epoch_msec - TimedTable.expire_age:
                    del self.tm[key]
                    del_count += 1
            if args.verbose > 0 and del_count > 0:
                print(f"Deleted {del_count} old records")

    def values(self):
        """Return the map values

        :return:
        """
        return self.tm.values()


def siteName2locH(name: str) -> str:
    """Translate a siteName to location hierarchy by replacing '/' with ' > '

    :param name: siteName string
    :return:    locationHierarchy name
    """
    # 23 == len('Location/All Locations/')
    return name[23:].replace('/', ' > ')


def local_reader(file_names: list, range_start: float, verbose: int = 0):
    """Read each .csv and .csv.gz file in file_names.
    Yield (record, polledTime) for each record.polledTime>=range_start

    :param file_names: 		sorted list of local files to read
    :param range_start: 	minimum epoch seconds filter
    :param verbose: 		diagnostic messaging level
    :return: 				generator yields (record, polledTime)
    """
    for file_name in file_names:  		# files by ascending time_stamp
        m = re.fullmatch(pat_file_name, file_name)  # match table_name with version?
        if not m:  						# no match
            continue  					# skip this file
        time_stamp = m.group(1)
        if int(time_stamp) / 1000.0 < range_start:  # collecting started < start of the day?
            if args.verbose > 0:
                print(f"{file_name} before start of report")
            continue  					# Yes, ignore
        # Found next file to read. setup stream to read from
        if file_name[-7:] == '.csv.gz':  # compressed csv file
            full_path = os.path.join(gz_path, file_name)
            stream = gzip.open(full_path, mode='rt')  # stream unzips the gz
        elif file_name[-4:] == '.csv':  # uncompressed csv file
            full_path = os.path.join(csv_path, file_name)
            stream = open(full_path, 'r', newline='')  # stream is just the file
        else:  							# unexpected file type
            print(f"Unexpected file_name={file_name}. Ignored.")
            continue
        if args.verbose > 0:
            print(f"reading {strfTime(int(m.group(1)))} {full_path}")
        dict_reader = csv.DictReader(stream)

        for counts_rec in dict_reader:
            yield counts_rec, time_stamp  # record_dict, poll time_stamp
    return  							# don't read any more files


def range_reader(selection: list, range_start: float,
                 verbose: int = 0):
    """Read sorted list of S3 objects. Yield
    Yield (record, polledTime) for each record.polledTime>=range_start

    :param selection: 		sorted list of S3 objects
    :param range_start: 	minimum polledTime filter (epoch seconds)
    :param verbose: 		diagnostic messaging level
    :return: 				yields (record_dict, poll time_stamp)
    """
    for source in selection:  			# for each file
        time_stamp = int(key_split(source['Key'])['msec'])
        if int(time_stamp) / 1000.0 < range_start:  # collecting started < start of the day?
            if args.verbose > 0:
                print(f"{source['key']} before start of report")
        if args.verbose > 0:
            print(f"reading {source['Key']}")
        try:  							# catch any unexpected error
            aws_object = s3.Object(bucket_name=bucket, key=source['Key'])
        except Exception as e:
            print(f"s3.Object({bucket}, {source['Key']} causes {e}")
            continue
        else:
            with ExitStack() as stack:  # when exiting this block, automatically...
                bucket_stream = aws_object.get()['Body']  # aws_object reader
                stack.callback(bucket_stream.close)  # ... bucket_stream.close()
                unzipped_stream = gzip.open(bucket_stream, mode='rt')  # unzip(aws_object)
                stack.enter_context(unzipped_stream)  # ... unzipped_stream.close()
                csv_reader = csv.DictReader(unzipped_stream)  # csv(unzip(aws_object))

                for rec in csv_reader:
                    yield rec, time_stamp  # record_dict, poll time_stamp


# Parse command line for opts
parser = ArgumentParser(description='''Collect HistoricalClientCounts csv files.
Filter by day and AP name. Report client count histograms.''')
parser.add_argument('--dateindex', action='store', type=int, default=5,
                    help='index of the 1st field of the date range in the Key. Default=5. Original was 4.')
parser.add_argument('--filtermin', action='store', type=int, default=3,
                    help='Report only APs with range of client count >=')
parser.add_argument('--minutes', action='store', type=int, dest='bucket_minutes', default=30,
                    help='summarize samples into time windows of this duration')
parser.add_argument('--mindate', action='store', default=None,
                    help='YYYY/MM/DD minimum for range of subkeys Default=None')
parser.add_argument('--maxdate', action='store', default=None,
                    help='YYYY/MM/DD maximum for range of subkeys. Default=None')
parser.add_argument('--prefix', action='store', default='cwru-data/network/wifi/ncsdata/dar5/',
                    help='bucket + initial prefix. Default=cwru-data/network/wifi/ncsdata/dar5/HistoricalClientCounts/')
parser.add_argument('--verbose', action='count', default=0,
                    help="increment diagnostic message level")
parser.add_argument('--email', action='append', default=[],
                    help='each email address to the receive the reports')
parser.add_argument('--title', action='append', default=[],
                    help='title for each set of files')
# Workaround since the Python 3.6 library does not include the "extend" action
# parser.add_argument('--names', action='extend', dest='names', nargs='*', default=[],
parser.add_argument('--name', action='append', dest='names', default=[],
                    help='each AP name prefix to include')
args = parser.parse_args()

# Separate email and name lists into separate profiles
sep = '/'
num = args.names.count(sep)
emails = args.email.count(sep)
titles = args.title.count(sep)
if num != args.email.count(sep) or num != args.title.count(sep):
    print(f"Number of email ({emails}), title ({titles}), and names ({num}) profiles are not equal")
    exit(1)
profiles = []
e_start = t_start = n_start = 0
for i in range(num):
    e_end = args.email.index(sep, e_start)
    t_end = args.title.index(sep, t_start)
    n_end = args.names.index(sep, n_start)
    names = args.names[n_start:n_end]
    name_pat = ',' if len(names) == 0 else '|'.join(names)  # RE to match the AP names
    profiles.append({'email': args.email[e_start:e_end],
                     'title': ' '.join(args.title[t_start:t_end]),
                     'names': names, 'name_pat': name_pat})
    if args.verbose:
        print(f"{i} email={profiles[i]['email']} title={profiles[i]['title']} name_pat={name_pat}")
    e_start = e_end+1
    t_start = t_end+1
    n_start = n_end+1
names = args.names[n_start:]
name_pat = ',' if len(names) == 0 else '|'.join(names)  # RE to match the AP names
profiles.append({'email': args.email[e_start:],
                 'title': ' '.join(args.title[t_start:]),
                 'names': names, 'name_pat': name_pat})
if args.verbose:
    print(f"{num} email={profiles[num]['email']} title={profiles[num]['title']} name_pat={name_pat}")
# the resulting files for each profile[i] will be in directory<i>. e.g. 0, 1, ...
# ensure that the needed directories exist
for folder in range(len(profiles)):
    if not os.path.isdir(str(folder)):
        os.mkdir(str(folder))
fileRE = table_name = 'HistoricalClientCounts'
# regular expressions for mapLocation cleanup
faceplate_re = r'[0-9]{2,3}-[0-9]{2}-[sSbB]{0,1}[0-9]{1,3}b?-[a-zA-Z0-9][0-9]*'
nth = r'sub-basement|basement|ground|\?|1st|2nd|3rd|4th|5th|6th|7th|8th|9th|10th|11th|12th|13th'
nameth = r'first|second|third|fourth|fifth|sixth|seventh|eight|ninth|tenth|eleventh|twelfth|thirteenth'
nth_floor = re.compile('(' + nth + '|' + nameth + ')[ _]floor', flags=re.IGNORECASE)
named = re.compile(r'basement|sub-basement|ground|mezz[a-z]*|attic|penthouse|rooftop|top floor', flags=re.IGNORECASE)
thingy = r'elevator[s]|cafeteria|hallway|SER room|stairs|stairwell|'
floor_n = re.compile(r'floor[ _][0-9]+', flags=re.IGNORECASE)
inside = re.compile(f'inside of |inside |in ', flags=re.IGNORECASE)
good = re.compile(r'(by |near )?((' + thingy + r')|((room |rm )?[a-z]?[0-9]+-?[a-z]?))', flags=re.IGNORECASE)

apd_objects = []
sites_objects = []
from_aws = args.mindate is not None and args.maxdate is not None
if from_aws:  							# reading range of days from AWS?
    range_start = strpTime(args.mindate, '%Y/%m/%d')
    day_start = range_start
    range_end = strpTime(args.maxdate, '%Y/%m/%d') + 1.5*day_secs
    # define and populate TimeMachines for AccessPointDetails and sites
    # apd = TimeMachine('AccessPointDetails', key_source="lambda x: x['@id']")
    # apd.load_gz(filename=os.path.join(tm_path, 'AccessPointDetails.json.gz'))
    # sites = TimeMachine('sites', key_source="lambda x: x['groupId']")
    # sites.loose = 1  	# accept data for times greater than or equal to epoch_msec
    # sites.load_gz(filename=os.path.join(tm_path, 'sites.json.gz'))
    # sites_groupId = sites  			# for clarity that index is groupId
    # sites_locHier = TableIndex(sites, key_source="lambda rec: rec['name'][23:].replace('/', ' > ')")
    # get list of AWS objects to read
    s3 = boto3.resource('s3')
    selection = [x for x in listRangeObjects(args.prefix + 'HistoricalClientCounts/', args.mindate,
                                             args.maxdate, args.dateindex, fileRE, verbose_1(args.verbose))]
    bucket, s, prefix = args.prefix.partition('/')
    selection.sort(key=lambda x: x['Key'])
    print_selection(selection, lambda x: x['Key'], verbose=args.verbose)
    if input('Proceed? {Yes,No}: ').lower() != 'yes':
        print('Operation cancelled')
        sys.exit(1)
    a_reader = range_reader(selection, args.verbose)
    if not use_time_machines:
        # get equivalent lists of sources on AWS for AccessPointDetails and sites
        apd_objects = [x for x in listRangeObjects(args.prefix + 'AccessPointDetails/', args.mindate,
                                                   args.maxdate, args.dateindex, 'AccessPointDetails',
                                                   verbose_1(args.verbose))]
        sites_objects = [x for x in listRangeObjects(args.prefix + 'sites/', args.mindate,
                                                     args.maxdate, args.dateindex, 'sites',
                                                     verbose_1(args.verbose))]

    def apMac(row) -> str:  			# get the mac_address_octets field
        """return row['macAddress_octets']
        
        :param row:     AccessPointDetails record
        :return:        macAddress octets
        """
        return row['macAddress_octets']  # from pre-processed AccessPointDetails record

else:  # No. Reading from collect's local files
    # Data from midnight might not be available in a csv until after 4am on transition to
    # daylight savings because the HistoricalClientCounts view is collected every 3 hours.
    t = time()
    lt = fromTimeStamp(t)
    if lt.hour < 4 or lt.hour == 4 and lt.minute < 30:
        print("warning: data for the end of the day might not be available")
    t -= ((lt.hour * 60 + lt.minute) * 60 + lt.second)
    day_end = t - t % 60  				# midnight this morning
    t -= 22 * 60 * 60  					# a time early yesterday
    lt = fromTimeStamp(t)
    t -= ((lt.hour * 60 + lt.minute) * 60 + lt.second)
    day_start = t - t % 60 + 0.001  	# 1msec after midnight yesterday morning
    if args.verbose > 0:
        print(f"day is {strfTime(day_start)} to {strfTime(day_end)}")
    range_start = day_start  			# range is a single day
    range_end = day_end
    # apd_reader = Cpi.Reader(my_cpi, 'v4/data/AccessPointDetails')
    # sites_reader = Cpi.Reader(my_cpi, 'v4/op/groups/sites')

    def apMac(row: dict) -> str:  		# get the macAddress_octets field
        """return row['macAddress']['octets']
        
        :param row:     AccessPointDetails record
        :return:        macAddress octets
        """
        return row['macAddress']['octets']  # from raw AccessPointDetails API record

    # get a directory list of files compressed yesterday noon
    file_names = os.listdir(gz_path)
    # extend with a directory list of files collected since noon yesterday
    file_names.extend(os.listdir(csv_path))
    file_names.sort()  					# sort ascending by time_stamp then file_name
    # if args.verbose:
    # 	print(f"will examine {', '.join(file_names)}")
    a_reader = local_reader(file_names, args.verbose)

apd = TimedTable('AccessPointDetails', '@id', 'v4/data/AccessPointDetails', apd_objects)
sites = TimedTable('sites', 'groupId', 'v4/op/groups/sites', sites_objects)

saved_rec: Union[dict, None] = None     # record and time-stamp not yet processed
saved_time: Union[float, None] = None
time_stamp: Union[float, None] = None
ignored = 0  					# number of records ignored because < range_start
collectionTime = 0  				# less than any real collectionTime
counts_rec = {}  					# initial value to enter inner while loop
while day_start < range_end - 1 and counts_rec is not None:  # for each day in the range of dates
    if args.verbose > 0:
        print(f"starting {strfTime(day_start)}")
    day_end = day_start + 26 * 60 * 60  # 1am, 2am, or 3am tomorrow morning
    lt = fromTimeStamp(day_end + 1)  	# to tuple
    day_end -= lt.hour * 60 * 60  		# adjusted for possible savings time change

    # Build sites_LH, a mapping from an Access Point's locationHierarchy --> building and floor
    # read in sites table to map from locationHierarchy to building and floor

    if from_aws:  # reading from AWS?
        # Yes. Set the TimeMachines to epoch msec at end of day, to include changes during the day
        apd.set_epoch_msec(int(1000 * (day_start+day_secs)))
        sites.set_epoch_msec(int(1000 * (day_start+day_secs)))
        apd_reader = apd.values()  		# fresh generator
        sites_reader = sites.values()  	# fresh generator
    sites_LH = {'Root Area': {'building': 'not defined', 'floor': 'not defined', 'siteType': 'Floor Area'}}
    for row in sites.values():
        name = row['name']  # Location/All Locations/campus(/outdoorArea|(/building(/floor)?)?
        locationHierarchy = siteName2locH(name)  # ->
        siteType = row.get('siteType', None)
        if siteType is None:  			# undefined siteType to ignore?
            if name != 'Location/All Locations':  # and not the root 'Location/All Locations'?
                print(f"No siteType for name={name}, groupName={row.get('groupName', None)}")
        elif siteType == 'Floor Area':  # floor area?
            row['building'] = name.split('/')[-2]
            row['floor'] = name.split('/')[-1]
        elif siteType == 'Outdoor Area':  # outdoor area
            row['building'] = name.split('/')[-1]  # outdoor area name used for building
            row['floor'] = ''  			# has no floor
        elif siteType == 'Building':  	# building
            pass  						# not needed for this mapping
        elif siteType == 'Campus':
            pass
        else:
            print(f"unknown siteType {siteType} for name={name}, groupName={row.get('groupName', None)}")
        sites_LH[locationHierarchy] = row

    # build apd_mac, a mapping from AP's mac
    # to {'name':ap_name, 'building':building_name, 'floor': floor_name, 'mapLocation: mapLocation}
    apd_mac = dict()
    for row in apd.values():
        apName = row['name']
        if 'mapLocation' in row:
            orig = row['mapLocation'].split('|')[-1]
        else:
            print(f"No mapLocation field for {apName}. Using 'default location'")
            orig = 'default location'
        map_loc: str = orig  			# provide a hint
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
        mac_address = apMac(row)  # get row['macAddress_octets'] or row['macAddress']['octets']
        try:
            site = sites_LH[row['locationHierarchy']]
        except KeyError:
            print(f"No sites_LH['{locationHierarchy}'] for APD[{mac_address}].mapLocation={row['mapLocation']}")
            continue  # drop this AP from mapping
        apd_mac[mac_address] = {'name': apName, 'building': site['building'],
                                'floor': site['floor'], 'mapLocation': map_loc}
    pat_file_name = r'([0-9]+)_' + table_name + r'(v[1-9])?\.csv.*'  # regex for file to read

    # data-structures to build for each day
    samples = [{} for i in profiles]  # {ap_mac: [apName, [[epochSeconds, authCount, count], ...]}
    bad_mac = {}  						# dict of bad mac addresses in records
    first_time = None                   # epoch seconds of first sample read
    last_time = None                    # epoch seconds of last sample read
    while collectionTime < day_end:  	# for each sample in this day
        if saved_rec is None:  			# have we read ahead?
            try:  						# No saved record. Read next counts record
                counts_rec, time_stamp = a_reader.__next__()
            except StopIteration:
                counts_rec = collectionTime = None
                break
        else:  							# Use saved record that isn't yet processed
            counts_rec = saved_rec
            time_stamp = saved_time
            saved_rec = None  			# and mark as having been used
        collectionTime = int(counts_rec.get('collectionTime', time_stamp)) / 1000.0  # msec -> seconds
        if collectionTime < range_start:
            ignored += 1
            continue
        if counts_rec['type'] != 'ACCESSPOINT' or counts_rec['subkey'] != 'All':
            continue  					# ignore fn record types
        if collectionTime >= range_end:  # collected after the end of the range?
            counts_rec = None  			# Yes. We're done
            break  		# Any following records will after the end of the range too.

        mac = counts_rec['key']
        mac = mac.replace(':', r'')       # convert xx:xx:xx:xx:xx:xx to xxxxxxxxxxxx
        # mac = ''.join([mac[i:i + 2] for i in (0, 3, 6, 9, 12, 15)])  # previous implementation
        apd_rec = apd_mac.get(mac, None)  # map mac to AP details
        if apd_rec is None:             # lookup failed
            bad_mac[mac] = counts_rec['key']  # for reporting later
            continue  # ignore the record

        name = apd_rec['name'].lower()  # AP name
        for profile in range(len(profiles)):
            m = re.match(profiles[profile]['name_pat'], name)
            if not m:  					# AP's name matches the list of requested names?
                continue  				# No match. Ignore record in this profile
            if first_time is None:  	# first record?
                first_time = collectionTime  # Yes, save first record's collection time
            last_time = collectionTime  	# update last record's collection time
            if mac not in samples[profile]:
                samples[profile][mac] = [apd_rec['name'], []]
            samples[profile][mac][1].append((collectionTime, int(counts_rec['count']), int(counts_rec['authCount'])))
    # dropped out of while loop for the day.
    saved_rec = counts_rec  			# save record and time_stamp for processing ...
    saved_time = time_stamp  			# ... in the next day
    if args.verbose > 0 and ignored > 0:  # Records in this file with collectionTime < day_start?
        print(f"{ignored} records prior to start of range. Ignored.")
        ignored = 0

    max_bad_report = 10000 if args.verbose > 1 else 20
    for mac, incoming in bad_mac.items():
        if max_bad_report <= 0:
            print(f"... {len(bad_mac)-max_bad_report} additional unknown MAC addresses")
            break
        print(f"MAC address {incoming} -> {mac} not in AccessPointDetails")
        max_bad_report -= 1
    # Report the day's statistics for each profile
    for profile in range(len(profiles)):
        # {ap_mac: [apName, [[epochSeconds, authCount, count], ...], ...]}
        ap_list = [(lst, mac) for (mac, lst) in samples[profile].items()]
        # ap_list is [apName, [[epochSeconds, authCount, count], ...], ...]
        ap_list.sort()  	# sort by [apName, ...], a proxy for geographical location
        # construct output headers and bucket start times
        field_names = ['name', 'building', 'floor', 'mapLocation']
        bucket_minutes = args.bucket_minutes  # minutes per output bucket
        bucket_titles = []
        bucket_starts = []
        for minutes in range(0, 24 * 60 + bucket_minutes, bucket_minutes):  # start_time for each bucket ...
            bucket_starts.append(day_start + minutes * 60)  # in epoch seconds
            title = f"{minutes // 60:02}:{minutes % 60:02}"  # time of day as hh:mm
            bucket_titles.append(title)
            field_names.append(title)
        del field_names[len(field_names) - 1]  # needed the final bucket, but won't report it
        field_names.append('base')
        field_names.append('max')
        for field in [0, 1]:  				# separate report for total and auth_only
            lt = fromTimeStamp(day_start)
            yyyy_mm_dd = f"{lt.year}-{lt.month:02}-{lt.day:02}"
            out_name = os.path.join(str(profile), f"apClientCounts{'Tot' if field == 0 else 'Auth'}{yyyy_mm_dd}.csv")
            with open(out_name, 'wt', newline='') as report_file:
                dict_writer = csv.DictWriter(report_file, field_names)
                dict_writer.writeheader()
                for lst, mac in ap_list:
                    lst = lst[1]
                    # lst is [[epochSeconds, authCount, count], ...], ...]
                    lst.sort()  			# sort samples ascending by epochSeconds
                    vals = [x[1 + field] for x in lst]  # the values
                    the_min = the_low = min(vals)  # minimum val[i]
                    cnt = vals.count(the_min)
                    if cnt == 1:  			# One one sample with the minimum?
                        the_low += 1  		# use higher value for the_low
                    the_max = max(vals)  	# maximum val[i]
                    if the_max - the_low < args.filtermin:  # sufficient range of values?
                        continue  			# no filter from output
                    # write the record
                    ap = apd_mac[mac]
                    rec = {'name': ap['name'], 'building': ap['building'], 'floor': ap['floor'],
                           'mapLocation': ap['mapLocation'], 'max': the_max, 'base': the_min}

                    for buc in range(len(bucket_starts) - 1):  # for each bucket except last
                        first = 0
                        while first < len(lst) and lst[first][0] < bucket_starts[buc]:
                            first += 1
                        last = first
                        while last < len(lst) and lst[last][0] < bucket_starts[buc + 1]:
                            last += 1
                        if first == last:  	# no samples for this bucket
                            a_max = the_min - 1
                        else:
                            a_max = max(vals[first:last + 1])
                        rec[bucket_titles[buc]] = a_max - the_min
                    dict_writer.writerow(rec)
            which = 'total' if field == 0 else 'authenticated only'
            message = ' '.join(["The Access Point", which,
                                "client counts report is attached for", profiles[profile]['name_pat']])
            print(message)
            if platform.system() == 'Linux' and len(args.email) > 0:  # on a linux system and addressees?
                try:
                    params = [r'/usr/bin/mailx', '-s', "Access Point Client Counts report", '-a', out_name]\
                             + profiles[profile]['email']
                    print(f"params={params}")
                    subprocess.run(params, check=True, input=message.encode())
                except subprocess.CalledProcessError as e:
                    print(f"mailx failed: {e}")
    day_start += day_secs
if args.verbose > 0:
    print(f"Reached a record with collection time > day end. Done reading.")
