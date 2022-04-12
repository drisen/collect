#!/usr/bin/python3
# Copyright 2020 Dennis Risen, Case Western Reserve University
#

from awslib import key_split, listRangeObjects, print_selection
import boto3
from contextlib import ExitStack
import cpi
import csv
import gzip
from mylib import credentials, strfTime
from optparse import OptionParser
import os
import platform
import re
import sys
import subprocess
import time
from timeMachine import TimeMachine

csv_path = 'files'  # directory for collect's output files from after noon yesterday
gz_path = 'files/copied'  # directory for files copied and compressed noon yesterday
tm_path = 'C:/Users/dar5/Google Drive/Case/PyCharm/awsstuff/prod'  # path to timeMachines
day_secs = 24*60*60					# number of seconds in a day
tol = 5								# allowable time offset between collectionTimes in a sample

def siteName2locH(name: str) -> str:
	# 23 == len('Location/All Locations/')
	return name[23:].replace('/', ' > ')

def local_reader(file_names: list, range_start: float,
				verbose: int = 0):
	for file_name in file_names:			# files by ascending time_stamp
		m = re.fullmatch(pat_file_name, file_name)  # match table_name with version?
		if not m:							# no match
			continue						# skip this file
		time_stamp = m.group(1)
		if int(time_stamp)/1000.0 < range_start:  # collecting started < start of the day?
			if options.verbose:
				print(f"{file_name} before start of report")
			continue						# Yes, ignore
		# Found next file to read. setup stream to read from
		if file_name[-7:] == '.csv.gz':		# compressed csv file
			full_path = os.path.join(gz_path, file_name)
			stream = gzip.open(full_path, mode='rt')  # stream unzips the gz
		elif file_name[-4:] == '.csv':		# uncompressed csv file
			full_path = os.path.join(csv_path, file_name)
			stream = open(full_path, 'r', newline='')  # stream is just the file
		else:								# unexpected file type
			print(f"Unexpected file_name={file_name}. Ignored.")
			continue
		if options.verbose:
			print(f"reading {strfTime(int(m.group(1)))} {full_path}")
		dict_reader = csv.DictReader(stream)

		for counts_rec in dict_reader:
			yield counts_rec, time_stamp  # record_dict, pol time_stamp
	return  # don't read any more files

def range_reader(selection: list, range_start: float,
			verbose: int = 0):
	for source in selection:		# for each file
		time_stamp = int(key_split(source['Key'])['msec'])
		if int(time_stamp)/1000.0 < range_start:  # collecting started < start of the day?
			if options.verbose:
				print(f"{source['key']} before start of report")
		if options.verbose:
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
				csv_reader = csv.DictReader(unzipped_stream)  # csv(unzip(aws_object))

				for rec in csv_reader:
					yield rec, time_stamp  # recrd_dict, poll time_stamp

# Parse command line for opts
parser = OptionParser(usage='''usage: %prog [opts]
Collect HistoricalClientCounts csv files. Filter by day and AP name. Report client count histograms.''',
		version='%prog v0.6')
parser.add_option('--dateindex', action='store', type='int', dest='dateindex',
	default=5, help='index of the 1st field of the date range in the Key. Default=5. Original was 4.')
parser.add_option('--email', action='append', dest='email',
		default=[], help='Each --email adds an email address to the receive the reports')
parser.add_option('--filtermin', action='store', type='int', dest='filtermin',
		default=3, help='Report only APs with range of client count >=')
parser.add_option('--minutes', action='store', type='int', dest='bucket_minutes',
		default=30, help='summarize samples into time windows of this duration')
parser.add_option('--mindate', action='store', type='string', dest='mindate',
	default=None, help='minimum for range of subkeys Default=None')
parser.add_option('--maxdate', action='store', type='string', dest='maxdate',
	default=None, help='maximum for range of subkeys. Default=None')
parser.add_option('--prefix', action='store', dest='prefix',
	default='cwru-data/network/wifi/ncsdata/dar5/',
	help='bucket + initial prefix. Default=cwru-data/network/wifi/ncsdata/dar5/')
parser.add_option('--name', action='append', dest='names',
		default=[], help='One --name for each AP name prefix to include')
parser.add_option('--verbose', action='count', dest='verbose',
		default=0, help="turn on diagnostic messages")
(options, args) = parser.parse_args()

# build a regular expression for match the AP names
if options.names:
	pat_names = '|'.join(options.names)
else:									# No names provided
	pat_names = '.'						# pat allows all ap names
if options.verbose:
	print(f"filter = {pat_names}")

fileRE = table_name = 'HistoricalClientCounts'

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

from_aws = options.mindate is not None and options.maxdate is not None
if from_aws:							# reading range of days from AWS?
	range_start = time.mktime(time.strptime(options.mindate, '%Y/%m/%d'))
	day_start = range_start
	range_end = time.mktime(time.strptime(options.maxdate, '%Y/%m/%d'))+24*day_secs
	# define and populate TimeMachines for AccessPointDetails and sites
	apd = TimeMachine('AccessPointDetails', key_source="lambda x: x['@id']")
	apd.load_gz(filename=os.path.join(tm_path, 'AccessPointDetails.pkl.gz'))
	sites = TimeMachine('sites', key_source="lambda x: x['groupId']")
	sites.loose = 1					# accept data for times greater than or equal to epoch_msec
	sites.load_gz(filename=os.path.join(tm_path, 'sites.pkl.gz'))
	# sites_groupId = sites  # for clarity that index is groupId
	# sites_locHier = TableIndex(sites, key_source="lambda rec: rec['name'][23:].replace('/', ' > ')")
	# get list of AWS objects to read
	s3 = boto3.resource('s3')
	selection = [x for x in listRangeObjects(options.prefix, options.mindate,
				options.maxdate, options.dateindex, fileRE)]
	bucket, s, prefix = options.prefix.partition('/')
	selection.sort(key=lambda x: x['Key'])
	print_selection(selection, lambda x: x['Key'], verbose=options.verbose)
	if input('Proceed? {Yes,No}: ').lower() != 'yes':
		print('Operation cancelled')
		sys.exit(1)
	a_reader = range_reader(selection, options.verbose)

	def apMac(row) -> str:				# get the mac_address_octets field
		return row['macAddress_octets']

else:									# No. Reading from collect's local files
	# Data from midnight might not be available in a csv until after 4am on transition to
	# daylight savings because the HistoricalClientCounts view is collected every 3 hours.
	t = time.time()
	lt = time.localtime(t)
	if lt.tm_hour < 4 or lt.tm_hour == 4 and lt.tm_min < 30:
		print("warning: data for the end of the day might not be available")
	t -= ((lt.tm_hour*60 + lt.tm_min)*60 + lt.tm_sec)
	day_end = t - t % 60				# midnight this morning
	t -= 22*60*60						# a time early yesterday
	lt = time.localtime(t)
	t -= ((lt.tm_hour*60 + lt.tm_min)*60 + lt.tm_sec)
	day_start = t - t % 60 + 0.001		# 1msec after midnight yesterday morning
	if options.verbose:
		print(f"day is {strfTime(day_start)} to {strfTime(day_end)}")
	range_start = day_start				# range is a single day
	range_end = day_end
	cred = credentials('ncs01.case.edu') 	# get default login credentials
	my_cpi = cpi.Cpi(cred[0], cred[1]) 	# for CPI server instance
	apd_reader = cpi.Cpi.Reader(my_cpi, 'v4/data/AccessPointDetails')
	sites_reader = cpi.Cpi.Reader(my_cpi, 'v4/op/groups/sites')

	def apMac(row: dict) -> str:		# get the macAddress_octets field
		return row['macAddress']['octets']

	# get a directory list of files compressed yesterday noon
	file_names = os.listdir(gz_path)
	# extend with a directory list of files collected since noon yesterday
	file_names.extend(os.listdir(csv_path))
	file_names.sort()					# sort ascending by time_stamp then file_name
	# if options.verbose:
	# 	print(f"will examine {', '.join(file_names)}")
	a_reader = local_reader(file_names, options.verbose)

saved_rec: dict = None					# record and time-stamp not yet processed
saved_time: float = None				#
ignored = 0  # number of records ignored because < range_start
collectionTime = 0						# less than any real collectionTime
counts_rec = {}							# initial value to enter inner while loop
while day_start < range_end-1 and counts_rec is not None:  # for each day in the range of dates
	day_end = day_start + 26*60*60 		# 1am, 2am, or 3am tomorrow morning
	lt = time.localtime(day_end+1)		# to tuple
	day_end -= lt.tm_hour*60*60			# adjusted for possible savings time change

	# Build sites_LH, a mapping from an Access Point's locationHierarchy --> building and floor
	# read in sites table to map from locationHierarchy to building and floor
	if from_aws:						# reading from AWS?
		apd.epoch_msec = int(1000*day_start)  # Yes. Set the TimeMachines to epoch msec
		sites.epoch_msec = int(1000*day_start)
		apd_reader = apd.values()		# fresh generator
		sites_reader = sites.values() 	# fresh generator
	sites_LH = {'Root Area': {'building': 'not defined', 'floor': 'not defined', 'siteType': 'Floor Area'}}
	for row in sites_reader:
		name = row['name']				# Location/All Locations/campus(/outdoorArea|(/building(/floor)?)?
		locationHierarchy = siteName2locH(name)  # ->
		siteType = row.get('siteType', None)
		if siteType is None:			# undefined siteType to ignore?
			if name != 'Location/All Locations':  # and not the root 'Location/All Locations'?
				print(f"No siteType for name={name}, groupName={row.get('groupName', None)}")
		elif siteType == 'Floor Area': 	# floor area?
			row['building'] = name.split('/')[-2]
			row['floor'] = name.split('/')[-1]
		elif siteType == 'Outdoor Area':  # outdoor area
			row['building'] = name.split('/')[-1]  # outdoor area name used for building
			row['floor'] = ''			# has no floor
		elif siteType == 'Building': 	# building
			pass						# not needed for this mapping
		elif siteType == 'Campus':
			pass
		else:
			print(f"unknown siteType {siteType} for name={name}, groupName={row.get('groupName', None)}")
		sites_LH[locationHierarchy] = row

	# build apd_mac, a mapping from AP's mac
	# to {'name':ap_name, 'building':building_name, 'floor': floor_name, 'mapLocation: mapLocation}
	apd_mac = dict()
	for row in apd_reader:
		apName = row['name']
		if 'mapLocation' in row:
			orig = row['mapLocation'].split('|')[-1]
		else:
			print(f"No mapLocation field for {apName}. Using 'default location'")
			orig = 'default location'
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
			print(f"locationHierarchy={row['locationHierarchy']} is not in the sites table")
			continue						# drop this AP from mapping
		apd_mac[mac_address] = {'name': apName, 'building': site['building'],
								'floor': site['floor'], 'mapLocation': map_loc}
	pat_file_name = r'([0-9]+)_' + table_name + r'(v[1-9])?\.csv.*'  # regex for file to read

	# data-structures to build for each day
	samples = dict() 		# {ap_mac: [apName, [[epochSeconds, authCount, count], ...]}
	bad_mac = {}  						# dict of bad mac addresses in records
	first_time = None  					# epoch seconds of first sample read
	last_time = None  					# epoch seconds of last sample read
	while collectionTime < day_end: 	# for each sample in this day
		if saved_rec is None:
			try:						# No saved record. Read next counts record
				counts_rec, time_stamp = a_reader.__next__()
			except StopIteration:
				counts_rec = collectionTime = None
				break
		else:							# Use saved record that isn't yet processed
			counts_rec = saved_rec
			time_stamp = saved_time
			saved_rec = None 			# and mark as having been used
		collectionTime = int(counts_rec.get('collectionTime', time_stamp)) / 1000.0  # msec -> seconds
		if collectionTime < range_start:
			ignored += 1
			continue
		if counts_rec['type'] != 'ACCESSPOINT' or counts_rec['subkey'] != 'All':
			continue  					# ignore fn record types
		if collectionTime >= range_end:  # collected after the end of the range?
			counts_rec = None			# Yes. We're done
			break  		# Any following records will after the end of the range too.

		mac = counts_rec['key']  		# convert xx:xx:xx:xx:xx:xx mac to xxxxxxxxxxxx
		mac = ''.join([mac[i:i+2] for i in (0, 3, 6, 9, 12, 15)])
		try:
			apd_rec = apd_mac[mac]  	# map mac to AP details
		except KeyError:				# lookup failed
			bad_mac[mac] = counts_rec['key']  # for reporting later
			continue					# ignore the record
		name = apd_rec['name'].lower() 	# AP name
		m = re.match(pat_names, name)
		if not m:						# AP's name matches the list of requested names?
			continue					# No match. Ignore record
		if first_time is None:			# first record?
			first_time = collectionTime  # Yes, save first record's collection time
		last_time = collectionTime		# update last record's collection time
		if mac not in samples:
			samples[mac] = [apd_rec['name'], []]
		samples[mac][1].append((collectionTime, int(counts_rec['count']), int(counts_rec['authCount'])))
	# dropped out of while loop for the day.
	saved_rec = counts_rec				# save record and time_stamp for processing ...
	saved_time = time_stamp				# ... in the next day
	if options.verbose and ignored > 0:  # Records in this file with collectionTime < day_start?
		print(f"{ignored} records prior to start of range. Ignored.")
		ignored = 0

	for mac, incoming in bad_mac.items():
		print(f"unknown mac address {incoming} -> {mac}")
	# Report the day's statistics'
	# {ap_mac: [apName, [[epochSeconds, authCount, count], ...], ...]}
	ap_list = [(lst, mac) for (mac, lst) in samples.items()]
	# ap_list is [apName, [[epochSeconds, authCount, count], ...], ...]
	ap_list.sort()	 # sort by apName, a proxy for geographical location
	# construct output headers and bucket start times
	field_names = ['name', 'building', 'floor', 'mapLocation']
	bucket_minutes = options.bucket_minutes  # minutes per output bucket
	bucket_titles = []
	bucket_starts = []
	for minutes in range(0, 24*60+bucket_minutes, bucket_minutes):  # start_time for each bucket ...
		bucket_starts.append(day_start+minutes*60)  # in epoch seconds
		title = f"{minutes//60:02}:{minutes%60:02}"  # time of day as hh:mm
		bucket_titles.append(title)
		field_names.append(title)
	del field_names[len(field_names)-1]  # needed the final bucket, but won't report it
	field_names.append('base')
	field_names.append('max')
	for field in [0, 1]:				# separate report for total and auth_only
		lt = time.localtime(day_start)
		yyyy_mm_dd = f"{lt.tm_year}-{lt.tm_mon:02}-{lt.tm_mday:02}"
		out_name = f"apClientCounts{'Tot' if field == 0 else 'Auth'}{yyyy_mm_dd}.csv"
		with open(out_name, 'w', newline='') as report_file:
			dict_writer = csv.DictWriter(report_file, field_names)
			dict_writer.writeheader()
			for lst, mac in ap_list:
				lst = lst[1]
				# lst is [[epochSeconds, authCount, count], ...], ...]
				lst.sort()				# sort samples ascending by epochSeconds
				vals = [x[1 + field] for x in lst]  # the values
				the_min = the_low = min(vals)  # minimum val[i]
				cnt = vals.count(the_min)
				if cnt == 1:			# One one sample with the minimum?
					the_low += 1		# use higher value for the_low
				the_max = max(vals)  	# maximum val[i]
				if the_max - the_low < options.filtermin:  # sufficient range of values?
					continue			# no filter from output
				# write the record
				ap = apd_mac[mac]
				rec = {'name': ap['name'], 'building': ap['building'], 'floor': ap['floor'],
					'mapLocation': ap['mapLocation'], 'max': the_max, 'base': the_min}

				for buc in range(len(bucket_starts)-1):  # for each bucket except last
					first = 0
					while first < len(lst) and lst[first][0] < bucket_starts[buc]:
						first += 1
					last = first
					while last < len(lst) and lst[last][0] < bucket_starts[buc+1]:
						last += 1
					if first == last: 	# no samples for this bucket
						a_max = the_min - 1
					else:
						a_max = max(vals[first:last + 1])
					rec[bucket_titles[buc]] = a_max - the_min
				dict_writer.writerow(rec)
		message = f"The Access Point {'total' if field == 0 else 'authenticated only'} client counts report is attached"
		if platform.system() == 'Linux' and len(options.email) > 0:  # on a linux system and addressees?
			try:
				subprocess.run([r'/usr/bin/mailx', '-s', f"Access Point Client Counts report",
					'-a', out_name] + options.email, check=True, input=message.encode())
			except subprocess.CalledProcessError as e:
				print(f"mailx failed: {e}")
	day_start += day_secs
if options.verbose:
	print(f"Reached a record with collection time > day end. Done reading.")
