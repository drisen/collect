#!/usr/bin/python3
#
# Collect.py is Copyright 2019 Dennis Risen, Case Western Reserve University
#
from cpi import Cpi
from cpi import Pager, SubTable, Table, date_bad, find_table, allTypes, to_enum
from cpi import all_table_dicts, production, real_time
import json
from mylib import *
from optparse import OptionParser
import os
import pprint
import sys
import time
import threading

TAU = 20 			# time-constant for recordsPerHour learning. Samples or Days
""" To do
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
- Write a test daemon, then configure this as a daemon
- Write report for correct data in op/group/sites
- when ingesting into the data lake
	create username table (UID, username) and allow access only to the UID
	when creating the AWS tables: define roles; GRANT SELECT (almost *) to PUBLIC;
	use available DB-native types such as JSON, INET, MACADDR, ...

Not to do
- report fields with total count != recCnt
- csPager add polling in the danger zone

"""

def check_fields(table: SubTable, record: dict):
	"""fields==>check field types present; enums==> check enum values present

	Parameters:
		table (SubTable):		the Table
		record (dict):			a flattened record
	"""
	if not table.check_fields and not table.check_enums: 	# no checking to do?
		return							# Yes, quickly return
	for key in record:					# for each field ...
		val = record[key]
		if table.check_fields:
			typ = type(val) 			# type of current field value
			table.field_counts[key][typ] += 1  # The number of instances of each type
		if not table.check_enums:
			continue
		ftk = table.fieldTypes.get(key, None)
		if ftk is not None and isinstance(ftk['values'], dict):  # defined ENUM?
			table.field_values[key][val] += 1

def check_fields_init(sub_table: SubTable, fields: bool = True, enums: bool = True):
	"""Initialize table for check_fields and field_report.
	Parameters:
		sub_table (SubTable):	Table or SubTable
		fields (bool):			True to count field value type(s)
		enums (bool):			True to count ENUM values

	"""
	sub_table.field_counts = Dict(2)  # count of instances of each field
	sub_table.field_values = Dict(2)  # count of each value for each enum field
	sub_table.check_fields = fields
	sub_table.check_enums = enums

def collect(tables: dict, real_time: bool = False, semaphore: threading.Semaphore = None):
	"""Collect the tables in tables dictionary
	Parameters:
		tables (dict):			tables to collect {table_name: table, ...}
		real_time (bool):		True for real-time priority collector
		semaphore (threading.Semaphore): the low-priority collector's Semaphore
	"""
	my_name = '+' if real_time else '-'
	print(my_name)
	myCpi = Cpi(options.username, options.password,
					baseURL=f"https://{options.server}/webacs/api/",
					semaphore=None if real_time else semaphore)  # CPI server instance
	# Obtain CPI's rate limiting parameters
	tableURL = 'v4/op/rateService/rateLimits'
	cpiReader = Cpi.Reader(myCpi, tableURL, verbose=mylib.verbose_1(options.verbose))
	for rec in cpiReader:
		Cpi.maxResults = rec['limitPageSize']
		Cpi.rateWait = Cpi.windowSize = rec['windowSize'] / 1000.0
		Cpi.segmentSize = Cpi.windowSize / rec['windowSegments']
		Cpi.perUserThreshold = rec['perUserThreshold']
		break  # only expecting the single record
	else:  								# there was no record
		logErr(f"Could not read CPI's {tableURL}")
		sys.exit(1)

	if not options.reset:  				# user didn't ask to reset?
		read_state('realtime.json' if real_time else 'collect.json', tables)  # Restore state from previous run

	# On start-up, collection might be well behind schedule.
	# Reduce batch sizes to reduce the maximum time until each table's oldest
	# records are collected.
	if not real_time:
		Pager.timeScaleSave = Pager.timeScale
		Pager.timeScale = Pager.timeScale / 2  # 1/2-size batches until after first sleep

	loopForever = True
	cycle = 0
	while loopForever:
		loopForever = not options.single  # Loop forever, except for a single poll
		if options.verbose > 0 and cycle % 10 == 0:  # Every 10th table retrieval ...
			# Report the current scheduling information for each table
			print(
f"\n{my_name}{'lastId':>12},{'minSec':>20},{'maxTime':>20},{'nextPoll':>20},{'startPoll':>20}, recs/Hr, Hrs, tableName")
			for tbl_name in tables:
				tbls = tables[tbl_name]
				for tbl in tbls:
					print(f"{my_name}{tbl.lastId:12}, ", end='')
					print(','.join("{:>20}".format(strfTime(t) if isinstance(t, str) and t != '0' or t > 0 else '-')
							for t in (tbl.minSec, tbl.maxTime, tbl.nextPoll, tbl.startPoll)), end='')
					secs = anyToSecs(tbl.maxTime)
					hours = int((time.time() - secs) / 3600) if secs is not None and secs != 0.0 else ''
					print(f",{int(tbl.recordsPerHour):8},{hours:4}, {tbl.tableName}")
		cycle += 1

		# Check for pending shutdown

		# Select highest priority table by (1)overdue poll then (2)min nextPoll time
		tbl: Table = None
		nextPoll = sys.float_info.max
		for tbl_name in tables:  		# over all names in the tables dict
			if len(options.table_name) > 0 and tbl_name not in options.table_name:
				continue				# ignore this table_name
			tbls = tables[tbl_name]  	# [table, ...] with shared table_name
			for t in tbls:  			# for all versions of a table_name
				tblnp = t.nextPoll
				if t.polled and tblnp < time.time():  # overdue polled?
					tbl = t  			# Yes, any overdue poll is 1st priority
					break  			# break out of nested loops w/ answer is table
				if tblnp < nextPoll:  	# earlier nextpoll needed?
					tbl, nextPoll = t, tblnp  # Yes, update w/ better 2nd priority
			else:  						# inner loop didn't find a 1st priority
				continue  				# so continue looking for best 2nd priority
			break  						# inner loop found a 1st priority
		if tbl is None:					# no table to find
			print(f"No {'real time' if real_time else 'production'} tables to poll")
			return
		delta = tbl.nextPoll - time.time()
		if delta > 0 and loopForever:  	# sleep except for single table
			# Update daemon status
			if options.verbose > 0:
				print(f"{my_name} Sleeping {int(delta)} seconds", end=' ', flush=True)
			time.sleep(delta)  			# Sleep until requested poll time
			if not real_time:
				Pager.timeScale = Pager.timeScaleSave  # Caught-up. Restore batch size
		# Update daemon status

		# Collect requested records and write to csv file stamped with epochMillis
		tbl.polledTime = time.time()
		version = tbl.version if tbl.version != "v1" else ""
		file_name = os.path.join(outputPath,
			f"{str(int(1000 * tbl.polledTime))}_{tbl.tableName}{version}")
		printIf(True, f"{my_name}Collecting {file_name} ", end='')
		check = tbl.polledTime - tbl.checked_time > 24 * 60 * 60
		if check and tbl.tableName == 'ClientDetails':
			tbl.minSec = 0		# occasionally get all records, not just updated records

		# initialize for check_fields and field_report
		check_fields_init(tbl, check or options.fields, check or options.enums)

		tbl.open_writer(file_name)  	# open table DictWriter and write header record
		for name in tbl.subTables:  	# for each sub_table ...
			sub_table = tbl.subTables[name]
			sub_table.recCnt = 0
			if len(sub_table.select) == 0:  # ... any fields defined to be output?
				continue  				# No. So don't open an output file
			sub_table.open_writer(tbl.file_name + '_' + name)  # Yes, open its DictWriter
			check_fields_init(sub_table, check or options.fields, check or options.enums)
		first_rec = True
		first_time = lastTime = None  	# init for case that table has <2 records
		if real_time:					# Am I the priority collector?
			semaphore.acquire()			# Yes. Pause the low-priority collector
			time.sleep(Cpi.windowSize) 	# age-out other Cpi's activity.
		for rec in tbl.generator(server=myCpi, table=tbl, verbose=verbose_1(options.verbose)):
			flat = dict()
			# Flatten the tree into a single level dict with hierarchical field names.
			# Recursively output sub_table records, not including in flattened results
			flatten(rec, flat, tbl, '')
			if tbl.timeField is not None:  # is there a timeField?
				if first_rec:
					lastTime = first_time = flat[tbl.timeField]  # remember first value
					first_rec = False
				else:
					lastTime = flat[tbl.timeField]  # remember last value

			tbl.writer.writerow(flat)
			check_fields(tbl, flat)
		if real_time:						# Am I the priority collector?
			time.sleep(Cpi.windowSize) 		# Yes. age-out my Cpi's activity.
			semaphore.release()				# release the low-priority collector
		# When reading has completed: report, close, and rename the output
		if tbl.check_fields or tbl.check_enums:
			an_err, results = field_report(tbl, options.check_verbose)
			if an_err:
				logErr(results)
			elif len(results) > 0:
				print(my_name+results)

		errorCnt = getattr(tbl, 'errorCnt', 0)
		if options.verbose > 0 and errorCnt > 0:  # report unavailable/unresponsive APs?
			print(f"{my_name}{errorCnt} errors reading {tbl.tableName}")
		# Adjust the recordsPerHour
		if tbl.polled:  				# is table polled? (state is all records)
			new_RPH = ((TAU - 1) * tbl.recordsPerHour + tbl.recCnt) / TAU
		else:  							# table is not polled (batch is a time series)
			now = time.time()
			try:  						# in case of bad time data
				first_time = anyToSecs(first_time)
				lastTime = anyToSecs(lastTime)
				if (now - first_time) > tbl.rollup or (now - lastTime) > tbl.rollup:
					# Some records might be missing. Can't learn from this
					new_RPH = tbl.recordsPerHour
				else:  					# time range is good
					hours = (lastTime - first_time) / (60 * 60)
					# weighted by number of hours of data
					new_RPH = ((TAU - hours / 24) * tbl.recordsPerHour + (hours / 24) * (tbl.recCnt / hours)) / TAU
			except TypeError:  			# ignore bad time data
				new_RPH = tbl.recordsPerHour  # default bad timeFields
				logErr(f"{tbl.tableName} bad batch timeFields: {first_time} - {lastTime}")
			except ZeroDivisionError:  # change in time was zero
				new_RPH = tbl.recordsPerHour  # default if we no change in timeFields
		printIf(options.verbose > 0, f"{my_name}{tbl.recCnt} records processed. lId={tbl.lastId}, lS={strfTime(tbl.minSec)},",
				f"mS={strfTime(tbl.maxTime)}, nP={strfTime(tbl.nextPoll)}", end=' ')
		if options.verbose > 0 and new_RPH != tbl.recordsPerHour:
			print(f"rph: {int(tbl.recordsPerHour)}-->{int(new_RPH)}")
		elif options.verbose > 0:
			print('')
		else:
			pass
		tbl.recordsPerHour = new_RPH

		tbl.close_writer('part' if real_time else None)  # close DictWriter & rename file extn
		if check:				# was this the periodic checking of fields and enums?
			tbl.checked_time = tbl.polledTime  # Yes. Done for another day

		# report, close, and rename each of the subTables' output files
		for name in tbl.subTables:
			sub_table: SubTable = tbl.subTables[name]
			if len(sub_table.select) == 0:  # any fields SELECTed for output?
				continue  				# No. So no file was opened.
			if options.fields or options.enums:
				an_err, results = field_report(sub_table, options.check_verbose)
				if an_err:
					logErr(results)
				elif len(results) > 0:
					print(my_name+results)
			sub_table.close_writer('part' if real_time else None)  	# close DictWriter & rename file extn
		# A collection completed.
		if not options.single:  		# not --single
			write_state('realtime.json' if real_time else 'collect.json', tables)  # save state

def field_report(sub_table: SubTable, verbose: int) -> tuple:
	"""Report each field that is extra, missing, undefined or type/value mis-matched

	Parameters:
		sub_table (SubTable):	table to report
		verbose (int):			True to report all counts

	Returns:
		(error, str,)			(True if report found error(s), report string)
	"""
	def details(enum: bool, defined: object, findings: dict) -> tuple:
		"""Report details for a single attribute

		Parameters:
			enum (bool):		True if this is an ENUM; otherwise False
			defined (object):	{name:index, ...} if ENUM else type(expected data)
			findings (dict):	{(value if ENUM else class):count, ...}
		returns:
			(error, str):	(True iff error(s), details string,)
		"""
		error = False
		v = []
		for found in findings:			# for each value found
			err = (not enum) and found != defined or enum and found not in defined
			error = error or err
			if err or verbose > 0: 		# unexpected or verbose listing?
				v.append((findings[found], found if enum else str(found)[8:-2]))  # add to list
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
	# Each SELECTed field must be present and correct enum values
	for key in select:					# for each SELECTed field
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
	# field_counts contains only fields that are not SELECTed (i.e. expected)
	for key in field_counts:			# for each unexpected field
		if key in field_types:			# known field?
			check = field_types[key].get('check', True)  # field should be checked?
		else:
			check = True
		if verbose > 0 or check:
			err, s = details(False, None, field_counts[key])
			error = error or (check and err)
			result += f"{sub_table.tableName} {'unSELECTed' if key in field_types else 'unknown'} field {key} has {s}\n"
	return error, result

def flatten(tree: dict, result: dict, table: Table, path: str = ''):
	"""Flattens a tree of dicts to a single level dict with pathname keys.
	Process sub_table: On the way down, replace xxxs:{xxx:list} with xxxs:list
	On the way back up, write each record in the list as a table_things record
	and do not include in the flattened results

	Parameters:
		tree (dict):	input tree of dict
		result (dict):	output flattened dict
		table (SubTable):	Table defining the fields and sub_tables
		path (str):		pathname to top of the tree

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
			if ft is not None and ft['name'] == 'List':  # ... is a List?
				# Each sub-table with name 'xxxs', is {'xxx':[...]}
				result[path] = val 		# Yes, return the JSON named as parent
			else:						# No, error
				logErr(f"Undefined array of {path} in {table.tableName}")
				table.field('List', path, False)  # define it to silence msgs
				result[path] = val
		elif ft is None:				# undefined simple field?
			result[new_path] = val 		# pass through to output
		elif ft['name'] == 'DateBad' and isinstance(val, str):  # Date text with UTC error
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
				continue				# no SELECTed fields to output
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
				flatten(rec, rec_dict, table, '')  # recurse deeper with results to rec_dict
				check_fields(sub_table, rec_dict)
				empty = True
				for k in rec_dict:
					if k in sub_table.select:
						empty = False 	# at least one field is selected for output
						break
				if not empty:			# any field(s) present to output?
					sub_table.writer.writerow(rec_dict)  # yes, write to output
					sub_table.recCnt += 1
			# Note that subTable does not return anything into parent results
		elif isinstance(val, dict): 	# compound structure
			flatten(val, result, table, new_path)
		else:
			continue					# primitive was processed during first pass

def write_state(file_name: str, tables: dict):
	"""Writes all table's dynamic state variables to a JSON file

	Parameters:
		file_name (str):	file_name to write JSON-encoded state
		tables (dict):		{tableName:table, ...}

	"""
	state = dict()
	for key in tables:					# collect state of each table_name in production
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

# Parse command line for opts
parser = OptionParser(usage='''usage: %prog [opts] directory
Collect statistics from CPI and write as csv files to directory''', version='%prog v0.6')
parser.add_option('--checkVerbose', action='store_true', dest='check_verbose',
	default=False, help="turn on detailed field checking reporting")
parser.add_option('--enum', action='store_true', dest='enum',
	default=False, help='output all ENUM DDL')
parser.add_option('--enums', action='store_true', dest='enums',
	default=False, help='Check values of each ENUM field.')
parser.add_option('--fields', action='store_true', dest='fields',
	default=False, help='Check records for missing, unknown or mis-typed fields.')
parser.add_option('--hive', action='store_true', dest='hive',
	default=False, help='output Hive data definition for selected table')
parser.add_option('--password', action='store', type='string', dest='password',
	default=None, help="password to use to login to CPI")
parser.add_option('--prefix', action='store', type='string', dest='prefix',
	default='data', help="API prefix. Default = data")
parser.add_option('--realtime', action='store_true', dest='realtime',
	default=False, help='Run real-time collection in a separate thread')
parser.add_option('--reset', action='store_true', dest='reset', default=False,
	help='Skip loading saved state. I.e. start from nothing saved.')
parser.add_option('--server', action='store', dest='server', default="ncs01.case.edu",
	help='CPI server name. default=ncs01.case.edu')
parser.add_option('--single', action='store_true', dest='single',
	default='', help="process a single poll (of highest priority table) and don't update saved state")
parser.add_option('--SQL', action='store_true', dest='SQL',
	default=False, help='output SQL data definition for specified table')
parser.add_option('--table', action='append', type='string', default=[], dest='table_name',
	help='Operate only on specified table. Use multiple --table for multiple tables.')
parser.add_option('--username', action='store', type='string', dest='username',
	default=None, help="user name to use to login to CPI")
parser.add_option('--ver', action='store', type='string', dest='ver',
	default=None, help="API version. Default matches any version")
parser.add_option('--verbose', action='count', dest='verbose', default=0,
	help="turn on diagnostic messages")
(options, args) = parser.parse_args()

if len(options.table_name) > 0:			# provided specific table names?
	# production tables to ignore
	to_ignore = [tn for tn in production if tn not in options.table_name]
	# Reduce wait times when catching-up on fewer tables
	# Fewer files in parallel * less total impact
	Pager.catchup = Pager.catchup*(1+len(production)-len(to_ignore))/len(production)**2

if options.SQL or options.hive:			# output table definitions?
	for table_name in production:
		tbl = find_table(options.tableName, [all_table_dicts], options.ver)
		if options.SQL:
			print(tbl.to_sql())
		if options.hive:
			# print(table.toHive())
			print(pprint.pformat(tbl.table_columns(), indent=2, width=120))
	sys.exit(0)

if options.enum:
	# print all ENUM definitions
	print(to_enum(allTypes))
	sys.exit(0)

if len(args) != 1:
	print('No output directory path specified')
	sys.exit(1)
outputPath = args[0]
if options.password is None:			# No password provided?
	try:
		cred = credentials(options.server, options.username)
	except KeyError:
		print(f"No username/password credentials found for {options.server}")
		sys.exit(1)
	options.username, options.password = cred
# The real_time and low_priority collect threads each have their own Cpi instance.
# This allows the real_time to block the low_priority for the duration of each poll.
semaphore = threading.Semaphore()		# for realtime to be able to block myCpi
if options.realtime and len(real_time) > 0:  # start real-time collection thread too?
	print('Starting realtime collect')
	rt = threading.Thread(name='realtime', target=collect,
				kwargs={'tables': real_time, 'real_time': True, 'semaphore': semaphore})
	rt.start()
if len(production) > 0:
	collect(tables=production, real_time=False, semaphore=semaphore)

if options.realtime and len(real_time) > 0:	 # did we start real-time collection
	rt.join()							# Yes. Join before exiting