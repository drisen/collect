#! /usr/bin/python3
#
# snr.py Copyright 2019 by Dennis Risen, Case Western Reserve University
#
import sys, csv, math, re
import cpi
from mylib import *
from optparse import OptionParser

def dBm(mwatt:float) -> int:
	'''Convert mwatt to dBm'''
	try:
		return int(10*math.log10(mwatt))
	except:
		return float('NaN')
	
def select(source:dict, *fields) -> dict:
	'''Return new dict with *fields, as present, copied from source'''
	result = dict()
	for field in fields:
		try:	result[field] = source[field]
		except:	pass
	return result
	
def mwatt(dBm:int) -> float:
	'''Convert dBm to mwatt'''
	return math.pow(10.0, dBm/10.0)

# Parse command line for options
parser = OptionParser(usage='''usage: %prog [options]
Report from rxNeighbors.csv the SNR at each AP''', version='%prog v0.6')
parser.add_option('--allchannels', action='store_true', dest='allchannels'
	, default=False, help="Include noise from all channels in band, not just co-channel")
parser.add_option('--band', action='store', choices=('2.4','5.0','both'), dest='bands'
	, default='5.0', help="band to analyze: 2.4, 5.0 or both (default=5.0)")
parser.add_option('--csv', action='store_true', dest='csv'
	, default=False, help="output report in csv format")
parser.add_option('--file', action='store', type='string', dest='file'
	, default=None, help="input rxNeighbors.csv file")
parser.add_option('--full', action='store_true', dest='full'
	, default=False, help="Include AP model suffix in model name")
parser.add_option('--inventory', action='store', dest='inventory'
	, default=None, help="output file for AP model qty and channel qty by building report")
parser.add_option('--maxConcurrent', action='store', type='int', dest='maxConcurrent'
	, default=None, help="maximum number of reader threads. specify 1 if library seems not thread-safe")
parser.add_option('--nameregex', action='store', type='string', dest='nameregex'
	, default=None, help="only AP names that match this regex.")
parser.add_option('--neighbors', action='store', dest='neighbors'
	, default=None, help="output file for noise and neighbor RSSI by AP report")
parser.add_option('--password', action='store', type='string', dest='password'
	, default=None, help="password to use to login to CPI")
parser.add_option('--twenty', action='store_true', dest='twenty'
	, default=False, help="Report specific 20 MHz channels, not 40 MHz pairs")
parser.add_option('--username', action='store', type='string', dest='username'
	, default=None, help="user name to use to login to CPI")
parser.add_option('--rxlimit', action='store', type='int', dest='rxlimit'
	, default=-100, help="report only the neighbors with RSSI>rxlimit")
parser.add_option('--utilization', action='store', type='int', dest='util'
	, default=50, help="neighbor's assumed utilization (default=50)")
parser.add_option('--verbose', action='store_true', dest='verbose'
	, default=False, help="turn on diagnostic messages")
(options,args) = parser.parse_args()

# verify and convert options to internal form
if options.bands=='both':
	options.bands = ['2.4', '5.0']
elif options.bands[0]=='5':
	options.bands = ['5.0']
elif options.bands[0]=='2':
	options.bands = ['2.4']
else:
	print(f"Unknown --band {options.bands}. Specify 2.4, 5.0 or both")
	sys.exit(1)

nameregex = options.nameregex
if nameregex is not None:
	# Remove enclosing quotes, if any
	if nameregex[0]==nameregex[-1] and nameregex[0] in {'"', "'"}:
		print(f"Removing enclosing quotes from nameregex: {nameregex}-->{nameregex[1:-1]}")
		nameregex = nameregex[1:-1]
	options.nameregex = nameregex
	nameregex = re.compile(nameregex, flags=re.I)# compile now for error-check
	printIf(options.verbose, f"Report includes only AP names matching {options.nameregex}")
else: nameregex = None

server = 'ncs01.case.edu'
if options.password is None:		# No password provided?
	if options.username is None and options.file is None:
		print(f"Must specify username to access rxNeighbors table")
		sys.exit(1)					# default user can't access rxNeighbors
	cred = credentials(server, options.username)
	if cred is None:				# credentials() couldn't find too?
		print(f"No username/password found for {options.username} at {server}")
		sys.exit(1)
	options.username,options.password = cred

if options.rxlimit>0:				# user specified a positive RSSI?
	print(f"Correcting rxlimit {options.rxlimit} to a negative number {-options.rxlimit}")
	options.rxlimit= -options.rxlimit# correct to negative dBm
	
# create CPI server instance
myCpi = cpi.cpi(options.username, options.password, baseURL='https://'+server+'/webacs/api/')
if not options.maxConcurrent is None:
	myCpi.maxConcurrent = options.maxConcurrent

options.util = (options.util/100.0)	# convert integer percent to float factor

'''Build the following structures for calculating co-channel interference
APById={APD.@id:AP, ...}			index to APs by apId
APByMac={APD.macAddress_octets:AP, ...}	index to APs by MAC
AP={'apId':APD.@id					CPI's unique @id:int for the AP
	, 'radios:{'2.4 GHz':radio, '5.0 GHz':radio}
	, 'macAddress_octets':APD.macAddress_octets	base MAC:str of AP's radios
	, 'locationHierarchy':APD.locationHierarchy	'Case Campus > building > floor'
	, 'model':APD.model					AP model number:str
	, 'name':APD.name					AP name:str
	}
radio={'channel':int(RadioDetails.channelNumber)
	, 'channelWidth':RadioDetails.channelWidth
	, 'powerLevel':RadioDetails.powerLevel
	, 'neighbors':{rxNeighbors.neighborApId:rxNeighbor, ...}
	, 'noise':0.0						co-channel interference mwatt
rxNeighbor={'neighborApId':rxNeighbors.neighborApId
	, 'neighborApName':rxNeighbors.neighborApName
	, 'neighborChannel':rxNeighbors.neighborChannel
	, 'neighborRSSI':rxNeighbors.neighborRSSI
'''
APById = dict()						# index to APs by apId
APByMac = dict()					# index to APs by baseMacAddress
chans = dict()						# {buildingName:{channel:cnt, ...}, ...}
models = dict()						# {buildingName:{model:cnt, ...}, ...}
# map channelNumber to channel pair
pairs = dict()
for i in range(36,148):
	pairs[i] = int((i-4)/8)*8+4
for i in range(149,165):
	pairs[i] = int((i-5)/8)*8+5
pairs[165] = 165

printIf(options.verbose, "Reading AccessPointDetails")
# Build each AP from AccessPointDetails table
reader = cpi.cpi.Reader(myCpi, 'v4/data/AccessPointDetails'
, filters={".full":"true", ".nocount":"true"}, verbose=options.verbose)
for rec in reader:
	AP = select(rec, '@id', 'locationHierarchy', 'model', 'name')
	AP['macAddress_octets'] = macAddress_octets = rec['macAddress']['octets']
	AP['radios'] = dict()			# radio-specific info goes here
	if AP['@id'] in APById:			# already an AP with this @id?
		print(f"@id in rec={rec}")
		print(f"duplicates AP={AP}")
		continue					# ignore duplicate
	if macAddress_octets in APByMac:# already an AP with this @id?
		print(f"macAddress_octets in rec={rec}")
		print(f"duplicates AP={AP}")
		continue					# ignore duplicate
	APById[AP['@id']] = AP
	APByMac[macAddress_octets] = AP
	# Count radio models by filtered AP name
	nameSplit = AP['name'].upper().split('-')
	bldg = nameSplit[0] if len(nameSplit)>1 else 'other'
	if nameregex is not None and not nameregex.match(bldg):
		continue		# AP will not be reported. Don't include in model counts
	m = re.fullmatch(r'AIR-[CL]?AP(.*)-K9', rec['model'])
	model = m.group(1)[:(5 if options.full else 4)]+m.group(1)[-2:] if m else rec['model']
	try:
		models[bldg][model]+= 1
	except:
		try:
			models[bldg][model] = 1
		except:
			models[bldg] = dict()
			models[bldg][model] = 1

printIf(options.verbose, "Reading RadioDetails")
# Build each radio from RadioDetails table
reader = cpi.cpi.Reader(myCpi, 'v4/data/RadioDetails'
, filters={".full":"true", ".nocount":"true"}, verbose=options.verbose)
for rec in reader:
	baseRadioMac = rec['baseRadioMac']['octets']
	AP = APByMac.get(baseRadioMac, None)
	if AP is None:					# Bad reference to AP?
		print(f"RadioDetails.baseRadioMac={baseRadioMac} not in APD. Radio ignored.")
		continue					# Yes, ignore this record
	if rec['apName']!=AP['name']:	# AP name mismatch?
		print(f"RadioDetail.apName={rec['apName']}!=APD.name={AP['name']}. Radio ignored.")
		continue					# Yes, ignore the record
	# create information for this radio
	radio = select(rec, 'channelWidth', 'powerLevel', 'slotId')
	channelNumber = rec.get('channelNumber', None)
	if channelNumber is None:		# No channelNumber?
		print("No RadioDetails.channelNumber for {rec['apName']}.}")
	else:
		try:					# convert channelNumber:str to channelNumber:int
			channelNumber = int(channelNumber[1:])# skip over leading '_'
		except:
			print(f"{rec['apName']} bad RadioDetails.channelNumber={channelNumber}")
	slotId = radio['slotId']
	radio['channelNumber'] = channelNumber
	radio['noise'] = 0.0			# 0.0 mw of initial noise
	radio['neighbors'] = list()
	if slotId in AP['radios']:		# Already a radio for this band?
		print(f"{rec['apName']} duplicate {slotId} radio. Ignored.")
	else:
		AP['radios'][slotId] = radio# add the radio to the AP
	if channelNumber<=11:
		continue
	# record the 5.0 GHz channel numbers used by each building
	nameSplit = AP['name'].upper().split('-') # building
	bldg = nameSplit[0] if len(nameSplit)>1 else 'other'
	if nameregex is not None and not nameregex.match(bldg):
		continue		# AP will not be reported. Don't include in channel counts
	if not options.twenty:
		channelNumber = pairs[channelNumber]	# report 40 MHz channels
	try:
		chans[bldg][channelNumber]+= 1
	except:
		try:
			chans[bldg][channelNumber] = 1
		except:
			chans[bldg] = dict()
			chans[bldg][channelNumber] = 1

def itemName(d:dict) -> list:
	s = set()
	for building in d:
		for model in d[building]:
			s.add(model)
	return sorted(s)
		
if options.inventory is not None:
	# report the AP models and 5.0 GHz channel qty in use by each building
	mdl = itemName(models)
	mdl = dict((mdl[i],i) for i in range(len(mdl)))	# mapping from model to index
	chan = sorted(itemName(chans))
	chan = dict((chan[i],i) for i in range(len(chan)))	# map from channel to index
	fhdr = '{:^' + str(12+(8 if len(mdl)<2 else 0)+(8 if options.full else 7)*len(mdl)-1) + '}Unique {:^' + str(4*len(chan)) + '}\n'
	fhdr2 = '{:' + str(12+(8 if len(mdl)<2 else 0)) + '}'
	fbldg = '{:' + str(15+(8 if len(mdl)<2 else 0)) + '}'
	fmdl = '{:' + str(4 if options.full else 3) + '}'
	with open(options.inventory, 'w') as out:
		out.write(fhdr.format('AP Qty by Model & Domain'
		,'5 GHz Radio Qtys by ' + ('20' if options.twenty else '40') + 'MHz channel'))
		out.write(fhdr2.format('Building') + ' '.join(mdl) + ' chan')
		out.write(' '.join(f"{c:3}" for c in chan) + '\n')
		lst = sorted(chans.keys())
		for bldg in lst:
			if nameregex is None or nameregex.match(bldg):
				out.write(fbldg.format(bldg))
				m = list(models[bldg].get(model, 0) for model in mdl)
				out.write("    ".join(fmdl.format(qty if qty!=0 else ' ') for qty in m)) 
				out.write(f"{len(set(chan for chan in chans[bldg])):4}")
				for channel in chan:
					qty = chans[bldg].get(channel, 0)
					out.write(f"{qty if qty!=0 else ' ' :4}")
				out.write('\n')
		out.write(fhdr2.format('Building') + ' '.join(mdl) + ' chan')
		out.write(' '.join(f"{c:3}" for c in chan) + '\n')
		out.write('\n"Unique chan" column is the number of unique 40 MHz channels in use\n')
		if nameregex!=None:
			out.write(f"Reporting includes only AP names that match {options.nameregex}\n")

if not options.neighbors:
	sys.exit()
printIf(options.verbose, "processing rxNeighbors")
out = open(options.neighbors,'w')
if options.file is not None:		# input file specified?
	# Obtain rxNeighbors table from csv file
	infile = open(options.file, 'r', newline='')
	reader = csv.DictReader(infile)
else:								# Obtain RxNeighbors directly from CPI
	# find the rxNeighbors definition
	for dictionary in [cpi.production, cpi.archive]:# in cpitables' dictionaries
		tbls = dictionary.get('rxNeighbors', None)
		if tbls is not None:
			tbl = tbls[0]
			break
	else:
		print(f"Can't find definition for rxNeighbors CPI table")
		sys.exit(1)		
	reader = tbl.generator(myCpi, tbl, verbose=options.verbose, nameregex=nameregex)

for row in reader:
	neighbor = dict()			# construct here
	# DictReader returns each field as string. Fields in csv file are flattened
	apId = int(row['apId'])
	macAddress_octets = row['macAddress']['octets'] if options.file is None else row['macAddress_octets'] # AP's base MAC
	neighbor['ApId'] = neighborApId = int(row['neighborApId'])
	neighbor['ApName'] = neighborApName = row['neighborApName']
	neighbor['Channel'] = neighborChannel = int(row['neighborChannel'])
	neighbor['RSSI'] = neighborRSSI = int(row['neighborRSSI'])
	neighbor['slotId'] = neighborSlotId = int(row['neighborSlotId'])
	slotId = int(row['slotId'])
	AP = APById.get(apId, None)	# AP structure for AP
	if AP is None:				# Unknown apId?
		print(f"Unknown apId={apId} with neighbor={neighborApName} on channel={neighborChannel}. Ignored.")
		continue
	if macAddress_octets!=AP['macAddress_octets']:	# bad MAC?
		print(f"rxNeighbors.macAddress_octets={macAddress_octets}!={AP['macAddress_octets']}=APByMac[{apId}].APD.macAddress_octets. Ignored.")
		continue				# ignore mis-correlated data
	radio = AP['radios'].get(slotId, None)# AP's radio for this slot
	if radio is None:
		print(f"{AP['name']} slot {slotId} not defined in RadioDetails, but has a neighbor")
	try:						# neighbor radio
		neighborRadio = APById[neighborApId]['radios'][neighborSlotId]
	except:
		print(f"Couldn't find neighborApId={neighborApId} radio{neighborSlotId}. Ignored.")
		continue
	try:					# Map 5.0GHz channelNumbers to 40MHz lower channel
		channelNumber = pairs[channelNumber]	
	except:	pass
	try:	neighborChannel = pairs[neighborChannel]
	except:	pass
	if radio['channelNumber']!=neighborChannel and not options.allchannels:
		continue				# Yes, ignore this rxNeighbor
	if radio['powerLevel']==0 or neighborRadio['powerLevel']==0:#Radio(s) off?
		continue				# Yes, ignore this rxNeighbor
	# Passed all tests. 
	# Each AP transmits NDP packets on each channel at power level 1.
	# Adjust RSSI by 3dB/level * (neighborPowerLevel-1).
	mw = options.util*mwatt(neighborRSSI-3*(neighborRadio['powerLevel']-1))
	radio['noise']+= mw			# add milliwatts to noise		
	radio['neighbors'].append(neighbor)
if options.file is not None:	# reading from file
	infile.close()
else:							# reading from CPI
	names = ', '.join(sorted((APById[id]['name'] if id in APById else 'Unknown') for id in tbl.errorList))
	if len(names)>0:
		print(f"APs {names} didn't return neighbor status")

printIf(options.verbose,"reporting results")
# Report the results, sorted by apName
lst = sorted((APById[apId]['name'].upper(), apId) for apId in APById)
fhdr = '{:18}{:>9}' + 8*('   neighbor '[(-10 if options.allchannels else -11):] + 'RSSI')+'\n'
fneighbor = '{:>'+str(10 if options.allchannels else 11)+'}{:4}'
fforeign = '{:>'+str(11 if options.allchannels else 12)+'}{:3}'
if options.csv:						# csv output?
	out.write(f"{'apName_slot, noise, '}{','.join(['neighbor'+str(i)+', RSSI'+str(i) for i in range(1,10)])}\n")
else:
	out.write(fhdr.format(' AP name[.slot]', 'noise dBm'))
for band in options.bands:
	for sortKey,apId in lst:
		AP = APById[apId]
		name = AP['name']			# get the possibly mixed-case name
		if nameregex is not None and not nameregex.match(name):
			continue				# ignore AP if its name does't match the filter
		nameSplit = name.split('-')
		# AP's qualifier is name without last 2 fields
		qual = '-'.join(nameSplit[0:-2]).upper() if len(nameSplit)>2 else None 
		for slotId in AP['radios']:	# for each radio
			radio = AP['radios'][slotId]# the radio
			theBand = '2.4' if radio['channelNumber']<=11 else '5.0'
			if band!=theBand:		# not the band that is being processed?
				continue			# ignore this radio now
			namecopy = name
			if theBand=='2.4' and slotId!=0 or theBand=='5.0' and slotId!=1:
				namecopy+= f".{slotId}"	# append unusual slotId to name
			if options.csv:			# csv output?
				out.write(f",{namecopy},{dBm(radio['noise'])}")
			else:					# text columns output
				out.write(f"{namecopy:23}{dBm(radio['noise']):4}")
			neighbors = radio['neighbors']
			# sort neighbors by descending RSSI
			n = sorted((-neighbors[i]['RSSI'], i) for i in range(len(neighbors)))
			for negRSSI,i in n:
				if -negRSSI<options.rxlimit:# RSSI less than limit?
					break			# yes, ignore all remaining in sorted list
				neighbor = neighbors[i]
				ApName = neighbor['ApName']
				nslotId = neighbor['slotId']
				if theBand=='2.4' and nslotId!=0 or theBand=='5.0' and nslotId!=1:
					ApName+= f".{slotId}"# append unusual nslotId to ApName
				nSplit = ApName.split('-')
				if options.csv:		# csv output?
					out.write(f",{ApName},{-negRSSI}")
				else:				# text columns output
					if '-'.join(nSplit[0:-2]).upper()==qual:# neighbor has same?
						ApName = '-'.join(nSplit[-2:])[(-9 if options.allchannels else -10):]
						out.write(fneighbor.format(ApName, -negRSSI))# only SER-WAP
					else:			# different qualifier
						ApName = ApName[(-10 if options.allchannels else -11):]# last 10+ chars w/o spacing
						out.write(fforeign.format(ApName, -negRSSI))
			out.write('\n')
out.write('\nAn AP or neighbor name has a ".slot#" suffix iff the slot is non-default for the channel. e.g. the XOR radio operating in 5 GHz.\n')
out.write("Each neighbor column is the shortened neighbor name. ")
out.write("For an AP in the same building, the last 2 fields; otherwise the last "
	+ str(10 if options.allchannels else 11) + " characters.\n")
out.write("For each AP:\n")
out.write("    The RSSI reported for each neighbor is the RSSI in dBm as seen by the AP\n")
s =  '' if options.allchannels else " co-channel"
out.write(f"    The noise dBm at the AP is {options.util} * the sum of each{s} neighbor RSSI reduced by its tx level. 0 mwatt --> nan dBm\n")
out.write(f"Reporting APs")
s = [f" in {' and '.join(options.bands)} band(s) with RSSI greater than {options.rxlimit} dBm"]
if nameregex!=None:
	s.append(f"AP name that matches {options.nameregex}")
out.write(' and '.join(s) + '\n')
if len(names)>0:
	out.write(f"APs {names} didn't respond to a RxNeighbor status request.\n")

out.close()