#! /usr/bin/python3
#
# neighbors.py Copyright (C) 2019 by Dennis Risen, Case Western Reserve University
#
"""
function and application to report, for each AP slot slot, the co-channel noise from neighboring APs
"""
""" To Do
Extend to process 6GHz channels too
"""

import csv
import math
from argparse import ArgumentParser
import re
import sys
from time import time
from typing import Union

from cpiapi import all_table_dicts, Cpi, Cache
from mylib import credentials, printIf, secsToMillis, strfTime, verbose_1

# pairs maps channelNumber to the lower channel of a 40MHz channel pair
pairs = dict()
for i in range(36, 148):
    pairs[i] = int((i-4)/8)*8+4
for i in range(149, 165):
    pairs[i] = int((i-5)/8)*8+5
pairs[165] = 165
# lists each allowable band code and maps it a possibly different internal code
bands = {'2.4': ['2.4'], '5.0': ['5.0'], '6.0': ['6.0']}


def dBm(mwatt: float) -> Union[int, float]:
    """Convert ``mwatt`` to int dbm. Returns NaN when out of range for an int

    :param mwatt:   milliwatts
    :return:        dBm or NaN if out of range for an integer
    """
    try:
        return int(10*math.log10(mwatt))
    except ValueError:
        return float('NaN')


def map_chan(channel: int) -> int:
    """Map 5.0GHz channel number to 40MHz lower channel.

    :param channel:     Primary of possibly bonded ``channel``
    :return:            lower channel for ``channel``
    """
    try:
        channel = pairs[channel]
    except KeyError:
        pass
    return channel


def select(source: dict, *fields) -> dict:
    """Like relational SELECT ``*fields`` from ``source``.

    Ignores fields missing in ``source``

    :param source:  dict
    :param fields:  field names of the fields to be included
    :return:        new dict with selected fields from ``source``
    """
    result = dict()
    for field in fields:
        try:
            result[field] = source[field]
        except KeyError:
            pass
    return result


def mwatt(dbm: int) -> float:
    """Convert int dbm to mwatt."""
    return math.pow(10.0, dbm / 10.0)


def neighbors(inventory: str, neighbors_filename: str, outfile: str, age: float = 5.0,
              allchannels: bool = False, band: list = ['5.0'].copy(),
              csv_format: bool = False, full: bool = False,
              infile: Union[str, None] = None, maxConcurrent: int = 10,
              name_regex: Union[str, None] = None,
              server: str = 'ncs01.case.edu',
              twenty: bool = False, rxlimit: int = -90, util: float = 0.25,
              username: Union[str, None] = None, verbose: int = 0):
    """Report the co-channel noise from neighboring APs from each selected slot of each selected AP

    :param inventory:   Report filename for AP model qty and channel qty by building
    :param neighbors_filename:   Report filename for noise and neighbor RSSI by AP
    :param outfile:     [optional] output rxNeighbors.csv filename
    :param age:         Obtain rxNeighbors poll from the cache if < `age` days old
    :param allchannels: Include noise from all channels in band. False co-channel only
    :param band:        Band(s) to analyze: 2.4, 5.0, 6.0
    :param csv_format:  Output all reports in csv format
    :param full:        Include AP model suffix in model name
    :param infile:      Instead of polling, input from this filename,
    :param maxConcurrent maximum number of reader threads
    :param name_regex:  Filter CPI GET for AP names that match this regex
    :param server:      CPI server name
    :param twenty:      Report specific 20 MHz channels, not 40 MHz pairs
    :param rxlimit:     Report only the neighbors with RSSI>rxlimit
    :param util:        Neighbor's assumed utilization
    :param username     user name to use to login to CPI
    :param verbose:     Diagnostic message detail level
    :return:
    """

    # verify and convert arguments to internal form
    for b in band:
        if b not in bands:
            raise ValueError(f"Unknown --band {b}. Specify one of {bands}")

    regex_string = name_regex           # remember un-compiled string
    name_regex = re.compile(name_regex, flags=re.I)  # compile now for error-check

    try:
        username, password = credentials(server, username)
    except KeyError:
        raise KeyError(f"No username/password found for {username} at {server}")

    # create CPI server instance
    myCpi = Cpi(username, password, baseURL='https://' + server + '/webacs/api/')

    '''Build the following structures for calculating co-channel interference
    APById={APD.@id:AP, ...}			index to APs by apId
    APByMac={APD.macAddress_octets:AP, ...}	index to APs by MAC
    AP={'apId':APD.@id					CPI's unique @id:int for the AP
        , 'radios:{'2.4 GHz':radio, '5.0 GHz':radio}
        , 'macAddress_octets':APD.macAddress_octets	base MAC:str of AP's radios
        , 'locationHierarchy':APD.locationHierarchy	'Case Campus > building > floor'
        , 'model':APD.model		        AP model number:str
        , 'name':APD.name				AP name:str
        }
    radio={'channel':int(RadioDetails.channelNumber)
        , 'channelWidth':RadioDetails.channelWidth
        , 'powerLevel':RadioDetails.powerLevel
        , 'neighbors':{rxNeighbors.neighborApId:rxNeighbor, ...}
        , 'noise':0.0					co-channel interference mwatt
    rxNeighbor={'neighborApId':rxNeighbors.neighborApId
        , 'neighborApName':rxNeighbors.neighborApName
        , 'neighborChannel':rxNeighbors.neighborChannel
        , 'neighborRSSI':rxNeighbors.neighborRSSI
    '''
    APById = dict()						# index to APs by apId
    APByMac = dict()					# index to APs by baseMacAddress
    channels = dict()					# {buildingName:{channel:cnt, ...}, ...}
    models = dict()						# {buildingName:{model:cnt, ...}, ...}

    printIf(verbose, "Reading AccessPointDetails")
    # Build each AP from AccessPointDetails table
    reader = Cache.Reader(myCpi, 'v4/data/AccessPointDetails', age=age, verbose=verbose)
    for rec in reader:
        AP = select(rec, '@id', 'locationHierarchy', 'model', 'name', 'reachabilityStatus')
        AP['macAddress_octets'] = macAddress_octets = rec['macAddress']['octets']
        AP['radios'] = dict()			# radio-specific info goes here
        if AP['@id'] in APById:			# already an AP with this @id?
            print(f"@id in rec={rec}")
            print(f"duplicates AP={AP}")
            continue					# ignore duplicate
        if macAddress_octets in APByMac:  # already an AP with this @id?
            print(f"macAddress_octets in rec={rec}")
            print(f"duplicates AP={AP}")
            continue					# ignore duplicate
        APById[AP['@id']] = AP
        APByMac[macAddress_octets] = AP
        nameSplit = AP['name'].upper().split('-')
        bldg = nameSplit[0] if len(nameSplit) > 1 else 'other'
        if name_regex is not None and not name_regex.match(bldg):
            continue		# AP will not be reported. Don't include in model counts
        # Count radio models by filtered AP name
        m = re.fullmatch(r'AIR-[CL]?AP(.*)-K9', rec['model'])
        model = m.group(1)[:(5 if full else 4)] + m.group(1)[-2:] if m else rec['model']
        try:
            models[bldg][model] += 1
        except KeyError:
            try:
                models[bldg][model] = 1
            except KeyError:
                models[bldg] = dict()
                models[bldg][model] = 1

    printIf(verbose, "Reading RadioDetails ")
    # Build each radio from RadioDetails table
    reader = Cache.Reader(myCpi, 'v4/data/RadioDetails', age=age, verbose=verbose)
    for rec in reader:
        baseRadioMac = rec['baseRadioMac']['octets']
        AP = APByMac.get(baseRadioMac, None)
        if AP is None:					# Bad reference to AP?
            print(f"RadioDetails.baseRadioMac={baseRadioMac} not in APD. Radio ignored.")
            continue					# Yes, ignore this record
        if rec['apName'] != AP['name']:  # AP name mismatch?
            print(f"RadioDetails.apName={rec['apName']}!=APD.name={AP['name']}.")
        # create information for this radio
        radio = select(rec, 'channelWidth', 'powerLevel', 'slotId')
        channelNumber = rec.get('channelNumber', None)
        if channelNumber is None:		# No channelNumber?
            print("No RadioDetails.channelNumber for {rec['apName']}.}")
            continue                    # ignore this radio
        else:
            try:					    # convert channelNumber:str to channelNumber:int
                channelNumber = int(channelNumber[1:])  # skip over leading '_'
            except ValueError:
                if not (AP['model'].startswith('C9120') and radio['slotId'] == 6 and rec['radioType'] == 'Unknown'):
                    print(f"{rec['apName']}.{radio['slotId']} {rec['radioType']} {rec['radioRole']} "
                          + f"is {AP['model']} w/bad RadioDetails.channelNumber={channelNumber}")
                continue                # ignore a radio with e.g Unknown channel number
        slotId = radio['slotId']
        radio['channelNumber'] = channelNumber
        radio['noise'] = 0.0			# 0.0 mw of initial noise
        radio['neighbors'] = list()
        if slotId in AP['radios']:		# Already a radio for this band?
            print(f"{rec['apName']} duplicate {slotId} radio. Ignored.")
        else:
            AP['radios'][slotId] = radio  # add the radio to the AP
        if channelNumber <= 11:
            continue
        # record the 5.0 GHz channel numbers used by each building
        nameSplit = AP['name'].upper().split('-')  # building
        bldg = nameSplit[0] if len(nameSplit) > 1 else 'other'
        if name_regex is not None and not name_regex.match(bldg):
            continue		# AP will not be reported. Don't include in channel counts
        if not twenty:
            channelNumber = map_chan(channelNumber)
        try:
            channels[bldg][channelNumber] += 1
        except KeyError:
            try:
                channels[bldg][channelNumber] = 1
            except KeyError:
                channels[bldg] = dict()
                channels[bldg][channelNumber] = 1

    if inventory is not None:
        # report the AP models and 5.0 GHz channel qty in use by each building
        mdl = sorted({model for building in models for model in models[building]})
        mdl = dict((mdl[i], i) for i in range(len(mdl)))  # mapping from model to index
        chan = sorted({channel for building in channels for channel in channels[building]})
        chan = dict((chan[i], i) for i in range(len(chan)))  # map from channel to index
        # construct formats based on length of model name and channel
        f_hdr = '{:^' + str(12 + (8 if len(mdl) < 2 else 0) +
                            (8 if full else 7)*len(mdl) - 1) + '}Unique {:^' + str(4*len(chan)) + '}\n'
        fhdr2 = '{:' + str(12+(8 if len(mdl) < 2 else 0)) + '}'
        fbldg = '{:' + str(15+(8 if len(mdl) < 2 else 0)) + '}'
        fmdl = '{:' + str(4 if full else 3) + '}'
        with open(inventory, 'w') as out:
            out.write(f_hdr.format('AP Qty by Model & Domain',
                '5 GHz Radio Qtys by ' + ('20' if twenty else '40') + 'MHz channel'))
            out.write(fhdr2.format('Building') + ' '.join(mdl) + ' chan')
            out.write(' '.join(f"{c:3}" for c in chan) + '\n')
            lst = sorted(channels.keys())
            for bldg in lst:
                if name_regex is None or name_regex.match(bldg):
                    out.write(fbldg.format(bldg))
                    m = list(models[bldg].get(model, 0) for model in mdl)
                    out.write("    ".join(fmdl.format(qty if qty != 0 else ' ') for qty in m))
                    out.write(f"{len(set(chan for chan in channels[bldg])):4}")
                    for channel in chan:
                        qty = channels[bldg].get(channel, 0)
                        out.write(f"{qty if qty != 0 else ' ' :4}")
                    out.write('\n')
            out.write(fhdr2.format('Building') + ' '.join(mdl) + ' chan')
            out.write(' '.join(f"{c:3}" for c in chan) + '\n')
            out.write('\n"Unique chan" column is the number of unique 40 MHz channels in use\n')
            if name_regex is not None:
                out.write(f"Reporting includes only AP names that match {regex_string}\n")
            out.write(f"This report generated at {strfTime(time())}\n")

    # find the rxNeighbors table definition, for reading or writing a csv file
    for dictionary in all_table_dicts:  # in cpitables' dictionaries
        tbls = dictionary.get('rxNeighbors', None)
        if tbls is not None:
            tbl = tbls[0]
            break
    else:
        print(f"Can't find definition for rxNeighbors CPI table")
        sys.exit(1)

    if neighbors_filename is not None:  # supplied output file for noise & neighbor RSSI?
        out = open(neighbors_filename, 'w')  # open report file
    else:
        out = None                      # no output will be produced

    if maxConcurrent is not None:
        myCpi.maxConcurrent = maxConcurrent  # override default
    printIf(verbose, "processing rxNeighbors")
    nowMsec = secsToMillis(time())
    # initialize reader to read from file, cache, or CPI
    if infile is not None:		        # input file specified?
        # Obtain rxNeighbors table from csv file
        infile = open(infile, 'r', newline='')
        reader = csv.DictReader(infile)
        sourceMsec = None				# polledTime is initially unknown
        printIf(verbose, f"Reading rxNeighbors data from {infile}")
    elif age and age > 0.0:             # OK to use cached data
        infile = None                   # keep IDE happy
        reader = Cache.Reader(myCpi, tbl, age=age, verbose=verbose, name_regex=name_regex)
        tbl.errorList = []              # no errors it we don't actually GET from CPI
        sourceMsec = None               # polledTime is initially unknown
        printIf(verbose, f"Reading rxNeighbors data from cache, if available")
    else:								# Obtain rxNeighbors directly from CPI
        infile = None                   # keep IDE happy
        reader = tbl.generator(myCpi, tbl, verbose=verbose_1(verbose),
                               name_regex=name_regex)
        sourceMsec = nowMsec            # polledTime is now
        printIf(verbose, f"Reading rxNeighbors data from CPI via generator")

    # initialize rxWriter to write raw rxNeighbors detail to csv file
    rxWriter: Union[csv.DictWriter, None]
    if outfile is not None:		        # requested rxNeighbors output csv file?
        outfile = open(outfile, 'w', newline='')
        rxWriter = csv.DictWriter(outfile, fieldnames=tbl.select, restval='', extrasaction='ignore')
        rxWriter.writeheader()
    else:
        outfile = None                  # No. No csv file will be written

    # read and process al rxNeighbor records from requested source
    rec_cnt = 0             # number of records read so far, for diagnostic messages
    for row in reader:
        if infile is None:		        # reading directly from CPI API?
            #                             Yes. Flatten fields to canonic csv form
            row['macAddress_octets'] = row['macAddress']['octets']
            del row['macAddress']
            row['neighborIpAddress_address'] = row['neighborIpAddress']['address']
            del row['neighborIpAddress']
            row['polledTime'] = nowMsec
        if sourceMsec is None:			# sourceMsec unknown?
            sourceMsec = int(row['polledTime'])  # remember the polledTime of the source
        if outfile is not None:         # writing raw rxNeighbors data to csv?
            rxWriter.writerow(row)      # Yes
        rec_cnt += 1
        if verbose > 0 and rec_cnt % 1000 == 0:
            print(f"{rec_cnt:4} records")

        neighbor = dict()				# neighbor constructed here
        # Ensure that fields are correctly type-cast
        apId = int(row['apId'])         # polled access point' Id
        slotId = int(row['slotId'])  # polled access point's radio slotId reporting this neighbor
        macAddress_octets = row['macAddress_octets']  # AP's base MAC
        neighbor['ApId'] = neighborApId = int(row['neighborApId'])
        neighbor['ApName'] = neighborApName = row['neighborApName']
        neighbor['Channel'] = neighborChannel = int(row['neighborChannel'])
        neighbor['RSSI'] = neighborRSSI = int(row['neighborRSSI'])
        neighbor['slotId'] = neighborSlotId = int(row['neighborSlotId'])
        AP = APById.get(apId, None)		# get AP reported by AccessPointDetails API
        if AP is None:					# Unknown apId?
            print(f"Unknown apId={apId} hears neighbor={neighborApName} "
                  + f"on channel={neighborChannel} at {neighborRSSI}dBm.")
            continue                    # ignore record.
        if name_regex is not None and not re.search(name_regex, AP['name']):  # AP name was not requested?
            print(f"Unrequested {AP['name']} w/apId={apId} hears neighbor={neighborApName} "
                  + f"on channel={neighborChannel} at {neighborRSSI}dBm.")
            continue                    # ignore record.
        if macAddress_octets != AP['macAddress_octets']: 	# bad MAC?
            print(f"rxNeighbors {neighborApName}'s macAddress_octets={macAddress_octets}!={AP['macAddress_octets']}"
                + f"=APByMac[{apId}].APD.macAddress_octets for {AP['name']}")
            continue					# ignore mis-correlated data
        radio = AP['radios'].get(slotId, None)  # AP's radio for this slot
        if radio is None:
            print(f"{AP['name']} slot {slotId} is not defined in RadioDetails, but hears "
                  + f"neighbor {neighborApName} slotId {neighborSlotId} at {neighborRSSI}dBm")
            continue
        try:							# lookup neighbor radio's RadioDetails
            neighborRadio = APById[neighborApId]['radios'][neighborSlotId]
        except KeyError:
            print(f"{AP['name']} slot{slotId}  hears unknown {neighborApName} w/ApId={neighborApId} "
                  + f"slot{neighborSlotId} at {neighborRSSI}dBm.")
            continue
        channelNumber = map_chan(radio['channelNumber'])
        neighborChannel = map_chan(neighborChannel)
        if channelNumber != neighborChannel and not allchannels:
            continue					# Yes, ignore this rxNeighbor
        if radio['powerLevel'] == 0 or neighborRadio['powerLevel'] == 0:  # Radio(s) off?
            continue				    # Yes, ignore this rxNeighbor
        # Passed all tests.
        # Each AP transmits NDP packets on each channel at power level 1.
        # Adjust RSSI by 3dB/level * (neighborPowerLevel-1).
        mw = util*mwatt(neighborRSSI - 3*(neighborRadio['powerLevel'] - 1))
        radio['noise'] += mw			# add milliwatts to noise
        radio['neighbors'].append(neighbor)
    if verbose > 0:
        print(f"finished reading rxNeighbors")
    if infile is not None:		        # reading from infile
        infile.close()
        names = ''
        unreachable = ''
    else:								 # reading from CPI
        unreachable = {aid for aid in APById if name_regex is None or re.search(name_regex, APById[aid]['name'])
                       and APById[aid]['reachabilityStatus'] != 'REACHABLE'}
        names = ', '.join(sorted((APById[aid]['name'] if aid in APById else 'Unknown')
                                 for aid in tbl.errorList if aid not in unreachable))
        unreachable = ', '.join(sorted(APById[aid]['name'] for aid in unreachable))
        if len(unreachable) > 0:
            print(f"APs with reachabilityStatus!='REACHABLE': {unreachable}")
        if len(names) > 0:
            print(f"APs {names} didn't return neighbor status")
    if outfile is not None:
        outfile.close()

    printIf(verbose, "reporting results")
    # Report the results, sorted by apName
    lst = sorted((APById[apId]['name'].upper(), apId) for apId in APById)
    f_hdr = '{:18}{:>9}' + 8*('   neighbor '[(-10 if allchannels else -11):] + 'RSSI') + '\n'
    f_neighbor = '{:>' + str(10 if allchannels else 11) + '}{:4}'
    f_foreign = '{:>' + str(11 if allchannels else 12) + '}{:3}'
    if out is not None:
        if csv_format:					# csv output?
            out.write(f"{'apName_slot, noise, '}{','.join(['neighbor'+str(i)+', RSSI'+str(i) for i in range(1,30)])}\n")
        else:
            out.write(f_hdr.format(' AP name[.slot]', 'noise dbm'))
    for aband in band:
        for sortKey, apId in lst:
            AP = APById[apId]
            name = AP['name']			# get the possibly mixed-case name
            if name_regex is not None and not name_regex.match(name):
                continue				# ignore AP if name doesn't match the filter
            nameSplit = name.split('-')
            # AP's qualifier is name without last 2 fields
            qual = '-'.join(nameSplit[0:-2]).upper() if len(nameSplit) > 2 else None
            for slotId in AP['radios']:  # for each radio
                radio = AP['radios'][slotId]  # the radio
                theBand = '2.4' if radio['channelNumber'] <= 11 else '5.0'
                if aband != theBand:    # not the band that is being processed?
                    continue			# ignore this radio now
                namecopy = name
                if theBand == '2.4' and slotId != 0 or theBand == '5.0' and slotId != 1:
                    namecopy += f".{slotId}"  # append unusual slotId to name
                if out is not None:
                    if csv_format:		# csv output?
                        out.write(f",{namecopy},{dBm(radio['noise'])}")
                    else:				# text columns output
                        out.write(f"{namecopy:23}{dBm(radio['noise']):4}")
                neighbors = radio['neighbors']
                # sort neighbors by descending RSSI
                n = sorted((-neighbors[i]['RSSI'], i) for i in range(len(neighbors)))
                for negRSSI, i in n:
                    if -negRSSI < rxlimit:  # RSSI less than limit?
                        break			# yes, ignore all remaining in sorted list
                    neighbor = neighbors[i]
                    ApName = neighbor['ApName']
                    nslotId = neighbor['slotId']
                    if theBand == '2.4' and nslotId != 0 or theBand == '5.0' and nslotId != 1:
                        ApName += f".{slotId}"  # append unusual nslotId to ApName
                    nSplit = ApName.split('-')
                    if out is not None:
                        if csv_format:  # csv output?
                            out.write(f",{ApName},{-negRSSI}")
                        else:			# text columns output
                            if '-'.join(nSplit[0:-2]).upper() == qual:  # neighbor has same?
                                ApName = '-'.join(nSplit[-2:])[(-9 if allchannels else -10):]
                                out.write(f_neighbor.format(ApName, -negRSSI))  # only SER-WAP
                            else:		# different qualifier
                                ApName = ApName[(-10 if allchannels else -11):]  # last 10+ chars w/o spacing
                                out.write(f_foreign.format(ApName, -negRSSI))
                if out is not None:
                    out.write('\n')
    if out is not None:
        out.write('\nA radio name has a ".slot#" suffix iff its slot is '
            + 'non-default for the channel. e.g. the XOR radio operating in 5 GHz.\n')
        out.write("Each neighbor column is the shortened neighbor name. ")
        out.write("For a radio in the same building, the last 2 fields; otherwise the last "
                  + str(10 if allchannels else 11) + " characters.\n")
        out.write("For each radio:\n")
        s = '' if allchannels else " co-channel"
        out.write(f"    The RSSI reported for each{s} neighbor is the RSSI in dBm as seen by the radio\n")
        out.write(f"    The noise dBm at the radio is {util}"
            + "* the sum of each{s} neighbor RSSI reduced by its tx level. 0 mwatt --> nan dBm\n")
        out.write(f"Reporting radios in the {' and '.join(band)} band(s)")
        if name_regex is None:
            out.write(".\n")
        else:
            out.write(f" with AP name that matches {regex_string}.\n")
        out.write(f"Reporting rxNeighbors with RSSI greater than {rxlimit} dBm\n")
        if len(unreachable) > 0:
            out.write(f"APs {unreachable} were unreachable")
        if len(names) > 0:
            out.write(f"APs {names} didn't respond to a RxNeighbor status request.\n")
        out.write(f"This report generated at {strfTime(time())}")
        if infile is not None:
            out.write(f", from data polled at {strfTime(sourceMsec)}")
        out.write("\n")
        out.close()


if __name__ == '__main__':
    # Parse command line for options
    parser = ArgumentParser(description='For each AP slot, report the co-channel noise from neighboring APs')
    parser.add_argument('inventory', action='store',
                        help="report filename for AP model qty and channel qty by building")
    parser.add_argument('neighbors', action='store',
                        help="report filename for noise and neighbor RSSI by AP")
    parser.add_argument('outfile', action='store', default=None, nargs='?',
                        help="[optional] output rxNeighbors.csv file")
    parser.add_argument('--allchannels', action='store_true', default=False,
                        help="Include noise from all channels in band. Default: co-channel only")
    parser.add_argument('--band', action='append',
                        choices=('2.4', '5.0', 'all'), default='5.0',
                        help="enter each band to analyze: 2.4, 5.0, and/or 6.0. (default=5.0)")
    parser.add_argument('--age', action='store', type=float, default=5.0,
                        const=0.0, nargs='?',
                        help='Obtain rxNeighbors poll from the cache if < age days old')
    parser.add_argument('--csv', action='store_true', default=False, dest='csv_format',
                        help="output reports in csv format")
    parser.add_argument('--full', action='store_true', default=False,
                        help="Include AP model suffix in model name")
    parser.add_argument('--infile', action='store', default=None,
                        help="input rxNeighbors.csv filename, instead of polling")
    # CPI appears to now concurrently work on only maxConcurrent of the requests
    # and queue the excess. Thus there is no speedup beyond keeping its queue non-empty
    parser.add_argument('--maxConcurrent', action='store', type=int, default=10,
                        help="maximum number of reader threads.")
    parser.add_argument('--name_regex', action='store', default=None,
                        help="filter CPI GET for AP names that match this regex.")
    parser.add_argument('--server', action='store', default="ncs01.case.edu",
                        help='CPI server name. default=ncs01.case.edu')
    parser.add_argument('--twenty', action='store_true', default=False,
                        help="Report specific 20 MHz channels, not 40 MHz pairs")
    parser.add_argument('--username', action='store', default=None,
                        help="user name to use to login to CPI")
    parser.add_argument('--rxlimit', action='store', type=int, default=-90,
                        help="report only the neighbors with RSSI>rxlimit")
    parser.add_argument('--utilization', action='store', type=int, dest='util',
                        default=25, help="neighbor's assumed utilization (default=25)")
    parser.add_argument('--verbose', action='count', default=0,
                        help="increase diagnostic messages")
    args = parser.parse_args()

    if args.name_regex is not None:
        # Remove enclosing quotes, if any
        if args.name_regex[0] == args.name_regex[-1] and args.name_regex[0] in {'"', "'"}:
            print(f"Removing enclosing quotes from name_regex: {args.name_regex}-->{args.name_regex[1:-1]}")
            args.name_regex = args.name_regex[1:-1]
        printIf(args.verbose, f"Report includes only AP names matching {args.name_regex}")

    if args.rxlimit > 0:                # user specified a positive RSSI?
        print(f"Correcting rxlimit {args.rxlimit} to a negative number {-args.rxlimit}")
        args.rxlimit = -args.rxlimit    # correct to negative dbm

    args.util = (args.util/100.0)	    # convert integer percent to float factor

    inv = args.inventory
    nei = args.neighbors
    outfile_name = args.outfile
    args = vars(args)                   # convert Namespace to dict
    del args['inventory']
    del args['neighbors']
    del args['outfile']
    neighbors(inv, nei, outfile_name, **args)
