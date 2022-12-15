#! /usr/bin/python3
#
# apdCheck.py Copyright (C) 2018 Dennis Risen, Case Western Reserve University
#
"""
Read access point definitions from AccessPointDetails API, and check
for syntactically valid mapLocation information
"""

from argparse import ArgumentParser
import re
import sys

import cpiapi
from credentials import credentials

""" To Do
- load the GroupSpecifications table
- load and verify the sites table. map name
    from Location/All Locations[/{campusName}[/{building}[/floor]]] to {campusName}[ > {buildingName}[ > {floor}]]
    extract campusName, buildingName and floor into separate fields
    use the GroupSpecifications table to obtain parent building
- select * from sites where sites.name=AccessPointDetails.locationHierarchy
    verify that
        apd.mapLocation[0]==sites.buildingName
        apd.mapLocation[1]==sites.locationAddress
        apd.mapLocation[2] starts with sites.floor
"""


def dig_down(row: dict, path: str):
    """Return value of row.path. Path has form name [_ name]* ."""
    i = path.find('_')
    if i < 0:  							# Simple attribute name?
        return row[path]  				# Yes, return attribute value
    return dig_down(row[path[:i]], path[i + 1:])  # No, dig_down down into structure for value


def load_apd(report: bool) -> dict:
    """Returns a dict of the contents of the AccessPointDetails table
    optionally report problem(s) with each entry in the table

    Parameters:
        report (bool): True to write report of mapLocation problems
    Returns:
        (dict): 	of {macAddress : ['macAddress': macAddress, 'name': name,]}
"""
    last_std_fields = ''

    def report_err(name: str):
        """Print "name" problem code and AP info. Incr "name" problem count"""
        nonlocal last_std_fields
        report_txt[name]['cnt'] += 1
        if name in omit_detail:
            return
        if report and last_std_fields == std_fields:  # Same AP?
            print(f"{report_txt[name]['name']:13}")  # just output problem
        elif report:  					# different AP
            print(f"{report_txt[name]['name']:13} {std_fields}")  # full output
        else:
            pass
        last_std_fields = std_fields  	# remember last, for simplified output

    def build_dict(result: dict, *entries):
        """Add a problem entry to result for each entry tuple."""
        for name, brief, full in entries:
            result[name] = {"cnt": 0, "name": brief, "full": full}

    # error counters and their text
    report_txt = dict()
    build_dict(report_txt, ("questioned", "question mark", "APs with a question-mark in the mapLocation."),
               ("noFaceplate", "No faceplate", "APs w/o a faceplate number"),
               ("macAdd", "MAC in Loc.", "MAC addresses in the mapLocation."),
               ("apnamesyntax", "name syntax", "APs with bad name syntax."),
               ("unassigned", "no CPI map", 'APs not in CPI maps and mapLocation="default location". Dropped.'),
               ("noloc", "No floor loc",
                'APs with mapLocation="default location". Set mapLocation = Building > Floor.'),
               ("manset", "Loc & not Map", "APs not in CPI maps, but manually assigned a mapLocation."),
               ("hasComma", "Comma in loc", "Comma(s) in the mapLocation (replaced with %)"),
               ("locSyntax", "mapLoc fields", "APs with not exactly 3 fields in the mapLocation."),
               ("duplMac", "", "duplicate macAddresses. Fatal error."),
               ("noMap", "not in Maps", "APs that are otherwise not assigned a location the Site Maps")
               )

    dupl_mac_cnt = 0  					# duplicate macAddresses found. fatal error
    faceplate_cnt = 0  					# number of records w/o faceplate number

    # loose syntax of faceplate to match on common errors too
    separators = r'|'  					# Allow only pipe as a mapLocation field separator
    faceplate_re = r'[0-9]{2,3}-[0-9]{2}-[sS]{0,1}[0-9]{1,3}-[a-zA-Z0-9][0-9]*'
    # was r'([a-zA-Z]-)?[a-zA-Z0-9]+-[mMpP][0-9]+-[O]?[wW][0-9]+(-(OW|R|RW)[0-9]{2})?'
    name_pat = r'((MatherQuad|CaseQuad|SouthRes)-)?[a-zA-Z0-9]+-[mMpP][0-9]+-[O]?[wW][0-9]+(-(OW|R|RW)[0-9]{2})*'
    leading_faceplate_re = '^' + faceplate_re + ' '

    apd_reader = cpiapi.Cache.Reader(my_cpi, 'v4/data/AccessPointDetails', age=0.5)
    apd_dict = {}
    if report:
        print(f'{"   Problem":13} {"    macAddress":17} {"  ipAddress":14} {"     apName":20}"'
              + f' "locationHierarchy", "mapLocation"')
    for row in apd_reader:
        output = True  					# assume that this record will be output
        # obtain the required fields from this record
        apName = row['name']
        locationHierarchy = row['locationHierarchy']
        mac_address = ':'.join(row['macAddress']['octets'][i:i + 2] for i in (0, 2, 4, 6, 8, 10))
        if 'mapLocation' in row:  		# AP has a mapLocation?
            map_location = row['mapLocation']  # Yes. text supplied to output
        else:  							# No
            map_location = ''  			# provide zero-length string
        ipAddress = row['ipAddress']['address']

        if args.filterAttr is not None and args.filterRE is not None:
            if not re.fullmatch(args.filterRE, dig_down(row, args.filterAttr), flags=re.I):
                continue  				# skip record

        # location is hierarchy w/o the leftmost node (either 'Root Area > ' or 'campusName > ')
        location = re.sub(r'^[^>]*> ', '', locationHierarchy, count=1)

        if args.verbose:
            print(f"{mac_address}|{locationHierarchy}|{map_location}")
        std_fields = f'{mac_address:17} {ipAddress:14} {apName:20} "{locationHierarchy}", "{map_location}"'
        if re.search(r'\?', map_location):  # Question mark(s) in the mapLocation?
            report_err("questioned")
        has_faceplate = False
        if re.search(leading_faceplate_re, map_location):  # starts with a faceplate number?
            # Replace the faceplate number with location
            map_location = re.sub(leading_faceplate_re, location + ' ', map_location, count=1)
            has_faceplate = True
        # Silently remove any fn faceplate number(s) from the mapLocation
        map_location, i = re.subn('[ ]?' + faceplate_re, '', map_location)
        if i > 0:
            has_faceplate = True
        if not has_faceplate:
            report_err("noFaceplate")
        mac_re = r'([0-9A-Fa-f][0-9A-Fa-f]:){5}[0-9A-Fa-f]{2},?'
        if re.search(mac_re, map_location):  # One or more MAC address?
            map_location, i = re.subn(mac_re, '', map_location)  # Remove
            report_err("macAdd")
        if not re.fullmatch(name_pat, apName):
            # AP name has incorrect syntax
            report_err("apnamesyntax")
        no_map_reported = False
        if map_location == 'default location' and locationHierarchy == 'Root Area':
            # AP has not been assigned a location in CPI and there is no mapLocation info
            report_err("unassigned")
            no_map_reported = True
            output = False  			# no information to warrant outputting
        elif map_location == 'default location' and locationHierarchy != 'Root Area':
            # AP has been assigned to a building/floor, but not to a specific location
            # provide a better location text from locationHierarchy
            report_err("noloc")
            map_location = location
            no_map_reported = True
        else:  # mapLocation!='default location'
            if locationHierarchy == 'Root Area':
                # AP has not been assigned a location in CPI, but mapLocation is manually set
                # No way to improve. Output it as is
                report_err("manset")
                no_map_reported = True
            if not re.fullmatch(r'[^|]+\|[^|]+\|[^|]+', map_location):
                # bad mapLocation syntax
                report_err("locSyntax")
        # Check for any commas in the input mapLocation
        map_location, i = re.subn(r',', '%', map_location)
        if i > 0:
            report_err("hasComma")
        sd_rec = sd_id.get(row['serviceDomainId'], None)
        if sd_rec is None or sd_rec.get('domainType', None) not in {'FLOOR_AREA', 'OUTDOOR_AREA'}:
            if not no_map_reported:     # don't report twice
                report_err("noMap")
        # map each '|' in the mapLocation to ',' for output to 911Cellular
        map_location, i = re.subn(r'\|', r',', map_location)
        if output:
            if args.verbose:
                print(f"--> ... {map_location}\n")
            if mac_address in apd_dict:
                dupl_mac_cnt += 1
                print(f'record: {std_fields}\nduplicates record with mapLocation="{apd_dict[mac_address]["name"]}"')
            else:
                apd_dict[mac_address] = {'macAddress': mac_address, 'name': map_location}

    # output summary of errors
    print('\n Qty Code ' + (13 - 4) * ' ' + 'Description')
    print(4 * '-' + ' ' + 13 * '-' + ' ' + 40 * '-')
    tot_cnt = 0
    for name in report_txt:
        entry = report_txt[name]
        if entry['cnt'] > 0:
            print(f"{entry['cnt']:4} {entry['name']:13} {entry['full']}")
        tot_cnt += entry['cnt']

    if dupl_mac_cnt > 0:
        print(f"{dupl_mac_cnt:4} duplicate macAddresses. Fatal error.")
    print('----')
    print(f"{tot_cnt:4} total errors in input\n")
    if faceplate_cnt > 0:
        print(f"{faceplate_cnt:4} other faceplate numbers removed from the output mapLocation")
    if dupl_mac_cnt > 0:
        print('Processing terminated because of duplicate MAC(s)')
        sys.exit(2)
    return apd_dict


parser = ArgumentParser(description='Produce error report on AccessPointDetails table')
parser.add_argument('--filterAttr', action='store', default=None,
                    help='filter by exact RegEx match of filterAttr by filterRE')
parser.add_argument('--filterRE', action='store', default=None,
                    help='Regular expression for filtering')
parser.add_argument('--nofaceplate', action='store_false', dest='faceplate', default=True,
                    help='Do not report missing faceplate')
parser.add_argument('--nolocsyntax', action='store_false', dest='locsyntax', default=True,
                    help='Do not report mapLocations with != 3 fields')
parser.add_argument('--report', action='store_true', default=False,
                    help='report all location problems in [possibly filtered] input')
parser.add_argument('--verbose', action='count', default=0,
                    help='prints processing detail')
args = parser.parse_args()

omit_detail = set()
if not args.faceplate:
    omit_detail.add('noFaceplate')
if not args.locsyntax:
    omit_detail.add('locSyntax')

cred = credentials.credentials('ncs01.case.edu')  # get default login credentials
my_cpi = cpiapi.Cpi(cred[0], cred[1])  # server instance

sd_reader = cpiapi.Cache.Reader(my_cpi, 'v4/data/ServiceDomains', age=0.5)
sd_id = {row['@id']: row for row in sd_reader}

if False:
    # prepare sitesByHierarchy table for access by AccessPointDetails.locationHierarchy
    cred = credentials('ncs01.case.edu')  # get in credentials
    myCpi = cpi.Cpi(cred[0], cred[1])  # server instance
    # read GroupSpecifications table
    gsReader = cpi.Cpi.Reader(myCpi, 'v4/data/GroupSpecification')
    groupSpec = dict()
    for rec in gsReader:
        groupSpec[rec['@id']] = rec

    # read and augment the sites table
    siteReader = cpi.Cpi.Reader(myCpi, 'v4/op/groups/sites')
    siteByHierarchy = dict()  # records indexed by locationHierarchy
    siteByGroupId = dict()  # records indexed by groupId
    for rec in siteReader:
        # translate name format to locationHierarchy format
        name = rec.get('name', '')
        siteType = rec.get('siteType', '')
        errs = []
        if len(name) < 23:  # DEFAULT location?
            locationHierarchy = 'Root Area'
        else:  # normal "Location/All Locations/site/building/floor"
            locationHierarchy = re.sub(r'/', ' > ', name[23:])
        rec['locationHierarchy'] = locationHierarchy
        # verify that data is appropriate for each type of site
        if siteType == 'Floor Area':
            for field in ['latitude', 'longitude', 'locationAddress']:
                if field in rec and not rec[field] == '':
                    errs.append(f"has {field}")
        elif siteType in {'Building', 'Outdoor Area'}:
            for field in ['latitude', 'longitude', 'locationAddress']:
                if field in rec and not rec[field] == '':
                    pass
                else:
                    errs.append(f"does not have {field}")
        elif siteType in ('Campus', ''):
            pass
        else:
            errs.append(f'has unknown siteType="{siteType}"')
        if len(errs) > 0:
            print(f"{siteType:13}{rec['name']}", ', '.join(errs))
        siteByGroupId[rec['groupId']] = rec
        siteByHierarchy[locationHierarchy] = rec
    fields = ['groupId', 'latitude', 'longitude', 'locationAddress', 'locationGroupType', 'name', 'locationHierarchy']
    print(fields)
    # inherit Floor Area's attributes from parent Building
    for groupId in siteByGroupId:
        rec = siteByGroupId[groupId]
        if rec.get('siteType', None) == 'Floor Area':  # a Floor Area?
            try:
                gsFloor = groupSpec[groupId]  # assoc. group record
                siteBuilding = siteByGroupId[gsFloor['parentId']]  # parent
                for field in ('locationAddress', 'longitude', 'latitude'):
                    if rec.get(field, None) is None and field in siteBuilding:
                        rec[field] = siteBuilding[field]
            except KeyError:
                print(f"Couldn't locate parent Building of {rec['name']}")
    # print sites sorted by locationHierarchy
    lst = [x for x in siteByHierarchy]
    lst.sort()
    for locationHierarchy in lst:
        rec = siteByHierarchy[locationHierarchy]
        print(list(rec.get(x, None) for x in fields))

if args.filterAttr is not None and args.filterRE is not None:
    print('Filtering by full RegEx match of "' + args.filterRE + '" pattern to ' + args.filterAttr + ' attribute')

if args.report:  # load AccessPointDetails table?
    apdDict = load_apd(args.report)
