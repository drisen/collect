#
# client_bands.py Copyright (C) 2021 Dennis Risen, Case Western Reserve University
#
"""
Wireless clients do not necessarily select the AP and protocol that we would
prefer. To what extent does this occur? What are the relevant predictors,
and how might we better support more appropriate selection?

This simple utility application to access utilization 802.11 band, protocol,
and SSID by each client MAC. It outputs a csv summary of the number of times,
that each client MAC has used a 802.11 band, protocol, and SSID.
Pivoting on the data by other attributes could be facilitated by collecting
interesting candidates such as building and AP type.

Reads sessions data from the ClientDetails or ClientSessions csv.gz files in the
'dir-path' directory with filenames matching `pat` and times between `min_time` and `max_time`.

"""
from collections import defaultdict
import csv
import gzip
import os
import re
import time

import mylib
""" To Do
Extend to handle more 6GHz too
"""

# {macAddress: {'apMacAddress: {apMacAddress: cnt}, ...},
# 'protocol': {protocol: cnt, ...},
# 'ssid': {ssid: cnt, ...}}
clients = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
protocols = set()       # each protocol value found in input
ssids = set()           # each SSID value found input
two_protocols = {'DOT11B', 'DOT11G', 'DOT11N2_4GNZ', 'DOT11AX2_4GHZ', 'UNDEFINED', 'UNKNOWN'}
min_time = now = time.time()
max_time = 0.0

# clientDetails = False
clientDetails = True
if clientDetails:
    pat = r'.*_ClientDetailsv4\.csv(\.gz)?'
    dir_path = 'collect_cd'
    sum_filename = 'cd_summary.csv'
else:
    pat = r'.*_ClientSessionsv4\.csv(\.gz)?'
    dir_path = 'collect_cs'
    sum_filename = 'cs_summary.csv'

# process each csv[.gz] file
for filename in os.listdir(dir_path):
    m = re.fullmatch(pat, filename)
    if not m:
        print(f"ignoring {filename}")
        continue
    print(f"processing {filename}")
    with gzip.open(os.path.join(dir_path, filename), 'rt') if m.group(1) == '.gz' \
            else open(os.path.join(dir_path, filename), 'rt', newline='') as cs_file:
        reader = csv.DictReader(cs_file)
        for rec in reader:
            mac_dict = clients[rec['macAddress']]  # dict for this mac
            # build the sets of distinct protocols and SSID
            protocol = rec['protocol']
            band = 'two_four' if protocol in two_protocols else 'five'
            protocols.add(protocol)
            ssids.add(rec['ssid'])
            mac_dict['band'][band] += 1
            for attr in ('apMacAddress', 'protocol', 'ssid'):
                mac_dict[attr][rec[attr]] += 1
            # note the time span covered by the records
            if clientDetails:
                updateTime = float(rec['updateTime']) / 1000.0
                max_time = max(max_time, updateTime)
                min_time = min(min_time, updateTime)
            else:
                sessionEndTime = float(rec['sessionEndTime']) / 1000.0
                sessionStartTime = float(rec['sessionStartTime']) / 1000.0
                if sessionEndTime <= now:  # session has ended?
                    max_time = max(max_time, sessionEndTime)
                    min_time = min(min_time, sessionEndTime)
                max_time = max(max_time, sessionStartTime)
                min_time = min(min_time, sessionStartTime)
print(f"records span at least {mylib.strfTime(min_time)} to {mylib.strfTime(max_time)}")

# write the summary
protocols = list(protocols)
protocols.sort()
ssids = list(ssids)
ssids.sort()
field_names = ['clientMac', 'two_four', 'five'] + protocols + ssids + ['ap1', 'ap2', 'ap3']
with open(sum_filename, 'w', newline='') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=field_names, restval='')
    writer.writeheader()
    for mac, mac_rec in clients.items():
        sum_rec = {'clientMac': mac}
        for category in ('band', 'protocol', 'ssid'):
            for column, cnt in mac_rec[category].items():
                sum_rec[column] = cnt
        aps = [(v, a) for a, v in mac_rec['apMacAddress'].items()]
        aps.sort(reverse=True)
        for i in range(min(3, len(aps))):
            sum_rec[f"ap{i + 1}"] = aps[i][0]
        writer.writerow(sum_rec)
