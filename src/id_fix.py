#
# Copyright 2020 Dennis Risen, Case Western Reserve University
#
import csv
import gzip
from os import listdir, path
from re import fullmatch
"""
Correct for 2020-01-23 reset of event_seq in the ncs01.case.edu Oracle database
by adding at least 1,026,181,420 - 90,477,778 = 935,703,642 to each record since
the reset.
"""

source_dir = r'C:\Users\dar5\Google Drive\Case\PyCharm\collect\events_prod\events'
dest_dir = r'C:\Users\dar5\Google Drive\Case\PyCharm\collect\events_prod\events_fixed'
pat = r'[0-9]+_Eventsv4\.csv'

files = listdir(source_dir)
for fn in files:
    if not fullmatch(pattern=pat, string=fn):
        continue
    print(fn)
    with open(path.join(source_dir, fn), 'r', newline='') as in_file:
        reader = csv.DictReader(in_file)
        with gzip.open(filename=path.join(dest_dir, fn+'.gz'), mode='wt') as zip_stream:
            writer = csv.DictWriter(zip_stream, fieldnames=reader.fieldnames)
            writer.writeheader()
            for rec in reader:
                rec['eventId'] = str(int(rec['eventId']) + 1000000000)
                writer.writerow(rec)
