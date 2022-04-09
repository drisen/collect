# collect package
Applications that collect and process statistics from CPI
- **apdCheck.py** Read access point definitions AccessPointDetails API and check
for syntactically valid mapLocation information
- **client_bands.py** Read ClientDetails or ClientSessions csv[.gz] files and output a csv summary
of each MAC's usage of the 802.11 bands and protocols
- **client_count** Read from AWS S3 or files:./files to produce csv reports, apClientCountsAuthYYYY-MM-DD and apClientCountsTotYYYY-MM-DD
of the number of wireless clients associated with each AP
in the listed buildings in each 30 minute interval for either (from CPI server)
the previous day, or (from AWS S3) during the specified range of dates
- **collect.py** Continuously collect statistics from CPI APIs on the API
definitions listed in cpiapi.production and write each sample to ./files.
- **collect_cd.py** Polls the CPI ClientDetails API in real-time, writing its most significant fields
to time-stamped NNNNNNNNNN_ClientDetailsv4.csv files in the collect_cd sub-directory.
It starts a new file every N hours, and keeps a collection status in collect_cd.json
to facilitate appending only the new polls.
- **collect_cd.py** Polls the CPI ClientSessions API in real-time, writing its most significant fields
to time-stamped NNNNNNNNNN_ClientSessionsv4.csv files in the collect_cs sub-directory.
It starts a new file every N hours, and keeps a collection status in collect_cs.json
to facilitate appending only the new polls.
- **compare.py** Compares the results of parallel operation of collect.py for
regression testing.
- **parts.py** Aggregates multiple nnn_tablename.part files produced by collect_c[ds]
into a single nnn_tablename.csv file and deletes the nnn_tablename.parts.
- **tracker** Uses ClientSessions join ClientDetails API real-time data collection of client
device associations to APs stored in AWS S3 to calculate estimated
[infection] exposure risks by AP cell and individual user.
Writes a csv report of the top risks by location and user.
Assists exposure tracking by producing a report of an individual users
location over time.


