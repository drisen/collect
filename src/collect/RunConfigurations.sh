# collection of command-line snippets
# ensure using up-to-date pip and build
python -m pip install --upgrade pip build setuptools
# ensure using up-to-date requirements
python -m pip install -r requirements.txt --upgrade
# build a release
python -m build

# For e.g. the AccessPointDetails and RadioDetails table definitions in cpiapi, output their enum and table DDL in hive and SQL
python collect.py --enum --hive --SQL --table=AccessPointDetails --table=RadioDetails
testing
# Read access point definitions from AccessPointDetails API, and check for syntactically valid mapLocation information
python apdCheck.py --nofaceplate --report
# Realtime poll the ClientDetails API
python colect_cd.py
# For each of 2 sets of selected building sets over a range of dates, read AWS data for a date range and email reports to the listed recipients for that building set.
python client_count.py --mindate=2022/12/01 --maxdate=2022/12/03 --verbose --email=tjv30@case.edu --email=dar5@case.edu --email=/ --email=wxw265@case.edu --email=dar5@case.edu --email=/ --email=wxw265@case.edu --email=dar5@case.edu
--title=/ --title=Samson --title=/ --title=Campus
--name=brb- --name=ksl- --name=nord- --name=tvuc- --name=pbl- --name=white-
--name=/ --name=samson- --name=/ --name=brb- --name=eastwing- --name=westwing- --name=sears- --name=wrb- --name=pathology-
# poll the ClientSessions API in real-time
python collect_cs.py
# forget what data has been previously collected and continuously collect only the specified tables
python collect.py --table=ClientSessions --enums --fields --verbose --verbose --verbose files
# test collect production with verbose diagnostics messages
python collect.py --enums --fields --username=dar5 --reset --verbose --verbose --verbose files
# report a snapshot (current or any cached in the past 5 days) of the ap inventory and rxNeighbors data for selected buildings
python neighbors.py --name_regex=samson --allchannels --age=5 --username=dar5 --verbose samson_inv_1216.txt samson_nei_1216.txt
