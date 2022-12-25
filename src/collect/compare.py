#
# compare.py Copyright (C) 2020 Dennis Risen, Case Western Reserve University
#
"""
Compares parallel production, e.g. from different versions, in 'files' and 'prod' directories
"""

from argparse import ArgumentParser
import csv
import functools
import os
import statistics
import sys

import awslib
from cpiapi import all_table_dicts, find_table, numericTypes, SubTable, Table
from mylib import strfTime, verbose_1

""" TO DO
AccessPointDetail: 1st sample on laptop was started 297 seconds later --> some different results
    file transfer from server to laptop converts e accented with ' to A~ (c)  eg cafe'
files\1586059800001_ClientDetailsv4 differences with prod\1586059800030_ClientDetailsv4.csv. file had 1 extra record
files\1586098327892_HistoricalClientCountsv4.csv [97504854440]:[97504883591] 
    23146 records ignored after prod\1586098328527
    the retrieval from laptop took much longer, thus the additional available records at the end
files\1586021854789_HistoricalClientStatsv4.csv: [97410907619]:[97430608067]
    58827 records ignored with prod\1586043454982_ and 1586000254856_
    this is the entire files\1586021854789 file from 2020-04-05Y13:37:34 -- 38:04
files\1586089718629_HistoricalRFCountersv4.csv: [97495248031]:[97496342636]
    9522 records ignored after prod\1586089422380_HistoricalRFCountersv4.csv
files\1585999382999_HistoricalRFLoadStatsv4.csv: [97410378579]:[97425741152]
    133360 records ignored. [mid-compare] to prod\1586035383257_ prod\1586053383345
files\1586036874403_HistoricalRFStatsv4.csv: [97411507250]:[97427876316]
    16257 records ignored with prod\1586000874489_ and prod\1586054874753_
files\1586126874408_HistoricalRFStatsv4.csv vs prod\1586126875074: 1402 at and vs sum=1355 distributed
Report compare_rec summary
"""
col_width = 30
max_diff = 20			# Limit for number of cases per table attribute to report
spaces = ' '*col_width					# column width for columnar output


def compare(sources: list, source_names: list, transform: callable,
        key_func: callable, rec_func: callable, verbose: int = 0):
    """Compare 2 streams of records

        Parameters:
            sources (list):			[generator, ...]
            source_names (list):	[str, ...]
            transform (callable)	apply transform(record) to each record
            key_func (callable):	key function returns key for record
            rec_func (callable):	process corresponding records
            verbose (int):			increase diagnostic messages over default=0

    """
    rec_cnt = list(0 for i in range(len(sources)))  # count of records in each source

    def end_skipped(i: int):
        nonlocal count, start_keys, end_keys, source_names, verbose
        if count > 0 and start_keys[i] is not None:
            if verbose > 1:
                print(f"{spaces*i}{start_keys[i]}:{end_keys[i]} {count} record{'s' if count>1 else ''} ignored.")
            start_keys[i] = None
            count = 0

    def extend(i: int):
        nonlocal count, start_keys, end_keys, transform, rec_cnt
        if count == 0:
            start_keys[i] = keys[i]
        end_keys[i] = keys[i]
        count += 1
        recs[i] = get_rec(sources[i], transform)
        rec_cnt[i] += 0 if recs[i] is None else 1
        keys[i] = None if recs[i] is None else key_func(recs[i])

    recs = [get_rec(sources[0], transform), get_rec(sources[1], transform)]
    for i in (0, 1):
        rec_cnt[i] += 0 if recs[0] is None else 1
    keys = [None if recs[i] is None else key_func(recs[i]) for i in range(len(recs))]
    count = 0							# count of sequence of un_matched records
    start_keys = [None, None]
    end_keys = [None, None]
    while recs[0] is not None and recs[1] is not None:  # while there are 2 streams
        if verbose > 0:
            print(f"{str(keys[0]):30}{keys[1]}")
        if keys[0] < keys[1]:
            end_skipped(1)				# complete possible skipping of stream 1
            extend(0)					# start, or continue, skipping of stream 0
        elif keys[0] > keys[1]:
            end_skipped(0)				# complete possible skipping of stream 0
            extend(1)					# start, or continue, skipping of stream 1
        else:							# record keys are equal
            text = rec_func(recs, keys[0])  # process the matching records
            k = keys[0]
            for i in range(len(sources)):  # for each stream
                end_skipped(i)			# complete possible skipping
                recs[i] = get_rec(sources[i], transform)  # get new record
                rec_cnt[i] += 0 if recs[i] is None else 1
                keys[i] = None if recs[i] is None else key_func(recs[i])
            if len(text) > 0:
                print(f"{k}: {text}") 	# output notes from record processing
    i = 0 if recs[0] is not None else 1 if recs[1] is not None else None
    if i is not None:					# is one stream still remaining?
        end_skipped(1-i)				# end skipping in the fn stream
        while True:
            if verbose > 0:
                print(f"{spaces*i}{keys[i]} alone")
            if count == 0:
                start_keys[i] = keys[i]
            end_keys[i] = keys[i]
            count += 1
            recs[i] = get_rec(sources[i], transform)
            rec_cnt[i] += 0 if recs[i] is None else 1
            if recs[i] is None:
                break
            keys[i] = key_func(recs[i])
        end_skipped(i)					# report any final skipped records
    if verbose > 0:
        print(f"{rec_cnt[0]:26} {'==' if rec_cnt[0]==rec_cnt[1] else '!='} {rec_cnt[1]}")


def compare_diff(table: SubTable) -> str:
    """Report the count of differences for each field in table

    Parameters:
        table:		the table definition
    Returns:		comma-separated differences text
    """
    fc = [i for i in table.field_counts.items()]
    fc.sort()
    fc = dict(fc)						# sorted field_counts

    diff = []
    for attr in fc:
        diff.append(f"{attr}({fc[attr]})")
    return ', '.join(diff)


def compare_rec(recs: list, keys: list, table: SubTable) -> str:
    """Compare each data field of the 2 records. Reporting differences into result.

    Parameters:
        recs:		[record from a, record from b]
        keys:		???
        table:		SubTable definition

    Returns:		difference text
    """

    def post_diff(attr: str):
        """Accumulate difference in records[attr] to nonlocal result."""
        nonlocal recs, table, result
        global max_diff
        if recs[0][attr] == recs[1][attr]:
            return						# fields are equal -- nothing to say
        try:  # increment inequality count for attr
            table.field_counts[attr] += 1
        except KeyError:
            table.field_counts[attr] = 1
        if table.field_counts[attr] > max_diff:  # report the difference?
            return						# No
        if table.field_counts[attr] == max_diff:  # last time?
            s = f", {attr}(...,...)"

        s = f", {attr}({recs[0].get(attr, None)},{recs[1].get(attr, None)})"
        result += s						# post attribute difference

    result = ''							# initially nothing to report
    attrs = table.select				# All attributes
    for attr in attrs:					# compare each field
        if attr == 'polledTime': 		# Except never compare the 'polledTime'
            continue
        try:
            post_diff(attr)
        except KeyError:				# one or both records missing attr
            post_diff(attr)
    if len(result) > 2:
        return result[2:]
    else:
        return ''


def csv_files(file_list: list, transform: callable, key_func: callable,
            formatter: callable = print, verbose: int = 0):
    """Generator to read a list of csv files.
    Yields dict generated by DictReader.
    Drops records that key function classifies as in non-ascending order.

    Parameters:
        file_list (list):		[{'prefix': str, 'msec': int, 'tablename': str,
                            'version': int, 'suffix': str, 'file_name': str)}, ...]}
        transform (callable):	called w/ each record for field transformations
        key_func (callable):	key function
        formatter (callable):	function to output messages
        verbose (int):			increase level of diagnostic messages over default=0
    """
    keys_old = None				# Initially, no old record keys
    discards = 0				# Count of discarded records in sequence
    msg = []					# Queue of messages deferred until before next yield
    msg2 = []					# Queue of messages deferred until after next yield

    def msg_put():
        nonlocal msg, msg2
        for m in msg:
            formatter(m)
        msg = msg2
        msg2 = []

    for file in file_list: 				# for each file in file list
        file_name = os.path.join(file['prefix'], file['file_name'])
        time_stamp = strfTime(int(file['msec']))
        with open(file_name, 'r', newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rec_cnt = 0
            delta_list = []				# list of keys[0]-keys_old[0]
            for rec in csv_reader: 		# Read each dict returned by DictReader
                transform(rec)
                keys = key_func(rec)
                if rec_cnt == 0: 		# first record of the file?
                    if verbose > 0:
                        msg.append(f"{keys} Opened {file_name} {time_stamp}")
                    rec_cnt += 1
                if keys_old is None: 	# No previous record?
                    keys_old = keys
                    msg_put()
                    yield rec 			# No previous record. Yield new record
                    msg_put()
                else:					# Yes, there is a previous record
                    if len(keys) > 0:
                        delta_list.append(keys[0]-keys_old[0])
                    if keys_old <= keys:  # old <= rec?
                        if verbose > 1 and len(keys) > 0:  # Check for missing key values?
                            try:
                                if int(keys[0]) != int(keys_old[0]) + 1:
                                    a = keys_old.copy()
                                    b = keys.copy()
                                    a[0] = int(a[0])+1
                                    b[0] = int(b[0])-1
                                    msg2.append(f"{a}:{b} key{'s' if b[0]>a[0] else ''} missing")
                            except ValueError:
                                pass
                        if verbose > 0 and discards > 0:  # Report discarded records?
                            msg.append(f"{discard_start}:{discard_last} {discards} "
                                + f"out of order record{'s' if discards>1 else ''}"
                                + " discarded.")
                        discards = 0 	# End of block of out-of-order records
                        keys_old = keys.copy()
                        msg_put()
                        yield rec 		# yield the record
                        msg_put()
                    else:				# old > rec. discard rec
                        if discards == 0:
                            discard_start = keys
                        discard_last = keys
                        discards += 1
                keys_prev = keys
        if verbose > 0:
            if rec_cnt > 0:				# some records in this file:
                if len(delta_list) > 0:
                    num_buckets = min(30, len(delta_list))
                    buc_size = len(delta_list)/num_buckets
                    buckets = []
                    start = 0
                    while start < len(delta_list):
                        next_start = start+buc_size
                        buckets.append(int(statistics.mean(delta_list[int(start):int(next_start)])))
                        start = next_start
                    msg.append(f"{buckets}")
                msg.append(f"{keys_prev} Closed {file_name}")
            else:						# no records in this file
                msg.append(f"Opened and Closed {file_name} {time_stamp}")
    if verbose > 0 and discards > 0: 	# Report discarded records?
        msg.append(f"{discard_start}:{discard_last} {discards} out of order record"
            + f"{'s' if discards>1 else ''} discarded.")
    discards = 0  						# End of block of out-of-order records
    msg_put()
    msg_put()
    return


def get_rec(iterator: callable, transform: callable):
    """Get one record from iterable, returning None on StopIteration

    Parameters:
        iterator:		iterable that returns a record
        transform:		???
    """
    try:
        rec = next(iterator)
        # rec = iterator.__next__()
        transform(rec)
        return rec
    except StopIteration:				# source is exhausted
        return None


def key_func_template(rec: dict, keys: list) -> list:
    """Key function that returns a composite key for rec.

    Parameters:
        rec:		a record
        keys:		list of key names

    Returns:		list of key values
    """
    return [rec[x] for x in keys]


def merge(sources: list, transform: callable, key_func: callable,
        de_dup: callable, verbose: int = 0):
    """Merge N dict streams
    Parameters:
        sources (list):			[generator, ...]
        transform (callable):	apply transform(record) to each record
        key_func (callable):	key function returns key for record
        de_dup (callable):		index of record to drop, or None for neither
        verbose (int):			increase diagnostic message level beyond default=0

    """
    rec_cnt = list(None for i in range(len(sources)))
    active = []
    for i in range(len(sources)):
        rec = get_rec(sources[i], transform)
        rec_cnt[i] += 0 if rec is None else 1
        if rec is not None:
            active.append((key_func(rec), i, rec))
    while len(active) >= 2:				# while 2 or more streams remain
        active.sort()					# identify the 2 with lowest key values
        a, b = active[0], active[1] 	# 2 streams with lowest keys
        if a[0] == b[0]:  				# Records have equal keys.
            index = de_dup(a[2], b[2])  # Which record to drop?
            if index is not None: 		# one to be dropped?
                active.pop(index)
                continue
            # No, neither to be dropped
            x, source, rec = active.pop(0)  # Output 1st
            yield rec  # output
            rec = get_rec(sources[source], transform)
            rec_cnt[source] += 0 if rec is None else 1
            if rec is not None:  		# another record in this stream?
                active.append([key_func(rec), source, rec])  # replenish
    while len(active) >= 1:				# Yield the remainder of the last source
        x, source, rec = active.pop(0)  # Output 1st
        yield rec  # output
        rec = get_rec(sources[source], transform)
        rec_cnt[source] += 0 if rec is None else 1
        if rec is not None:  			# another record in this stream?
            active.append([key_func(rec), source, rec])  # replenish


def my_columns(text: object, column: int):
    print(f"{' '*col_width*column}{str(text)}")


def to_int(rec: dict, keys: list):
    """Convert each rec[keys[i]] to int."""
    for key in keys:
        rec[key] = int(float(rec[key]))  # allow N+ or n*.N*


if __name__ == '__main__':
    parser = ArgumentParser(description='Compare the csv files from e.g. two versions versions of collect.py')
    parser.add_argument('--source', action='append', dest='source',
        help='path to directory with files')
    parser.add_argument('--verbose', action='count', dest='verbose', default=0,
        help='increase diagnostic message detail')
    args = parser.parse_args()

    source_paths = args.source
    if len(source_paths) != 2:
        print(f"Must supply exactly 2 sources directories")
        sys.exit(1)
    table_names = set()					# each table_name encountered in any source
    data_sets = []						# {tablename: [{}, ...], ...}
    for i in range(len(source_paths)): 	# for each source directory
        dir_path = source_paths[i]		# the directory path name
        names = os.listdir(dir_path) 	# list of each file name in directory
        tables = dict()					# build {table_name: [...], ...}
        for file_name in names:			# for each file in the directory
            m = awslib.key_split('object/'+file_name)  # include placeholder prefix
            if m is None or m['suffix'] != 'csv':  # .csv file?
                continue				# No, exclude file that is not .csv
            m['file_name'] = file_name
            m['prefix'] = dir_path  # replace placeholder prefix with directory path
            try:
                tables[m['tablename']].insert(0, m)  # add to list for tablename
            except KeyError:			# tables[tablename] entry does not exist
                tables[m['tablename']] = [m]  # create new list for tablename
        for table_name in tables:		# for each table with csv files
            tables[table_name].sort(key=lambda obj: obj['msec'])  # by timestamp
            table_names.add(table_name)
        data_sets.append(tables)

    table_names = list(table_names)		# convert set to list
    table_names.sort()					# ... sorted by table_name
    print(f"Comparing contents of {source_paths[0]} to {source_paths[1]} directories")
    if args.verbose > 0:
        print(f"for tables: {table_names}")
    for table_name in table_names:		# for each table with csv file(s)
        print(f"\nPROCESSING {table_name} FILES")
        table = find_table(table_name, all_table_dicts)
        if table is None:
            print(f"Cant find definition for {table_name}. Skipping this table.")
            continue
        # supply keys:list so  call is key_func(rec)
        keys = []
        for k in table.key_defs:
            if k[1] != 'polledTime': 	# drop 'polledTime' key for record compares
                keys.append(k[1])
        key_func = functools.partial(key_func_template, keys=keys)
        if table_name not in {'sites'}:  # retrieval ordered by primary key(s)?
            order_func = functools.partial(key_func_template, keys=keys)
        else:							# No, do not insist on ordered records
            order_func = functools.partial(key_func_template, keys=[])

        numeric = []					# list of keys to be transformed to int
        for key in table.key_defs:
            if key[0] in numericTypes:
                numeric.append(key[1])
        transform = functools.partial(to_int, keys=numeric)

        generators = []					# generator for this table in each data_set
        for i in range(len(data_sets)):
            formatter = functools.partial(my_columns, column=i)  # columnar generator messages
            if table_name in data_sets[i]:
                # create a record generator for this source
                generators.append(csv_files(file_list=data_sets[i][table_name],
                    transform=transform, key_func=order_func, formatter=formatter,
                    verbose=verbose_1(args.verbose)))
            else:
                print(f"{spaces*i}{table_name} not in {source_paths[i]}")
        if len(generators) != len(data_sets):  # table_name missing from some source(s)
            print(f"{table_name} will not be compared")
            continue
        rec_func = functools.partial(compare_rec, table=table)
        if table_name == ('ClientSessions' or isinstance(table, Table) and table.polled
                or table.parent is not None and table.parent.polled):
            # Compare corresponding files one at a time
            common = min(len(data_sets[0][table_name]), len(data_sets[1][table_name]))
            for j in range(common):
                generators = []
                for i in (0, 1):
                    formatter = functools.partial(my_columns, column=i)  # columnar generator messages
                    generators.append(csv_files(file_list=[data_sets[i][table_name][j]],
                        transform=transform, key_func=order_func,
                        formatter=formatter, verbose=verbose_1(args.verbose)))
                compare(sources=generators, source_names=source_paths,
                        transform=transform, key_func=key_func, rec_func=rec_func,
                        verbose=verbose_1(args.verbose))
            # report any extra files in one of the dataSets
            longer = 0 if len(data_sets[0]) > len(data_sets[1]) else 1
            if len(data_sets[longer][table_name]) > common:
                print(f"{' '*col_width*longer}Extra files not compared:")
                for i in range(common, len(data_sets[longer][table_name])):
                    print(f"{' ' * col_width * longer}{data_sets[longer][table_name][i]}")

        else:					# Compare all files for a table in a combined stream
            compare(sources=generators, source_names=source_paths,
                    transform=transform, key_func=key_func, rec_func=rec_func,
                    verbose=verbose_1(args.verbose))
        s = compare_diff(table)		# report count of differences for each field
        if len(s) > 0:
            print(s)
