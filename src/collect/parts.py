#!/usr/bin/python3
# parts.py Copyright (C) 2020 Dennis Risen, Case Western Reserve University
#
"""
Aggregates multiple nnn_tablename.part files produced by collect_c[ds]
into a single nnn_tablename.csv file and deletes the nnn_tablename.parts.
"""

from collections import defaultdict
from mylib import logErr
from os import listdir, mkdir, remove, rename, rmdir
from os.path import isdir, join
import re
from typing import Union

path = 'files'							# (relative) path to collect output
dir_pat = re.compile(r"([0-9]+)_([a-z]+)(v[0-9])?((_[a-z]+)*)", flags=re.IGNORECASE)

# cleanup directories from failed aggregation of parts
dirs_list = [fn for fn in listdir(path)
             if re.fullmatch(dir_pat, fn) and isdir(join(path, fn))]
for file_name in dirs_list:				# incomplete parts processing
    in_path = join(path, file_name)  	# path to this directory
    print(f"Recovering incomplete processing of {in_path} directory.")
    in_dir = listdir(in_path)			# list of files in this directory
    if len(in_dir) > 0:					# some files in the directory?
        result = file_name + '.csv'		# Yes. target result has this name
        if result in in_dir:			# Had produced result?
            print(f"Which has {result} file and {len(in_dir)-1} other files")
            for fn in in_dir:			# Yes. delete all files in the directory ...
                if fn == result:		# ... except for the result file
                    continue
                remove(join(in_path, fn))
            rename(join(in_path, result), join(path, result))  # move result out
        else:							# No result. return parts to path and cleanup
            print(f"Which has no result .csv file and {len(in_dir)} other files")
            for fn in in_dir:
                if fn[-5:] == '.part':  # a '.part' file?
                    rename(join(in_path, fn), join(path, fn))  # move out
                else:					# No
                    remove(join(in_path, fn))
    rmdir(in_path)						# finally, remove the directory

# aggregate .part files
file_pat = re.compile(r"([0-9]+)_([a-z]+)(v[0-9])?((_[a-z]+)*)(\.part)",
                    flags=re.IGNORECASE)
# parts_list = [fn for fn in file_list if fn[-5:] == '.part']  # ... that are '.part'
parts_dict = defaultdict(list)  # {Table_name+version+SubTable_name: [timestamp, ...}
for file_name in listdir(path):			# list of files in collect's output directory
    m = re.fullmatch(file_pat, file_name)
    if m:								# a .part file from collect?
        table_name = m.group(2)+m.group(3)+m.group(4)  # [Sub]Table name, w/o stamp
        parts_dict[table_name].append(int(m.group(1)))
for tbl, stamp_list in parts_dict.items():  # for each table_name
    continue_parts_dict = False  		# to break from inner loops
    if len(stamp_list) == 1:			# just one file?
        rename(join(path, f"{stamp_list[0]}_{tbl}.part"),  # Yes. simple rename
            join(path, f"{stamp_list[0]}_{tbl}.csv"))
        continue
    stamp_list.sort()					# No.  [time_stamp, ...] sorted
    in_path = join(path, f"{stamp_list[-1]}_{tbl}")  # Aggregate files
    mkdir(in_path)		# create the directory in which we aggregate the parts ...
    for stamp in stamp_list: 			# move each .part file into the directory
        fn = f"{stamp}_{tbl}.part"
        rename(join(path, fn), join(in_path, fn))
    # Aggregate each .part file to a .tmp file
    header_rec: Union[str, None] = None  # csv header record not yet initialized
    out_fn = join(in_path, f"{stamp_list[-1]}_{tbl}.tmp")  # the .tmp file name
    with open(out_fn, 'w') as out_file:
        is_header = True				# first record is a header record
        for stamp in stamp_list:		# for each part
            fn = join(in_path, f"{stamp}_{tbl}.part")
            with open(fn, 'r') as in_file:
                for line in in_file:
                    if is_header: 		# first record of a file?
                        if header_rec is None:  # Yes. 1st record of first file?
                            header_rec = line  # Yes. save the initial header
                            out_file.write(line)  # and output the initial header
                        is_header = False  # Yes. Copy remaining records
                        if header_rec == line:  # same csv header?
                            continue
                        logErr(f"Header record for {fn} is different. Aggregation into {out_fn} aborted.")
                        continue_parts_dict = True  # break out to next table
                    out_file.write(line)
                    if continue_parts_dict:  # break out to next table?
                        break			# Yes
            is_header = True 			# skip the 1st record in the next file
            if continue_parts_dict:		# break out to next table?
                break					# Yes
    if continue_parts_dict:				# skip processing this table?
        continue						# Yes. iterate to next table
    rename(out_fn, out_fn[:-4]+'.csv') 	# rename .tmp to .csv is atomic completion
    for stamp in stamp_list:			# delete each of the .part files
        remove(join(in_path, f"{stamp}_{tbl}.part"))
    rename(out_fn[:-4]+'.csv', join(path, f"{stamp_list[-1]}_{tbl}.csv"))  # move .csv up
    rmdir(in_path)						# and delete now empty directory
