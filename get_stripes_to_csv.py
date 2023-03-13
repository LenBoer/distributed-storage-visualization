7# This file is part of IOBAT.
# 
# IOBAT is free software: you can redistribute it and/or modify it under the terms
#  of the GNU General Public License as published by the Free Software Foundation, 
# either version 3 of the License, or (at your option) any later version.
# 
# IOBAT is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
# See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along with IOBAT. 
# If not, see <https://www.gnu.org/licenses/>. 

import os
import subprocess
import sys
import json
import pandas as pd
import re


# http://wiki.lustre.org/Configuring_Lustre_File_Striping
# http://doc.lustre.org/lustre_manual.xhtml#managingstripingfreespace


def lfs_getstripe(filepath):
    """
    Query lfs stripe information for filepath.

    Args:
        filepath (str): path to display information

    Returns:
        list of dictionaries

    """
    assert os.path.exists(filepath)
    cmdline = ["lfs", "getstripe", filepath]
    raw = subprocess.run(cmdline, stdout=subprocess.PIPE).stdout.decode("utf-8")

    entries = []

    # several of these only exist in Progressive File Layouts, as such we need to handle those differently
    blocks = raw.split("\n\n")
    blocks.pop()

    if len(blocks) > 1:
        for i, block in enumerate(blocks):
            # the first lines of the first block contain the file name and other general information, which we want to seperate from the other components
            lines = block.split("\n")
            if i==0:
                entry = {}
                entry["filename"] = lines.pop(0)
                entry["lcm_layout_gen"] = lines.pop(0).split()[1]
                entry["lcm_mirror_count"] = lines.pop(0).split()[1]
                entry["lcm_entry_count"] = lines.pop(0).split()[1]
                entries.append(entry)


            reading_osts = False
            entry = {}

            for line in lines:
                attributes = line.split()
                if len(attributes) == 2:
                    attributes = line.split()
                    key = attributes[0][:-1]
                    value = attributes[1]
                    entry[key] = value

                elif len(attributes) == 1:
                    reading_osts = True
                    osts = []

                else:
                    ost = {}
                    ost["l_ost_idx"] = attributes[4][:-1]
                    ost["l_fid"] = attributes[6][1:-1]
                    osts.append(ost)
            
                
            if reading_osts:
                entry["osts"] = osts
            entries.append(entry)


    else:
        if len(blocks) == 0:
            return {"error": raw}
        entry = {}
        lines = blocks[0].split("\n")
        entry["filename"] = lines.pop(0)
        reading_osts = False
        osts = []

        for line in lines:
            attributes = line.split()

            if len(attributes) == 2:
                key = attributes[0][:-1]
                value = attributes[1]
                entry[key] = value

            elif len(attributes) > 2 and reading_osts==True:
                ost = {}
                ost["obdidx"] = attributes[0]
                ost["objid"] = attributes[1]
                ost["group"] = attributes[3]
                osts.append(ost)

            else:
                reading_osts = True

        entry["osts"] = osts
        entries.append(entry)

    return entries



def lfs_setstripe(filepath, args, verbose=False):
    """
    Set Lustre striping information.

    Parameters:
        filepath (str): path to file to apply
        args (list): extra arguments

    Returns:
        stdout of subprocess

    """
    assert not os.path.exists(filepath)
    assert len(args) > 0
    cmdline = ["lfs", "setstripe"] + args + [filepath]
    if verbose:
        print(" ".join(cmdline))

    return subprocess.run(cmdline, stdout=subprocess.PIPE).stdout.decode("utf-8")



def lfs_df_to_csv():
    """
    Query the filesystem disk space usage of each MDS/OSD

    Parameters:
        n/a

    Returns:
        List of all active OSTs and their available disk space

    """
    cmdline = ["lfs", "df"]
    raw = subprocess.run(cmdline, stdout=subprocess.PIPE).stdout.decode("utf-8")

    # by splitting the df-output by empty lines we have alternating blocks of MDT/OST information and filesystem summaries
    raw_entries = raw.split("\n\n")

    # collumns for the MDTs/OSTs
    id, blocks, used, available, use_percent, mounted_on, partition, storage_type = ([] for i in range(8))
    # collumns for the overall summary
    summary_name, avg_blocks, avg_Used, avg_Available, avg_Use_percent = ([] for i in range(5))

    i = len(raw_entries)-1
    while i >= 0:
        if i%2==0:
            entries = raw_entries[i].split("\n")
            for entry in entries[1:]:
                attributes = entry.split()
                id.append(attributes[0])
                blocks.append(int(attributes[1]))
                used.append(int(attributes[2]))
                available.append(int(attributes[3]))
                use_percent.append(int(attributes[4][:-1]))
                mounted_on.append(attributes[5])
                partition.append(filesystem_summary[5])
                if "MDT" in attributes[5]:
                    storage_type.append("MDT")
                else:
                    storage_type.append("OST")
        else:
            filesystem_summary = raw_entries[i].split()
            avg_blocks.append(int(filesystem_summary[1]))
            avg_Used.append(int(filesystem_summary[2]))
            avg_Available.append(int(filesystem_summary[3]))
            avg_Use_percent.append(int(filesystem_summary[4][:-1]))
            summary_name.append(filesystem_summary[5])
        i -= 1

    df = pd.DataFrame({"id": id, "blocks": blocks, "used": used, "available": available, "use_percent": use_percent, "mounted_on": mounted_on, "partition": partition, "storage_type": storage_type})
    summary = pd.DataFrame({"summary_name": summary_name, "avg_blocks": avg_blocks, "avg_Used": avg_Used, "avg_Available": avg_Available, "avg_Use_percent": avg_Use_percent})

    return df, summary



def directory_stats(directorypath):

    # set up collumns for the final table
    names = []
    size = []
    links = []
    user_id = []
    group_id = []
    atime = []
    mtime = []
    ctime = []
    device = []

    i = 0
    for path, dirnames, filenames in os.walk(directorypath):
        for name in filenames:
            if not os.path.exists(os.path.join(path, name)):
                continue
            # this is just to measure the elapsed time, as going through millions of files can take hours
            i += 1
            if i%10000==0:
                print(i)

            # collect various file statistics in lists to turn it into a DataFrame
            stats = os.stat(os.path.join(path, name))
            names.append(os.path.join(path, name))
            size.append(stats.st_size)
            device.append(stats.st_dev)
            links.append(stats.st_nlink)
            user_id.append(stats.st_uid)
            group_id.append(stats.st_gid)
            atime.append(stats.st_atime)
            mtime.append(stats.st_mtime)
            ctime.append(stats.st_ctime)

    statistic = pd.DataFrame({"name": names, "size": size, "links": links, "user_id": user_id, "group_id": group_id, "atime": atime, "mtime": mtime, "ctime": ctime, "device": device})
    return statistic



df_csv, summary_csv = lfs_df_to_csv()
df_csv.to_csv("df.csv", index = False)

stats = directory_stats(sys.argv[1])
stats.to_csv("directory_stats.csv", index = False)

files = []
i = 0
for path, dirnames, filenames in os.walk(sys.argv[1]):
    for name in filenames:
        if not os.path.exists(os.path.join(path, name)):
            continue
        i += 1
        files.append(lfs_getstripe(os.path.join(path, name)))
        if i%10000==0:
            print(i)

json_files = json.dumps(files, indent=4)
with open("json_files.json", "w") as outfile:
    outfile.write(json_files)