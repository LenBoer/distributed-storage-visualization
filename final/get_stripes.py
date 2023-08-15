import os
import subprocess
import sys
import json
import pandas as pd
import re



def lfs_getstripe(filepath):
    """
    Query lfs stripe information for filepath.

    Args:
        filepath (str): path to display information

    Returns:
        list of dictionaries

    """
    # send the getstripe command and save the raw information for further processing
    assert os.path.exists(filepath)
    cmdline = ["lfs", "getstripe", filepath]
    raw = subprocess.run(cmdline, stdout=subprocess.PIPE).stdout.decode("utf-8")

    entries = []

    # several of these blocks only exist in Progressive File Layouts, as such we need to handle those differently
    blocks = raw.split("\n\n")
    blocks.pop()

    #PFL
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

            # sanity check for empty blocks, if a file is not stored anywhere
            reading_osts = False

            entry = {}
            # afterwards all blocks contain first information about this block as key-value pairs:
            for line in lines:
                attributes = line.split()
                if len(attributes) == 2:
                    attributes = line.split()
                    key = attributes[0][:-1]
                    value = attributes[1]
                    entry[key] = value
            # and then the actual layout information (which OSTs are used) 
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

    #NoPFL
    else:
        # Files withoput any PFL just have one block, but still have a first part of general information as key-value pairs, and then the layout information 
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
        # OST/MDT information
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

        # summary of previous information
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
    """
    Gather the "stat()" information for all files in a directory and subdirectories in a .csv table

    Parameters:
        filepath (str): path to gather statistics for

    Returns:
        pandas DataFrame of all files and their statistics 

    """

    # set up collumns for the table
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
    # find files in directory and subdirectories
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



def get_IO_stripes(input_names, output_names):
    """
    Gather information for two groups of files

    Parameters:
        input_names (list):
        output_names (list):

    Returns:
        two pandas DataFrame for both lists of files
        one csv file for additional information for all files (in this case just size, to measure overlap between both groups for each OST)


    """

    inputs = input_names.split(",")
    outputs = output_names.split(",")
    input_data = []
    output_data = []
    names = []
    sizes = []

    # collect all stripe information on both the input files and the output files
    for file in inputs:
        input_data.append(lfs_getstripe(file))
        names.append(file)
        sizes.append(os.stat(file).st_size)

    for file in outputs:
        output_data.append(lfs_getstripe(file))
        names.append(file)
        sizes.append(os.stat(file).st_size)

    stats_df = pd.DataFrame({"name": names, "size": sizes})

    return input_data, output_data, stats_df



#**************************************************************************---Examplary Use---**************************************************************************

# collect striping data for a list of input and output files, as well as their size
ins, outs, stats_df  = get_IO_stripes(sys.argv[1], sys.argv[2])

# finally write it to JSON-files
input_files = json.dumps(ins, indent=4)
with open("inputs.json", "w") as outfile:
    outfile.write(input_files)

output_files = json.dumps(outs, indent=4)
with open("outputs.json", "w") as outfile:
    outfile.write(output_files)

stats_df.to_csv("stats_df.csv", index = False)
