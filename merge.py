import shutil

import pandas as pd
import argparse
import os
import fnmatch
import logging
from operator import itemgetter


def gen_find(filepat, top):
    for path, dirlist, filelist in os.walk(top):
        for name in fnmatch.filter(filelist, filepat):
            yield os.path.join(path, name)


def gen_open(filenames):
    for name in filenames:
        yield open(name)


def gen_cat(sources):
    new_file = ""
    for s in sources:
        name = str(s).split(" ")[1].split("name=")[1]
        if new_file != name:
            new_file = name
        for line in s:
            yield line, name


def get_lines(line):
    new_file = ""
    dd ={}
    counter = 0
    for l in line:
        counter += 1
        name = l[1]
        if new_file != name:
            header = l[0].split(",")
            if new_file:
                res = dd
                yield res, new_file
            new_file = name
        else:
            if len(header) == 4:
                nline = l[0].split(",")
                dd[counter] = {
                    header[0]: nline[0],
                    header[1]: nline[1],
                    header[2]: nline[2],
                    header[3].split("\n")[0]: nline[3]
                }
            else:
                dd[counter] = {
                    header[0]: nline[0],
                    header[1]: nline[1],
                    header[2].split("\n")[0]: nline[2],
                }



def get_file_parse(item_filed):
    for item in item_file:
        counter += 1
        for foo in sorted(item[0], key=itemgetter("PATH")):
            print(foo)


def getdata_to_df(fi, predix):
    prv = spl = []
    dh = {}
    flag_false = False
    predix_meters = pd.read_csv(predix)
    index = list(range(73, 1000))
    for file in fi:
        field = pd.read_csv(file.name).sort_values(by=["OBJECT_ID", "OBJECT_TYPE"])
        length_header = len(field.keys())
        if length_header == 4:
            res = pd.merge(predix_meters, field, on=['object_type'.upper(), 'metering_id'.upper()], how='outer')
        elif length_header == 3:
            res = pd.merge(predix_meters, field, on=['object_type'.upper()], how='outer')
        for idx in res.index:
            if type(res.loc[idx, 'PATH']) == str:
                spl = res.loc[idx, 'PATH'].split(">")
                if len(spl) > 2:
                    res.loc[idx, 'OBJECT_ID'] = str(int(res.loc[idx, 'OBJECT_ID']))
                    res.loc[idx, 'predix_tag_name'] = file.name.split("/")[-1].split("-")[0] + "." + \
                                res.loc[idx, 'OBJECT_ID'] + "." + res.loc[idx, 'PREDIX_CODE'] + "." + res.loc[idx, 'PREDIX_TAG']
                    res.loc[idx, 'original_tag_name'] = res.loc[idx, 'PATH'] + ">" + res.loc[idx, 'OBJECT_DESCRIPTION'] + \
                                " | " + res.loc[idx, 'LOCAL_DESCRIPTION']
                prev = res.loc[idx, 'PATH'].split(">")
        ret = res[['predix_tag_name', 'original_tag_name']].reset_index(drop=True).dropna()

        yield ret


def get_deduplicated(data):
    vyvod_list = ['predix_tag_name,original_tag_name,company_code,source_system\n']
    counter = 0
    it1_vyvod = it0_vyvod = ""
    pr = ""

    for item in data:
        item.sort_values(by="predix_tag_name")
        for it in item.values:
            sp = it[0]
            spl = len(it[1])
            if sp != pr:
                counter += 1
                company_code = it0_vyvod.split(".")[0]
                if it0_vyvod and it1_vyvod:
                    vyvod_list.append("\"" + it0_vyvod + "\"" + "," + "\"" + it1_vyvod + "\"" + "," + "\""
                                      + company_code + "\"" + "," + "\"" + "ODS" + "\"" + "\n")
                max = spl
                it1_vyvod = it[1]
                it0_vyvod = it[0]
            else:
                if spl > max:
                    max = spl
                    it1_vyvod = it[1]
                    it0_vyvod = it[0]
            pr = it[0]
    res = vyvod_list
    return res


def save_to_dir(input):
    output = [*input]
    if os.path.exists("./merged"):
        shutil.rmtree(path="./merged", ignore_errors=True)
    os.mkdir("./merged/", mode=0o777)
    with open(file="./merged/" + "file_merged" + ".csv", encoding="utf-8", mode="w+") as fn:
            logging.info("Starting saving files in dir ./merged/file_merged" + ".csv")
            for v in output:
                fn.writelines(v)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--predixmeters", type=str, help="Filename data from predix_meters in csv", default="./predix_meters2.csv".upper())
    parser.add_argument("-l", "--log", action="store", type=str, help="Log filename.", default="app_merge.log")
    logging.info("Starting merging files in dir ./out/")
    args = parser.parse_args()
    predix = args.predixmeters
    logs = args.log
    filenames = gen_find("*.csv", "./out/")
    txtfiles = gen_open(filenames)
    line = gen_cat(txtfiles)
    lines = get_lines(line)
    get_file_parse(lines)
    print('End')
    # try:
    #     res = getdata_to_df(txtfiles, predix)
    #     sav = get_deduplicated(res)
    #     save_to_dir(sav)
    # except Exception:
    #     logging.exception("No files in ./out")