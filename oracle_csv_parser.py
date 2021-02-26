#!/usr/bin/env python3
"""
Spooloff Oracle data file parser to csv file
"""

from optparse import OptionParser
import fnmatch
import logging
import os
import re


def gen_find(filepattern, top):
    for path, _, filelist in os.walk(top):
        break
    for name in fnmatch.filter(filelist, filepattern):
        yield os.path.join(path, name)


def gen_open_files(filenames):
    for name in filenames:
        f = open(name)
        logging.info("Open file %s or process" % name)
        yield f
        f.close()


def gen_cat(sources):
    logging.info("Starting processing the file")
    for s in sources:
        name = str(s.name)
        for line in s:
            yield line, name


def get_mapped_value(iterator):
    for i in range(len(iterator)):
        if not iterator[0].startswith("\n") and not iterator[0].startswith("-"):
            it = re.sub(r",\s+", ",", iterator[0]).strip(" ")
            return {iterator[1]: it}


def cur_val_check(cv, lv):
    if not cv.split(",")[0]:
        fv = len(cv.split(","))
        if not lv:
            lv = cv
        lv = re.sub(r"\n", "", lv)
        res = lv + " " + cv.split(",")[-1]
        return True, res
    if len(cv.split(",")) <= 1:
        return False, ""
    return False, cv


def get_line_for_parse(line):
    last_value = ""
    ret = {}
    strline = map(get_mapped_value, line)
    for item in strline:
        if isinstance(item, dict):
            for k, v in item.items():
                fname = k
                current_value = v
                flag, rt = cur_val_check(current_value, last_value)
                last_value = v
                if flag:
                    ret.setdefault(fname, []).pop(-1)
                ret.setdefault(fname, []).append(rt)
    return ret


def writer_to_csv(dt):
    counter = 0
    for k, v in dt.items():
        counter += 1
        k = re.sub(r"[\'./]", "", str(k))
        k = re.sub(r"\[|", "", str(k))
        if not os.path.exists("./out"):
            logging.info("Creating dir ./out/")
            os.mkdir("./out/")
        with open(file="./out/" + k + "_out_" + ".csv", encoding="utf-8", mode="w+") as fn:
            logging.info("Starting saving files to path ./out/%s" % k + "_out_" + ".csv")
            fn.writelines(v)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-d", "--dir", action="store", type=str, help="Dir with spool offs Oracle data files. Default=./",
                  default="./")
    op.add_option("-m", "--filemask", action="store", type=str, help="Files mask like *.txt. Default=*.txt",
                  default="*.txt")
    op.add_option("-l", "--log", action="store", type=str, help="Log filename.", default="app_spool_parser.log")
    # op.description("Example python(version) oraclespooloffparser.py -d <directr> ")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.dir:
        try:
            logging.info("Starting processing files for dir %s" % opts.dir)
            filenames = gen_find(opts.filemask, opts.dir)
            txtfiles = gen_open_files(filenames)
            txtline = gen_cat(txtfiles)
            line = get_line_for_parse(txtline)
            writer_to_csv(line)
        except Exception as e:
            logging.exception("No files in %s", opts.dir)
