"""
Spooloff Oracle data file parser to csv file
"""

from optparse import OptionParser
import fnmatch
import logging
import os
import re


def gen_find(filepattern, top):
    for path, dirlist, filelist in os.walk(top):
        for name in fnmatch.filter(filelist, filepattern):
            yield os.path.join(path, name)


def gen_open(filenames):
    for name in filenames:
        yield open(name)


def gen_cat(sources):
    start = False
    new_file = ""
    for s in sources:
        name = str(s).split(" ")[1].split("name=")[1]
        if new_file != name:
            start = False
            new_file = name
        for line in s:
            if line.startswith("\n"):
                start = True
            if start:
                yield line, name


def get_header(line):
    text = ""
    for idx in range(len(line) - 2):
        if idx > 0 and not line[idx].startswith("-"):
            elem = re.sub(r"\s+", " ", line[idx]).strip(" ")
            if not elem in text:
                text = text + " " + elem
                text = re.sub(r"\s+", " ", text).strip(" ")
                text = re.sub(" ", ",", text)
            header = text
            header = header.split(" ")
    return header


def find(pat, line):
    match = re.findall(pat, line)
    if match:
        return match
    else:
        return False


def get_tags(line):
    start = False
    counter1 = 0
    counter2 = 0
    new_file = ""
    header = []
    res = []
    ret = {}
    counter3 = counter4 = cs = 0
    lst = []
    for elem in line:
        if new_file != elem[1].upper().strip("'./"):
            new_file = elem[1].upper().strip("'./")
            counter1 = 0
            start = False
            header = []
            res = []
            lst = []
        lst.append(elem[0])
        if elem[0].startswith("-"):
            start = False
            counter2 = counter1
            counter3 += 1
        elif elem[0] and counter2 and counter1 > counter2:
            counter2 = counter1
            counter4 += 1
        if counter4 > counter3:
            start = True
            counter2 = counter3 = counter4 = 0
        if start:
            if not header:
                header = get_header(lst)
            else:
                begin = re.search("^\s+\n", lst[counter1])
                if begin:
                    el = lst[counter1 - 2].strip("\n") + "      " + lst[counter1 - 1]
                    el = re.sub("\s+$", "", re.sub("^\s+", "", el))
                    el = re.sub(" {1,4}", " ", el)
                    el = re.sub(",|:", ".", el)
                    list_elem = re.split("\s+\s", el)
                    cs += 1
                    res.append(list_elem)
                    ret[str(new_file)] = {str(header): res}
        counter1 += 1
    return ret



def writer_to_csv(dt):
    counter = 0
    for k, v in dt.items():
        counter += 1
        v = re.sub("],|:", "\n", str(v))
        v = re.sub("[\[\]{}\"]", "", str(v))
        v = re.sub("', ", ",", str(v))
        v = re.sub("'", "", str(v))
        k = re.sub("]", "\n", str(k))
        k = re.sub("\[|", "", str(k))

        if not os.path.exists("./out"):
            os.mkdir("./out/", mode=0o777)
        with open(file="./out/" + k.split("/")[1] + "_out_" + ".csv", encoding="utf-8", mode="w+") as fn:
            logging.info("Starting saving files in dir ./out/%s" %  k.split("/")[1] + "_out_" + ".csv")
            fn.writelines(v)



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
        op = OptionParser()
        op.add_option("-d", "--dir", action="store", type=str, help="Dir with spool offs Oracle data files. Default=./", default="./")
        op.add_option("-m", "--filemask", action="store", type=str, help="Files mask like *.txt. Default=*.txt", default="*.txt")
        op.add_option("-l", "--log", action="store", type=str, help="Log filename.", default="app_spool_parser.log")
        op.description("Example python(version) oraclespooloffparser.py -d <directr> ")
        (opts, args) = op.parse_args()
        logging.basicConfig(filename=opts.log, level=logging.INFO,
                            format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
        if opts.dir:
            try:
                logging.info("Starting processing files at in dir %s" % opts.dir)
                filenames = gen_find(str(opts.filemask), opts.dir)
                txtfiles = gen_open(filenames)
                txtline = gen_cat(txtfiles)
                tags = get_tags(txtline)
                writer_to_csv(tags)
            except Exception:
                logging.exception("No files in %s", opts.dir)


