from optparse import OptionParser
import csv
import fnmatch
import logging
import os
from datetime import datetime

FIELD_NAME = 'BND'
COMPANY_CODE = 'BN'
SOURCE_SYSTEM = 'ODS'
OUTPUT_FILENAME = str(datetime.date(datetime.now())) + '_merged.csv'


def gen_find_files(filepattern, top, file):
    if top:
        for path, _, filelist in os.walk(top):
            logging.info("Files {} find in dir {}.".format(filelist, path))
            break
        for name in fnmatch.filter(filelist, filepattern):
            yield os.path.join(path, name)
    else:
        yield file


def gen_open_files(filename):
    f = open(filename)
    logging.info("File {} open for processing".format(filename))
    return f


def sorting_file(file):
    row = []
    rows_sort = {}
    flag = False
    logging.info("Starting sorting opened file")
    name = file.name.split("/")[-1].split(".")[0]
    f_csv = csv.DictReader(file)
    for r in f_csv:
        if not 'WELL' in r['OBJECT_TYPE'] and 'METERING_ID' in r.keys() and r['METERING_ID']:
            r['OBJECT_TYPE'] = int(r['OBJECT_TYPE'])
            r['METERING_ID'] = int(r['METERING_ID'])
            row.append(r)
            flag = True
        elif not 'WELL' in r['OBJECT_TYPE'] and not 'METERING_ID' in r.keys():
            r['OBJECT_TYPE'] = int(r['OBJECT_TYPE'])
            row.append(r)
            flag = False
    if flag:
        rows_sort[str(name)] = sorted(row, key=lambda r: (r['OBJECT_TYPE'], r['METERING_ID']))
        row.clear()
        return rows_sort, name
    rows_sort[str(name)] = sorted(row, key=lambda r: (r['OBJECT_TYPE']))
    row.clear()
    return rows_sort, name


def merge_data(predix_data, oracle_data):
    row = []
    logging.info("Starting merging data.")
    for x in oracle_data[0].values():
        z = x.copy()
        for data in z:
            if 'METERING_ID' in data.keys():
                for y in predix_data[0].values():
                    for datap in y:
                        if data['OBJECT_TYPE'] == datap['OBJECT_TYPE'] and data['METERING_ID'] == datap['METERING_ID']:
                            data.update(datap)
                row.append(data)
            else:
                for y in predix_data[0].values():
                    for datap in y:
                        if data['OBJECT_TYPE'] == datap['OBJECT_TYPE']:
                            data.update(datap)
                row.append(data)
        return row, oracle_data[1][:3]


def reformat_dict(fdict, field_name):
    fd = []
    logging.info("Starting reformat data.")
    for num, item in enumerate(fdict[0]):
        if 'PREDIX_CODE' in item:
            predix_tag_name = field_name + ".S" + str(item['OBJECT_ID']).strip() + '.' + item['PREDIX_CODE'] + '.' + \
                              item['PREDIX_TAG']
            original_tag_name = item['PATH'] + '>' + item['OBJECT_DESCRIPTION'] + ' | ' + item['LOCAL_DESCRIPTION']
            fd.append({'predix_tag_name': predix_tag_name, 'original_tag_name': original_tag_name})
    rows_sort = sorted(fd, key=lambda r: (r['predix_tag_name']))
    return rows_sort, fdict[1]


def get_deduplicated(data_dict, company_code, source_system):
    logging.info("Starting deduplicate string lines.")
    vyvod_list = ['predix_tag_name,original_tag_name,company_code,source_system\n']
    maxl = len(data_dict[0][0]['original_tag_name'])
    predix_tag_name = data_dict[0][0]['predix_tag_name']
    original_tag_name = data_dict[0][0]['original_tag_name']
    ddl = len(data_dict[0])
    for idx in range(1, ddl):
        current_predix_tag_name = data_dict[0][idx]['predix_tag_name']
        prev_predix_tag_name = data_dict[0][idx - 1]['predix_tag_name']
        current_original_tag_name = len(data_dict[0][idx]['original_tag_name'])
        if current_predix_tag_name == prev_predix_tag_name and idx < ddl:
            if current_original_tag_name > maxl:
                maxl = current_original_tag_name
                predix_tag_name = data_dict[0][idx]['predix_tag_name']
                original_tag_name = data_dict[0][idx]['original_tag_name']
        else:
            vyvod_list.append("\"" + predix_tag_name + "\"" + "," +
                              "\"" + original_tag_name + "\"" + "," +
                              "\"" + company_code + "\"" + "," +
                              "\"" + source_system + "\"" + "\n")
            maxl = current_original_tag_name
            predix_tag_name = current_predix_tag_name
            original_tag_name = data_dict[0][idx]['original_tag_name']
    vyvod_list.append("\"" + predix_tag_name + "\"" + "," +
                      "\"" + original_tag_name + "\"" + "," +
                      "\"" + company_code + "\"" + "," +
                      "\"" + source_system + "\"" + "\n")
    res = vyvod_list
    return res, data_dict[1]


def save_to_dir(input, num, outputname):
    output = "".join(input[0])
    if not os.path.exists("merged"):
        logging.info("Creating dir ./merged")
        os.mkdir("merged/", mode=0o777)
    with open(file="./merged/" + str(num) + "_" + str(input[1]) + "_" + str(outputname),
              encoding="utf-8", mode="w+") as fn:
        logging.info("Starting saving files in dir ./merged/" + str(outputname))
        return fn.writelines(output)


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-p", "--predixmeters", action="store", type=str, help="File data from predix_meters in csv",
                        default="./predix_meters2.csv".upper())
    op.add_option("-o", "--oraclefile", action="store", type=str, help="File from with oracle data")
    op.add_option("-d", "--dir", action="store", type=str, help="Directory with files for merge (without predix file).")
    op.add_option("-f", "--fieldname", action="store", type=str, help="Input field name for output csv file. "
                                                            "Default is " + FIELD_NAME,
                        default=FIELD_NAME)
    op.add_option("-c", "--companycode", action="store", type=str, help="Input company code for output csv file. "
                                                              "Default is " + COMPANY_CODE,
                        default=COMPANY_CODE)
    op.add_option("-s", "--sourcesystem", action="store", type=str, help="Input source system  for output csv file. "
                                                               "Default is " + SOURCE_SYSTEM,
                        default=SOURCE_SYSTEM)
    op.add_option("-n", "--outputname", action="store", type=str, help="Output filename. "
                                                              "Default is " + OUTPUT_FILENAME,
                        default=OUTPUT_FILENAME)
    op.add_option("-l", "--log", action="store", type=str, help="Log filename.", default="app_merge.log")
    (opts, args) = op.parse_args()
    predix = opts.predixmeters
    logs = opts.log
    ora_data = opts.oraclefile
    ora_dir = opts.dir
    logging.basicConfig(filename=logs, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    try:
        with open(predix, encoding='utf-8') as file:
            predix_sort = sorting_file(file)
        for num, file in enumerate(gen_find_files('*.csv', ora_dir, ora_data)):
            ff = gen_open_files(file)
            oracle_sort = sorting_file(ff)
            merged_data = merge_data(predix_sort, oracle_sort)
            reform_data = reformat_dict(merged_data, opts.fieldname)
            input = get_deduplicated(reform_data, opts.companycode, opts.sourcesystem)
            save_to_dir(input, num, opts.outputname)
    except Exception as e:
        logging.exception(
            "Error occurred with {}. Maybe the file '{}' not exist or dir '{}' is empty".format(e, ora_data, ora_dir))
