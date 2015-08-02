# use
# python3 photo-transfer.py -input /Users/Shared/Photos\ Library.photoslibrary/Masters/ -output /Volumes/photo

from datetime import datetime
import argparse
import exifread
import shutil
import os
import re
import sqlite3

EXIF_DATE_TAG = 'EXIF DateTimeOriginal'
DATE_FMT = '%Y-%m-%d-%H-%M-%S'
TIMESTAMP = '%d-%m-%Y %H:%M:%S - '
DB_TIMESTAMP_FMT = '%d-%m-%Y %H:%M:%S'

parser = argparse.ArgumentParser(description='Process files.')
parser.add_argument('-input', required=True, metavar='input_path', help='input path')
parser.add_argument('-output', required=True, metavar='output_path', help='output path')

args = parser.parse_args()
execution_date = datetime.now()
execution_date_str = execution_date.strftime(DATE_FMT)

LOG_FILE = 'photo-transfer-' + execution_date_str + '.log'
WARNING_FILE = 'photo-transfer-' + execution_date_str + '.warning'
ERROR_FILE = 'photo-transfer-' + execution_date_str + '.error'
THRESHOLD_FILE = 'threshold.cfg'
DATABASE = 'photos.db'
SQL_INSERT = 'INSERT INTO photos (original, copy, timestamp) VALUES (?,?,?)'
SQL_CREATE = 'CREATE TABLE photos (original VARCHAR(512) PRIMARY KEY, copy VARCHAR(512), timestamp VARCHAR(20))'
SQL_SELECT = 'SELECT * FROM photos WHERE original = ?'


def log(filename, level, message):
    with open(filename, 'at') as log:
        print(datetime.now().strftime(TIMESTAMP) + level + ' - ' + message, file=log)

def info(message):
    log(LOG_FILE, 'INFO', message)

def warning(message):
    log(LOG_FILE, 'WARN', message)
    log(WARNING_FILE, 'WARN', message)

def error(message):
    log('ERROR', message)
    log(ERROR_FILE, 'ERROR', message)


def should_process(file, threshold, conn):
    return not find_original(conn, file)


def print_all_tags(tags):
    for tag in tags.keys():
        print("Key: %s, value %s" % (tag, tags[tag]))


def original_date(file):
    with open(file, 'rb') as f:
        tags = exifread.process_file(f, stop_tag=EXIF_DATE_TAG, details=False)
        if EXIF_DATE_TAG in tags.keys():
            try:
                date_object = datetime.strptime(str(tags[EXIF_DATE_TAG]), "%Y:%m:%d %H:%M:%S")
                return True, date_object
            except:
                warning('File ' + file + ' has invalid ' + EXIF_DATE_TAG + '=[' + str(tags[EXIF_DATE_TAG]) + ']')
        return False, datetime.fromtimestamp(os.path.getmtime(file))


def write_execution_date(date):
    with open(THRESHOLD_FILE, 'wt') as threshold:
        threshold.write(date.strftime(DATE_FMT))


def get_or_create_path(path, date):
    for subpath in (str(date.year), str(date.month), str(date.day)):
        path = os.path.join(path, subpath)
        if not os.path.isdir(path):
            os.mkdir(path)
    return path


def read_execution_date():
    info('Get last processing date...')
    if os.path.exists(THRESHOLD_FILE):
        with open(THRESHOLD_FILE, 'rt') as threshold:
            datestr = threshold.read()
            try:
                last_execution = datetime.strptime(datestr, DATE_FMT)
                info('Found last processing date=' + datestr)
                return last_execution
            except:
                warning('Last execution date not readable, will process all files')
                return datetime(1970, 1, 1)
    else:
        info('No last processing date, will process all files')
        return datetime(1970, 1, 1)


def rename_copy(filename):
    path, basename = os.path.split(filename)

    # find and remove extension
    components = basename.split('.')
    if len(components) > 1:
        file_without_extension = components[-2]
        extension = '.' + components[-1]
    else:
        file_without_extension = components[0]
        extension = ''

    # add version to file
    verspattern = '\((\d+)\)$'
    version = re.search(verspattern, file_without_extension)
    if version and version.group(1).isdigit():
        num_version = int(version.group(1))
        num_version = num_version + 1
        file_without_extension = file_without_extension.split('(')[0]
    else:
        num_version = 1
    
    file_without_extension = file_without_extension + '(' + str(num_version) + ')'
    file_with_extension = file_without_extension + extension
    return os.path.join(path, file_with_extension)


def process_file(full_filename, file, file_date, conn):
    path = get_or_create_path(args.output, file_date)
    copy = os.path.join(path, file)
    if os.path.exists(copy):
        copy = rename_copy(copy)
        warning('File ' + full_filename + " already exists in destination, will be renamed to " + copy)
    shutil.copy2(full_filename, copy)
    persist_original(conn, full_filename, copy)
    info('Copied file ' + file + ' to ' + path)


def open_database():
    if not os.path.exists(DATABASE):
        return create_database()
    else:
        return sqlite3.connect(DATABASE)

def create_database():
    info('Create DATABASE named ' + DATABASE)
    conn = sqlite3.connect(DATABASE)
    curs = conn.cursor()
    curs.execute(SQL_CREATE)
    conn.commit()
    return conn

def find_original(connection, original):
    curs = conn.cursor()
    curs.execute(SQL_SELECT, (original,))
    result = curs.fetchall()
    return len(result) > 0

def persist_original(connection, original, copy):
    curs = conn.cursor()
    curs.execute(SQL_INSERT, (original, copy, datetime.now().strftime(DB_TIMESTAMP_FMT)))
    conn.commit()

if __name__ == "__main__":
    info('### Start parsing path: ' + args.input)
    start = datetime.now().timestamp()
    since = read_execution_date()
    counter = 0
    processed_counter = 0
    conn = open_database()
    for root, dirs, files in os.walk(args.input):
        for file in files:
            full_filename = os.path.join(root, file)
            info('Check file ' + full_filename)
            counter = counter + 1
            if should_process(full_filename, since, conn):
                from_exif, file_date = original_date(full_filename)
                info('Select file ' + full_filename + ' date=' + str(file_date) + ' (EXIF=' + str(from_exif) + ')')
                process_file(full_filename, file, file_date, conn)
                processed_counter = processed_counter + 1
    end = datetime.now().timestamp()
    diff = end-start
    write_execution_date(execution_date)
    info('### Finished parsing path: ' + args.input)
    info('### Checked ' + str(counter) + ' files, copied ' + str(processed_counter) + ' files')
    info('### Processed in : ' + str(diff) + 's')