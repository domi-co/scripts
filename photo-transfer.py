"""This scripts takes two arguments :
   input : input path where all files in directory and subdirectories will be processed
   output: output path where all files will be copied

The script looks for all files in input path and will copy them in output path
The output path is determined by the EXIF original date and if not found the file creation date
For example if a picture image.jpg with EXIF original date 19.11.2001 is located in
/input/whatever/path/image.jpg
it will be copied to :
/output/2001/11/19/image.jpg

All processed files are stored in a sqlite3 database with original path, copied path, date of copy
Therefore if the scripts is ran twice on the same input path, only new files will be processed

If a file is located twice in different subpathes of input, they should be copied on the same
ouptut path as the output path is calculated from the EXIF orignal date. Therefore, in order to
avoid any loss of data, in this case the copy is renamed by adding a version in parenthesis at the
end of the filename.
Example :
image.jpg becomes image(1).jpg
image(1).jpg becomes image(2).jpg

use :
python3 photo-transfer.py -input /Users/Shared/Photos\ Library.photoslibrary/Masters/ -output /Volumes/photo

Copyright : Dominique Coissard
Date : 02.08.2015"""

from datetime import datetime
import argparse
import exifread
import shutil
import os
import re
import sqlite3

EXIF_DATE_TAG = 'EXIF DateTimeOriginal'
DATE_FMT = '%Y-%m-%d-%H-%M-%S'

parser = argparse.ArgumentParser(description='Process files.')
parser.add_argument('-input', required=True, metavar='input_path', help='input path')
parser.add_argument('-output', required=True, metavar='output_path', help='output path')

args = parser.parse_args()
execution_date = datetime.now()
execution_date_str = execution_date.strftime(DATE_FMT)

LOG_FILE = 'photo-transfer-' + execution_date_str + '.log'
WARNING_FILE = 'photo-transfer-' + execution_date_str + '.warning'
ERROR_FILE = 'photo-transfer-' + execution_date_str + '.error'
DATABASE = 'photo-transfer.db'
SQL_INSERT = 'INSERT INTO photo_transfer (original, copy, timestamp) VALUES (?,?,?)'
SQL_CREATE = 'CREATE TABLE photo_transfer (original VARCHAR(512) PRIMARY KEY, copy VARCHAR(512), timestamp VARCHAR(30))'
SQL_SELECT = 'SELECT * FROM photo_transfer WHERE original = ?'

#
# Log functions
#
def log(filename, level, message):
    """Log a message to the file filename, with sepcific level
    :param full_filename: filename for the log file
    :param level: log level
    :param message: message
    :return: None
    """
    with open(filename, 'at') as log_file:
        print(datetime.now().isoformat(), level, '-', *message, file=log_file)

def info(*message):
    """Log info message
    :param message: message to write on info log file
    :return: None
    """
    log(LOG_FILE, 'INFO', message)

def warning(*message):
    """Log warning messsage
    :param message: message to write on warning log file
    :return: None
    """
    log(LOG_FILE, 'WARN', message)
    log(WARNING_FILE, 'WARN', message)

def error(*message):
    """Log error message
    :param message: message to write on error log file
    :return: None
    """
    log('ERROR', message)
    log(ERROR_FILE, 'ERROR', message)

def should_process(file, connection):
    """Test if a file has already been processed or not
    :param file: file to check
    :param connection: connection to the database
    :return: true if the file must be copied, false otherwise
    """
    return not find_original(connection, file)


#
# Database functions
#
def open_database():
    """Open or crete sqlite3 database
    :return: None
    """
    if not os.path.exists(DATABASE):
        return create_database()
    else:
        return sqlite3.connect(DATABASE)

def create_database():
    """Create database for storing processed files.
    :return: connection to the database
    """
    info('Create DATABASE named', DATABASE)
    connection = sqlite3.connect(DATABASE)
    curs = connection.cursor()
    curs.execute(SQL_CREATE)
    connection.commit()
    return connection

def find_original(connection, original):
    """Search for a file path in the database.
    :param connection: connection to the database
    :param original: file path
    :return: True if found, false otherwise
    """
    curs = connection.cursor()
    curs.execute(SQL_SELECT, (original,))
    result = curs.fetchall()
    return len(result) > 0

def persist_original(connection, original, copy):
    """Store an original file path in the database with the copy path and the current
    timestamp in ISO 8601 format
    :param connection: connection to the database
    :param original: original file path
    :param copy: copy file path
    :return: None
    """
    curs = connection.cursor()
    curs.execute(SQL_INSERT, (original, copy, datetime.now().isoformat()))
    connection.commit()


def original_date(filename):
    """Find original date of a picture using EXIF original date
    if not found, use file creation date
    :param filename: file for which we want the original date
    :return: a tuple containing True/False if the date comes from EXIF, and the date as object
    """
    with open(filename, 'rb') as file_obj:
        tags = exifread.process_file(file_obj, stop_tag=EXIF_DATE_TAG, details=False)
        if EXIF_DATE_TAG in tags.keys():
            try:
                date_object = datetime.strptime(str(tags[EXIF_DATE_TAG]), "%Y:%m:%d %H:%M:%S")
                return True, date_object
            except:
                warning('File', filename, 'has invalid', EXIF_DATE_TAG, '= [',
                        str(tags[EXIF_DATE_TAG]), ']')
        return False, datetime.fromtimestamp(os.path.getmtime(filename))


def get_or_create_path(path, date):
    """Return the path based on path parameter and add year/month/day to it.
    If this path does not exist, it will be created
    :param path: base path
    :param date: date used to compute full path
    :return: full path
    """
    for subpath in (str(date.year), str(date.month), str(date.day)):
        path = os.path.join(path, subpath)
        if not os.path.isdir(path):
            os.mkdir(path)
    return path


def rename_copy(filename):
    """Change copy filename using the following algorithm :
    If filename contains at its end before extension '(num)', increases num + 1
    If not, add (1) at the end of the file (before extension)
    Used when a copy already exists
    :param filename: current filename
    :return: renamed filename
    """
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
        num_version += 1
        file_without_extension = file_without_extension.split('(')[0]
    else:
        num_version = 1

    file_without_extension = file_without_extension + '(' + str(num_version) + ')'
    file_with_extension = file_without_extension + extension
    return os.path.join(path, file_with_extension)


def process_file(filename, short_filename, date, connection):
    """Copy a file to the correct path, changing its name if it already exists
    :param filename: full filename to be copied
    :param short_filename: base filename without extension
    :param date: file original date
    :param connection: connection to database to persist copy
    :return: None
    """
    path = get_or_create_path(args.output, date)
    copy = os.path.join(path, short_filename)
    if os.path.exists(copy):
        copy = rename_copy(copy)
        warning('File', filename, 'already exists in destination, will be renamed to', copy)
    shutil.copy2(filename, copy)
    persist_original(connection, filename, copy)
    info('Copied file ', short_filename, ' to ', path)


if __name__ == "__main__":
    info('### Start parsing path :', args.input)
    start = datetime.now().timestamp()
    counter = 0
    processed_counter = 0
    conn = open_database()
    for root, dirs, files in os.walk(args.input):
        for current_file in files:
            full_filename = os.path.join(root, current_file)
            info('Check file', full_filename)
            counter += 1
            if should_process(full_filename, conn):
                from_exif, file_date = original_date(full_filename)
                info('Select file', full_filename, 'date =', str(file_date), 'EXIF =',
                     str(from_exif))
                process_file(full_filename, current_file, file_date, conn)
                processed_counter += 1
    end = datetime.now().timestamp()
    diff = end-start
    info('### Finished parsing path :', args.input)
    info('### Checked', str(counter), 'files, copied', str(processed_counter), 'files')
    info('### Processed in :', str(diff), 's')
