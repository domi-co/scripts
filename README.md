# photo-transfer.py
## Description
The script looks for all files in input path and will copy them in output path.  
The output path is determined by the EXIF original date and if not found the file creation date.

For example if a picture image.jpg with EXIF original date 19.11.2001 is located in :

    /input/whatever/path/image.jpg

it will be copied to :

    /output/2001/11/19/image.jpg

A trace of all processed files is kept in a sqlite3 database with original path, copied path, date of copy.  
Therefore if the script is ran twice on the same input path, only new files will be processed.

If a file is located twice in different subpathes of input, they should be copied on the same
ouptut path as the output path is calculated from the EXIF orignal date. Therefore, in order to
avoid any loss of data, in this case the copy is renamed by adding a version in parenthesis at the
end of the filename and the first copy is not overwritten.

Example :
* image.jpg becomes image(1).jpg
* image(1).jpg becomes image(2).jpg

## Usage
This scripts takes two arguments :
* input : input path where all files in directory and subdirectories will be processed
* output: output path where all files will be copied
   
Example : python3 photo-transfer.py -input /Users/Shared/Photos\ Library.photoslibrary/Masters/ -output /Volumes/photo

## Prerequesite
In order to use this script, one needs :
* python 3
* exifread external library :  
    `pip3 install exifread`
