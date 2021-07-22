"""
Eric Swanson
The purpose of this script is to convert the S3 filepath for one image in the
USGS CoastCam bucket. This usees a test S3 bucket and not the public-facing cmgp-CoastCam bucket.
The old filepath is in the format s3:/cmgp-coastcam/cameras/[station]/products/[long filename].
The new filepath is in the format s3:/cmgp-coastcam/cameras/[station]/[camera]/[year]/[day]/raw/[longfilename].
day is the format ddd_mmm.nn. ddd is 3-digit number describing day in the year.
mmm is 3 letter abbreviation of month. nn is 2 digit number of day of month.
filenames are in the format [unix datetime].[camera in format c#].[file format].jpg
This script splits up the filepath of the old path to be used in the new path. The elements used in the
new path are the [station] and [long filename]. Then it plits up the filename to get elements used in the new path.
[unix datetime] is used to get [year], [day], and [camera]. unix2datetime() converts the unix time in the filename
to a human-readable datetime object and string. Once the new filepath is created, the S3 buckets are accessed using
fsspec and the image is copied from one path to another use the fsspec copy() method.
For this test script, the source filepath is
s3://test-cmgp-bucket/cameras/caco-01/products/1576260000.c2.snap.jpg
The destination filepath is
s3://test-cmgp-bucket/cameras/caco-01/c2/2019/347_Dec.13/raw/1576260000.c2.snap.jpg
The filename is 1576260000.c2.snap.jpg
"""
import numpy as np
import os
import time
import fsspec #will need fs3 package to use s3 in fsspec
import numpy as np
import imageio
import calendar
import datetime
from dateutil import tz


###### FUNCTIONS ######
def unix2datetime(unixnumber):
    """
   Developed from unix2dts by Chris Sherwood. Updates by Eric Swanson.
    Get datetime object and string (in UTC) from unix/epoch time. datetime object is "aware",
    meaning it always references a specific point in time rather than a time relative to local time.
    datetime object will be the same regardless of what timezone the function is run in.
    Input:
        unixnumber - string containing unix time (aka epoch)
    Returns:
        date_time_string, date_time_object in utc
    """

    # images other than "snaps" end in 1, 2,...but these are not part of the time stamp.
    # replace with zero
    ts = int( unixnumber[:-1]+'0')
    date_time_obj =  datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    date_time_str = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')
    print("object timestamp: " + str(date_time_obj))
    return date_time_str, date_time_obj

###### MAIN ######
coastcam_bucket = "s3://test-cmgp-bucket/cameras/"
source_filepath = "s3://test-cmgp-bucket/cameras/caco-01/products/1576260000.c2.snap.jpg" #old filepath with format s3:/cmgp-coastcam/cameras/[station]/products/[filename] 

old_path = source_filepath.replace(coastcam_bucket, '') #removes the coastcam start of filepath from the source url so we can extract necesary info to create new url
old_path_elements = old_path.split("/") #splits up elements between forward slashes in the old filepath into a list

#remove empty space elements from the list
#list will have 3 elements: "[station]", "products", "[image filename]"
for elements in old_path_elements:
	if len(elements) == 0: #if string element is ''
		old_path_elements.remove(elements)

station = old_path_elements[0]
filename = old_path_elements[2]

filename_elements = filename.split(".") #splits up elements of filename into a list
image_unix_time = filename_elements[0]
image_camera = filename_elements[1] 
image_type = filename_elements[2]
image_file_type = filename_elements[3]

image_date_time, date_time_obj = unix2datetime(image_unix_time) #convert unix time to date-time str in the format "yyyy-mm-dd HH:MM:SS"
year = image_date_time[0:4]
month = image_date_time[5:7]
day = image_date_time[8:10]

#day format for new filepath will have to be in format ddd_mmm.nn
#use built-in python function to convert from known variables to new date
#timetuple() method returns tuple with several date and time attributes. tm_yday is the (attribute) day of the year
day_of_year = str(datetime.date(int(year), int(month), int(day)).timetuple().tm_yday)

#can use built-in calendar attribute month_name[month] to get month name from a number. Month cannot have leading zeros
month_word = calendar.month_name[int(month)] #get full month in word form
month_formatted = month_word[0:3] #month in the mmm word form

new_format_day = day_of_year + "_" + month_formatted + "." + day


new_filepath = coastcam_bucket + station + "/" + image_camera + "/" + year + "/" + new_format_day + "/raw/" #file not included
new_path_filename = new_filepath + filename
print("new filepath: " + new_path_filename)

#Use fsspec to copy image from old path to new path
fs = fsspec.filesystem('s3', profile='coastcam')
fs.copy(source_filepath, new_path_filename)

#####destination for test is s3://test-cmgp-bucket/cameras/caco-01/c2/2019/347_Dec.13/raw/1576260000.c2.snap.jpg#####


