#retrieve filepath info from old AWS S3 filepath and filename.
#use this info to create new filepath in s3
#what we need for new filepath: station, camera, year, day, filename
#get station, filename from old filepath name
#get year,day, camera from filename.
#will need to create day in new filepath in format ddd_mmm.nn. ddd is 3-digit number describing day in the year. mmm is 3 letter abbreviation of month. nn is 2 digit number of day of month.
#might have to account for leap days?
#filenames are in the format [unix datetime].[camera in format c#].[file format].jpg
#write first for converting one file, then write later to loop through for given s3 folder
#save object data
import numpy as np
import datetime
import os
import fsspec
#will need fs3 package to use s3 in fsspec
import numpy as np
import imageio
import calendar
from dateutil import tz


def unix2dts(unixnumber, timezone='eastern'):
    """
    From Chris Sherwood
    Get local time from unix number
    Input:
        unixnumber - string containing unix time (aka epoch)
    Returns:
        date_time_string, date_time_object in utc
    TODO: not sure why this returns the correct value without specifying that input time zone is eastern
    """
    if timezone.lower() == 'eastern':
        tzone = tz.gettz('America/New_York')
    elif timezone.lower() == 'utc':
        tzone = tz.gettz('UTC')

    # images other than "snaps" end in 1, 2,...but these are not part of the time stamp.
    # replace with zero
    ts = int( unixnumber[:-1]+'0')
    date_time_obj =  datetime.datetime.utcfromtimestamp(ts)
    date_time_str = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')
    return date_time_str, date_time_obj


coastcam_bucket = "s3://test-cmgp-bucket/cameras/"
source_filepath = "s3://test-cmgp-bucket/cameras/caco-01/products/1576260000.c2.snap.jpg" #old filepath with format s3:/cmgp-coastcam/cameras/[station]/products/[filename] 

old_path = source_filepath.replace(coastcam_bucket, '') #removes the coastcam start of filepath from the source url so we can extract necesary info to create new url
old_path_elements = old_path.split("/") #splits up elements between forward slashes in the old filepath into a list

#remove empty space elements from the list
#list will have 3 elements: "[station]", "products", "[image filename]"
for elements in old_path_elements:
	if len(elements) == 0: #if string element is ''
		old_path_elements.remove(element)


station = old_path_elements[0]
filename = old_path_elements[2]

filename_elements = filename.split(".") #splits up elements of filename into a list
image_unix_time = filename_elements[0]
image_camera = filename_elements[1] 
image_type = filename_elements[2]
image_file_type = filename_elements[3]

image_date_time, date_time_obj = unix2dts(image_unix_time) #convert unix time to date-time str in the format "yyyy-mm-dd HH:MM:SS"
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
print(new_path_filename)

fs = fsspec.filesystem('s3', profile='coastcam')

#read image from old filepath
with fs.open(source_filepath) as f_read:
    im = imageio.imread(f_read)

#write to new file path
with fs.open(new_path_filename, 'wb') as f_write:
    imageio.imwrite(f_write,im,format='jpg')

#####destination for test is s3:/cmgp-coastcam/cameras/caco-01/c2/2019/347_Dec.13/raw/1576260000.c2.snap.jpg#####


