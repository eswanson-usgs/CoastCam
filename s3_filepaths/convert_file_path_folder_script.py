#Eric Swanson
#Expands on convert_file_path_single_script.py to iterate over an entire folder to copy all the
#images. Also adds handling for non-standard image files and produces correct number of files in a csv.
import numpy as np
import os
import time
import fsspec #will need fs3 package to use s3 in fsspec
import numpy as np
import imageio
import calendar
import datetime
import csv

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
    return date_time_str, date_time_obj

def write2csv(csv_list, csv_path):
    """
    Write data pertaining to the copied image files to a csv speified by the user.
    Input:
        csv_list - list (of lists) containing data to be written to csv. Each list includes source filepath & destination filepath
        csv_path - desried location of generated csv file
    Return:
        None. However, csv file will appear in filepath the user specified.
    """
    
    fieldnames = ['source filepath', 'destination filepath'] #header

    #datetime info for naming csv
    now = datetime.datetime.now()
    now_string = now.strftime("%d-%m-%Y %H_%M_%S")
    csv_name = csv_path + 'image copy log ' + now_string + '.csv'

    #write to file
    with open(csv_name, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(csv_list)
    return
        
    
station = 'caco-01' #will be passed in as variable for func version of script

coastcam_bucket = "s3://test-cmgp-bucket/cameras/"
source_folder = "s3://test-cmgp-bucket/cameras/"+station+"/products/" #source folder filepath with format s3:/cmgp-coastcam/cameras/[station]/products/[filename] 

#access list of images in source folder using fsspec
fs = fsspec.filesystem('s3', profile='coastcam')
image_list = fs.glob(source_folder+'/*') #caco-01 for testing

#list of common image types
common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

#used to track copied images in a csv
csv_path = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam/s3_filepaths/csv/"
csv_list = []

for image in image_list:
    for image_type in common_image_list: #loop through list of possible image types
        good_ending = False #variable to check if file ends with image type in common_image_list
        if image.endswith(image_type):
            good_ending = True
            break
    if image.endswith('.txt') or good_ending == False:
        continue #This is not an image. Skip file
    else: #this is an image
        filename = image.replace('test-cmgp-bucket/cameras/caco-01/products/', '') #removes but the filename so we can extract necesary info to create new url
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

        source_filepath = source_folder + filename
        dest_filepath = coastcam_bucket + station + "/" + image_camera + "/" + year + "/" + new_format_day + "/raw/" + filename #file not included

        #Use fsspec to copy image from old path to new path
        fs.copy(source_filepath, dest_filepath)

        csv_entry = [source_filepath, dest_filepath]
        csv_list.append(csv_entry)

#create csv file
now = datetime.datetime.now()
now_string = now.strftime("%d-%m-%Y %H_%M_%S")
csv_name = 'image copy log ' + now_string + '.csv'
write2csv(csv_list, csv_path)






