"""
Eric Swanson
The purpose of this script is to convert the S3 filepath for all images in a folder in a
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
fsspec and the image is copied from one path to another use the fsspec copy() method. This is done using the function
copy_s3_image(). Only common image type files will be copied. Images are copied using multithreading
in the concurrent.futures module.
write2csv() is used to write the source and destination filepath to a csv file.
"""
##### REQUIERD PACKAGES #####
import os
import time
#will need fs3 package to use s3 in fsspec
import fsspec 
import imageio
import calendar
import datetime
import csv
import concurrent.futures

##### FUNCTIONS #####
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
    time_stamp = int( unixnumber[:-1]+'0')
    date_time_obj =  datetime.datetime.fromtimestamp(time_stamp, tz=datetime.timezone.utc)
    date_time_str = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')
    return date_time_str, date_time_obj

def check_image(file):
    """
    Check if the file is an image (of the proper type)
    Input:
        file - (string) filepath of the file to be checked
    Output:
        isImage - (bool) variable saying whether or not file is an image
    """
    
    common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']
    
    isImage = False
    for image_type in common_image_list:
        if file.endswith(image_type):
            isImage = True
    return isImage
        

def copy_s3_image(source_filepath):
    """
    Copy an image file from its old filepath in the S3 bucket with the format
    s3://[bucket]/cameras/[station]/products/[long filename]. to a new filepath with the format
    s3://[bucket]/cameras/[station]/[camera]/[year]/[day]/raw/[filename]
    day is in the format day is the format ddd_mmm.nn. ddd is 3-digit number describing day in the year.
    mmm is 3 letter abbreviation of month. nn is 2 digit number of day of month.
    filenames are in the format [unix datetime].[camera in format c#].[file format].[image format]
    New filepath is created and returned as a string by this function.
    Input:
        source_filepath - (string) current filepath of image where the image will be copied from.
    Output:
        destination_filepath - (string) new filepath image is copied to.
    """
    
    isImage = check_image(source_filepath)

    if isImage == True:
        source_filepath = "s3://" + source_filepath
        old_path_elements = source_filepath.split("/")

        #remove empty space elements from the list
        #list will have 5 elements: "[bucket]", "cameras", "[station]", "products", "[image filename]"
        for elements in old_path_elements:
            if len(elements) == 0: 
                old_path_elements.remove(elements)

        bucket = old_path_elements[1]
        station = old_path_elements[3]
        filename = old_path_elements[5]

        filename_elements = filename.split(".")
        #check to see if filename is properly formatted
        if len(filename_elements) != 4:
            return 'Not properly formatted. Not copied.'
        else:
            image_unix_time = filename_elements[0]
            image_camera = filename_elements[1] 
            image_type = filename_elements[2]
            image_file_type = filename_elements[3]

            #convert unix time to date-time str in the format "yyyy-mm-dd HH:MM:SS"
            image_date_time, date_time_obj = unix2datetime(image_unix_time) 
            year = image_date_time[0:4]
            month = image_date_time[5:7]
            day = image_date_time[8:10]
            
            #day format for new filepath will have to be in format ddd_mmm.nn
            #timetuple() method returns tuple with several date and time attributes. tm_yday is the (attribute) day of the year
            day_of_year = str(datetime.date(int(year), int(month), int(day)).timetuple().tm_yday)

            #can use built-in calendar attribute month_name[month] to get month name from a number. Month cannot have leading zeros
            month_word = calendar.month_name[int(month)]
            #month in the mmm word form
            month_formatted = month_word[0:3] 

            new_format_day = day_of_year + "_" + month_formatted + "." + day
            
            new_filepath = "s3:/" + "/" + bucket + "/cameras/" + station + "/" + image_camera + "/" + year + "/" + new_format_day + "/raw/" #file not included
            destination_filepath = new_filepath + filename

            #Use fsspec to copy image from old path to new path
            file_system = fsspec.filesystem('s3', profile='coastcam')
            file_system.copy(source_filepath, destination_filepath)
            return destination_filepath
    #if not image, return blank string. Will be used to determine if file copy needs to be logged in csv
    else:
        return 'Not an image. Not copied.'


def write2csv(csv_list, csv_path):
    """
    Write data pertaining to the copied image files to a csv speified by the user.
    Input:
        csv_list - list (of lists) containing data to be written to csv. Each list includes source filepath & destination filepath
        csv_path - desried location of generated csv file
    Return:
        None. However, csv file will appear in filepath the user specified.
    """

    #header
    fieldnames = ['source filepath', 'destination filepath'] 

    now = datetime.datetime.now()
    now_string = now.strftime("%d-%m-%Y %H_%M_%S")
    csv_name = csv_path + 'image copy log ' + now_string + '.csv'

    with open(csv_name, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(csv_list)
    return 
     

##### MAIN #####
print("start:", datetime.datetime.now())
#source folder filepath with format s3:/cmgp-coastcam/cameras/[station]/products/[filename]
source_folder = "s3://cmgp-coastcam/cameras/rincon/products/"  

#station caco-01 for testing
file_system = fsspec.filesystem('s3', profile='coastcam')
image_list = file_system.glob(source_folder+'/*')

common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

#used to track copied images in a csv
csv_path = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam/s3_filepaths/csv/"
csv_list = []

#ProcessPoolExecutor is used for multithreading (allows multiple instances of function to be run at once)
#result contains filepaths that were copied (in order that they exist in image_list)
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = executor.map(copy_s3_image, image_list)

    #iterate over outputted destination filepaths. Need to iterate  i because results is a generator, not a list.
    #create csv entry pairs from source and destination filepaths. This includes non-image files.
    i = 0
    for dest_filepath in results:
        source_filepath = "s3://" + image_list[i]
        csv_entry = [source_filepath, dest_filepath]
        csv_list.append(csv_entry)
        i = i + 1
        
now = datetime.datetime.now()
now_string = now.strftime("%d-%m-%Y %H_%M_%S")
csv_name = 'image copy log ' + now_string + '.csv'
write2csv(csv_list, csv_path)
print("end:", datetime.datetime.now())




