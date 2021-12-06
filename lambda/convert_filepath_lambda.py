"""
Eric Swanson
The purpose of this script is to convert the S3 filepath for one image in the
USGS CoastCam bucket. This usees a test S3 bucket and not the public-facing cmgp-CoastCam bucket.
The old filepath is in the format s3:/cmgp-coastcam/cameras/[station]/products/[long filename].
The new filepath is in the format s3:/cmgp-coastcam/cameras/[station]/[camera]/[year]/[day]/raw/[longfilename].
day is the format ddd_mmm.nn. ddd is 3-digit number describing day in the year.
mmm is 3 letter abbreviation of month. nn is 2 digit number of day of month.
filenames are in the format [unix datetime].[camera in format c#].[file format].[image format]
This script splits up the filepath of the old path to be used in the new path. The elements used in the
new path are the [station] and [long filename]. Then it plits up the filename to get elements used in the new path.
[unix datetime] is used to get [year], [day], and [camera]. unix2datetime() converts the unix time in the filename
to a human-readable datetime object and string. Once the new filepath is created, the S3 buckets are accessed using
fsspec and the image is copied from one path to another use the fsspec copy() method.
This is done using the function copy_s3_image().
For this test script, the source filepath is
s3://test-cmgp-bucket/cameras/caco-01/products/1576260000.c2.snap.jpg
The destination filepath is
s3://test-cmgp-bucket/cameras/caco-01/c2/2019/347_Dec.13/raw/1576260000.c2.snap.jpg
The filename is 1576260000.c2.snap.jpg
"""

##### REQUIRED PACKAGES #####
import json
import urllib.parse
import os
import time
import boto3
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

def get_new_key(old_key):
    '''
    Get the new key (filepath) for an image in S3. The old key will have the format
    cameras/[station]/products/[long filename]. The new key will have the format
    cameras/[station]/[camera]/[year]/[day]/raw/[filename]
    day is in the format day is the format ddd_mmm.nn. ddd is 3-digit number describing day in the year.
    mmm is 3 letter abbreviation of month. nn is 2 digit number of day of month.
    filenames are in the format [unix datetime].[camera in format c#].[file format].[image format]
    New filepath is created and returned as a string by this function.
    Input:
        old_key - (string) current filepath of image where the image will be copied from.
    Output:
        new_key - (string) new filepath image is copied to.
    '''

    old_path_elements = old_key.split("/")

    station = old_path_elements[1]
    filename = old_path_elements[3]

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
        
        new_key = "cameras/" + station + "/" + image_camera + "/" + year + "/" + new_format_day + "/raw/" + filename
    return new_key
                    
###### MAIN ######
def lambda_handler(event, context):
    '''
    This function is executed when the Lambda function is triggered on a new image upload.
    '''
    
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print('object key:',key)
    
    #get reformatted filepath for image
    new_key = get_new_key(key)
    print(new_key)
    copy_source = {
    'Bucket': bucket,
    'Key': key
    }
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        waiter = s3.get_waiter('object_exists')
        waiter.wait(Bucket=bucket, Key=key)
        s3.copy(copy_source, bucket, new_key)
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e


