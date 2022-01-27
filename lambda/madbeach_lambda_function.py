"""
Author: Eric Swanson - eswanson@contractor.usgs.gov
Lambda function is triggered by image uploaded to the Madeira Beach S3 coastcam bucket at with the prefix ('directory') cameras/madeira_beach/products/. Lambda function will not trigger 
if an image is uploaded to a different prefix in the S3 bucket. This image will be copied to the same S3 bucket at the prefix cameras/[station]/[camera]/[year]/[day]/raw/ .
This function will also copy the file with the new filename to the /products directory and delete the old file.
The day is formatted as [day of year]_mmm.[day of the month].
The year, day, and camera are derived from the filename. 

"""

##### REQUIRED PACKAGES #####
import json
import urllib.parse
import os
import time
import boto3
import calendar
import datetime
from dateutil import tz
import sys

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
    
def unixFromFilename(filename):
    '''
    Given a filename in the format [unix time].[camera number].[image type].jpg, return image unix time
    Input:
        filename (string) - name of the file in the format statewd above
    Output:
        filename_elements[0] (string) - first elements of the split up filename. This is the unix time string.
    '''

    filename_elements = filename.split(".")
    #unix time is first element of the filename
    return filename_elements[0]

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

def getPathElements(filepath):
    '''
    Given a (S3) filepath, return a list of the elements (subfolders, filename) in the filepath
    Inputs:
        filepath (string) - S3 filepath
    Outputs:
        path_elements (list) - list of elements in the file path        
    '''
    path_elements = filepath.split("/")

    #remove empty space elements from the list
    for elements in path_elements:
        if len(elements) == 0: 
            path_elements.remove(elements)
    return path_elements
       

def get_new_keys(old_key):
    '''
    Get the new keys (filepath) for an image in S3. The old key will have the format
    cameras/[station]/products/[long filename]. The new key will have the format
    cameras/[station]/[camera]/[year]/[day]/raw/[filename]
    There will also be a second key generated that renames the file in the old filepath (/products directory) with a new name that matches the format rest of the CoastCam cameras.
    The filename will have the format [unix time].[camera number].[image type].[file extension]
    day is in the format day is the format ddd_mmm.nn. ddd is 3-digit number describing day in the year.
    mmm is 3 letter abbreviation of month. nn is 2 digit number of day of month.
    filenames are in the format [unix datetime].[camera in format c#].[file format].[image format]
    New filepath is created and returned as a string by this function.
    Input:
        old_key - (string) current filepath of image where the image will be copied from.
    Output:
        new_key - (string) new filepath image is copied to.
        new_product_key - (string) filepath with a renamed file for the /products directory
    '''

    old_path_elements = old_key.split("/")

    station = old_path_elements[1]
    filename = old_path_elements[3]

    filename_elements = filename.split(".")

    #check to see if filename is properly formatted
    if len(filename_elements) == 4:
        print('Not copied.')
        return
    else:
        image_unix_time = filename_elements[0]
        image_camera = filename_elements[7]
        image_type = filename_elements[8]
        image_file_type = filename_elements[9]

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

        #reformat camera number
        cam_num = "c" + str(image_camera.split("Camera")[1])

        #reformat filename
        new_filename = image_unix_time + "." + cam_num + "." + image_type + "." + image_file_type
        
        new_key = "cameras/" + station + "/" + cam_num + "/" + year + "/" + new_format_day + "/raw/" + new_filename
        new_product_key = "cameras/madeira_beach/products/" + new_filename
        
    return new_key, new_product_key


###### MAIN ######
print('Loading function')

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

def lambda_handler(event='none', context='none'):
    '''
    This function is executed when the Lambda function is triggered on a new image upload.
    '''

    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    print('bucket:', bucket)
    print('unformatted key:',key)
    
    #get reformatted filepath for image
    new_key, new_product_key = get_new_keys(key)
    print('formatted key:', new_key, '\n')
    print('key for new filename in /products:', new_product_key)
    
    copy_source = {
    'Bucket': bucket,
    'Key': key
    }
    
    bucket_resource = s3_resource.Bucket(bucket)
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        waiter = s3.get_waiter('object_exists')
        waiter.wait(Bucket=bucket, Key=key)
        
        s3.copy(copy_source, bucket, new_key)
        print(f'{new_key} copied')
        s3.copy(copy_source, bucket, new_product_key)
        print(f'{new_product_key} copied')
        
        #delete file with old filename
        s3.delete_object(Bucket=bucket, Key=key)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
        
