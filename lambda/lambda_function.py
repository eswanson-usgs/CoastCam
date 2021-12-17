"""
Author: Eric Swanson - eswanson@contractor.usgs.gov
Lambda functions is triggered by image uploaded to an S3 coastcam bucket at with the prefix ('directory') cameras/[station]/products/. Lambda function will not trigger 
if an image is uploaded to a different prefix in the S3 bucket. This image will be copied to the same S3 bucket at the prefix cameras/[station]/[camera]/[year]/[day]]/raw/ .
The day is formatted as [day of year]_mmm.[day of the month].
The year, day, and camera are derived from the filename. This function assumes the file has the format [unix time].[camera].timex.jpg -- currently this function will only 
merge timex images. Once these images are copied, a rectified (merge) image is created. This function will use imagery from all cameras at the same station that have a timex
image at the same unix time. This merege image is uploaded to the bucket at the prefix cameras/[station]/cx/merge/[year]/[day]/[unix time].timerx.merge.jpg.
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
import sys

from coastcam_funcs import *
from calibration_crs import *
from rectifier_crs import *

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


##### CLASSES #####
class Camera:
    '''
    This class represents a camera object for a coastcam stationS
    '''    
    def __init__(self, camera_number, filepath):
        '''
        Initialization function for Camera class. Set class attribute values
        Inputs:
            camera_number (string) - camera number string
            filepath (string) - S3 filepath for camera folder
        Outputs:
            none
        '''
        self.camera_number = camera_number
        self.filepath = filepath

    def onlyTimex(self):
        '''
        Remove all image files from the camera file list except for timex images
        Inputs:
            none
        Outputs:
            none
        '''
        keep_list = []
        #need to first make list of files that will be kept. If you directly removed the unmatched files,
        #it would not iterate through the whole loop because the list would be shorter
        for file in self.file_list:
            if re.match(".+timex*", file):
                keep_list.append(file)
        self.file_list = keep_list

    def createDict(self):
        '''
        Create dictionary of values for the class. The key value is a unix time. The corresponding data value
        is the S3 filepath for the image the specified unix time.
        Inputs:
            none
        Outputs:
            none
        '''
        self.unix_list = []
        self.unix_file_dict = {}
        for file in self.file_list:
            filename = getFilename(file)
            unix_time = unixFromFilename(filename)
            self.unix_list.append(unix_time)
            self.unix_file_dict[unix_time] = file


                    
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
    new_key = get_new_key(key)
    print('formatted key:', new_key, '\n')
    
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
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    key_elements = getPathElements(new_key)
    station = key_elements[1]
    
    ###only want timex images merged###
    if key_elements[-1].endswith('timex.jpg'):
        #get list of cameras for station
        camera_list = []
        camera_prefix = 'cameras/' + station + '/'
        folders = s3.list_objects(Bucket=bucket, Prefix=camera_prefix, Delimiter='/')
        for obj in folders.get('CommonPrefixes'):
            path = obj['Prefix']
            path_elements = path.split('/')
            subfolder = path_elements[2]
            #camera folder will always be "c*" where * is the camera number (length of 2 characters).
            #Only want camera folders beside cx
            if subfolder.startswith('c') and len(subfolder) == 2 and subfolder != 'cx':
                camera_list.append(subfolder)
    
        yaml_lists = []
        cameras = []
        #start iterator at 1 because cameras start at c1
        i = 1
        for cam in camera_list:
          camera_filepath = camera_prefix + cam
          cameras.append(Camera(cam.upper(), camera_filepath))
          
          file_names = [station.upper()+"_"+cam.upper()+"_extr", 
                        station.upper()+"_"+cam.upper()+"_intr",
                        station.upper()+"_"+cam.upper()+"_metadata",
                        station.upper()+"_localOrigin"]
          yaml_lists.append(file_names)
    
        extrinsic_cal_files = []
        intrinsic_cal_files = []
        metadata_files = []
        for file_list in yaml_lists:
            extrinsic_cal_files.append(file_list[0] + '.yaml')
            intrinsic_cal_files.append(file_list[1] + '.yaml')
            metadata_files.append(file_list[2] + '.yaml')
    
        #YAML files are located in S3
        #store YAML files in Lambda function /tmp directory while function executes
        i = 0
        for file in extrinsic_cal_files:
            file_path = 'cameras/parameters/' + station + '/' + file
            #!!! in AWS Lambda console use '/tmp/, but when working locally use './tmp/'
            download_path = '/tmp/' + file
            extrinsic_cal_files[i] = download_path
            with open(download_path, 'wb') as yaml_file:
                s3.download_fileobj(bucket, file_path, yaml_file)
            i = i + 1
        
        i = 0
        for file in intrinsic_cal_files:
            file_path = 'cameras/parameters/' + station + '/' + file
            download_path = '/tmp/' + file
            intrinsic_cal_files[i] = download_path
            with open(download_path, 'wb') as yaml_file:
                s3.download_fileobj(bucket, file_path, yaml_file)
            i = i + 1
                
        i = 0       
        for file in metadata_files:
            file_path = 'cameras/parameters/' + station + '/' + file
            download_path = '/tmp/' + file
            metadata_files[i] = download_path
            with open(download_path, 'wb') as yaml_file:
                s3.download_fileobj(bucket, file_path, yaml_file)
            i = i + 1
        
        #only 1 local origin file, don't need to do loop
        file_path = 'cameras/parameters/' + station + '/' + file_names[3] + '.yaml'
        download_path = '/tmp/' + file_names[3] + '.yaml'
        with open(download_path, 'wb') as yaml_file:
            s3.download_fileobj(bucket, file_path, yaml_file)
        
        #create YAML dictionaries
        local_origin = yaml2dict(download_path)
        metadata_list = []
        intrinsics_list = []
        extrinsics_list = []
        for file in metadata_files:
            metadata_list.append(yaml2dict(file))
        extrinsics_list = []
        for file in extrinsic_cal_files:
            extrinsics_list.append( yaml2dict(file) )
        intrinsics_list = []
        for file in intrinsic_cal_files:
            intrinsics_list.append( yaml2dict(file) )
        
        calibration = CameraCalibration(metadata_list[0],intrinsics_list[0],extrinsics_list[0],local_origin)

        xmin = 0.
        xmax = 500.
        ymin = 0.
        ymax = 700.
        dx = 1.
        dy = 1.
        z =  0.
        
        rectifier_grid = TargetGrid(
            [xmin, xmax],
            [ymin, ymax],
            dx,
            dy,
            z
        )
        
        rectifier = Rectifier(rectifier_grid)
        
        year = key_elements[3]
        day = key_elements[4]
        unix_time = unixFromFilename(key_elements[-1]) 
        
        #keep track of which cameras have imagery for the given unix time
        image_files_list = []
        time_cam_list = []
        for camera in cameras:
            image_filepath = camera.filepath + '/' + year + '/' + day + '/raw/' + unix_time + '.' + camera.camera_number.lower() + '.timex.jpg'
            try:
                #if exists, download image to tmp folder
                s3.head_object(Bucket=bucket, Key=image_filepath)
                
                download_path = '/tmp/' + unix_time + '.' + camera.camera_number.lower() + '.timex.jpg'
                with open(download_path, 'wb') as img_file:
                    s3.download_fileobj(bucket, image_filepath, img_file)
                    
                image_files_list.append(download_path)
                time_cam_list.append(camera.camera_number)
            except:
                print(f'{camera.camera_number} does not have an image at time {unix_time}')
        
        #rectify imagery        
        if len(time_cam_list) != len(cameras):
            temp_intrinsics = []
            temp_extrinsics = []
    
            c = 0
            for camera in cameras:
                if camera.camera_number not in time_cam_list:
                    c = c + 1
                else:
                    temp_intrinsics.append(intrinsics_list[c])
                    temp_extrinsics.append(extrinsics_list[c])
                    c = c + 1
                    
            rectified_image = rectifier.rectify_images(metadata_list[0], image_files_list, temp_intrinsics, temp_extrinsics, local_origin)      
        else:
            rectified_image = rectifier.rectify_images(metadata_list[0], image_files_list, intrinsics_list, extrinsics_list, local_origin)
        
        ofile = '/tmp/' + unix_time + '.timex.merge.jpg'
        imageio.imwrite(ofile,np.flip(rectified_image,0),format='jpg')
        
        upload_key = 'cameras/' + station + '/cx/merge/' + year + '/' + day + '/' + unix_time + '.timex.merge.jpg'
        with open(ofile, 'rb') as merged_img:
            s3.upload_fileobj(merged_img, bucket, upload_key)
            
        print(f'{upload_key} uploaded to S3')

        
    ###images that are not timex will only be copied
    else:
        print(f'{new_key} copied')
