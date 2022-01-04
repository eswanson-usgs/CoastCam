'''
Eric Swanson
Purpose:  Rectify the images using code from Chris Sherwood as a basis. Once rectified, transfer images to new folder in S3.
This script is designed to work on a 'scope' specfiied by the user. The user inputs a filepath and the script will determine
whether to rectify imagery for a single unix time, day, year, or entire station in S3.
Description: 
This script is run through the command prompt. The user enters an S3 filepath as a command line argument. An example way to run this
script on Windows command prompt is:
py S3_RECTIFY.py s3://test-cmgp-bucket/cameras/caco-01/
The filepath is obatined from the command line as a variable and split up into elements consisting of the filepath subfolders
or image filename. Using these elements, the filepath is checked to see if it's a proper length. If it it's too long or too short,
the user will be asked to input a filepath until one of proper length is entertained. If the filepath contains a filename at the
end, the script will make sure it is in a .jpg format-- the user will be asked to re-enter the filepath if not. A list of cameras
for the camera station directory that the filepath exists under will be obtained, represented as Pyton objects.
For each camera, a set of YAML files for the camera (intrinsics, extrinsics, metadata, and local origin info) will be read
and stored as dictionary objects. These dictionaries are stored in lists (one each for intrinsics, extrinsics, and metadata),
except for the local origin info.
Depending on the length of the filepath, a 'scope' for the amount imagery to be rectified will be defined. The possible scopes are
unix time, day, year, and station. Basically, the longer the filepath, the smaller the scope and the less amount imagery that
will be rectified. For the scope to be 'unix time', the user must input a filepath that ends with a filename. This is because the
unix time is contained in the filename.
If the scope is for a single unix time: the station, year, and day are obtained from the filepath. For each camera, the given day
is searched for the given unix time. The script keeps track of which cameras have imagery for that time. Only timex images
are used. If the number of cameras with imagery for that time is less than the total number of cameras for that station,
only the intrinsics and extrinsics for the cameras that have imagery will be used. Otherwise, every extrinsic and intrisinsic
dictionary is used. Using the extrinsics, instrinsics, metadata, and local origin info, a 'merged' image is created. This image
is copied to the appropriate 'merge' directory in S3.
If the scope is for a day: for each camera, an S3 filepath for a list of all the "raw" images for the camera's corresponding S3 filepath is obtained. A dictionary. has_time_dict,
is created to keep track of which cameras have imagery for each unix time. They key values are unix times and the data values
are lists of camera numbers. Each list of imagery is stripped of all images except
timex images using the onlyTimex() function. Using the createUnixDict() function, a dictionary is created.
The key values for this dictionary are unix time and the corresponding data values are the S3 filepaths for
each timex image. A global image file dictionary is created. The key values for the dictionary are unix times, and the data values
are lists of images that correspond to each unix time. The unix time dictionary (for files) for each camera is looped through. 
If the dictionary has an entry for a unix time and gobal image file dictionary does not currently have an entry for that same time, an entry is
created in the global dictionary and the image file from the camera is appended to the list in the global dictionary entry. If an entry
already exists for the corresponding unix time in the global dictionary, the image file is appended to the list in this dictionary
without creating a new entry. Then, for each unix time
where at least image file exists, a rectified "merge" image is created using the rectify_images() method from rectifier_crs.py.
If there is a unix time that does not have a image from each camera, then a rectified image is created using the files from the other camera(s)
that do exist for the that unix time. Each rectified image is added to a list of rectified images, and each unix timen that has
at least one image file is also stored in a list (as a string). Each rectified image is copied to a new S3 filepath.
Finally, this image is written to a new "rectified" filepath in S3. This is done through the mergeDay() function.
If the scope is for a year: each camera at the station is checked to see if contains imagery for that year. If not, a flag is set
to denote that the camera has no imagery. A global list of day directories in the year directory is obtained. For each day,
a list of cameras is created. Only cameras that have imagery (in S3) for that day are added to the list. Using this list,
rectification is performed on all the images for that day using the mergeDay() function. Multithreading parallelization is used
to speed up the rectification and copying process.
If the scope is for the entire station: a list of years in the station directory is created. For every year in this list, imagery
is rectified following the steps listed above for the scope for a year.
The old filepath for unrectified images is in the format :
s3://[S3 bucket]/cameras/[station]/[camera]/[year]/[day]/raw/[image file name]
The new filepath for rectified images is:
s3://[S3 bucket]/cameras/[station]/merge/[year]/[day]/[image filename]
required files:
coastcam_funcs.py
calibration_crs.py
rectifier_crs.py
'''

##### REQUIRED PACKAGES ######
from pathlib import Path
from PIL import Image
from datetime import datetime
import imageio
import fsspec
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector
import csv
import yaml
import imageio
import calendar
import datetime
from dateutil import tz
import os
import re
import sys
import concurrent.futures

from coastcam_funcs import *
from calibration_crs import *
from rectifier_crs import *


##### FUNCTIONS ######    
def DBtoDict(connection, station, camera_number):
    '''
    Read from the database connection and create 4 dictionaries: one for camera extrinsics, one for 
    intrinsic camera parameters,one for camera metadata, and one for the local grid origin and orientation information
    Inputs:
        connection (mysql.connector.connection_cext.CMySQLConnection) - Object that acts as the connection to MySQL
        station (string) - Describes the station where the cameras is located. Ex. 'CACO-01'
        camera_number (string) - Camera to get paramters for, in the format number C#. Ex. 'C1'
    Outputs:
        extrinsics (dict) - dictionary of camera extrinsic parameters
        intrinsics (dcit) - dictionary of camera intrinsic parameters
        metadata (dict) - dictionary of camera metadata
        local_origin (dict) - dictionary of local grid info 
    '''
    
    query = "SELECT * FROM camera WHERE name="+"'"+station+"' AND camera_number="+"'"+camera_number+"'"
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    
    #each row in cursor is a dictionary. Only get one row.
    for row in cursor:
        dictionary = row
        
    #dict of extrinsic camera parameters
    extrinsics = {
        "x": dictionary["x"],
        "y": dictionary["y"],
        "z": dictionary["z"],
        "a": dictionary["a"],
        "t": dictionary["t"],
        "r": dictionary["r"]
    }
    
    #dict of instrinsic camera parameters
    intrinsics = {
        "NU": dictionary["NU"],
        "NV": dictionary["NV"],
        "c0U": dictionary["c0U"],
        "c0V": dictionary["c0V"],
        "fx": dictionary["fx"],
        "fy": dictionary["fy"],
        "d1": dictionary["d1"],
        "d2": dictionary["d2"],
        "d3": dictionary["d3"],
        "t1": dictionary["t1"],
        "t2": dictionary["t2"]
    }
    
   #dict of camera metadata formatted to USACE
    metadata = {
        #name is station name
        "name": dictionary["name"],
        "serial_number": dictionary["serial_number"],
        "camera_number": dictionary["camera_number"],
        "calibration_date": dictionary["calibration_date"].strftime("%Y-%m-%d"),
        "coordinate_system": dictionary["coordinate_system"]
     }
    
    #dict of local grid info
    local_origin = {
        "x": dictionary["x_origin"],
        "y": dictionary["y_origin"],
        "angd": dictionary["angd"]
    }
    
    return extrinsics, intrinsics, metadata, local_origin

def getFilename(filepath):
    '''
    Given a filepath, retrieve the filename at the end of it.
    Input:
        filepath (string) - full filepath of file
    Ouput:
        path_elements[-1] (string) - last element of the split up filepath. This is the filename.
    '''

    path_elements = filepath.split("/")
    #filename is last element of filepath
    return path_elements[-1]

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

def checkPathLength(path_elements):
    '''
    Check if the inputted filepath is the proper length by checking the list of the path elements. Length of list should be
    between 4 and 9. If the list has length 9 but the filename (path_elements[8]) doesn't end in .jpg, the output boolean
    will be set to false.
    Inputs:
        path_elements (list) - list of elements (subfolders or filename) of filepath
    Outputs:
        isCorrectLength (bool) - true if filepath is in the correct length range, false if not.
    '''
    isProperLength = True
    if len(path_elements) < 4:
        isProperLength = False
        print('Filepath is too short. S3 filepath must include at least a camera station subfolder. Remember to include forward slashes (/) between subfolders.')
    elif len(path_elements) > 9:
        isProperLength = False
        print('Filepath is too long. Longest possible filepath format is s3://[bucket]/cameras/[station]/[camera number]/[year]/[formatted day]/raw/[filename]')
    elif len(path_elements) == 9 and path_elements[8].endswith('.jpg') == False:
        isProperLength = False
        print("Filename must end with '.jpg'. Filename format is [unix time].[camera number].[image format].jpg")
    return isProperLength

def onlyTimex(file_list):
    '''
    Remove all image files from the file list except for timex images
    Inputs:
        file_list (list) - list of image files
    Outputs:
        timex_list (list) - list of only timex images
    '''
    timex_list = []
    #need to first make list of files that will be kept. If you directly removed the unmatched files,
    #it would not iterate through the whole loop because the list would be shorter
    for file in file_list:
        if re.match(".+timex*", file):
            timex_list.append(file)
    return timex_list

def createUnixDict(file_list):
    '''
    Create dictionary of values for unix times. The key value is a unix time. The corresponding data value
    is the S3 filepath for the image the specified unix time.
    Inputs:
        file_list (list) list of files usewd to create the dictionary
    Outputs:
        unix_file_dict (dict) - dictionary where the key id the unix time and the data value is an S3 filepath
    '''
    unix_file_dict = {}
    for file in file_list:
        filename = getFilename(file)
        unix_time = unixFromFilename(filename)
        unix_file_dict[unix_time] = file
    return unix_file_dict

def mergeDay(args):
    '''
    Rectifies a day of imagery in S3 and uploads the rectified image to a year subfolder in a 'merge' directory in S3
    Inputs:
        args (tuple) - arguments tuple with elements: year, day, file_system, metadata_list, intrinsics_list, extrinsics_list,
        local_origin, cameras, station_filepath
    Outputs:
        None
    '''
    year = args[0]
    day = args[1]
    file_system = args[2]
    metadata_list = args[3]
    intrinsics_list = args[4]
    extrinsics_list = args[5]
    local_origin = args[6]
    cameras = args[7]
    station_filepath = args[8]

    #day_cam_list keeps track of which cameras have files for that day.
    day_cam_list = []
    for cam in cameras:
        if cam.no_year_flag == 1:
            continue
        else:
            full_day_path = station_filepath + cam.camera_number.lower() + '/' + year + '/' + day
            if full_day_path not in cam.day_list:
                print(cam.camera_number + " does not have day: " + day)
            else:
                day_cam_list.append(cam)            

    #do steps for rectifying a DAY of imagery only for cameras with imagery for given day
    #dictionary contains lists of image files used for rectification . The key = unix time, the data = list of image files from each camera
    image_files_dict = {}
    #dict used to keep track of which cameras have imagery for which unix time. Key = unix time, data = list of cameras with imagery for that time
    has_time_dict = {}
    for cam in day_cam_list:
        file_list = file_system.glob(cam.filepath + '/' + year + '/' + day + '/raw/')
        file_list = onlyTimex(file_list)
        unix_file_dict = createUnixDict(file_list)

        #For each unix time where image is taken, create list of images from each camera for that time
        for entry in unix_file_dict:
            if entry not in image_files_dict:
                image_files_dict[entry] = []
                has_time_dict[entry] = []
                image_files_dict[entry].append(unix_file_dict[entry])
                has_time_dict[entry].append(cam.camera_number)
            #if unix time key already exists in dict, add another file for that time to the corresponding list
            else:
                image_files_dict[entry].append(unix_file_dict[entry])
                has_time_dict[entry].append(cam.camera_number)

    rectified_image_list = []
    unix_time_list = [] 

    for unix_time in has_time_dict:
        unix_time_list.append(unix_time)
        #If image doesn't exist for each camera, create rectified image from only cameras with image file for given unix time
        if len(image_files_dict[unix_time]) != len(cameras):
            #instrinsics, extrinsics only for cameras who have file for corresponding unix time
            temp_intrinsics = []
            temp_extrinsics = []

            c = 0
            for cam in cameras:
                if cam.no_year_flag == 1:
                    c = c + 1
                elif cam.camera_number not in has_time_dict[unix_time]:
                    c = c + 1
                else:
                    temp_intrinsics.append(intrinsics_list[c])
                    temp_extrinsics.append(extrinsics_list[c])
                    c = c + 1
            rectified_image = rectifier.rectify_images(metadata_list[0], image_files_dict[unix_time], temp_intrinsics, temp_extrinsics, local_origin, fs=file_system)
            rectified_image_list.append(rectified_image)        
        else:
            rectified_image = rectifier.rectify_images(metadata_list[0], image_files_dict[unix_time], intrinsics_list, extrinsics_list, local_origin, fs=file_system)
            rectified_image_list.append(rectified_image)

    k =0 
    for image in rectified_image_list:
        ofile = unix_time_list[k]+'.timex.merge.jpg'
        imageio.imwrite('./rectified images/' + ofile,np.flip(image,0),format='jpg')
        
        #access S3 and write image
        #Ex. rectified image filepath: s3://test-cmgp-bucket/cameras/caco-01/cx/merge/2019/347_Dec.13/1576270801.timex.merge.jpg
        rectified_filepath = station_filepath + 'cx/merge' + '/' + year + '/' + day + '/' + ofile
        with file_system.open(rectified_filepath, 'wb') as rectified_file:
            imageio.imwrite(rectified_file,np.flip(image,0),format='jpg') 
        k = k + 1

    return ''

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
        

##### MAIN #####
print("start:", datetime.datetime.now(), '\n')

file_system = fsspec.filesystem('s3', profile='coastcam')
        
#Enter filepath as argument in command line
filepath = sys.argv[1]

#S3 filepath can only contain forward slash (/), not backslash (\). Need extra \ as an escape character in '\\'
filepath = '/'.join(filepath.split('\\'))
path_elements = getPathElements(filepath)
    
isProperLength = checkPathLength(path_elements)
while isProperLength == False:
    filepath = input('Please input S3 filepath of correct length and format: ')
    path_elements = getPathElements(filepath)
    isProperLength = checkPathLength(path_elements)

station = path_elements[3]
#each folder in directory is dictionary object. 'Key' key in the dict is the subfolder filepath
split_path = filepath.split(station)
station_filepath = split_path[0] + station + '/'
#"s3://" is not retrieved from filesystem when using the fsspec glob() function, which causes issues
station_filepath = station_filepath.replace("s3://", '')
directory = file_system.listdir(station_filepath)

camera_list = []
for item in directory:
    subfolder = (item['Key'].split('/')[-1])
    #camera folder will always be "c*" where * is the camera number (length of 2 characters). Only want camera folders beside cx
    if subfolder.startswith('c') and len(subfolder) == 2 and subfolder != 'cx':
        camera_list.append(subfolder)

yaml_list = []
cameras = []
#start iterator at 1 because cameras start at c1
i = 1
for cam in camera_list:
  #ex. filepath: s3://test-cmgp-bucket/cameras/caco-01/c1/
  camera_filepath = station_filepath + cam
  cameras.append(Camera(cam.upper(), camera_filepath))
  
  file_names = [station+"_"+cam.upper()+"_extr", 
                station+"_"+cam.upper()+"_intr",
                station+"_"+cam.upper()+"_metadata",
                station+"_localOrigin"]
  yaml_list.append(file_names)

# These are the USGS image filename format.
extrinsic_cal_files = []
intrinsic_cal_files = []
metadata_files = []
for lists in yaml_list:
    extrinsic_cal_files.append(lists[0] + '.yaml')
    intrinsic_cal_files.append(lists[1] + '.yaml')
    metadata_files.append(lists[2] + '.yaml')

# dict providing origin and orientation of the local grid
local_origin = yaml2dict(file_names[3]+'.yaml')
# Dict providing the metadata that the Axiom code infers from the USACE filename format

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

if metadata_list[0]['coordinate_system'].lower() == 'xyz':
    print('Extrinsics are local coordinates')
elif metadata_list[0]['coordinate_system'].lower() == 'geo':
    print('Extrinsics are in world coordinates')
else:
    print('Invalid value of coordinate_system: ',metadata['coordinate_system'])

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

#use len of path elements to define scope of imagery to be rectified: for single unix time, day, year, or station
#example filepath format of max length: s3://[bucket]/cameras/[station]/[camera number]/[year]/[day]/raw/[filename]
if len(path_elements) == 4 or len(path_elements) == 5:
    scope = 'station'
elif len(path_elements) == 6:
    scope = 'year'
elif len(path_elements) == 7 or len(path_elements) == 8:
    scope = 'day'
elif len(path_elements) == 9:
    scope = 'unix time'
print('scope:', scope)



#UNIX TIME
if scope == 'unix time':
    year = path_elements[5]
    day = path_elements[6]
    unix_time = unixFromFilename(path_elements[-1])        

    #keep track of which cameras have imagery for the given unix time
    image_files_list = []
    time_cam_list = []
    for camera in cameras:
        image_filepath = camera.filepath + '/' + year + '/' + day + '/raw/' + unix_time + '.' + camera.camera_number.lower() + '.timex.jpg'
        if not file_system.exists(image_filepath):
            print(f'{camera.camera_number} does not have an image at time {unix_time}')
        else:
            image_files_list.append(image_filepath)
            time_cam_list.append(camera.camera_number)

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
        rectified_image = rectifier.rectify_images(metadata_list[0], image_files_list, temp_intrinsics, temp_extrinsics, local_origin, fs=file_system)      
    else:
        rectified_image = rectifier.rectify_images(metadata_list[0], image_files_list, intrinsics_list, extrinsics_list, local_origin, fs=file_system)

    ofile = unix_time + '.timex.merge.jpg'
    imageio.imwrite('./rectified images/' + ofile,np.flip(rectified_image,0),format='jpg')

    rectified_filepath = station_filepath + 'cx/merge' + '/' + year + '/' + day + '/' + ofile
    print(rectified_filepath)
    with file_system.open(rectified_filepath, 'wb') as rectified_file:
        imageio.imwrite(rectified_file,np.flip(rectified_image,0),format='jpg') 



#DAY          
elif scope == 'day':
    year = path_elements[5]
    day = path_elements[6]

    for camera in cameras:
        year_path = camera.filepath + '/' + year
        camera.day_list = file_system.glob(year_path + '/')
        if not file_system.exists(year_path):
            print(camera.camera_number + " does not have year: " + year)
            #for camera, keep track if it has the current year. Used below to skip processing for S3 day folders
            camera.no_year_flag = 1 
        else:
            camera.no_year_flag = 0

    args = (year, day, file_system, metadata_list, intrinsics_list, extrinsics_list, local_origin, cameras, station_filepath)
    mergeDay(args)



#YEAR
elif scope == 'year':
    year = path_elements[5]

    #year_cam_list keeps track of which cameras have files for that year.
    year_cam_list = []
    for camera in cameras:
        year_path = camera.filepath + '/' + year
        if not file_system.exists(year_path):
            print(camera.camera_number + " does not have year: " + year)
            #for camera, keep track if it has the current year. Used below to skip processing for S3 day folders
            camera.no_year_flag = 1 
        else:
            year_cam_list.append(camera)
            camera.no_year_flag = 0
            
    for camera in year_cam_list:
        #create global list of days that have imagery in S3 based on list of days from each camera
        global_day_list = []
        for camera in cameras:
            camera.day_list = file_system.glob(camera.filepath + '/' + year + '/')
            for day in camera.day_list:
                #remove extra stuff from filepath to get formatted day
                day = day.split('/')[-1]
                if day not in global_day_list:
                    global_day_list.append(day)
    
    #ProcessPoolExecutor is used for multithreading (allows multiple instances of function to be run at once)
    args = ((year, day, file_system, metadata_list, intrinsics_list, extrinsics_list, local_origin, cameras, station_filepath) for day in global_day_list)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(mergeDay, args)    



#STATION    
else: #scope == 'station'
    #create global list of years that have imagery in S3 based on list of years from  each camera
    global_year_list = []
    for camera in cameras:
        camera.year_list = file_system.glob(camera.filepath + '/')
        for year in camera.year_list:
            #remove extra stuff from filepath to get year
            year = year.split('/')[-1]
            if year not in global_year_list:
                global_year_list.append(year)

    for year in global_year_list:
        print('\n', year)
        #year_cam_list keeps track of which cameras have files for that year.
        year_cam_list = []
        for camera in cameras:
            year_path = camera.filepath + '/' + year
            if year_path not in camera.year_list:
                print(camera.camera_number + " does not have year: " + year)
                #for camera, keep track if it has the current year. Used below to skip processing for S3 day folders
                camera.no_year_flag = 1 
            else:
                year_cam_list.append(camera)
                camera.no_year_flag = 0

        for camera in cameras:
            camera.day_list = file_system.glob(camera.filepath + '/' + year + '/')
                
        global_day_list = []
        for camera in year_cam_list:      
            for day in camera.day_list:
                #remove extra stuff from filepath to get formatted day
                day = day.split('/')[-1]
                if day not in global_day_list:
                    global_day_list.append(day)
        
        #ProcessPoolExecutor is used for multithreading (allows multiple instances of function to be run at once)
        args = ((year, day, file_system, metadata_list, intrinsics_list, extrinsics_list, local_origin, cameras, station_filepath) for day in global_day_list)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(mergeDay, args)


print("end:", datetime.datetime.now())
