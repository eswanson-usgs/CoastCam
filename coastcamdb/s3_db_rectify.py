'''
Eric Swanson
Purpose: Access a pair of images on S3. Access the coastcamdb on AWS and get metadata. Rectify the images using code from
Chris Sherwood as a basis. Once rectified, transfer images to new folder in S3. For now this works on one set of images
in CACO-01.

Description:
Using the filepath url of an S3 folder, the station is obtained. For example, CACO-01. A csv file with the parameters
for logging into the coastcamdb on AWS is parsed using parseCSV() and the resulting parsed parameters are used to connect to the DB.
The DB is queried and the number of cameras for the station is obtained. The descriptors for the data fields in the DB are
also fetched using getDBdescriptors(). For each camera at the station, the script checks if yaml files for camera extrinsics,
intrinsics, metadata, and local origin exist. If not, the script fetches the data from the DB using DBtoDict() and creates
dictionary objects. Then, it creates yaml files using DBdict2yaml(). Three lists are created that contain extrinsics, intrinsics,
and metadata, respectively. These lists contain dictionary objects corresponding to each camera. For each camera, an S3 filepath for
a timex image is created S3 using hardcoded elements for the day folder, year folder, and iomatge unix time. This filepath is added
to a list of filepaths. Next, 4 dictionary objects are created from the yaml files: one each for extrinsics, instrinsics, metadata,
and local grid origin info. Using these dictionaries, a rectified image for the given unix time is created using the rectification
code from Chris Sherwood. This image is written to the local directory. Finally, this image is written to a new "rectified"
filepath in S3. The old filepath for unrectified images is in the format :
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

from coastcam_funcs import *
from calibration_crs import *
from rectifier_crs import *


##### FUNCTIONS ######
def parseCSV(filepath):
    '''
    Read and parse a CSV to obtain list of parameters used to access database.
    The csv file should have the column headers "host", "port", "dbname", "user",
    and "password" with one row of data containing the user's values
    Inputs:
        filepath (string) - filepath of the csv
    Outputs:
        db_list (list) - list of paramters used to access database
    '''
    
    db_list = []

    with  open(filepath, 'r') as csv_file:
        csvreader = csv.reader(csv_file)

        #extract data from csv. Have to use i to track row because iterator object csvreader is not subscriptable
        i = 0
        for row in csvreader:
            #i = 0 is column headers. i = 1 is data 
            if i == 1:
                db_list = row
            i = i + 1
                
    return db_list   
    
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

def getDBdescriptors(connection):
    '''
    Retrieve the entries from the "descriptors" table in the coastcamdb.
    Return the entries as a dictionary. These descriptors are text fields that describe the 
    meaning of the field names in the coastcamdb.
    Inputs:
        connection (mysql.connector.connection_cext.CMySQLConnection) - Object that acts as the connection to MySQL
    Output:
        descriptor_dict (dict) - dictionary of descriptor values
    '''
    
    query = "SELECT * FROM descriptors"
    #results are stored as dictionary iun cursor object
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    
    #each row in cursor is a dictionary. Only get one row, which. This is descriptor dict
    for row in cursor:
        descriptor_dict = row
    return descriptor_dict

def DBdict2yaml(dict_list, descriptor_dict, filepath, file_names):
    '''
    Create YAML files from a list of dictionaries. Create a YAML file for each
    dictionary in the list.
    Inputs:
        dict_list (list) - a list of dictionary objects
        descriptor_dict (dict) - dictionary of descriptors for fields from the DB
        filepath (string) - directory where YAML files will be saved
        file_names (list) - list of filenames for the new YAML files, ".yml" not included
    Outputs:
        none, but YAML files are created
    '''

    i = 0
    for dictionary in dict_list:
        path = filepath+"/"+file_names[i]+".yaml"
        
        with open(path, 'w') as file:
            for field in dictionary:
                #manually write in YAML formatting. YAML dump sometimes writes out of order
                file.write(field + ': ' + str(dictionary[field]) + '\n')
            
            #leave comments in yaml with text descriptions of the fields
            #ex. #x - x location of camera
            for field in dictionary:
                file.write('#' + field + ' - ' + descriptor_dict[field]+ '\n')
        i = i + 1
    return


##### MAIN #####
#S3 filepath for station
station_filepath = "s3://test-cmgp-bucket/cameras/caco-01/"

station_path_elements = station_filepath.split("/")

#remove empty space elements from the list
for elements in station_path_elements:
    if len(elements) == 0: 
        station_path_elements.remove(elements)

station = station_path_elements[3]

csv_filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/Python/db_access.csv"
csv_parameters = parseCSV(csv_filepath)

host = csv_parameters[0]
port = int(csv_parameters[1])
dbname = csv_parameters[2]
user = csv_parameters[3]
password = csv_parameters[4]

connection = mysql.connector.connect(user=user, password=password, host=host,database=dbname)

yaml_filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam/coastcamdb/"

station = "CACO-01"
#get items back in order they were queried using buffered=True. Results of query stored in cursor object
cursor = connection.cursor(buffered=True)
query = "SELECT camera_number FROM camera WHERE name="+"'"+station+"'"
cursor.execute(query)

descriptor_dict = getDBdescriptors(connection)

yaml_list = []

for row in cursor:
  camera_number = row[0]
  
  file_names = [station+"_"+camera_number+"_extr", 
                station+"_"+camera_number+"_intr",
                station+"_"+camera_number+"_metadata",
                station+"_localOrigin"]
  yaml_list.append(file_names)
  
  #flag used to determine if it's necessary to access DB and create YAML files
  yaml_flag = 0

  for file in file_names:
      if os.path.isfile(yaml_filepath + file + '.yaml'):
          yaml_flag = yaml_flag + 1

  #if 4 YAML files exist for station camera, don't need to access DB and create YAML files
  if yaml_flag == 4:
      continue
  else:
      extrinsics, intrinsics, metadata, local_origin = DBtoDict(connection, station, camera_number) 
      dict_list = [extrinsics, intrinsics, metadata, local_origin]
      
      #create YAML files
      DBdict2yaml(dict_list, descriptor_dict, yaml_filepath, file_names)

# These are the USGS image filename format.
extrinsic_cal_files = []
intrinsic_cal_files = []
metadata_files = []
for lists in yaml_list:
    extrinsic_cal_files.append(lists[0] + '.yaml')
    intrinsic_cal_files.append(lists[1] + '.yaml')
    metadata_files.append(lists[2] + '.yaml')

#Day, year, unix time set manully for testing
year = "/2019"
day = "/347_Dec.13"
unix_time = "1576270801"
image_files = []
for i in range(0,len(yaml_list)):
    #s3 filename
    filename = (unix_time + '.c' + str(i + 1) + '.timex.jpg')
    image_files.append(station_filepath + 'c' + str(i + 1) + year + day + "/raw/" + filename)
    
file_time, epoch_string = filetime2timestr(image_files[0], timezone='eastern')
file_system = fsspec.filesystem('s3', profile='coastcam')

# Dict providing the metadata that the Axiom code infers from the USACE filename format
metadata_list = []
for f in metadata_files:
    metadata_list.append(yaml2dict(f))
# dict providing origin and orientation of the local grid
local_origin = yaml2dict(file_names[3]+'.yaml')

extrinsics_list = []
for file in extrinsic_cal_files:
    extrinsics_list.append( yaml2dict(file) )
intrinsics_list = []
for file in intrinsic_cal_files:
    intrinsics_list.append( yaml2dict(file) )

# check test for coordinate system
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

rectifier = Rectifier(
    rectifier_grid
)

rectified_image = rectifier.rectify_images(metadata_list[0], image_files, intrinsics_list, extrinsics_list, local_origin, fs=file_system)
plt.imshow(rectified_image.astype(int))

# test rectifying a single image
single_file = image_files[0]
single_intrinsic = intrinsics_list[0]
single_extrinsic = extrinsics_list[0]
rectified_single_image = rectifier.rectify_images(metadata_list[0], [image_files[1]], [intrinsics_list[1]], [extrinsics_list[1]], local_origin, fs=file_system)
plt.imshow(rectified_single_image.astype(int))
plt.gca().invert_yaxis()
plt.xlabel('Offshore (m)')
plt.ylabel('Alongshore (m)')

# write a local file
ofile = epoch_string+'.timex.merge.jpg'
imageio.imwrite(ofile,np.flip(rectified_image,0),format='jpg')

#access S3 and write image
#Ex. rectified image filepath: s3://test-cmgp-bucket/cameras/caco-01/cx/merge/2019/347_Dec.13/1576270801.timex.merge.jpg
rectified_filepath = station_filepath + 'cx/merge' + year + day + '/' + ofile
with file_system.open(rectified_filepath, 'wb') as rectified_file:
    imageio.imwrite(rectified_file,np.flip(rectified_image,0),format='jpg') 

