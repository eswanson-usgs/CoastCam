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
s3://[S3 bucket]/cameras/[station]/rectified/[year]/[day]/[image filename]

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

# These .py files define the objects that load calibration data and do the rectification
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

    #read csv
    with  open(filepath, 'r') as f:
        #create csv reader object
        csvreader = csv.reader(f)

        #extract data from csv. Have to use i to track row because iterator object csvreader is not subscriptable
        i = 0
        for row in csvreader:
            #i = 0 is column headers. i = 1 is data 
            if i == 1:
                db_list = row
            i = i + 1
                
    return db_list   
    
def DBtoDict(conn, station, cam_num):
    '''
    Read from the database connection and create 4 dictionaries: one for camera extrinsics, one for 
    intrinsic camera parameters,one for camera metadata, and one for the local grid origin and orientation information
    Inputs:
        conn (mysql.connector.connection_cext.CMySQLConnection) - Object that acts as the connection to MySQL
        station (string) - Describes the station where the cameras is located. Ex. 'CACO-01'
        cam_num (string) - Camera to get paramters for, in the format number C#. Ex. 'C1'
    Outputs:
        extrinsics (dict) - dictionary of camera extrinsic parameters
        intrinsics (dcit) - dictionary of camera intrinsic parameters
        metadata (dict) - dictionary of camera metadata
        local_origin (dict) - dictionary of local grid info 
    '''
    
    #EX. query: SELECT * FROM camera WHERE station_name='CACO-01' AND camera_number='C1'
    query = "SELECT * FROM camera WHERE name="+"'"+station+"' AND camera_number="+"'"+cam_num+"'"
    cursor = conn.cursor(dictionary=True)
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

def getDBdescriptors(conn):
    '''
    Retrieve the entries from the "descriptors" table in the coastcamdb.
    Return the entries as a dictionary. These descriptors are text fields that describe the 
    meaning of the field names in the coastcamdb.
    Inputs:
        conn (mysql.connector.connection_cext.CMySQLConnection) - Object that acts as the connection to MySQL
    Output:
        descrip_dict (dict) - dictionary of descriptor values
    '''
    
    #query to get row of descriptors
    query = "SELECT * FROM descriptors"
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    
    #each row in cursor is a dictionary. Only get one row, which. This is descriptor dict
    for row in cursor:
        descrip_dict = row
    return descrip_dict

def DBdict2yaml(dict_list, descrip_dict, filepath, file_names):
    '''
    Create YAML files from a list of dictionaries. Create a YAML file for each
    dictionary in the list.
    Inputs:
        dict_list (list) - a list of dictionary objects
        descrip_dict (dict) - dictionary of descriptors for fields from the DB
        filepath (string) - directory where YAML files will be saved
        file_names (list) - list of filenames for the new YAML files, ".yml" not included
    Outputs:
        none, but YAML files are created
    '''
    
    i = 0
    
    #for each dictionary, write fields to individual YAML file
    for dictionary in dict_list:
        path = filepath+"/"+file_names[i]+".yaml"
        
        #write data and field descriptions to YAML
        with open(path, 'w') as file:
            #write data
            for field in dict_list[i]:
                file.write(field + ': ' + str(dict_list[i][field]) + '\n')
            
            #leave comments in yaml with text descriptions of the fields
            for field in dict_list[i]:
                #ex. #x - x location of camera
                file.write('#' + field + ' - ' + descrip_dict[field]+ '\n')
                
        i = i + 1
    return


##### MAIN #####
#S3 filepath for station
station_filepath = "s3://test-cmgp-bucket/cameras/caco-01/"

station_path_elements = station_filepath.split("/")

#remove empty space elements from the list
for elements in station_path_elements:
    #if string element is ''
    if len(elements) == 0: 
        station_path_elements.remove(elements)

station = station_path_elements[3]

###Use fsspec to copy image from old path to new path
##fs = fsspec.filesystem('s3', profile='coastcam')

#parse csv
csv_filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/Python/db_access.csv"
params = parseCSV(csv_filepath)

host = params[0]
port = int(params[1])
dbname = params[2]
user = params[3]
password = params[4]

#connect to the db
conn = mysql.connector.connect(user=user, password=password, host=host,database=dbname)

#directory to create YAML files in 
yaml_filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam/coastcamdb/"

#query to get number of cameras at a station
station = "CACO-01"
cursor = conn.cursor(buffered=True)
query = "SELECT camera_number FROM camera WHERE name="+"'"+station+"'"
cursor.execute(query)

#get dictionary of DB field descriptors
descrip_dict = getDBdescriptors(conn)

#A list of lists that will store the names of the yaml files
yaml_list = []

#For each camera, create dicts for extrinsics, intrinsics, metadata, and local origin
#create YAML files from these dicts
for row in cursor:
  cam_num = row[0]
  
  #Ex. file names: "CACO-01_C1_extr", "CACO-01_C1_intr", "CACO-01_C1_metadata", "CACO-01_localOrigin"
  file_names = [station+"_"+cam_num+"_extr", 
                station+"_"+cam_num+"_intr",
                station+"_"+cam_num+"_metadata",
                station+"_localOrigin"]
  #add YAML file names to list for this camera
  yaml_list.append(file_names)
  
  #flag used to determine if it's necessary to access DB and create YAML files
  yaml_flag = 0

  #check if files exists in folder. If a match is found in directory, increment YAMl flag
  for file in file_names:
      if os.path.isfile(yaml_filepath + file + '.yaml'):
          yaml_flag = yaml_flag + 1

  #if 4 YAML files exist for station camera, don't need to access DB and create YAML files
  if yaml_flag == 4:
      continue
  else:
      #access DB and get data
      extrinsics, intrinsics, metadata, local_origin = DBtoDict(conn, station, cam_num) 
      dict_list = [extrinsics, intrinsics, metadata, local_origin]
      
      #create YAML files
      DBdict2yaml(dict_list, descrip_dict, yaml_filepath, file_names)

# List of files...three for each camera. Calibration parameters are in .yaml format
# These are the USGS image filename format.
extrinsic_cal_files = []
intrinsic_cal_files = []
metadata_files = []
#for each camera, add calibration files + metadata to appropriate lists
for lists in yaml_list:
    extrinsic_cal_files.append(lists[0] + '.yaml')
    intrinsic_cal_files.append(lists[1] + '.yaml')
    metadata_files.append(lists[2] + '.yaml')

# read images from each camera from their respective filepaths. Day, year, unix time set manully for testing
year = "/2019"
day = "/347_Dec.13"
unix_time = "1576270801"
image_files = []
for i in range(0,len(yaml_list)):
    #s3 filename. Ex. 1576270801.c1.timex.jpg
    filename = (unix_time + '.c' + str(i + 1) + '.timex.jpg')
    #add full filepath to image_files list. Ex. s3://test-cmgp-bucket/cameras/caco-01/c1/2019/347_Dec.13/raw/1576270801.c1.timex.jpg
    image_files.append(station_filepath + 'c' + str(i + 1) + year + day + "/raw/" + filename)
    
ftime, e = filetime2timestr(image_files[0], timezone='eastern')
fs = fsspec.filesystem('s3', profile='coastcam')

# Dict providing the metadata that the Axiom code infers from the USACE filename format
metadata_list = []
for f in metadata_files:
    metadata_list.append(yaml2dict(f))
# dict providing origin and orientation of the local grid
local_origin = yaml2dict(file_names[3]+'.yaml')

# read cal files and make lists of cal dicts
extrinsics_list = []
for f in extrinsic_cal_files:
    extrinsics_list.append( yaml2dict(f) )
intrinsics_list = []
for f in intrinsic_cal_files:
    intrinsics_list.append( yaml2dict(f) )

# check test for coordinate system
if metadata_list[0]['coordinate_system'].lower() == 'xyz':
    print('Extrinsics are local coordinates')
elif metadata_list[0]['coordinate_system'].lower() == 'geo':
    print('Extrinsics are in world coordinates')
else:
    print('Invalid value of coordinate_system: ',metadata['coordinate_system'])
 
# print(extrinsics_list[0])
# print(extrinsics_list[0]['y']-local_origin['y'])

calibration = CameraCalibration(metadata_list[0],intrinsics_list[0],extrinsics_list[0],local_origin)
# print(calibration.local_origin)
# print(calibration.world_extrinsics)
# print(calibration.local_extrinsics)

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
#print(rectifier_grid.X)

rectifier = Rectifier(
    rectifier_grid
)

rectified_image = rectifier.rectify_images(metadata_list[0], image_files, intrinsics_list, extrinsics_list, local_origin, fs=fs)
plt.imshow(rectified_image.astype(int))

# test rectifying a single image
single_file = image_files[0]
single_intrinsic = intrinsics_list[0]
single_extrinsic = extrinsics_list[0]
rectified_single_image = rectifier.rectify_images(metadata_list[0], [image_files[1]], [intrinsics_list[1]], [extrinsics_list[1]], local_origin, fs=fs)
plt.imshow(rectified_single_image.astype(int))
plt.gca().invert_yaxis()
plt.xlabel('Offshore (m)')
plt.ylabel('Alongshore (m)')

# write a local file
ofile = e+'.rectified.jpg'
imageio.imwrite(ofile,np.flip(rectified_image,0),format='jpg')

#access S3 and write image
#Ex. rectified image filepath: s3://test-cmgp-bucket/cameras/caco-01/rectified/2019/347_Dec.13/1576270801.rectified.jpg
rectified_filepath = station_filepath + 'rectified' + year + day + '/' + ofile
print(rectified_filepath)
with fs.open(rectified_filepath, 'wb') as fo:
    imageio.imwrite(fo,np.flip(rectified_image,0),format='jpg') 

