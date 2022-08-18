'''
Eric Swanson
Purpose: Access the coastcamdb on AWS. Grab data from the DB and rectify a pair of images.
create dicts for extrinsics, instrinsics, and metadata. Rectify images using the code from
Chris Sherwood. 
Description:
Use parseCSV() to parse a csv file containing the necessary parameters to access the coastcamdb on AWS.
Connect to the DB using the mysql.connection.connector object. Access the DB and query how many cameras
exist for a given station. For each camera, create a set of 4 dictionaries of the extrinsic parameters, 
intrinsic parameters, metadata, and local grid info using the function DBtoDict(). Once these dictionaries
are created, they are used in the rectification code to rectify a pair of images.

required files:
CACO01_C1_EOBest.json
CACO01_C2_EOBest.json
CACO01_C1_IOBest.json
CACO01_C2_IOBest.json
1581508801.c1.timex.jpg
1600866001.c1.timex.jpg
1600866001.c2.timex.jpg
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
    #results are stored as dictionary iun cursor object
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


##### MAIN #####
csv_filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/Python/db_access.csv"
csv_parameters = parseCSV(csv_filepath)

host = csv_parameters[0]
port = int(csv_parameters[1])
dbname = csv_parameters[2]
user = csv_parameters[3]
password = csv_parameters[4]

#connect to the db
connection = mysql.connector.connect(user=user, password=password, host=host,database=dbname)

#make lists of cal dicts
extrinsics_list = []
intrinsics_list = []
metadata_list = []

station = "CACO-01"
#get items back in orer they were queried using buffered=true. cursor object stores results of query
cursor = connection.cursor(buffered=True)
query = "SELECT camera_number FROM camera WHERE name="+"'"+station+"'"
cursor.execute(query)
for row in cursor:
  camera_number = row[0]
  extrinsics, intrinsics, metadata, local_origin = DBtoDict(connection, station, camera_number)
  extrinsics_list.append(extrinsics)
  intrinsics_list.append(intrinsics)
  metadata_list.append(metadata)  

s3 = False # set to False to test local read, set to True to test bucket read
if s3:
    # read from S3 bucket
    image_directory='cmgp-coastcam/cameras/caco-01/products/'
    image_files = ['1600866001.c1.timex.jpg','1600866001.c2.timex.jpg']
    ftime = filetime2timestr(image_files[0], timezone='eastern')
    file_system = fsspec.filesystem('s3')
else:
    # local test files
    image_directory='./'
    image_files = ['1581508801.c1.timex.jpg','1581508801.c2.timex.jpg']
    ftime, epoch_string = filetime2timestr(image_files[0], timezone='eastern')

    file_system = None

image_paths = []
for file in image_files:
    image_paths.append(image_directory+file)

# check test for coordinate system
if metadata_list[0]['coordinate_system'].lower() == 'xyz':
    print('Extrinsics are local coordinates')
elif metadata_list[0]['coordinate_system'].lower() == 'geo':
    print('Extrinsics are in world coordinates')
else:
    print('Invalid value of coordinate_system: ',metadata_list[0]['coordinate_system'])

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

print(extrinsics_list[0])
print(intrinsics_list[0])
print(local_origin)

rectified_image = rectifier.rectify_images(metadata_list[0], image_paths, intrinsics_list, extrinsics_list, local_origin, fs=file_system)
plt.imshow(rectified_image.astype(int))
plt.show()

# test rectifying a single image
single_file = image_paths[0]
#print(single_file)
single_intrinsic = intrinsics_list[0]
single_extrinsic = extrinsics_list[0]
rectified_single_image = rectifier.rectify_images(metadata_list[0], [image_paths[1]], [intrinsics_list[1]], [extrinsics_list[1]], local_origin, fs=file_system)
plt.imshow(rectified_single_image.astype(int))
plt.gca().invert_yaxis()
plt.xlabel('Offshore (m)')
plt.ylabel('Alongshore (m)')
plt.show()

# write a local file
rectified_file = epoch_string+'.rectified.jpg'
imageio.imwrite(rectified_file,np.flip(rectified_image,0),format='jpg')

# make an annotated image
plt.imshow( rectified_image.astype(int))
plt.gca().invert_yaxis()
plt.xlabel('Offshore (m)')
plt.ylabel('Alongshore (m)')
# make a North arrow
angr = calibration.local_origin['angd']*np.pi/180.
dx = np.cos(angr)*90.
dy = np.sin(angr)*90.
plt.arrow(50,550,dx,dy,linewidth=2,head_width=25,head_length=30,color='white',shape='right')
plt.text(100,670,'N',color='white')
plt.text(220,670,ftime,fontsize=8,color='gold')
plt.title(epoch_string)
fp = epoch_string+'.rectified.png'
plt.savefig(fp,dpi=200)

# alongshore profile of RGB values at 
plt.plot(rectified_image[:,60,:])

