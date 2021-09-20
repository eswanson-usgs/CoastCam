'''
Eric Swanson
Purpose: Access the coastcamdb on AWS. Grab data from the DB and create YAML files for the extrinsics,
instrinsics, metadata, and the local grid info

Description:
First connect to the MySQL database using the mysql.connector.connect() method. Query the DB to get
the number of cameras for a given station. Then, get a dictionary of descriptors for all the fields in the
DB using the getDBdescriptors() function. For each camera at the given station, get the extrinsics, intrinsics,
metadata, and local grid info dictionaries using the DBtoDict() function. Finally, write these dictionaries to
YAML files using DBdict2yaml(). Each of these YAML files also have commented text descriptions for each of the fields.
'''

##### REQUIRED PACKAGES ######
from pathlib import Path
from datetime import datetime
import mysql.connector
import csv
import yaml
import datetime

#must have coastcam_funcs.py in the workiung directory
from coastcam_funcs import *

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
    
    #query to get row of descriptors
    query = "SELECT * FROM descriptors"
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
print("start:", datetime.datetime.now())
csv_filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/Python/db_access.csv"
csv_parameters = parseCSV(csv_filepath)

host = csv_parameters[0]
port = int(csv_parameters[1])
dbname = csv_parameters[2]
user = csv_parameters[3]
password = csv_parameters[4]

connection = mysql.connector.connect(user=user, password=password, host=host,database=dbname)

filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam/coastcamdb"

station = "CACO-01"
#get items back in order they were queried using buffered=True. Results of query stored in cursor object
cursor = connection.cursor(buffered=True)
query = "SELECT camera_number FROM camera WHERE name="+"'"+station+"'"
cursor.execute(query)

descriptor_dict = getDBdescriptors(connection)

for row in cursor:
  camera_number = row[0]
  extrinsics, intrinsics, metadata, local_origin = DBtoDict(connection, station, camera_number) 
  dict_list = [extrinsics, intrinsics, metadata, local_origin]
  
  #Ex. file names: "CACO-01_C1_extr", "CACO-01_C1_intr", "CACO-01_C1_metadata", "CACO-01_localOrigin"
  file_names = [station+"_"+camera_number+"_extr", 
                station+"_"+camera_number+"_intr",
                station+"_"+camera_number+"_metadata",
                station+"_localOrigin"] 
  
  #create YAML files using asynchronous parallel processing
  DBdict2yaml(dict_list, descriptor_dict, filepath, file_names)

print("end:", datetime.datetime.now())
