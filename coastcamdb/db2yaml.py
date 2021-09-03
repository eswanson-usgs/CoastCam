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
#parse csv
filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/Python/db_access.csv"
params = parseCSV(filepath)

host = params[0]
port = int(params[1])
dbname = params[2]
user = params[3]
password = params[4]

#connect to the db
conn = mysql.connector.connect(user=user, password=password, host=host,database=dbname)

#directory to create YAML files in 
filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam/coastcamdb"

#query to get number of cameras at a station
station = "CACO-01"
cursor = conn.cursor(buffered=True)
query = "SELECT camera_number FROM camera WHERE name="+"'"+station+"'"
cursor.execute(query)

#get dictionary of DB field descriptors
descrip_dict = getDBdescriptors(conn)

#For each camera, create dicts for extrinsics, intrinsics, metadata, and local origin
#create YAML files from these dicts
for row in cursor:
  cam_num = row[0]
  extrinsics, intrinsics, metadata, local_origin = DBtoDict(conn, station, cam_num) 
  dict_list = [extrinsics, intrinsics, metadata, local_origin]
  
  #Ex. file names: "CACO-01_C1_extr", "CACO-01_C1_intr", "CACO-01_C1_metadata", "CACO-01_localOrigin"
  file_names = [station+"_"+cam_num+"_extr", 
                station+"_"+cam_num+"_intr",
                station+"_"+cam_num+"_metadata",
                station+"_localOrigin"] 
  
  #create YAML files
  DBdict2yaml(dict_list, descrip_dict, filepath, file_names)
