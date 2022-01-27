#Eric Swanson
#access coastcamdb on Amazon RDS MySQL
#using packages pymysql and pandas

##### REQUIRED PACKAGES ######
import pandas as pd
import pymysql
import csv

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

##### MAIN #####


filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/Python/db_access.csv"
csv_parameters = parseCSV(filepath)

host = csv_parameters[0]
port = int(csv_parameters[1])
dbname = csv_parameters[2]
user = csv_parameters[3]
password = csv_parameters[4]

connection = pymysql.connect(host=host, user=user, port=port, passwd=password, db=dbname)

#test
result = pd.read_sql('select * from camera', con=connection)
print(result)
