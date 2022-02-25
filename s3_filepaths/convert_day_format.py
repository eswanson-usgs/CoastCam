"""
Eric Swanson
Script to make sure every day in the S3 filepath has the format [Day of year]_[Month].[Day of month]
where the day of the year is always with three digits, the month is in the abbreviated format,and the
day of the month is always 2 digits. This script adds leading zeros to the day of the year where necessary.
This script works an entire CoastCam station of imagery in S3.
"""
##### REQUIERD PACKAGES #####
import os
#will need fs3 package to use s3 in fsspec
import fsspec 
import datetime
import concurrent.futures


##### FUNCTIONS #####     
def copy_image(args):
    """
    Copy image to a new filepath with a reformatted day directory in S3.
    Input:
        args (tuple) - arguments tuple with elements: image, year, new_format_day, file_system
    Output:
        none
    """

    image = args[0]
    year = args[1]
    new_format_day = args[2]
    file_system = args[3]
    thing = image.split('/')

    filename = image.split('/')[-1]
    source_filepath = 's3://' + image
    destination_filepath = 's3://' + year + '/' + new_format_day + '/raw/' + filename
    file_system.copy(source_filepath, destination_filepath)

    return


##### MAIN #####
print("start:", datetime.datetime.now())
#source folder filepath with format s3:/cmgp-coastcam/cameras/[station]/products/[filename]
source_folder = "s3://cmgp-coastcam/cameras/dreaminn/" 

#station caco-01 for testing
file_system = fsspec.filesystem('s3', profile='coastcam')

subfolder_list = file_system.glob(source_folder)
cam_list = []
for subfolder in subfolder_list:
    sub_elements = subfolder.split('/')
    if sub_elements[3].startswith('c') and len(sub_elements[3]) == 2:
        cam_list.append(subfolder)

for cam in cam_list:
    
    if cam.endswith('cx'):
        #2/24/22 - do nothing for now. Merge not implemented in CoastCam bucket
        continue
    
##        year_list = file_system.glob('s3://' + cam + '/merge/')
##        for year in year_list:
##            
##            day_list = file_system.glob('s3://' + year + '/')
##            for day in day_list:
##                last_element = day.split('/')[-1]
##                if last_element == 'test':
##                    continue #ignore. Only used for testing purposes
##                else:
##                    day_of_year = last_element.split('_')[0]
##                    day_of_month = last_element.split('_')[1]
##                    if int(day_of_year) < 10:
##                        new_format_day = "00" + day_of_year + "_" + day_of_month
##
##                        day_filepath = 's3://' + day + '/'
##                        image_list = file_system.glob(day_filepath)
##                        for image in image_list:
##                            filename = image.split('/')[-1]
##                            source_filepath = 's3://' + image
##                            destination_filepath = 's3://' + year + '/' + new_format_day + '/' + filename
##                            file_system.copy(source_filepath, destination_filepath)
##
##                        
##                    elif (int(day_of_year) >= 10) and (int(day_of_year) < 100):
##                        new_format_day = "0" + day_of_year + "_" + day_of_month
##
##                        day_filepath = 's3://' + day + '/'
##                        image_list = file_system.glob(day_filepath)
##                        for image in image_list:
##                            filename = image.split('/')[-1]
##                            source_filepath = 's3://' + image
##                            destination_filepath = 's3://' + year + '/' + new_format_day + '/' + filename
##                            file_system.copy(source_filepath, destination_filepath)

    else:

        year_list = file_system.glob('s3://' + cam + '/')
        for year in year_list:

            day_list = file_system.glob('s3://' + year + '/')
            for day in day_list:
                last_element = day.split('/')[-1]
                if last_element == 'test':
                    continue #ignore. only used for testing
                else:
                    day_of_year = last_element.split('_')[0]
                    day_of_month = last_element.split('_')[1]
                    if int(day_of_year) < 10 and (len(day_of_year) == 1):
                        new_format_day = "00" + day_of_year + "_" + day_of_month

                        day_filepath = 's3://' + day + '/raw/'
                        image_list = file_system.glob(day_filepath)

                        #ProcessPoolExecutor is used for multithreading (allows multiple instances of function to be run at once)
                        args = ((image, year, new_format_day, file_system) for image in image_list)
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            results = executor.map(copy_image, args)
                        
                    elif (int(day_of_year) >= 10) and (int(day_of_year) < 100) and (len(day_of_year) == 2):
                        new_format_day = "0" + day_of_year + "_" + day_of_month
                        day_filepath = 's3://' + day + '/raw/'
                        image_list = file_system.glob(day_filepath)
                        
                        #ProcessPoolExecutor is used for multithreading (allows multiple instances of function to be run at once)
                        args = ((image, year, new_format_day, file_system) for image in image_list)
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            results = executor.map(copy_image, args)
                            
print("end:", datetime.datetime.now())






