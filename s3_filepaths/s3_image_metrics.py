"""
Eric Swanson
Purpose: for a day where images are captured, give metrics of how many of each image type
are captued. Image types are snap, timex, var, bright, dark, rundark.
Use the day folders in the S3 imagery bucket. Create a bar chart for each image type for given day.
"""

#####REQUIRED PACKAGES#####
import os
#will need fs3 package to use s3 in fsspec
import fsspec 
import numpy as np
import re
import matplotlib.pyplot as plt

#####FUNCTIONS#####


#####MAIN#####

#source day folder in format s3://cmgp-coastcam/cameras/[station]/[camera]/[year]/[day]/raw
#will search all camera folders (c1, c2, etc.)
source_folder = "s3://test-cmgp-bucket/cameras/caco-01/c1/2019/347_Dec.13/raw"

#get day
path_elements = source_folder.split("/")
#elements in list ['s3:', '', [bucket], 'cameras', [station], [camera], [year], [day], 'raw'
day_formatted = path_elements[7]
day_elements = day_formatted.split("_")
day = day_elements[1]

#access list of images in source folder using fsspec
#station caco-01 for testing
fs = fsspec.filesystem('s3', profile='coastcam')
image_list = fs.glob(source_folder+'/*')

#initialize counters for different image types
snap_count = 0
timex_count = 0
var_count = 0
bright_count = 0
dark_count = 0
rundark_count = 0

#list of common image types
common_image_list = ['.tif', '.tiff', '.bmp', '.jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

#loop through folder of images
#check if image is of proper file types
#if so, count image
for image in image_list:
    #loop through list of possible image types
    for image_type in common_image_list: 
        #variable to check if file ends with image type in common_image_list. False by default.
        good_ending = False 
        if image.endswith(image_type):
            good_ending = True
            break
    if image.endswith('.txt') or good_ending == False:
        #This is not an image. Skip file
        continue
    #this is an image
    else:
        #if snap
        if re.match(".+snap*", image):
            snap_count += 1
        #if timex
        if re.match(".+timex*", image):
            timex_count += 1
        #if var
        if re.match(".+var*", image):
            var_count += 1
        #if bright
        if re.match(".+bright*", image):
            bright_count += 1
        #if dark. Will also increment for instances of "rundark" unless explicitly stated otherwise
        if re.match(".+dark*", image) and not re.match(".+rundark*", image):
            dark_count += 1
        #if rundark
        if re.match(".+rundark*", image):
            rundark_count += 1


#x-coordinates (don't really mean anything)
x = [1, 2, 3, 4, 5, 6]

#Heghts of bars
height = [snap_count, timex_count, var_count, bright_count, dark_count, rundark_count]

#label for bars
tick_label = ['snap', 'timex', 'var', 'bright', 'dark', 'rundark']

#plotting bar chart
plt.bar(x, height, tick_label = tick_label, width = 0.8, color = ['green'])

#naming x-axis
plt.xlabel('image types')
#naming y-axis
plt.ylabel('type count')
#plot title
plt.title('image type count for ' + day)
#show plot
plt.show()



