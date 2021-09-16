"""
Eric Swanson
Purpose: for a day where images are captured, give metrics of how many of each image type
are captued. Image types are snap, timex, var, bright, dark, rundark.
Use the day folders in the S3 imagery bucket. Create a bar chart for each image type for given day.
"""

#####REQUIRED PACKAGES#####
#will need fs3 package to use s3 in fsspec
import fsspec 
import numpy as np
import re
import matplotlib.pyplot as plt



#####FUNCTIONS#####

def imageCountBarGraph(filepath):
    """
    Given the filepath of a day of images in an S3 bucket, produce a bar chart to show how many
    of each image type (snap, timex, var, bright, dark, rundark) are present for each day.
    Filepath follows the format: s3://cmgp-coastcam/cameras/[station]/[camera]/[year]/[day]/raw

    Inputs:
        filepath - (string) S3 filepath folder of images
    Outputs:
        None. Although this function produces a bar graph.
    """

    #get day
    path_elements = source_folder.split("/")
    #elements in list ['s3:', '', [bucket], 'cameras', [station], [camera], [year], [day], 'raw']
    day_formatted = path_elements[7]
    day_elements = day_formatted.split("_")
    day = day_elements[1]

    #station caco-01 for testing
    file_system = fsspec.filesystem('s3', profile='coastcam')
    image_list = file_system.glob(source_folder+'/*')

    #initialize counters for different image types
    snap_count = 0
    timex_count = 0
    var_count = 0
    bright_count = 0
    dark_count = 0
    rundark_count = 0

    common_image_list = ['.tif', '.tiff', '.bmp', '.jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

    #big loop to count image types
    for image in image_list:
        for image_type in common_image_list: 
            isImage = False 
            if image.endswith(image_type):
                isImage = True
                break
        if image.endswith('.txt') or isImage == False:
            continue
        else:
            if re.match(".+snap*", image):
                snap_count += 1
            if re.match(".+timex*", image):
                timex_count += 1
            if re.match(".+var*", image):
                var_count += 1
            if re.match(".+bright*", image):
                bright_count += 1
            #if dark. Will also increment for instances of "rundark" unless explicitly stated otherwise
            if re.match(".+dark*", image) and not re.match(".+rundark*", image):
                dark_count += 1
            if re.match(".+rundark*", image):
                rundark_count += 1


    #x-coordinates (don't really mean anything)
    x = [1, 2, 3, 4, 5, 6]

    height = [snap_count, timex_count, var_count, bright_count, dark_count, rundark_count]

    tick_label = ['snap', 'timex', 'var', 'bright', 'dark', 'rundark']

    plt.bar(x, height, tick_label = tick_label, width = 0.8, color = ['green'])

    #set y-ticks even spaced from min height to max height of bars
    plt.yticks(np.arange(min(height), max(height) + 1, 1))

    plt.xlabel('image types')
    plt.ylabel('type count')
    plt.title('image type count for ' + day)
    plt.show()
    return



#####MAIN#####

#source day folder in format s3://cmgp-coastcam/cameras/[station]/[camera]/[year]/[day]/raw
source_folder = "s3://cmgp-coastcam/cameras/caco-01/c1/2019/348_Dec.14/raw"

imageCountBarGraph(source_folder)


