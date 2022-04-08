#Eric Swanson
#This script will rectify (merge) a pair of images using their EXIF metadata. The calibration data needed for rectification is
#stored in the UserComment exif field. In UserComment, the necessary data is embedded as a nested dictionary in the 'data' field.
#This script will parse the USerComment field, extract the data, and then perform rectification

##### IMPORTS #####
import pyexiv2
import yaml
import ast
from PIL import Image
import imageio
import numpy as np
import matplotlib.pyplot as plt
from coastcam_funcs import *
from calibration_crs import *
from rectifier_crs import *


##### MAIN #####
image_files = ['1581508801.c1.timex.jpg','1581508801.c2.timex.jpg']

local_origin = {}

#extrinsic, intrinsic, and metadata dicts must be put in lists to comply with Calibration and Rectifier functions
extrinsics_list = []
intrinsics_list = []
metadata_list = []

for image in image_files:
    img = pyexiv2.Image(image)

    UserComment = img.read_exif()['Exif.Photo.UserComment']
    #convert UserComment from string to dict type
    UserComment = ast.literal_eval(UserComment)
    data_dict = UserComment['data']
    data_dict = ast.literal_eval(data_dict)

    extr_keys = ['x', 'y', 'z', 'a', 't', 'r']
    intr_keys = ['NU', 'NV', 'c0U', 'c0V', 'fx', 'fy', 'd1', 'd2', 'd3', 't1', 't2']
    metadata_keys = ['name', 'serial_number', 'camera_number', 'calibration_date', 'coordinate_system']
    local_org_keys = ['x_origin', 'y_origin', 'angd']

    extrinsics = {}
    intrinsics = {}
    metadata = {}

    #create dictionaries for extrinsics, intrinsics, metadata, local origin
    for key in data_dict:
        if key in extr_keys:
            extrinsics[key] = float(data_dict[key])

        elif key in intr_keys:
            intrinsics[key] = float(data_dict[key])

        elif key in metadata_keys:
            if key == 'serial_number':
                metadata[key] = int(data_dict[key])
            else:
                metadata[key] = data_dict[key]

        elif key in local_org_keys:
            if key == 'x_origin':
                local_origin['x'] = float(data_dict[key])
            elif key == 'y_origin':
                local_origin['y'] = float(data_dict[key])
            else:
                local_origin[key] = float(data_dict[key])

    extrinsics_list.append(extrinsics)
    intrinsics_list.append(intrinsics)
    metadata_list.append(metadata)

calibration = CameraCalibration(metadata_list[0], intrinsics_list[0] ,extrinsics_list[0], local_origin)

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

rectified_image = rectifier.rectify_images(metadata_list[0], image_files, intrinsics_list, extrinsics_list, local_origin)
plt.imshow(rectified_image.astype(int))
plt.show()
