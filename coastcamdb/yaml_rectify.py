'''
Eric Swanson
Purpose: Rectify a pair of images using a modified version of Chris Sherwood's rectification code.
Instead of reading JSON files for extrinsics and iontrinsics, use YAML files to read in extrinsics, intrinsics,
metadata, and local grid info.

required files:
CACO-01_C1_extr.yaml
CACO-01_C1_intr.yaml
CACO-01_C1_metadata.yaml
CACO-01_C2_extr.yaml
CACO-01_C2_intr.yaml
CACO-01_C1_metadata.yaml
CACO-01_localOrigin.yaml
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
import yaml

# These .py files define the objects that load calibration data and do the rectification
from coastcam_funcs import *
from calibration_crs import *
from rectifier_crs import *


##### MAIN #####
# List of files...three for each camera. Calibration parameters are in .json format
# These are the USGS image filename format
extrinsic_cal_files = ['CACO-01_C1_extr.yaml','CACO-01_C2_extr.yaml']
intrinsic_cal_files = ['CACO-01_C1_intr.yaml','CACO-01_C2_intr.yaml']
metadata_files = ['CACO-01_C1_metadata.yaml', 'CACO-01_C2_metadata.yaml']

s3 = False # set to False to test local read, set to True to test bucket read
if s3:
    # read from S3 bucket
    imdir='cmgp-coastcam/cameras/caco-01/products/'
    image_files = ['1600866001.c1.timex.jpg','1600866001.c2.timex.jpg']
    ftime = filetime2timestr(image_files[0], timezone='eastern')
    fs = fsspec.filesystem('s3')
else:
    # local test files
    imdir='./'
    image_files = ['1581508801.c1.timex.jpg','1581508801.c2.timex.jpg']
    ftime, e = filetime2timestr(image_files[0], timezone='eastern')

    fs = None

impaths = []
for f in image_files:
    impaths.append(imdir+f)
##print(impaths)
##print(ftime, e)
ftime = filetime2timestr(image_files[0], timezone='eastern')
print(ftime)

# Dict providing the metadata that the Axiom code infers from the USACE filename format
metadata_list = []
for f in metadata_files:
    metadata_list.append(yaml2dict(f))
# dict providing origin and orientation of the local grid
local_origin = yaml2dict('CACO-01_localOrigin.yaml')

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

print("metadata", metadata_list[0])
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



print(extrinsics_list[0])
print(intrinsics_list[0])
print(local_origin)


rectified_image = rectifier.rectify_images(metadata_list[0], impaths, intrinsics_list, extrinsics_list, local_origin, fs=fs)
plt.imshow(rectified_image.astype(int))
plt.show()

# test rectifying a single image
single_file = impaths[0]
#print(single_file)
single_intrinsic = intrinsics_list[0]
single_extrinsic = extrinsics_list[0]
# print(type(intrinsics_list))
# print(type(single_intrinsic))
# print(single_extrinsic)
rectified_single_image = rectifier.rectify_images(metadata_list[0], [impaths[1]], [intrinsics_list[1]], [extrinsics_list[1]], local_origin, fs=fs)
plt.imshow(rectified_single_image.astype(int))
plt.gca().invert_yaxis()
plt.xlabel('Offshore (m)')
plt.ylabel('Alongshore (m)')
plt.show()

# write a local file
print(e)
ofile = e+'.rectified.jpg'
imageio.imwrite(ofile,np.flip(rectified_image,0),format='jpg')

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
plt.title(e)
fp = e+'.rectified.png'
plt.savefig(fp,dpi=200)

# alongshore profile of RGB values at 
plt.plot(rectified_image[:,60,:])
