'''
From Chris Sherwood
Reectofy all images on S3 bucket for caco-01 and write to public read bucket
converted from jupyter notebook

Necessary packages:
pathlib
imageio
numpy
matplotlib
datetime
dateutil
pandas


Necessary files:
coastcam_funcs.py
calibration_crs.py
rectifier_crs.py

'''

##### REQUIRED PACKAGES #####
from pathlib import Path
import imageio
import fsspec
import numpy as np
import matplotlib.pyplot as plt
import datetime
from dateutil import tz
import pandas as pd

from coastcam_funcs import *
from calibration_crs import *
from rectifier_crs import *


###### MAIN ######
filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam/coastcamdb/1615401000.c1.snap.jpg"

sharpness, contrast = estimate_sharpness(filepath)
print(sharpness, contrast)
