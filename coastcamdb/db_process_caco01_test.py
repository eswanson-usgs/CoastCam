'''
Reectify pair of images using metadata from DB

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
filepath = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam/coastcamdb/16154010000.c1.snap.jpg"

sharpness, contrast = estimate_sharpness(filepath)
print(sharpness, contrast)
