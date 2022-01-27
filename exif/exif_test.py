#Eric Swanson
#test script to test adding exif tags

import exif
from datetime import datetime
from coastcam_funcs import *

image = exif.Image('1581508801.c1.timex.jpg')

extrinsics = yaml2dict('CACO-01_C1_extr.yaml')
instrinsics = yaml2dict('CACO-01_C1_intr.yaml')
metadata = yaml2dict('CACO-01_C1_metadata.yaml')
local_origin = yaml2dict('CACO-01_localOrigin.yaml')

#adds all parameters from YAML files into one dictionary
data_fields = {}
yaml_list = []
yaml_list.append(extrinsics)
yaml_list.append(instrinsics)
yaml_list.append(metadata)
yaml_list.append(local_origin)
for dictionary in yaml_list:
    for key in dictionary:
        data_fields[key] = dictionary[key]

image.user_comment = str(data_fields)

#write new tags to image
with open('1581508801.c1.timex.jpg', 'wb') as modified_image:
    modified_image.write(image.get_file())

