#From Chris Sherwood
import fsspec
import numpy as np
import imageio

url = 'cmgp-coastcam/cameras/caco-01/foo2.txt'
fs = fsspec.filesystem('s3')
with fs.open(url) as f:
    im = f.read()

print(im)

with fs.open('cmgp-coastcam/cameras/caco-01/products/1600866001.c2.timex.jpg') as f:
    im = imageio.read(f)
    
print(im)

#recent_list=fs.glob('cmgp-coastcam/cameras/caco-01/latest/*')
#print(recent_list[1])

#need fsspec and fs3 packages
