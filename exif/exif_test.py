#Eric Swanson
#test script to test adding exif tags


##### IMPORTS #####
import pyexiv2
import yaml
import ast

##### FUNCTIONS #####
def yaml2dict(yamlfile):
    """ Import contents of a YAML file as a dict
    Args:
        yamlfile (str): YAML file to read
    Returns:
        dictname (dict): dict interpreted from YAML file
    """
    dictname = None
    with open(yamlfile, "r") as infile:
        try:
            dictname = yaml.safe_load(infile)
        except yaml.YAMLerror as exc:
            print(exc)
    return dictname

def readYAMLcomments(yamlfile):
    """
    Read a YAML file and add its comments to a dict
    Args:
        yamlfile (str): YAML file to read
    Returns:
        comment_dict (dict): dict of YAML comments
    """
    comment_dict = {}
    with open(yamlfile, "r") as infile:
        lines = infile.readlines()
        for line in lines:
            if line.startswith('#'):
                #clean up comment line and add to dictionary
                line = line.replace('#', '')
                line = line.strip()
                comment_dict[line.split()[0] + '_comment'] = '#' + line
    return comment_dict


##### MAIN #####
img = pyexiv2.Image('1581508801.c1.timex.jpg')

#top level dict for exif UserComment tag
UserComment= {}
UserComment['Note'] = 'This comment provides camera location and direction (extrinsic calibration) and lens coefficients (intrinsic calibration'

#calibration paramters
extrinsics = yaml2dict('CACO-01_C1_extr.yaml')
intrinsics = yaml2dict('CACO-01_C1_intr.yaml')
metadata = yaml2dict('CACO-01_C1_metadata.yaml')


#Read comments from YAML file
extr_comments = readYAMLcomments('CACO-01_C1_extr.yaml')
extrinsics['variable descriptions'] = extr_comments
intr_comments = readYAMLcomments('CACO-01_C1_intr.yaml')
intrinsics['variable descriptions'] = intr_comments
metadata_comments = readYAMLcomments('CACO-01_C1_metadata.yaml')
metadata['variable descriptions'] = metadata_comments

UserComment['Extrinsics'] = extrinsics
UserComment['Intrinsics'] = intrinsics
UserComment['Metadata'] = metadata
use_string = str(UserComment)
test = use_string.split('{')
print(test)


#                   ######exif, iptc, and xmp tags are only placeholders######

#required exif: Copyright
#required exif (if available): ImageDescription, DateTimeOriginal, ModifyDate, GPSDateStamp, GPSTimeStamp
#recommended exif: Artist, Make, Model
#exif GPS fields: GPSAreaInformation, GPSMapDatum, GPSLatitude, GPSLatitudeRef, GPSLongitude, GPSLongitudeRef
exif_dict = {'Exif.Photo.UserComment': str(UserComment), 
             'Exif.Image.Copyright': 'USGS',
             'Exif.Image.ImageDescription': 'This is a CoastCam picture',
             'Exif.Image.DateTimeOriginal': '02-12-2020 12:00:01', #this is 'ModifyDate' when using exiftool
             'Exif.Image.DateTime': '02-16-2022 10:26:00',
             'Exif.Image.Artist': 'SPCMSC CCH Group',
             'Exif.Image.Make': 'camera make',
             'Exif.Image.Model': 'camera model',
             'Exif.GPSInfo.GPSAreaInformation': 'position post-processed from nearby GPS',
             'Exif.GPSInfo.GPSMapDatum': 'EPSG:4326 (WGS 84)',
             'Exif.GPSInfo.GPSDateStamp': '2020:02:12',
             'Exif.GPSInfo.GPSTimeStamp': '12:00:01',
             'Exif.GPSInfo.GPSLatitude':  extrinsics['x'],
             'Exif.GPSInfo.GPSLatitudeRef': 'N',
             'Exif.GPSInfo.GPSLongitude':  extrinsics['y'],
             'Exif.GPSInfo.GPSLongitudeRef': 'N'}             
img.modify_exif(exif_dict)

#required iptc: Credit, Contact
iptc_dict = {'Iptc.Application2.Credit': 'U.S. Geological Survey',
             'Iptc.Application2.Contact': 'eswanson@contractor.usgs.gov'}
img.modify_iptc(iptc_dict)

#required xmp: UsageTerms
#required xmp (if available): AttributionURL, Event, ImageUniqueID
#recommended xmp: ExternalMetadataLink, DateTimeDigitized, Contributor, PreservedFilename
xmp_dict = {'Xmp.xmp.UsageTerms': 'These data are preliminary or provisional and are subject to revision. They are being provided to meet the need for timely',
            'Xmp.xmp.AttributionURL': 'doi.org/data_release_url',
            'Xmp.xmp.Event': 'CoastCam monitoring',
            'Xmp.xmp.ImageUniqueID': 'usgs.gov/PID_url',
            'Xmp.xmp.ExternalMetadataLink': 'usgs.gov/PIR_metadata_url',
            'Xmp.xmp.DateTimeDigitized': '02-16-2022 10:26:00',
            'Xmp.xmp.Contributor': 'Eric Swanson',
            'Xmp.xmp.PreservedFilename': '1581508801.c1.timex.jpg'}
img.modify_xmp(xmp_dict)

img.close()


