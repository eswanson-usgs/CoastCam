#Eric Swanson
#test script to test adding exif tags


##### IMPORTS #####
import pyexiv2
import yaml

##### FUNCTIONS #####
def yaml2dict(yamlfile):
    """ Import contents of a YAML file as a dict
    Args:
        yamlfile (str): YAML file to read
    Returns:
        dict interpreted from YAML file
    """
    dictname = None
    with open(yamlfile, "r") as infile:
        try:
            dictname = yaml.safe_load(infile)
        except yaml.YAMLerror as exc:
            print(exc)
    return dictname


##### MAIN #####
##img = pyexiv2.Image('1581508801.c1.timex.jpg')
##data = img.read_exif()
##print(data)
##img.close()
##image = exif.Image('1581508801.c1.timex.jpg')


img = pyexiv2.Image('1581508801.c1.timex.jpg')

#calibration paramters
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



#                   ######exif, iptc, and xmp tags are only placeholders######

#required exif: Copyright
#required exif (if available): ImageDescription, DateTimeOriginal, ModifyDate, GPSDateStamp, GPSTimeStamp
#recommended exif: Artist, Make, Model
#exif GPS fields: GPSAreaInformation, GPSMapDatum, GPSLatitude, GPSLatitudeRef, GPSLongitude, GPSLongitudeRef
exif_dict = {'Exif.Photo.UserComment': str(data_fields), 
             'Exif.Image.Copyright': 'USGS',
             'Exif.Image.ImageDescription': 'This is a CoastCam picture',
             'Exif.Image.DateTimeOriginal': '02-12-2020 12:00:01',
             'Exif.Image.DateTime': '02-16-2022 10:26:00',
             'Exif.Image.Artist': 'SPCMSC CCH Group',
             'Exif.Image.Make': 'camera make',
             'Exif.Image.Model': 'camera model',
             'Exif.GPSInfo.GPSAreaInformation': 'position post-processed from nearby GPS',
             'Exif.GPSInfo.GPSMapDatum': 'EPSG:4326 (WGS 84)',
             'Exif.GPSInfo.GPSDateStamp': '2020:02:12',
             'Exif.GPSInfo.GPSTimeStamp': '12:00:01',
             'Exif.GPSInfo.GPSLatitude':  data_fields['x'],
             'Exif.GPSInfo.GPSLatitudeRef': 'N',
             'Exif.GPSInfo.GPSLongitude':  data_fields['y'],
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


