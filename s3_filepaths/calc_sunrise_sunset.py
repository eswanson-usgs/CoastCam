"""
Eric Swanson
Purpose: Calculate the sunrise or sunset for a given day.
Get date and time info from unix time.
Using Marconi beach caco-01 camera location. Eastern time.
Calculations checked by National Oceanographic and Atmospheric Association sunrise/sunset calcualtor
"""

#####REQUIRED PACKAGES#####
import numpy as np
import math
import os
import time
#will need fs3 package to use s3 in fsspec
import fsspec 
import numpy as np
import imageio
import calendar
import datetime
import csv
from astral import LocationInfo
from astral.sun import sun
from dateutil import tz

#####FUNCTIONS#####
def unix2datetime(unixnumber):
    """
    Developed from unix2dts by Chris Sherwood. Updates by Eric Swanson.
    Get datetime object and string (in UTC) from unix/epoch time. datetime object is "aware",
    meaning it always references a specific point in time rather than a time relative to local time.
    datetime object will be the same regardless of what timezone the function is run in.
    Input:
        unixnumber - string containing unix time (aka epoch)
    Returns:
        date_time_string, date_time_object in utc
    """

    # images other than "snaps" end in 1, 2,...but these are not part of the time stamp.
    # replace with zero
    ts = int( unixnumber[:-1]+'0')
    date_time_obj =  datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    date_time_str = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')
    return date_time_str, date_time_obj

def getSunriseSunset(unix_time, latitude, longitude, timezone, city = "", country = "United States"):
    """
    Given a unix time and a location, return the sunrise and sunset for the day in local time.

    Inputs:
        unix_time - (integer) unix time
        latitude - (float) latitude of the location
        longitude - (longitude) of the location
        timezone - (string) timezone of the location
        city (optional) - (string) city where user wants to get sunrise and sunset for
        Country (optional) - (string) country of location. Can assume United States for most part
    Outputs:
        sunrise - (string) time of sunrise in format HH:MM:SS
        sunset - (string) time of sunset in format HH:MM:SS
    """

    #Get datetime object. Returned object is in UTC, must convert to local.
    date_time_str, date_time_obj = unix2datetime(str(unix_time))
    #convert to EST

    #get sunrise and sunset
    #Need to first describe location with astral LocationInfo object
    #Then use astral sun object with attributes sunrise and sunset
    #sun object is a dictionary
    loc = LocationInfo(city, country, timezone, 41.8918, -69.9611)
    s = sun(loc.observer, date = date_time_obj, tzinfo = loc.timezone)
    #extract hour, minute, second of sunrise/sunset
    sunrise = str(s["sunrise"])[11:19]
    sunset = str(s["sunset"])[11:19]
    return sunrise, sunset
    


#####MAIN#####

#December 13, 2019 6:00pm GMT (2pm EST)
unix_time = 1576260000
latitude = 41.8918
longitude = -69.9611
timezone = "America/New_York"

sunrise, sunset = getSunriseSunset(unix_time, latitude, longitude, timezone, "Wellfleet", "United States")
print("Sunrise:", sunrise + ", Timezone:", timezone)
print("Sunset:", sunset + ", Timezone:", timezone)
        

