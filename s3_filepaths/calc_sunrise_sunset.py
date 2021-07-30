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


#####MAIN#####

#December 13, 2019 6:00pm GMT (2pm EST)
unix_time = str(1576260000)
latitude = 41.8918
longitude = -69.9611

#Get datetime object. Returned object is in UTC, must convert to local.
date_time_str, date_time_obj = unix2datetime(unix_time)
#convert to EST
date_time_obj = date_time_obj.replace(hour = (date_time_obj.hour - 4))

#get sunrise and sunset
#Need to first describe location with astral LocationInfo object
#Then use astral sun object with attributes sunrise and sunset
#sun object is a dictionary
city = LocationInfo("Wellfleet", "United States", "America/New_York", 41.8918, -69.9611)
s = sun(city.observer, date = date_time_obj, tzinfo = city.timezone)
#extract hour, minute, second of sunrise/sunset
sunrise = str(s["sunrise"])[12:19]
sunset = str(s["sunrise"])[12:19]
print("Sunrise:", sunrise + ", Timezone:", city.timezone)
print("Sunset:", sunset + ", Timezone:", city.timezone)










###For latitude- degrees north is positive and degrees south is negative
###For longitude- degrees east is positive and degrees west is negative
###timezone relative (in hours) to UTC
##timezone = -7 
##latitude = 41.8918
##longitude = -69.9611
###time of day is "sunrise" or "sunset"
##time_of_day = "sunrise"
##
###convert unix time to date-time str in the format "yyyy-mm-dd HH:MM:SS"
##date_time_string, date_time_obj = unix2datetime(unix_time) 
##year = date_time_string[0:4]
##month = date_time_string[5:7]
##day = date_time_string[8:10]
##hour = date_time_string[11:13]
###eastern time is UTC - 4
##local_hour = int(hour) - 4
##print("local hour", local_hour)
##minute = date_time_string[14:16]
##second = date_time_string[17:19]
##
###day format for new filepath will have to be in format ddd_mmm.nn
###use built-in python function to convert from known variables to new date
###timetuple() method returns tuple with several date and time attributes. tm_yday is the (attribute) day of the year
##day_of_year = int(datetime.date(int(year), int(month), int(day)).timetuple().tm_yday)
##
###Check for leap year. Denom is in equation for calculating gamma.
##if calendar.isleap(int(year)):
##    denom = 366
##else:
##    denom = 365
##
###fractional year, gamma = ((2pi)/denom)(day_of_year - 1 + ((hour - 12)/24))) in radians
##gamma = ((2*math.pi)/denom)*(int(day_of_year) - 1 + ((int(hour) - 12)/24))
##print("gamma", gamma)
##
###estimate equation of time (in minutes)
###eqtime = 229.18(0.000075 + 0.001868cos(gamma) – 0.032077sin(gamma) – 0.014615cos(2gamma)– 0.040849sin(2gamma))
##eqtime = 229.18*(0.000075 + (0.001868*math.cos(gamma)) - (0.032077*math.sin(gamma)) - (0.014615*math.cos(2*gamma)) - (0.040849*math.sin(2*gamma)))
##print("eqtime", eqtime)
##
###estimate solar declination angle (in radians)
###decl = 0.006918 – 0.399912cos(gamma) + 0.070257sin(gamma) – 0.006758cos(2γ) + 0.000907sin(2gamma) – 0.002697cos(3gamma) + 0.00148sin (3gamma)+
##decl = 0.006918 - (0.399912*math.cos(gamma)) + (0.070257*math.sin(gamma)) - (0.006758*math.cos(2*gamma)) + (0.000907*math.sin(2*gamma)) - (0.002697*math.cos(3*gamma)) + (0.00148*math.sin(3*gamma))
##print("decl", decl)
##
###calculate time_offset = eqtime + 4*longitude - 60*timezone , in minutes
###timezone relative to UTC. EST is -7 hours
##time_offset = eqtime + (4*longitude) - (60*timezone)
##print("time offset", time_offset)
##
###calculate true solar time, tst = hour*60 + minute + sc/60 + time_offset (use local hour for hour)
##tst = (local_hour*60) + int(minute) + (int(second)/60) + time_offset
##print("tst", tst)
##
###calculate solar hour angle, in degrees, ha = (tst/4) - 180
###ha = (tst/4) -180
###print("ha", ha)
##
###calculate solar zenith angle
###cos(phi) = sin(lat)sin(decl) + cos(lat)cos(decl)cos(ha)
###phi = acos( sin(latitude)sin(decl) + cos(lat)cos(decl)cos(ha))
##phi = math.acos(math.sin(latitude)*math.sin(decl) + math.cos(latitude)*math.cos(decl)*math.cos(ha))
###print("phi", phi)
##
###for case of sunrise/sunset, solar zenith angle, phi, is 90.833 degrees (approx. correction for
###atmospheric refraction at sunrise/sunset)
###hour angle, ha = +-arccos(cos(90.833)/(cos(latitude)cos(decl)) - tan(latitude)tan(decl))
###positive (+1) for sunrise, negative (-1) for sunset. Sign stored in variable.
##if time_of_day == "sunrise":
##    sign = 1
##elif time_of_day == "sunset":
##    sign = -1
##print("sign", sign)
###ha = sign*math.acos((math.cos(90.833)/(math.cos(latitude)*math.cos(decl))) - math.tan(latitude)*math.tan(decl))
###print("ha", ha)


