'''
Created on Feb 4, 2016

@author: nate
'''
from collections import defaultdict
import geopandas as gpd
import getopt
import json
import pandas as pd
import requests
from shapely.geometry import Point
import sys
import os
import argparse
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.chdir('/home/nate/dropbox-caeser/Data/CampusSchool')
addrs = {'records':[
                 {
                  'attributes':
                  {'Street':'61 s. cox st',
                   'City':'Memphis',
                   'State':'TN',
                   'Zip':'38104'}},
                  {
                  'attributes':
                  {'Street':'535 Zach H Curlin St',
                   'City':'Memphis',
                   'State':'TN',
                   'Zip':'38152'}
                  }]
         }
attrs = defaultdict()

def main(argv):
    infile = ''
    outfile = ''
    opts, args = getopt.getopt(argv, 'hi:o', ['ifile=', 'ofile='])
    for opt, arg in opts:
        if opt == '-h':
            print 'campus_geocoder.py -i <input> -o <output>'
        if opt in ('-i', '--ifile'):
            infile = arg
        if opt in ('-o', '--ofile'):
            outfile = arg
    return [infile, outfile]

def clean_address(df):
    cities = ('(memphis|cordova|germantown|collierville|bartlett|'
              'arlington|eads|millington|lakeland)')
    apts = ('(apt\.?|no\.?|unit|\#|suite|ste\.?|bldg|apartment)')
    df['street'] = df.ADDRESS.str.lower().str.split(cities).str[0]
    df['city'] = df.ADDRESS.str.lower().str.extract(cities)
    df.loc[df.city.isna(), "city"] = 'memphis'
    df['street'] = df.street.str.split(apts).str[0]
    
    state_zip = lambda x: df.ADDRESS.str.split(',').str[1].\
        str.strip().str.split(' ').str[x]

    df['state'] = state_zip(0)
    df['zip'] = state_zip(1)
    df.loc[df.state.isna(), "state"] = 'tn'
    df['objectid'] = df.index
    return df


def read_xls(inputfile, outputfile, sheetname=None):
    """creates json file from

    Args:
        inputfile: full path including extension to excel containing addresses
    Returns:
        None
    """
    sheetname = 'Sheet1' if not sheetname else sheetname
    addrlist = pd.read_excel(inputfile, sheet_name=sheetname)
    addrlist['long'] = None
    addrlist['lat'] = None
    addrlist['geom'] = None
    addrlist = clean_address(addrlist)
    url = ('https://gis5.memphis.edu/arcgis/rest/services/'
        'Geocoders/Midsouth_Composite/GeocodeServer/geocodeAddresses?')
    school = gpd.GeoSeries([Point(792099.5009925514, 308886.25072714686)])
    
    #addrlist.ADDRESS = addrlist.str.lower().str.split('Memphis').str[0]

    for i in addrlist.index:
        addrs = {'records':[]}
        addr = addrlist.ix[i].street
        city = addrlist.ix[i].city
        state = addrlist.ix[i].state
        zipcode = str(addrlist.ix[i].zip)
        obj = addrlist.ix[i].objectid

        row = {'Street':addr,
               'City':city,
               'State':state,
               'Zip':zipcode,
               'OBJECTID': obj}

        addrs['records'].append({'attributes':row})
        try:
            data = {'addresses':json.dumps(addrs).strip(),
                    'f':'json'}
            req = requests.post(url, data, verify=False)
#            req = requests.get(url+'?addresses={}&f=pjson'.
#                   format(json.dumps(addrs).strip()))
            resp = req.json()
            x = resp['locations'][0]['location']['x']
            y = resp['locations'][0]['location']['y']
            addrlist.loc[i,'long'] = x
            addrlist.loc[i,'lat'] = y
            addrlist.loc[i, 'geom'] = Point(x,y)
            dist = gpd.GeoSeries([Point(x,y)]).distance(school).values[0]/5280
            addrlist.loc[i, 'distance'] = dist
        except:
            print addrlist.ix[i]
    addrlist.drop(['long', 'lat', 'geom', 'objectid', 
                    'street', 'city', 'state', 'zip'],
                    axis=1, inplace=True)
    addrlist.to_csv(outputfile)


if __name__ == '__main__':
    desc = ('Add Euclidean distance to input addresses')    
    parser = argparse.ArgumentParser(description=desc,
            prefix_chars='-', add_help=True)
    parser.add_argument('-i', 
            help='Name of input excel file')
    parser.add_argument('-o', 
            help='Name of output csv, with extension')
    args = parser.parse_args()
    infile = args.i
    outfile = args.o
    # infile, outfile = main(sys.argv[1:])
    # print sys.argv[1:]
    # print outfile, raw_input()
    read_xls(infile, outfile)

