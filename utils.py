'''
Created on Mar 26, 2015

@author: nfergusn
'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function 
import collections
import csv
import os
import sys
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import json
import re


def geocode(addresses, index=None, columns=None):
    """

    Args:
        addresses (str,Pandas DataFrame): path to csv or Pandas DataFrame 
            containing addresses
        index (str): string denoting the name of the column to be used as an 
            index value. This value will be returned with matched records to 
            allow for joining to the original dataset.
        columns (list: str, optional): list containing string values for 
            columns to be used for geocoding
    Returns:
        List of Pandas DataFrames for both matched and unmatched address in 
            the form of [matched, unmatched]

    """
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    url = ("https://gis5.memphis.edu/arcgis/rest/services/"
                "Geocoders/Midsouth_Composite/GeocodeServer/"
                "geocodeAddresses?")
    addr = pd.read_csv(addresses) if type(addresses) == str else addresses.copy()
    if index:
        addr['OBJECTID'] = addr[index]
    else:
        addr['OBJECTID'] = addr.index
    if columns:
        addr.drop([col for col in addr.columns if col not in columns], 
                axis=1, inplace=True)
    addr.columns = [col.lower() for col in addr.columns]
    field_match = {'Street':['street', 'address', 'add', 'addr', 
                             'location_address'],
                    'City': ['city', 'place', 'town', 'fraction', 'city_name'],
                    'ZIP': ['zip_code', 'zip', 'zip5', 'postal_code'],
                    'State':['state', 'st', 'state_code'],
                    'OBJECTID': ['objectid', 'id', 'index', 'idx']}
    rename = dict()
    for col in addr.columns:
        new_field = [key for key, value in field_match.items() if col in value]
        if new_field:
            rename[col] = new_field[0]
    addr.rename(columns=rename, inplace=True)

    #add empty Series for any missing columns
    for col in addr.columns:
        if col not in field_match.keys():
            addr[col] = None

    addr_dict = addr.to_dict('records')
    len_addr_dict = len(addr_dict)
    req_limit = 1000 if len_addr_dict > 1000 else len_addr_dict
    results = []
    addr_dict_split = [addr_dict[x:x+req_limit] for x in 
                        range(0, len_addr_dict, req_limit)]
    chunks = len(addr_dict_split)
    #print 'Number of data segements: ', chunks
    start = 1
    for chunk in addr_dict_split:
     #   print '\t', start, ' of ', chunks
        start += 1
        req_recs = {'records':[{'attributes':rec} for rec in chunk]}      
        data = {'addresses':json.dumps(req_recs).strip(),
                'f':'json'}
        req = requests.post(url, data=data, verify=False)
        resp = req.json()
        for loc in resp['locations']:
            loc['attributes']['lat'] = loc['location']['y']
            loc['attributes']['lon'] = loc['location']['x']
        results.extend([i['attributes'] for i in resp['locations']])
    keep = []
    toss = []
    for result in results:
        if result['Score'] >= 70:
            keep.append(result)
        else:
            toss.append(result)
    return [pd.DataFrame.from_dict(keep), pd.DataFrame.from_dict(toss)]
    
def clean_address(addr, check=1):
    """
    Method to clean common address errors to increase match accuracy. Patterns
    that are evaluated include:
        +322 Scleaveland Ave
        +Pickett&Berra
        +123 N Main Apt 2
        +2547Walker Ave

    Args:
        addr (str): Address to be evaluated
        check (int): Value to determine which keys to use. the first regex
            should be checked as a last resort as it will mismatch any street
            name that begins with a cardinal direction (N,S,E,W) 
            (e.g. Winchester, Norris, etc.)
    Returns:
        clean_addr (str): Cleaned value if exists, otherwise, the same input
            string.
    """
    addr = str(addr) if type(addr) != unicode else addr

    apt_suffix = ['apt', 'suite', 'ste', '\#', 'unit', 'no', 'number', 'nu',
                    'apartment', ]
    rex = collections.OrderedDict([
            #322 Scleveland, 444 Nmain
            (r'[0-9]{1,}(\s)+(N|S|E|W)\.?([A-Za-z])', 
                r' \g<2> \g<3>'),
            #3547Walker Ave, 1420Gill
            (r'([0-9]{1,5})([A-Za-z]{1,})', 
                r'\g<1> \g<2>'),
            #Pickett&Berta, Winchester\\Elvis Presley 
            (r'([A-Za-z]+)(\&|\\\\|\@\s?)([A-Za-z\w]+)', 
                r'\g<1> and \g<3>'),
            #Mendenhall &  & Winchester
            (r'([A-Za-z0-9]+\s+)(\&\s*\&)(\s*[A-Za-z0-9]+)',
                r'\g<1>and\g<3>'),
            #3500 Block of Winchester
            (r'(?i)([0-9]{1,5}\s+)(block[\sof]*)(\s+[a-z]+)',
                r'\g<1>\g<3>'),
            #123 N Main Apt 2, 123 N Main #5
            (r'(\w\s)+(({})\.?\s+[0-9A-Za-z]+)'.format('|'.join(apt_suffix)), 
                r'\g<1>')])
    clean_addr = addr 
    regs = rex.keys()[1:] if check == 1 else rex.keys()
    for r in regs:
        if re.search(r, addr):
            clean_addr = re.sub(r, rex[r], addr)
    return clean_addr.strip()


def inflate(year_from, year_to, value):
    """
    returns inflation adjusted value
    year_from (str)-> year dollar is to be converted from
    year_to (str)-> year dollar is to be converted to
    value (float)-> value to be converted
    """
    if os.name == 'posix':
        os.chdir('/home/nate/source/Resources')
    else:
        os.chdir('E:\cloud\Dropbox\Workspaces\Python\caeser\Resources')
    cpi = pd.read_csv('cpi_1913-2015.csv', dtype={'Year': np.str})
    cpi.index = cpi.Year
    return value * (cpi.ix[year_to].Avg / cpi.ix[year_from].Avg)

def get_cpi(year_from, year_to):
    """
    returns cpi values for each year for calculation outside of utils module
    Args:
        year_from (str) -> year dollar is to be converted from
        year_to (str)-> year dollar is to be converted to
    """
    if os.name == 'posix':
        os.chdir('/home/nate/source/Resources')
    else:
        os.chdir('E:\cloud\Dropbox\Workspaces\Python\caeser\Resources')
    cpi = pd.read_csv('cpi_1913-2015.csv', index_col='Year')
    return [cpi.ix[year_from].Avg, cpi.ix[year_to].Avg]

def cursor_to_namedtuple(cursor):
    """
    modified from https://gist.github.com/jasonbot/3100403
    """
    Row = collections.namedtuple('Row', cursor.fields)
    for row in cursor:
        yield Row(*row)

def dbf_to_csv(filename):
    from dbfpy import dbf
    """
    converts dbf to csv
    filename -> full path to dbf
    """
    if filename.endswith('.dbf'):
        print("Converting %s to csv" % filename)
        csv_fn = filename[:-4]+ ".csv"
        with open(csv_fn,'wb') as csvfile:
            in_db = dbf.Dbf(filename)
            out_csv = csv.writer(csvfile)
            names = []
            for field in in_db.header.fields:
                names.append(field.name)
            out_csv.writerow(names)
            for rec in in_db:
                out_csv.writerow(rec.fieldData)
            in_db.close()
            print("Done...")
    else:
        print("Filename does not end with .dbf")

def pct_change(year1, year2):
    return (year2 - year1)/year1

def connection_properties(host, user='postgres', port='5432', db=None):
    """
    reads pgpass file from computer and uses to connect to postgres database
    returns dictionary with connection parameters assuming ssh tunnel that redirects
    from port 5432 to port 2222

    {'host':host name,
    'port': port,
    'db': database,
    'user': user,
    'password': password}

    """
    if os.name == 'posix':
        pgpass = ''.join([os.getenv('HOME'), '/.pgpass'])
    else:
        pgpass = ''.join([os.path.join(os.getenv('APPDATA'), 'postgresql'), '\\pgpass.conf'])
    with open(pgpass, 'r') as p:
        r = csv.reader(p, delimiter = ':')
        for row in r:
            if [row[0], row[1], row[3]] == [host, port, user]:
                cur_db = row[2] if not db else db
                return {'host':row[0],
                        'port': row[1],
                        'db': db,
                        'user': row[3],
                        'password': row[4]}

def connect(host, user, db, port='5432', password=None):
    parms = connection_properties(host, user, port)
    """sqlalchemy connection string in format
    dialect+driver://username:password@host:port/database"""
    cnxstr = 'postgresql://{0}:{1}@{2}:{3}/{4}'
    engine = create_engine(cnxstr.format(user, parms['password'], host,
                                         parms['port'],db))
    return engine

def pandas_to_arc(df,
                  workspace_path,
                  output_table,
                  keep_index=True,
                  cols=None,
                  get_cursor=False,
                  overwrite=False):
    """
    Used to export a pandas data frame to an ArcGIS table.
    Parameters:
    ----------
    df: pandas.DataFrame
        Data frame to export.
     workspace_path: string
        Full path to ArcGIS workspace location.
    output_table: string
        name of the output table.
    keep_index: bool, optional, default True
        If True, column(s) will be created from the index.
    cols: list <string>, optional, default None:
        List of fields/columns to include in output, if not provided
        all fields will be exported. Also, include index names here.
    get_cursor: bool, optional, default False
        If True, returns dictionary with field info and an
        arcpy search cursor.
    overwrite: bool, optional, default False
        If True, an existing table will be overwritten. If False,
        and a table already exists, an error will be thrown. Note:
        ArcGIS is sometime weird about schema locks, so an exception
        could potentially be thrown if there is an outstanding cursor.
    Returns
    -------
    out_flds: dictionary<string,int>
        Dictionary of field names in the result, keys are field names
        and values are indexes in the row.
    rows: iterator of tuples
        Returns the results of arcpy.da.SearchCursor() on the exported result.
    """
    import arcpy
    import pandas as pd
    import numpy as np

    # push the index into columns
    if keep_index:
        df = df.reset_index()

    # put the pandas series into a dictionary of arrays
    arr_values = {}
    arr_dtypes = []

    if cols is None:
        cols = df.columns

    for col in cols:
        arr = df[col].values

        # convert types to make ArcGIS happy
        if arr.dtype == np.object:
            arr = arr.astype(unicode)
        if arr.dtype == np.int64:
            max_val = arr.max()
            min_val = arr.min()
            if min_val < -2147483647 or max_val > 2147483647:
                arr = arr.astype(np.float64)
            else:
                arr = arr.astype(np.int32)
        if arr.dtype == np.bool:
            arr = arr.astype(np.int32)

        arr_values[col] = arr
        arr_dtypes.append((col, arr.dtype))

    # create the structured array
    s_arr = np.empty(len(df), dtype=arr_dtypes)
    for col in arr_values:
        s_arr[col] = arr_values[col]

    # now export to arc
    old_workspace = arcpy.env.workspace
    arcpy.env.workspace = workspace_path

    if overwrite:
        # delete existing table it if it exists
        if output_table in arcpy.ListTables():
            arcpy.Delete_management(output_table)

    arcpy.da.NumPyArrayToTable(s_arr, workspace_path + "/" + output_table)
    if get_cursor:
        fld_names = []
        out_flds = {}
        fld_idx = 0
        for curr_fld in arcpy.ListFields(output_table):
            fld_names.append(curr_fld.name)
            out_flds[curr_fld.name] = fld_idx
            fld_idx += 1
        rows = arcpy.da.SearchCursor(output_table, fld_names)
    else:
        out_flds = None
        rows = None

    # return the results
    if old_workspace is not None:
        arcpy.env.workspace = old_workspace
    return out_flds, rows

if __name__ == '__main__':
    pass
