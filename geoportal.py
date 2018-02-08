import os, sys
sys.path.append('/home/geonode/caeser_geonode')
os.path.append("DJANGO_SETTINGS_MODULE", "caeser_geonode.settings")
from geonode.layers.models import Layer, Attribute
import pandas as pd
from caeser import utils
from sqlalchemy import create_engine
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize
import string


stopwords = stopwords.words('english')

sources = {"hudhads":
                ("U.S. Department of Housing and Urban Development, "
                    "Housing Affordability Data System"),
            "caeser":
                ("Center for Applied Earth Science and Engineering Research, "
                    "University of Memphis"),
            "mscos":
                ("Memphis and Shelby County Office of Sustainability, "
                    "Mid-South Regional Greenprint"),
            "esri":
                "Environmental Systems Research Institute",
            "fars":
                ("Source: National Highway Traffic Safety Administration "
                    "Fatality Analysis Reporting System"),
            "lehd":
                "U.S. Census Bureau, Longitudinal Employer-Householder Dynamics",              
            "mata":
                "Memphis Area Transit Authority",
            "mpo":
                "Memphis Urban Area Metropolitan Planning Organization",
            "nhd":
                "U.S. Geological Survey, National Hydrography Dataset",
            "sca2016":
                "2016 Shelby County Assessor''s Certified Roll", 
            "fcc":
                "Federal Communications Commission; 08/19/2016", 
            "hudtai":
                ("U.S. Department of Housing and Urban Development, Transit " 
                    "Access Index"),
            "cnt":
                ("Center for Neighborhood Technology, "
                    "Housing + Transportation Index"),
            "regis":
                "Shelby County Regional GIS (ReGIS)",
            "epaurl":
                "U.S. Environmental Protection Agency, Uniform Resource Locator",         
            "sca2014":
                "2014 Shelby County Assessor''s Certified Roll",
            "epaaqs":
                "U.S. Environmental Protection Agency Air Quality System",    
            "cblehd":
                "U.S. Census Bureau, Longitudinal Employer-Householder",
            "usgbc":
                "U.S. Green Building Council", 
            "acs14":
                "U.S. Census Bureau; American Community Survey, 2010-2014",    
            "acs14mdn":
                ("U.S. Census Bureau; American Community Survey, "
                    "2010-2014 (3.17)"),
            "mlgw":
                "Memphis Light, Gas, and Water", 
            "epaasf":
                ("U.S. Environmental Protection Agency Air Quality System, "
                    "Annual Summary File 2016"), 
            "acs13":
                "U.S. Census Bureau; American Community Survey, 2009-2013",
            "acs13mdn":
                "U.S. Census Bureau; American Community Survey, 2009-2013 (3.17)",
            "fema":
                ("Federal Emergency Management Agency Flood Map Center; "
                    "2104 Shelby County Assessor''s Certified Roll"),
            "sca0414":
                "2004, 2014 Shelby County Assessor''s Certified Roll",
            "sca0416":
                "2004, 2016 Shelby County Assessor''s Certified Roll"}

def get_keywords(row):
    """
    Parse field_desc, description, and title columns from WWL data_sources
    and data_dictionary tables using NLTK to remove stop words.

    Args:
        table_desc (list: string): contains list comprised of strings 
            containing field description, variable description, and category 
            title from livability database.
    Returns:
        keywords (list: string): list of individual keywords to be loaded into
            layer metadata.
    """
    #TODO:
        #split description and title to build keywords for layer
    remove = stopwords + [p for p in string.punctuation]
    splitter = lambda x: [y.lower() for y in word_tokenize(x) if y not in remove]
    field_desc = splitter(row[1])
    title = splitter(row[4])
    return list(set(field_desc + title))


def update_layer_metadata(layer):
    """
    
    """
    print 'Updating metadata for ', layer.name
    geography, tbl_name = layer.name.split('_')[2:]
    tbl_name = layer.name.split('_')[-1]
    abstract_vals = {'layer':layer.name,
                     'source':sources[tbl_name],
                     'geography': geography
                     }
    abstract = ("{layer} is a selection of data pulled from the Community "
                "Foundation of Greater Memphis' WHEREweLIVE.midsouth.org "
                "website. The original source of the data is {source} and has "
                "been aggregated or summarized at the {geography} level and "
                "grouped with other variables from the same source for "
                "distribution. Aggregation or other processing was performed by"
                "the Center for Applied Earth Science and Engineering "
                "Research (CAESER) at the Herff College of Engineering at the "
                "University of Memphis.")

    purpose = ("{layer} is intended to allow individuals or organizations "
               "to understand community health from a variety of perspectives "
               "and to compare individual communties to other geographic areas."
               "Each variable has been summarized so that it can be compared "
               "against other geographic regions or locations without "
               "sacrificing scale.")

    field_desc = ("select field, field_desc, description, source, title "
                    "from data_dictionary "
                    "left join data_sources on descid = citation "
                    "where source = '{source}'")
    rows = cnx.execute(field_desc.format(**abstract_vals)).fetchall()
    keywords = list()
    attributes = {attribute.name: 
                    attribute for attribute in layer.attributes.all()}
    for row in rows:
        if row[0] in attributes.keys():
            attributes[row[0]].description = row[1]

        keywords.extend(get_keywords(row))
    layer.keywords.add(*keywords)
    layer.abstract = abstract.format(**abstract_vals)
    layer.purpose = purpose.format(**abstract_vals)
    layer.save()

def update_metadata(db):
    """
    """

    params = utils.connection_properties('caeser-geo.memphis.edu', db=db)
    cnxstr = "postgresql://{user}:{password}@{host}/{db}"
    engine = create_engine(cnxstr.format(**params), echo=True)
    cnx = engine.connect()
    
    lyrs = Layer.objectcs.filter(store=db)
    for lyr in lyrs:
        update_layer_metadata(lyr)


def build_wwl_views(db):
    """

    Args:
        year (string): date string for database year to be processed in format
            YYYY. Used to create connection parameters.
    Returns:
        None
    """
    params = utils.connection_properties('caeser-geo.memphis.edu', db=db)
    cn_str = 'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(cn_str.format(**params))
 
    conn = engine.connect()
    db_year = db.split('_')[-1]
    for geog in ['county', 'zip', 'tract', 'place', 'msa']:
        year = '2013' if geog == 'msa' else '2010'
        q_summary_fields = ("select column_name from information_schema.columns "
                    "where table_name = 'summary_cen_{0}_{1}'")
        summary_fields = [col[0] for col in conn.execute(\
                    q_summary_fields.format(geog, year)).fetchall()]

        for source, desc in sources.items():
            q_fields = ("select field from data_dictionary, data_sources "
                        "where descid = citation and source = '{}'")
            fields = conn.execute(q_fields.format(desc)).fetchall()
            #grab all columns from summary table to make sure columns exist before
            #executing query for view
            select_fields = ", ".join([f[0] for f in fields if f[0] in summary_fields])
            if select_fields:
                strs = {'geog':geog,
                        'fields':select_fields,
                        'source':source,
                        'year': year,
                        'db_year':db_year}
                q = ("create or replace view public.wwl_{db_year}_{geog}_{source} as "
                        "select row_number() over() geonodeid, g.geoid10, g.name10, "
                        "{fields}, wkb_geometry "
                        "from summary_cen_{geog}_{year} s "
                        "join geography.cen_{geog}_{year} g on g.geoid10 = s.geoid10")
                conn.execute(q.format(**strs))

func_arg = {'-build': build_wwl_views,
        '-update': update_metadata}

if __name__=='__main__':
    func_arg[sys.argv[1]](sys.argv[2])

