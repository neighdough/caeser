'''
This script checks all metadata for the Greenprint geoportal to make sure that all required elements exist prior to 
uploading it to the geoportal. If any required element is missing, it is updated with generic information depending
on the original provider of that particular dataset. 
'''

from lxml import etree
from datetime import date
import os
import re
import arcpy

xml_in_path = raw_input('XML in path: ')#r'\\gis4.memphis.edu\websites\HUD\Metadata\Modified'
xml_out_path = raw_input('XML out path: ')#r'\\gis4.memphis.edu\websites\HUD\Geoportal.gdb'

generic_abstract = '''This dataset was provided as is as part of the Mid-South Regional
                    Greenprint planning grant, a US Department of Housing and Urban Development funded project.
                     Data for this project was provided by a variety of organizations and is intended to be used
                     for general guidance and decision making by members of the Mid-South Greenprint Consortium.'''

generic_purpose = '''As no additional metadata were provided for this dataset, no record of 
                    its specific purpose or design is available. '''

generic_origin = ''

contact_CBANA = {'cntorg': 'Center for Community Building and Neighborhood Action',
                 'cntper': 'Phyllis Betts',
                 'addrtype': 'mailing and physical',
                 'address': 'University of Memphis, 4050 South Park Loop',
                 'city': 'Memphis',
                 'state': 'TN',
                 'postal': '38152',
                 'country': 'US',
                 'cntvoice':'9016782000',
                 'cntemail': 'pbetts@memphis.edu'}

contact_MSCOS = {'cntorg': 'Memphis and Shelby County Office of Sustainability',
                 'cntper': 'John Zeanah',
                 'addrtype':'mailing and physical',
                 'address':'125 N. Main, Rm 468',
                 'city': 'Memphis',
                 'state': 'TN',
                 'postal': '38103',
                 'country': 'US',
                 'cntvoice': '9015766601',
                 'cntemail': 'john.zeanah@memphistn.gov'}

today = date.today()
generic_pubdate = str(today)

progress_count = 1
for path, dir, files in os.walk(xml_in_path):
    for xml in files:
        print progress_count, ' of ', len(files), '... ', xml        
        xmlDoc = os.path.join(path,xml)#r'\\gis4.memphis.edu\websites\HUD\Metadata\BACKUP\AAA_Test.xml'
        parser = etree.XMLParser(remove_blank_text=True)
        docRoot = etree.parse(xmlDoc,parser).getroot()
        
        #set the generic contact information, CBANA or MSCOS
        if xml.split('_')[0] == 'CBANA':
            generic_origin = 'CBANA'
            contact = contact_CBANA
             
        else:
            generic_origin = 'Memphis and Shelby County Office of Sustainability'
            contact = contact_MSCOS
        
        #update idinfo and all required children
        idinfo = docRoot.find('idinfo')
        if docRoot.find('idinfo/descript') is None:
            descript = etree.SubElement(idinfo, 'descript')
        else:
            descript =  docRoot.find('idinfo/descript')
        if docRoot.find('idinfo/descript/abstract') is None:
            abstract = etree.SubElement(descript, 'abstract')
            abstract.text = generic_abstract
              
        if docRoot.find('idinfo/descript/purpose') is None:
            purpose = etree.SubElement(descript, 'purpose')
            purpose.text = generic_purpose    
         
        
        if docRoot.find('idinfo/citation/citeinfo') is None:
            citation = etree.SubElement(idinfo, 'citation')
            citeinfo = etree.SubElement(citation, 'citeinfo')
        else:
            citeinfo = docRoot.find('idinfo/citation/citeinfo')
        if docRoot.find('idinfo/citation/citeinfo/origin') is None:
            origin = etree.SubElement(citeinfo, 'origin')
            origin.text = generic_origin
        if docRoot.find('idinfo/citation/citeinfo/pubdate') is None:
            pubdate = etree.SubElement(citeinfo, 'pubdate')
            pubdate.text = generic_pubdate
        
        #update metainfo and all children
        if docRoot.find('metainfo/metc') is None:
            metc = etree.SubElement(docRoot.find('metainfo'), 'metc')
            cntinfo = etree.SubElement(metc, 'cntinfo')
            cntinfo.tag ='cntinfo'                           
        else:
            metc = docRoot.find('metainfo/metc')
        
        cntinfo = docRoot.find('metainfo/metc/cntinfo')
        #dictionary of cntinfo children, key value is root element, values are children
        cntinfo_children = {'cntorgp': ('cntorg', 'cntper'),
                            'cntaddr': ('addrtype', 'address', 'city', 'state', 'postal', 'country'),
                            'cntemail': '',
                            'cntvoice':''}
        
        for key in cntinfo_children.keys():
            if not key in (c.tag for c in cntinfo):
                child = etree.SubElement(cntinfo, key)
                #add child elements and values for cntinfo
                if len(cntinfo_children[key]) == 0:
                    child.text = contact[child.tag]
                for val in cntinfo_children[child.tag]:            
                    val = etree.SubElement(child, val)
                    if val.text is None:
                        val.text = contact[val.tag]      
         
        #split f into individual words, and create new keyword for each
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', xml.replace('.xml', ''))
        s1_list = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        s2 = s1_list.split('_')
        keyword_list = []
        for s in s2:
            if s != '':
                keyword_list.append(s)
                
        if docRoot.find('idinfo/keywords') is None:
            idinfo = docRoot.find('idinfo')
            etree.SubElement(idinfo, 'keywords')

        keywords = docRoot.find('idinfo/keywords')
        
        if docRoot.find('idinfo/keywords/theme') is None:
                etree.SubElement(keywords, 'theme')
        theme = keywords.find('theme')
            
        for keyword in keyword_list:
            etree.SubElement(theme, 'themekey').text = keyword
         
        with open(xmlDoc, 'w') as f:
            f.write(etree.tostring(docRoot, pretty_print=True))
        #Check the length of each dataset in order to determine how many characters to strip
        if len(xml.split('_')[0]) == 2:        
            fc = xml[3:].replace('.xml', '')            
        elif len(xml.split('_')[0]) == 3:        
            fc = xml[4:].replace('.xml', '')
        elif len(xml.split('_')[0]) == 4:
            fc = xml[5:].replace('.xml', '')
        else:
            fc = xml[6:].replace('.xml', '')
        out_xml = os.path.join(xml_out_path, xml.split('_')[0], fc)
        in_xml = os.path.join(xml_in_path, xml)
        print '\tImporting...'
        arcpy.MetadataImporter_conversion(in_xml, out_xml)
        print '\tUpgrading...',
        arcpy.UpgradeMetadata_conversion(out_xml, "FGDC_TO_ARCGIS")
        print '\tMetadata complete for ', fc        
        
        progress_count += 1
        
print 'Process complete!'
              
        