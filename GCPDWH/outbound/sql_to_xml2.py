'''
Created on April 4, 2019
Created By: Atul Guleria
This script takes SQL query as argument and stores the SQL result to a XML file in a bucket
'''

from time import gmtime, strftime
from google.cloud import bigquery
import argparse
import logging
import os
from datetime import datetime, timedelta
from google.cloud import storage
import pandas
import xml.etree.cElementTree as ET

now = datetime.now()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='C:/Softwares/AAAData-69668e42a7cf.json'
cwd = os.path.dirname(os.path.abspath(__file__))
if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    xml_folder = cwd[:folderup]+"\\outbound\\xml_data\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"
    xml_folder = cwd[:folderup]+"/outbound/xml_data/"
    
#Header tags for the XML as column names from SQL file
col_name=["SVC_FACL_ID", "TRK_ID", 'TRK_DRIVR_ID'] 
#SQL query for creating XML
sql = 'SELECT SVC_FACL_ID,TRK_ID,TRK_DRIVR_ID FROM LANDING.ERS_STAGE_SERVICE_TRUCK_bkp LIMIT 10'
#Reading all data from SQL in pandas Dataframe
df= pandas.read_gbq(sql, project_id='aaadata-181822', dialect='standard')
i = 0
#Creating root elements and record tags for XML
root = ET.Element("root")
doc = ET.SubElement(root, "detail_records")
#Iterating through all rows in Dataframe
while (i < len(df.index)):     
   doc = ET.SubElement(root, "MEMBERSHIP")
#Creating all the subelements as rows and columns from DStaframe   
   for (col,c) in zip(df.columns,col_name):
       ET.SubElement(doc, c).text = str(df[col][i])  
       tree = ET.ElementTree(root)
       tree.write(xml_folder+"filename.xml")            
   i = i+1    
   
   
    
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
# for index in df.iterrows():
#    print (df['SVC_FACL_ID'])         
#     for c in col:
#         root = ET.Element("root")
#         doc = ET.SubElement(root, "doc")
#  
#         ET.SubElement(doc, "field1", name="blah").text = "some value1"
#         ET.SubElement(doc, "field2", name="asdfasd").text = "some vlaue2"
#  
#         tree = ET.ElementTree(root)
#         tree.write(xml_folder+"filename.xml")