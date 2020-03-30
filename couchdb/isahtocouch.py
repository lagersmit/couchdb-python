# Copyright (c) 2019, Lagersmit B.V

"""
CLIENT - ISAH - CouchDB interface for Lagersmit Production Server
"""

"""
COMMON Packages
"""
from datetime import datetime
import json
import numpy as np
import pyodbc
import pandas
import hashlib
import base64
import re
import mimetypes

from couchdb import http, json, util
from couchdb import Document, Server

"""
Object of necessary server connections.
"""

from_date = "2020-01-27"

server = {
            'LAG-ISAH': pyodbc.connect(
                        "Driver={SQL Server Native Client 11.0};"
                        "Server= LAG-ISAH\ISAHMSSQL;"
                        "Database=LagersmitDB;"
                        "uid=powerbi;pwd=Lagersmit#1"
                        ),
            'couch': Server('http://admin:supreme@couchdb.lagersmit.com:5984')
            }

queries = {
            'seals': "SELECT dbo.T_SERVObject.LastUpdatedOn, dbo.T_SERVObject.ServObjectCode, dbo.T_SERVObject.ServObjectGrpCode, dbo.T_SERVObject.ServObjectTypeCode, dbo.T_SERVObject.Description, dbo.T_SERVObject.CreDate, dbo.T_SERVObject.InstDate, dbo.T_SERVObject.BasicMat, dbo.ST_IHC_ObjKnmk.Vesselsname, dbo.ST_IHC_ObjKnmk.OwnerCountryCode, dbo.ST_IHC_ObjKnmk.ImoCode, dbo.ST_IHC_ObjKnmk.Owner, dbo.T_DossierMain.OrdNr, dbo.T_ServObject.remark, dbo.T_ServObject.info, dbo.T_ServObject.PartCode, dbo.T_ServObject.DossierCode, dbo.T_ServObject.BasicMat, dbo.T_ServObject.Stand, dbo.T_ServObject.WarrStartDate, dbo.T_ServObject.WarrEndDate FROM dbo.T_SERVObject LEFT JOIN dbo.ST_IHC_ObjKnmk ON dbo.ST_IHC_ObjKnmk.ServObjectCode = dbo.T_SERVObject.ServObjectCode LEFT JOIN dbo.T_DossierMain ON dbo.T_DossierMain.DossierCode = dbo.T_ServObject.DossierCode WHERE (dbo.T_SERVObject.ServObjectCode LIKE 'X%' OR dbo.T_SERVObject.ServObjectCode LIKE 'B%' OR dbo.T_SERVObject.ServObjectCode LIKE 'L%') AND dbo.T_SERVObject.CreDate > '" + from_date + "'",
            'part':"SELECT T_Part.LastUpdatedOn, T_Part.PartCode AS part_code, T_Part.PurLocationCode AS location, T_VendorAddress.Name AS vendor, T_Part.VendPartCode AS vendor_part_code, T_Part.VendId AS vendor_id, T_Part.Description AS description, T_Quality.Description AS material, T_DocumentMain.DocPathName, T_DocumentMain.DocRefWords, T_DocumentMain.RevisionNr FROM T_Part LEFT JOIN T_DocumentMain ON T_Part.PartCode = T_DocumentMain.DocCode LEFT JOIN T_VendorAddress ON T_VendorAddress.VendId = T_Part.VendId LEFT JOIN T_Quality ON T_Quality.QualityCode = T_Part.QualityCode WHERE T_Part.LastUpdatedOn > '" + from_date + "'"
        }

uniqueKey = {
            'seals':'ServObjectCode',
            'part':'part_code'
        }

""" 
REQUIREMENTS:
--------------------------------

  * A source (ERP) server connection has to be defnined in the 'server' object 
    above
  * A couchdb connection has to be defined in the 'server' object above
  * A target (database) MUST be specified when calling Interpreter() (i.e. the 
    target couchdb e,g, seals, reports, vessels, engineers, etc...)
  * An interpretation query that gets source (ERP) db entries has to be 
    defined in the 'queries' object above. The key name for this query MUST
    be the same as the target db name.
"""        


class Interpreter(object):
    def __init__(self,target=None):
        self.db = []
        self.target = target
        self.couchObjects = {}
        self.isahObjects = []
        self.outOfDate = []
        self.newItems = []
        self.connect(target)
        self.getCouchObjects()
        self.getIsahObjects()
    
    
    
    def connect(self, target):
        self.target = target
        self.db = server['couch'][target]
        
    def getCouchObjects(self):
        for row in self.db.view('_design/index/_view/object'):
            self.couchObjects.update({
                                row['value']['object']:{
                                        'id':row['id'],
                                        'modified':row['value']['modified']
                                        }
                                })
            
    def uploadDocument(self,path,description='',rev='',tags=[],suffix=''):
        docDB = server['couch']['documents']
        
        # Open file at path and convert to Base64 string
        with open(path, "rb") as f:
            docstring = base64.b64encode(f.read()).decode('unicode_escape')
            
        # Generate document hashkey
        hashKey = hashlib.md5(open(path,'rb').read()).hexdigest()
        
        # Create new document structure
        decodedPath = re.findall("([\w\s_-]*)\.([\w]{2,4})$",path,re.IGNORECASE)[0]
        
#        mime = magic.Magic(mime=True)
#        content_type = mime.from_file(path)
        content_type = mimetypes.guess_type(path)[0] if mimetypes.guess_type(path)[0] else 'unknown'
        newContent = {
                '_id':hashKey + '_' + decodedPath[0] + suffix,
                'data':{
                    'document':{
                        'original_path':path,
                        'description':description,
                        'size':int(),
                        'filename':decodedPath[0],
                        'format':decodedPath[1],
                        'revision':rev,
                        'tags':tags
                        }
                    },
                '_attachments':{
                        "document":{
                                "content_type":content_type,
                                "data":docstring,
                                }
                        }
                }
                        
        # If the same document is already in db, do nothoing, else create           
        if not(newContent['_id'] in docDB):
            docDB.save(newContent)
        else:
            docDB[newContent['_id']]['_attachments']['document']['data'] = newContent['_attachments']['document']['data']

        # Return document file name and extension
        return (decodedPath[0], decodedPath[1], newContent['_id'], newContent)
    
    def getIsahObjects(self):
        self.isahObjects = self.__run_isah_query__(queries[self.target])
    
    def __object_in_couch__(self,objectId):
        if objectId in self.couchObjects:
            return self.couchObjects[objectId]
        else:
            return False
    
    def __run_isah_query__(self,query):
        data = pandas.read_sql_query(query, server['LAG-ISAH'])
        columnNames = list(data.columns.values)
        dataOut = []
        for index, row in data.iterrows():
            newLine = {}
            for col in columnNames:
                newLine.update({col:row[col]})
            dataOut.append(newLine)
        return dataOut
    
    def __out_of_date__(self):
        for obj in self.isahObjects:
            try:
                if datetime.strptime(str(obj['LastUpdatedOn']),"%Y-%m-%d %H:%M:%S.%f").timestamp() > datetime.strptime(self.couchObjects[obj[uniqueKey[self.target]].strip()]['modified'],"%Y-%m-%dT%H:%M:%S.%f").timestamp():
                    self.outOfDate.append(obj)
            except Exception as error:
                print('Exception raised for ' + obj[uniqueKey[self.target]] + ': ' + repr(error))
                if self.couchObjects:
                    if not obj[uniqueKey[self.target]].strip() in self.couchObjects.keys():
                        self.newItems.append(obj)
                    else:
                        self.outOfDate.append(obj)
                else:
                    self.newItems.append(obj)
                        
    def upsertAll(self):
        self.getCouchObjects()
        self.getIsahObjects()
        self.__out_of_date__()
        for item in self.newItems:
            doc = {
                    "_id":self.target + "_" + str(datetime.now().isoformat()),
                    "data":{self.target:{}},
                    "modified":str(datetime.now().isoformat())
                    }
            for key in item.keys():
                if not key.startswith('Doc') and not key.startswith('Rev'):
                    doc['data'][self.target][key] = str(item[key]).strip()
                else:
                    if key == "DocPathName":
                        if item['DocPathName']:
                            try:
                                result = self.uploadDocument(path=item['DocPathName'].strip(),description=item['description'],rev=item['RevisionNr'],tags=[item['DocRefWords']])
                                doc['data'][self.target]['documents'] = [{'id':result[2],'description':item['description'],'attachment_type':'Part Document'}]
                            except Exception as err:
                                print(repr(err) + ': document upload error')    
                    
            self.db.upsert(doc)
        self.newItems = []
        for item in self.outOfDate:
            doc = self.db.get(item[uniqueKey[self.target]])
            if doc:
                for key in doc.keys():
                    doc[key]= item[key]
                    self.db.upsert(doc)
        self.outOfDate = []