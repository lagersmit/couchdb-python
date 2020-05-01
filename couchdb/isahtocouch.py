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
        self.schema = {
                        'parts':{
                                'type':'object',
                                'title':'part',
                                'couchdb':'parts',
                                'uniqueKey':'part_code',
                                'query':"SELECT T_Part.LastUpdatedOn, T_Part.PartCode AS part_code, T_Part.PurLocationCode AS location, T_VendorAddress.Name AS vendor, T_Part.VendPartCode AS vendor_part_code, T_Part.VendId AS vendor_id, T_Part.Description AS description, T_Quality.Description AS material, T_DocumentMain.DocPathName, T_DocumentMain.DocRefWords, T_DocumentMain.RevisionNr FROM T_Part LEFT JOIN T_DocumentMain ON T_Part.PartCode = T_DocumentMain.DocCode LEFT JOIN T_VendorAddress ON T_VendorAddress.VendId = T_Part.VendId LEFT JOIN T_Quality ON T_Quality.QualityCode = T_Part.QualityCode WHERE T_Part.LastUpdatedOn > '2020-04-25'",
                                'properties':{
                                        "part_code": {
                                                'type':'string',
                                                'fcn':lambda row: row['part_code'].strip()
                                                },
                                        "location": {
                                                'type':'string',
                                                'fcn':lambda row: row['location'].strip()
                                                },
                                        "vendor": {
                                                'type':'string',
                                                'fcn':lambda row: row['vendor'].strip()
                                                },
                                        "vendor_part_code": {
                                                'type':'string',
                                                'fcn':lambda row: row['vendor_part_code'].strip()
                                                },
                                        "vendor_id": {
                                                'type':'string',
                                                'fcn':lambda row: row['vendor_id'].strip()
                                                },
                                        "description": {
                                                'type':'string',
                                                'fcn':lambda row: row['description'].strip()
                                                },
                                        "material": {
                                                'type':'string',
                                                'fcn':lambda row: row['material'].strip()
                                                },
                                        "documents":{
                                                'type':'array',
                                                'fcn':lambda row: [self.getDocument(row['DocPathName'].strip(),row['description'].strip(),row['RevisionNr'].strip(),row['DocRefWords'])] if row['DocPathName'] else []
                                                },
                                        'created':{
                                                'type':'string',
                                                'fcn':lambda row: str(datetime.now().isoformat())
                                                },
                                        'modified':{
                                                'type':'string',
                                                'fcn':lambda row:  str(datetime.now().isoformat())
                                                }
                                        }
                                },
                            'objects':{
                                    'type':'object',
                                    'title':'seal',
                                    'couchdb':'objects',
                                    'uniqueKey':'ServObjectCode',
                                    'query':"SELECT dbo.T_SERVObject.LastUpdatedOn, dbo.T_SERVObject.ServObjectCode, dbo.T_SERVObject.ServObjectGrpCode, dbo.T_SERVObject.ServObjectTypeCode, dbo.T_SERVObject.Description, dbo.T_SERVObject.CreDate, dbo.T_SERVObject.InstDate, dbo.T_SERVObject.BasicMat, dbo.ST_IHC_ObjKnmk.Vesselsname, dbo.ST_IHC_ObjKnmk.OwnerCountryCode, dbo.ST_IHC_ObjKnmk.ImoCode, dbo.ST_IHC_ObjKnmk.Owner, dbo.T_DossierMain.OrdNr, dbo.T_ServObject.remark, dbo.T_ServObject.info, dbo.T_ServObject.PartCode, dbo.T_ServObject.DossierCode, dbo.T_ServObject.BasicMat, dbo.T_ServObject.Stand, dbo.T_ServObject.WarrStartDate, dbo.T_ServObject.WarrEndDate FROM dbo.T_SERVObject LEFT JOIN dbo.ST_IHC_ObjKnmk ON dbo.ST_IHC_ObjKnmk.ServObjectCode = dbo.T_SERVObject.ServObjectCode LEFT JOIN dbo.T_DossierMain ON dbo.T_DossierMain.DossierCode = dbo.T_ServObject.DossierCode WHERE dbo.T_SERVObject.CreDate > '2020-04-01'",
                                    'properties':{
                                             "id": {
                                                'type':'string',
                                                'fcn':lambda row: row['ServObjectCode'].strip()
                                                },
                                             "lsnumber": {
                                                'type': 'string',
                                                'fcn':lambda row: row['OrdNr'].strip()
                                                },
                                              "part_code": {
                                                'type':'string',
                                                'fcn':lambda row: row['PartCode'].strip()
                                                },
                                              "dossier_code": {
                                                'type':'string',
                                                'fcn':lambda row: row['DossierCode'].strip() if row['Stand'] else ""
                                                },
                                              "basic_mat": {
                                                'type':'string',
                                                'fcn':lambda row: ""
                                                },    
                                              "stand": {
                                                'type':'string',
                                                'fcn':lambda row: row['Stand'].strip() if row['Stand'] else ""
                                                },
                                              "warranty": {
                                                'type':'object',
                                                'fcn':lambda row: {'start':row['WarrStartDate'].to_pydatetime().isoformat() if row['WarrStartDate'] else "",'end':row['WarrEndDate'].to_pydatetime().isoformat() if row['WarrEndDate'] else ""}
                                                },
                                              "registration": {
                                                'type':'array',
                                                'fcn':lambda row: [{'date':str(datetime.now().isoformat()),'id':'','reg_id':row['ImoCode'].strip()}] if row['ImoCode'] else []
                                                },
                                              "size": {
                                                'type':'number',
                                                'fcn':lambda row: self.getSealIdentifiers(row['Description'].strip())['size']
                                                },
                                              "order_date": {
                                                'type':'date',
                                                'fcn':lambda row: row['CreDate'].to_pydatetime().isoformat()
                                                },
                                              "install_date": {
                                                'type':'date',
                                                'fcn':lambda row: row['InstDate'].to_pydatetime().isoformat()
                                                },
                                              "memo": {
                                                'type':'string',
                                                'fcn':lambda row: row['info'].strip() if row['info'] else ""
                                                },
                                              "remark": {
                                                'type':'string',
                                                'fcn':lambda row: row['remark'].strip() if row['remark'] else ""
                                                },
                                              "location": {
                                                'type':'string',
                                                'fcn':lambda row: self.getLocation(row['ServObjectCode'].strip(),self.getSealIdentifiers(row['Description'].strip()))
                                                },
                                              "option_letters": {
                                                'type':'array',
                                                'fcn':lambda row: self.getSealIdentifiers(row['Description'].strip())['option_letters'].split("-")
                                                },
                                              "product_group": {
                                                'type':'string',
                                                'fcn':lambda row: self.getSealIdentifiers(row['Description'].strip())['product_group']
                                                },
                                              "type_code":  {
                                                'type':'string',
                                                'fcn':lambda row: row['ServObjectTypeCode'].strip()
                                                },
                                              "type_group": {
                                                'type':'string',
                                                'fcn':lambda row: row['ServObjectGrpCode'].strip(),
                                                },
                                              "type": {
                                                'type':'string',
                                                'fcn':lambda row: self.getSealIdentifiers(row['Description'].strip())['product_type']
                                                },
                                              "lipseals": {
                                                'type':'array',
                                                'fcn':lambda row: []
                                                },
                                              "bom": {
                                                'type':'array',
                                                'fcn':lambda row: self.getStructure(row['ServObjectCode'].strip())
                                                },
                                              "documents": {
                                                'type':'array',
                                                'fcn':lambda row: [self.getDocument(doc['DocPathName'], doc['Description'],doc['RevisionNr'],[doc['DocRefWords'],doc['Remark']]) for doc in self.__run_isah_query__("SELECT dbo.T_DocumentMain.DocPathName, dbo.T_DocumentMain.RevisionNr, dbo.T_DocumentMain.DocCode, dbo.T_DocumentMain.Description, dbo.T_DocumentMain.DocRefWords, dbo.T_DocumentMain.Remark FROM dbo.T_DocumentMain WHERE dbo.T_DocumentMain.DocId IN (SELECT dbo.T_DocumentDetail.DocId FROM dbo.T_DocumentDetail WHERE dbo.T_DocumentDetail.IsahPrimKey = '" + row['ServObjectCode'].strip() + "')")]
                                                },
                                              "readouts": {
                                                'type':'array',
                                                'fcn':lambda row: []
                                                },
                                              "lipsealbook": {
                                                'type':'array',
                                                'fcn':lambda row: []
                                                },
                                              'created':{
                                                    'type':'string',
                                                    'fcn':lambda row: str(datetime.now().isoformat())
                                                    },
                                              'modified':{
                                                    'type':'string',
                                                    'fcn':lambda row:  str(datetime.now().isoformat())
                                                    }
                                            }
                                    }
                        }
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
        self.db = server['couch'][self.schema[self.target]['couchdb']]
        
    def getCouchObjects(self):
        for row in self.db.view('_design/index/_view/object'):
            if row['value']:
                self.couchObjects.update({                   
                    row['value']['object']:{
                                'id':row['id'],
                                'modified':row['value']['modified']
                                }
                        })
            else:
                print(row['id'])
            
    def __upload_document__(self,path,description='',rev='',tags=[],suffix=''):
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
    
    def getDocument(self,path,description,rev,tags):
        try:
            result = self.__upload_document__(path=path,description=description,rev=rev,tags=[tags])
            doc = {'id':result[2],'description':description,'attachment_type':'Isah Document'}
        except Exception as err:
            print(repr(err) + ': document upload error')
            doc = {'id':path,'description':"Failed upload",'attachment_type':'Unknown attachment'}
        return doc
    
    def getIsahObjects(self):
        self.isahObjects = self.__run_isah_query__(self.schema[self.target]['query'])
    
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
                if datetime.strptime(str(obj['LastUpdatedOn']),"%Y-%m-%d %H:%M:%S.%f").timestamp() > datetime.strptime(self.couchObjects[obj[self.schema[self.target]['uniqueKey']].strip()]['modified'],"%Y-%m-%dT%H:%M:%S.%f").timestamp():
                    self.outOfDate.append(obj)
            except Exception as error:
                print('Exception raised for ' + obj[self.schema[self.target]['uniqueKey']] + ': ' + repr(error))
                if self.couchObjects:
                    if not obj[self.schema[self.target]['uniqueKey']].strip() in self.couchObjects.keys():
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
                    "_id":self.schema[self.target]['title'] + "_" + str(datetime.now().isoformat()),
                    "data":{self.schema[self.target]['title']:{}}
                    }
            for key in self.schema[self.target]['properties'].keys():
                    doc['data'][self.schema[self.target]['title']][key] = self.schema[self.target]['properties'][key]['fcn'](item)
                    print(self.schema[self.target]['properties'][key]['fcn'](item))
            self.db.upsert(doc)
        self.newItems = []
        for item in self.outOfDate:
            doc = self.db.get(self.couchObjects[item[self.schema[self.target]['uniqueKey']].strip()]['id'])
            if doc:
                for key in self.schema[self.target]['properties'].keys():
                    doc['data'][self.schema[self.target]['title']][key] = self.schema[self.target]['properties'][key]['fcn'](item)
            self.db.upsert(doc)
        self.outOfDate = []
        
        
        
        
        
        
        
    ### --- OBJECT SPECIFIC FUNCTIONS --- ###
    def getSealIdentifiers(self,strn):
        identifiers = re.match("(SUPREME|LIQUIDYNE)[-|\s]*\w*[-|\s]+([\w]{0,4})[-|\s]+([0-9]{3})[-|\s](.*)", strn.strip(), re.IGNORECASE)
        if not identifiers:
            identifiers = ['N/A','N/A','N/A',0,'N/A','N/A','N/A']
        return {'product_group':identifiers[1],'product_type':identifiers[2],'size':int(identifiers[3]),'option_letters':identifiers[4]}
    
    def getLocation(self,ID,identifiers):
        if ID[-1] == 'A' or ID[-1] == 'a':
            loc = 'Aft'
        elif ID[-1] == 'F' or ID[-1] == 'f':
            loc = 'Forward'
        elif identifiers['product_type'][0] == 'T' or identifiers['product_type'][0] == 't':
            loc = 'Thruster'
        elif identifiers['product_type'][0] == 'W' or identifiers['product_type'][0] == 'L':
            loc = 'Pump'
        else:
            loc = 'Other'
        return loc
    
    def getLipseals(self,type_code=[],material=[],ServObjectCode=''):
        # First, try to get as-built structure from isah
        if material:
            number = len(material)
        num = 1
        cnt = 0
        lipseals = []
        if not(material):
#            material = ['N/A']
            number = 0
        for lipseal in range(0,number):
            thisMaterial = material[cnt]
            lipseals.append({'number':str(num),"old_material":"","new_material":thisMaterial,"condition":[],"remarks":""})
            num += 1
            cnt += 1
        return lipseals
    
    def getStructure(self,ID):
        query = "SELECT dbo.T_AsBuiltStructure.Qty, dbo.T_AsBuiltStructure.DossierCode, dbo.T_AsBuiltStructure.ProdHeaderDossierCode, dbo.T_AsBuiltStructure.PartCode, dbo.T_Part.Description FROM dbo.T_AsBuiltStructure LEFT JOIN dbo.T_Part ON dbo.T_Part.PartCode = dbo.T_AsBuiltStructure.PartCode WHERE  dbo.T_AsBuiltStructure.ServObjectCode = '" + ID + "'"
        result = self.__run_isah_query__(query)
        bom = []
        for line in result:
            if line['Qty']:
                # Append line to bom list
                bom.append({'article_number':line['PartCode'].strip(),'description':line['Description'],'quantity':line['Qty'],'active':True,'prodDossier':line['ProdHeaderDossierCode'].strip(),'spare_part':False})
        return bom