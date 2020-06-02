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
import psycopg2

from couchdb import http, json, util
from couchdb import Document, Server

"""
Object of necessary server connections.
"""




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
    def __init__(self,target=None,date_limit="2020-01-01",master='isah',slave='couch'):
        self.db = []
        self.date_limit = date_limit
        self.servers = {
            'isah': pyodbc.connect(
                        "Driver={SQL Server Native Client 11.0};"
                        "Server= LAG-ISAH\ISAHMSSQL;"
                        "Database=LagersmitDB;"
                        "uid=powerbi;pwd=Lagersmit#1"
                        ),
            'couch': Server('http://admin:supreme@192.168.8.134:5984'),
            'reports':psycopg2.connect("host=reports.lagersmit.com dbname=lagersmit user=lagersmit password=lagersmit")
            }
        self.schema = {
                        'reports':{
                                'type':'object',
                                'title':'report',
                                'target_db':'servicereports_1',
                                'uniqueKey':'OrdNr',
                                'query':lambda row: self.__run_sql_query__("SELECT *, dbo.T_DossierMain.LastUpdatedOn AS 'modified', dbo.T_CustomerAddress.Name AS Customer FROM dbo.T_DossierMain LEFT JOIN dbo.T_CustomerAddress ON dbo.T_DossierMain.CustId = dbo.T_CustomerAddress.CustId WHERE dbo.T_DossierMain.OrdType = '06' AND dbo.T_DossierMain.LastUpdatedOn > '2020-05-25'"),
                                'properties':{
                                        'created':{
                                                'type':'date',
                                                'fcn':lambda row: str(datetime.now().isoformat())
                                                },
                                        'modified':{
                                                'type':'date',
                                                'fcn':lambda row:  str(datetime.now().isoformat())
                                                },
                                         "status": {
                                            "released": {
                                                    'type':'boolean',
                                                    'fcn':lambda row: False
                                                    },
                                            "approved_by": {
                                                    "type":"string",
                                                    "fcn":lambda row: ""
                                                },
                                            "approve_date": {
                                                    "type":"string",
                                                    "fcn":lambda row: ""
                                                },
                                            "revision": {
                                                    "type":"string",
                                                    "fcn":lambda row: ""
                                                },
                                            "revision_description": {
                                                    "type":"string",
                                                    "fcn":lambda row: "Original issue"
                                                }
                                          },
                                        "general_info": {
                                            "ls": {
                                                    "type":"string",
                                                    "fcn":lambda row: row['OrdNr']
                                                },
                                            "po": {
                                                    "type":"string",
                                                    "fcn":lambda row: row['OrdRef']
                                                },
                                            "report_date": "",
                                            "customer": {
                                                    "type":"string",
                                                    "fcn":lambda row: row['Customer']
                                                },
                                            "customer_reference": "",
                                            "yard": "",
                                            "city": {
                                                    "type":"string",
                                                    "fcn":lambda row: row['City']
                                                },
                                            "country": "",
                                            "service_group": {
                                                    "type":"string",
                                                    "fcn":lambda row: "Supreme"
                                                },
                                            "remarks": "",
                                            "internal_remarks": {
                                                    "type":"string",
                                                    "fcn":lambda row: row['Info']
                                                },
                                            "oil_type": "",
                                            "status": "",
                                            "cover_picture": {},
                                            "service_reasons": {
                                                    "type":"string",
                                                    "fcn":lambda row: [row['Description']]
                                                },
                                            "misc": {
                                              "berth": "",
                                              "stern_tube_drain": "",
                                              "stern_tube_drained_by": "",
                                              "forward_bearing": "",
                                              "aft_bearing": "",
                                              "findings_cause_remarks": ""
                                            }
                                          },
                                          "engineers": [],
                                          "vessels": [],
                                          "seals": [],
                                          "signatures": [],
                                          "contacts": [],
                                          "documents": [],
                                          "actions": [],
                                          "checklists": []
                                        }
                                },
                        'sites':{
                                'type':'object',
                                'title':'vessel',
                                'target_db':'sites_1',
                                'uniqueKey':'ImoCode',
                                'query':lambda row: self.__run_sql_query__("SELECT MAX(t1.LastUpdatedOn) AS modified, t1.Application, t1.ImoCode FROM ST_IHC_ObjKnmk t1 WHERE t1.ImoCode IS NOT NULL AND t1.ImoCode != 'NEE' AND t1.ImoCode !='' AND LastUpdatedOn > '" + self.date_limit + "' GROUP BY t1.ImoCode, t1.Application ORDER BY modified"),
                                'properties':{
                                        "registration_id":{
                                                "type":"string",
                                                "fcn":lambda row: row['ImoCode'].strip()
                                                },
                                        "registration_type":{
                                                "type":"string",
                                                "fcn":lambda row: "IMO"
                                                },
                                        "draught":{
                                                "type":"number",
                                                "fcn":lambda row: 0
                                                },
                                        "rpm":{
                                                "type":"number",
                                                "fcn":lambda row: 0
                                                },
                                        "mmsi":{
                                                "type":"string",
                                                "fcn":lambda row: ""
                                                },
                                        "objects":{
                                                "type":"array",
                                                "data":lambda row: self.__run_sql_query__("SELECT * FROM ST_IHC_ObjKnmk WHERE ImoCode = '" + row['ImoCode'] + "'"),
                                                "items":{
                                                        "type":"string",
                                                        "fcn":lambda row: row['ServObjectCode'].strip()
                                                        }
                                                },
                                        "names":{
                                                "type":"array",
                                                "data":lambda row: self.__run_sql_query__("SELECT * FROM ST_IHC_ObjKnmk WHERE ImoCode = '" + row['ImoCode'] + "'"),
                                                "items":{
                                                        "type":"object",
                                                        "properties":{
                                                            "date":{
                                                                    "type":"date",
                                                                    "fcn":lambda row: str(row['LastUpdatedOn'])
                                                                    },
                                                            "name":{
                                                                    "type":"string",
                                                                    "fcn":lambda row: row['Vesselsname'].strip().upper()
                                                                    }
                                                            }
                                                        }
                                                },
                                        "owner":{
                                                "type":"array",
                                                "data":lambda row: self.__run_sql_query__("SELECT * FROM ST_IHC_ObjKnmk WHERE ImoCode = '" + row['ImoCode'] + "'"),
                                                "items":{
                                                        "type":"object",
                                                        "properties":{
                                                            "date":{
                                                                    "type":"date",
                                                                    "fcn":lambda row: str(row['LastUpdatedOn'])
                                                                    },
                                                            "owner":{
                                                                    "type":"string",
                                                                    "fcn":lambda row: row['Principal']
                                                                    }
                                                            }
                                                        }
                                                },
                                        "country":{
                                                "type":"string",
                                                "fcn":lambda row: ""
                                                },
                                        "type":{
                                                "type":"string",
                                                "fcn":lambda row: row['Application']
                                                },
                                        "cover_picture":{
                                                "type":"string",
                                                "fcn":lambda row: ""
                                                },
                                        "active":{
                                                "type":"boolean",
                                                "fcn":lambda row: True
                                                },
                                        "documents":{
                                                "type":"other",
                                                "fcn":lambda row: []
                                                },
                                        "ais":{
                                                "type":"other",
                                                "fcn":lambda row: []
                                                },
                                        'created':{
                                                'type':'date',
                                                'fcn':lambda row: str(datetime.now().isoformat())
                                                },
                                        'modified':{
                                                'type':'date',
                                                'fcn':lambda row:  str(datetime.now().isoformat())
                                                }
                                        }
                                },
                        'parts':{
                                'type':'object',
                                'title':'part',
                                'target_db':'parts_1',
                                'uniqueKey':'part_code',
                                'query':lambda row: self.__run_sql_query__("SELECT T_Part.LastUpdatedOn AS modified, T_Part.PartCode AS part_code, T_Part.PurLocationCode AS location, T_VendorAddress.Name AS vendor, T_Part.VendPartCode AS vendor_part_code, T_Part.VendId AS vendor_id, T_Part.Description AS description, T_Quality.Description AS material, T_DocumentMain.DocPathName, T_DocumentMain.DocRefWords, T_DocumentMain.RevisionNr FROM T_Part LEFT JOIN T_DocumentMain ON T_Part.PartCode = T_DocumentMain.DocCode LEFT JOIN T_VendorAddress ON T_VendorAddress.VendId = T_Part.VendId LEFT JOIN T_Quality ON T_Quality.QualityCode = T_Part.QualityCode WHERE T_Part.LastUpdatedOn > '" + self.date_limit + "'"),
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
                                        "bom":{
                                                'type':'array',
                                                'data':lambda row: self.__run_sql_query__("SELECT dbo.T_BillOfMat.SubPartCode, dbo.T_BillOfMat.Qty, dbo.T_BillOfMat.Description FROM dbo.T_BillOfMat WHERE dbo.T_BillOfMat.PartCode = '" + row['part_code'].strip() + "'"),
                                                'items':{
                                                        'type':'object',
                                                        'properties':{
                                                                'article_number':{
                                                                        'type':'string',
                                                                        'fcn':lambda row: row['SubPartCode'].strip()
                                                                        },
                                                                'description':{
                                                                        'type':'string',
                                                                        'fcn':lambda row: row['Description']
                                                                        },
                                                                'quantity':{
                                                                        'type':'string',
                                                                        'fcn':lambda row: row['Qty']
                                                                        }
                                                                },
                                                        },
                                                'fcn':lambda row: [{'article_number':line['SubPartCode'].strip(), 'description':line['Description'],'quantity':line['Qty']} for line in self.__run_sql_query__("SELECT dbo.T_BillOfMat.SubPartCode, dbo.T_BillOfMat.Qty, dbo.T_BillOfMat.Description FROM dbo.T_BillOfMat WHERE dbo.T_BillOfMat.PartCode = '" + row['part_code'].strip() + "'")]
                                                },
                                        "documents":{
                                                'type':'other',
                                                'fcn':lambda row: [self.getDocument(row['DocPathName'].strip(),row['description'].strip(),row['RevisionNr'].strip(),row['DocRefWords'])] if row['DocPathName'] else []
                                                },
                                        'created':{
                                                'type':'date',
                                                'fcn':lambda row: str(datetime.now().isoformat())
                                                },
                                        'modified':{
                                                'type':'date',
                                                'fcn':lambda row:  str(datetime.now().isoformat())
                                                }
                                        }
                                },
                            'sales':{
                                'type':'object',
                                'title':'sale',
                                'target_db':'sales_1',
                                'uniqueKey':'DossierCode',
                                'query': lambda row:self.__run_sql_query__( "SELECT dbo.T_DossierMain.LastUpdatedOn AS modified, dbo.T_DossierMain.OrdNr, dbo.T_DossierMain.DossierCode, dbo.T_DossierMain.Description, dbo.T_DossierMain.QuotNr, dbo.T_DossierMain.CustId, dbo.T_DossierMain.OrdType, dbo.T_CustomerAddress.Name AS Customer, dbo.T_OrderType.Description AS OrderType, dbo.T_DossierMain.QuotRef, dbo.T_DossierMain.QuotDate, dbo.T_Employee.FirstName+' '+dbo.T_Employee.Name AS Seller FROM dbo.T_DossierMain LEFT JOIN dbo.T_Employee ON dbo.T_DossierMain.Seller = dbo.T_Employee.EmpId LEFT JOIN dbo.T_CustomerAddress ON dbo.T_DossierMain.CustId = dbo.T_CustomerAddress.CustId LEFT JOIN dbo.T_OrderType ON dbo.T_DossierMain.OrdType = dbo.T_OrderType.OrdType WHERE dbo.T_DossierMain.Quotdate > '" + self.date_limit + "' AND dbo.T_CustomerAddress.CustAddrCode = ''"),
                                'properties':{
                                        "dossier_code": {
                                                'type':'string',
                                                'fcn':lambda row: row['DossierCode'].strip()
                                                },
                                        "ls": {
                                                'type':'string',
                                                'fcn':lambda row: row['OrdNr'].strip()
                                                },
                                        "quotation": {
                                                'type':'string',
                                                'fcn':lambda row: row['QuotNr'].strip()
                                                },
                                        "description": {
                                                'type':'string',
                                                'fcn':lambda row: row['Description'].strip()
                                                },
                                        "customer": {
                                                'type':'string',
                                                'fcn':lambda row: row['Customer'].strip()
                                                },
                                        "seller": {
                                                'type':'string',
                                                'fcn':lambda row: row['Seller'].strip()
                                                },
                                        "order_type": {
                                                'type':'string',
                                                'fcn':lambda row: row['OrderType'].strip()
                                                },
                                        "quotation_reference": {
                                                'type':'string',
                                                'fcn':lambda row: row['QuotRef'].strip()
                                                },
                                        "quotation_date": {
                                                'type':'date',
                                                'fcn':lambda row: row['QuotDate'].to_pydatetime().isoformat()
                                                },
                                        "items":{
                                                'type':'array',
                                                'data':lambda row: self.__run_sql_query__("SELECT dbo.T_DossierDetail.DossierCode, dbo.T_DossierDetail.DetailCode, dbo.T_DossierDetail.DetailSubCode, dbo.T_DossierDetail.PartCode, dbo.T_DossierDetail.Description, dbo.T_DossierDetail.CalcQty FROM dbo.T_DossierDetail WHERE dbo.T_DossierDetail.DossierCode = '" + row['DossierCode'].strip() + "'"),
                                                'items':{
                                                        'type':'object',
                                                        'properties':{
                                                                    'id':{
                                                                        'type':'string',
                                                                        'fcn':lambda row: row['DetailCode']
                                                                            },
                                                                    'part_code':{
                                                                        'type':'string',
                                                                        'fcn':lambda row: row['PartCode'].strip()
                                                                            },
                                                                    'qty':{
                                                                        'type':'string',
                                                                        'fcn':lambda row: row['CalcQty']
                                                                            },
                                                                    'description':{
                                                                        'type':'string',
                                                                        'fcn':lambda row: row['Description'].strip()
                                                                            }
                                                                    }
                                                        },
                                                'fcn':lambda row: [{'id':line['DetailCode'],'part_code':line['PartCode'].strip(),'qty':line['CalcQty'],'description':line['Description'].strip()} for line in self.__run_sql_query__("SELECT dbo.T_DossierDetail.DossierCode, dbo.T_DossierDetail.DetailCode, dbo.T_DossierDetail.DetailSubCode, dbo.T_DossierDetail.PartCode, dbo.T_DossierDetail.Description, dbo.T_DossierDetail.CalcQty FROM dbo.T_DossierDetail WHERE dbo.T_DossierDetail.DossierCode = '" + row['DossierCode'].strip() + "'")]
                                                },
                                        "documents":{
                                                'type':'other',
                                                'fcn':lambda row: [self.getDocument(doc['DocPathName'], doc['Description'],doc['RevisionNr'],[doc['DocRefWords'],doc['Remark']]) for doc in self.__run_sql_query__("SELECT dbo.T_DocumentMain.DocPathName, dbo.T_DocumentMain.RevisionNr, dbo.T_DocumentMain.DocCode, dbo.T_DocumentMain.Description, dbo.T_DocumentMain.DocRefWords, dbo.T_DocumentMain.Remark FROM dbo.T_DocumentMain WHERE dbo.T_DocumentMain.DocId IN (SELECT dbo.T_DocumentDetail.DocId FROM dbo.T_DocumentDetail WHERE dbo.T_DocumentDetail.IsahPrimKey = '" + row['DossierCode'].strip() + "')")]
                                                },
                                        'created':{
                                                'type':'date',
                                                'fcn':lambda row: str(datetime.now().isoformat())
                                                },
                                        'modified':{
                                                'type':'date',
                                                'fcn':lambda row:  str(datetime.now().isoformat())
                                                }
                                        }
                                },
                            'objects':{
                                    'type':'object',
                                    'title':'seal',
                                    'target_db':'objects_1',
                                    'uniqueKey':'ServObjectCode',
                                    'query':lambda row: self.__run_sql_query__("SELECT dbo.T_SERVObject.LastUpdatedOn AS modified, dbo.T_SERVObject.ServObjectCode, dbo.T_SERVObject.ServObjectGrpCode, dbo.T_SERVObject.ServObjectTypeCode, dbo.T_SERVObject.Description, dbo.T_SERVObject.CreDate, dbo.T_SERVObject.InstDate, dbo.T_SERVObject.BasicMat, dbo.ST_IHC_ObjKnmk.Vesselsname, dbo.ST_IHC_ObjKnmk.OwnerCountryCode, dbo.ST_IHC_ObjKnmk.ImoCode, dbo.ST_IHC_ObjKnmk.Owner, dbo.T_DossierMain.OrdNr, dbo.T_ServObject.remark, dbo.T_ServObject.info, dbo.T_ServObject.PartCode, dbo.T_ServObject.DossierCode, dbo.T_ServObject.BasicMat, dbo.T_ServObject.Stand, dbo.T_ServObject.WarrStartDate, dbo.T_ServObject.WarrEndDate FROM dbo.T_SERVObject LEFT JOIN dbo.ST_IHC_ObjKnmk ON dbo.ST_IHC_ObjKnmk.ServObjectCode = dbo.T_SERVObject.ServObjectCode LEFT JOIN dbo.T_DossierMain ON dbo.T_DossierMain.DossierCode = dbo.T_ServObject.DossierCode WHERE dbo.T_SERVObject.CreDate > '" + self.date_limit + "'"),
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
                                                'fcn':lambda row: row['DossierCode'].strip()
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
                                                'properties':{
                                                        'start':{
                                                                'type':'string',
                                                                'fcn':lambda row: row['WarrStartDate'].to_pydatetime().isoformat() if row['WarrStartDate'] else ""
                                                                },
                                                        'end':{
                                                                'type':'string',
                                                                'fcn':lambda row: row['WarrEndDate'].to_pydatetime().isoformat() if row['WarrEndDate'] else ""
                                                                }
                                                        }
                                                },
                                              "registration": {
                                                'type':'other',
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
                                                'type':'other',
                                                'data':lambda row: re.findall("(N2|RS|F2|PT100|P2|C2|[NDLBFZGSQAPVMKCHOE]{1})",self.getSealIdentifiers(row['Description'].strip())['option_letters'],re.IGNORECASE),
                                                'items':{
                                                        'type':'string',
                                                        'fcn':lambda row: row
                                                        },
                                                'fcn':lambda row: re.findall("(N2|RS|F2|PT100|P2|C2|[NDLBFZGSQAPVMKCHOE]{1})",self.getSealIdentifiers(row['Description'].strip())['option_letters'],re.IGNORECASE),
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
                                                'type':'other',
                                                'fcn':lambda row: []
                                                },
                                              "bom": {
                                                'type':'other',
                                                'fcn':lambda row: self.getStructure(row['ServObjectCode'].strip())
                                                },
                                              "documents": {
                                                'type':'other',
                                                'fcn':lambda row: [self.getDocument(doc['DocPathName'], doc['Description'],doc['RevisionNr'],[doc['DocRefWords'],doc['Remark']]) for doc in self.__run_sql_query__("SELECT dbo.T_DocumentMain.DocPathName, dbo.T_DocumentMain.RevisionNr, dbo.T_DocumentMain.DocCode, dbo.T_DocumentMain.Description, dbo.T_DocumentMain.DocRefWords, dbo.T_DocumentMain.Remark FROM dbo.T_DocumentMain WHERE dbo.T_DocumentMain.DocId IN (SELECT dbo.T_DocumentDetail.DocId FROM dbo.T_DocumentDetail WHERE dbo.T_DocumentDetail.IsahPrimKey = '" + row['ServObjectCode'].strip() + "')")]
                                                },
                                              "readouts": {
                                                'type':'other',
                                                'fcn':lambda row: []
                                                },
                                              "lipsealbook": {
                                                'type':'other',
                                                'fcn':lambda row: []
                                                },
                                              'created':{
                                                    'type':'date',
                                                    'fcn':lambda row: str(datetime.now().isoformat())
                                                    },
                                              'modified':{
                                                    'type':'date',
                                                    'fcn':lambda row:  str(datetime.now().isoformat())
                                                    }
                                            }
                                    }
                        }
        self.target = target
        self.slaveObjects = {}
        self.masterObjects = []
        self.outOfDate = []
        self.newItems = []
        self.master = master
        self.slave = slave
        self.connect()
        
    
    
    
    def connect(self, target=False):
        if target:  
            self.target = target
        self.db = self.servers[self.slave][self.schema[self.target]['target_db']]
        
    def getSlaveObjects(self):
        for row in self.db.view('_design/index/_view/object'):
            if row['value']:
                self.slaveObjects.update({                   
                    row['value']['object']:{
                                'id':row['id'],
                                'modified':row['value']['modified']
                                }
                        })
            else:
                print(row['id'])
            
    def __upload_document__(self,path,description='',rev='',tags=[],suffix=''):
        docDB = self.servers[self.slave]['documents_1']
        
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
    
    def getmasterObjects(self):
        self.masterObjects = self.schema[self.target]['query'](None)
    
    def __object_in_couch__(self,objectId):
        if objectId in self.slaveObjects:
            return self.slaveObjects[objectId]
        else:
            return False
    
    def __run_sql_query__(self,query):
        if type(self.servers[self.master]).__name__ == 'Connection':
            data = pandas.read_sql_query(query, self.servers[self.master])
            columnNames = list(data.columns.values)
            dataOut = []
            for index, row in data.iterrows():
                newLine = {}
                for col in columnNames:
                    newLine.update({col:row[col]})
                dataOut.append(newLine)
        elif type(self.servers[self.master]).__name__ == 'connection':   
             cur = self.servers[self.master].cursor()
             cur.execute(query)
             dataOut = cur.fetchall()
        else:
            print('Master server not recognized...')
            dataOut = []
        return dataOut
    
    def __out_of_date__(self):
        self.newItems = []
        self.outOfDate = []
        self.getSlaveObjects()
        self.getmasterObjects()
        for obj in self.masterObjects:
            try:
                if datetime.strptime(str(obj['modified']),"%Y-%m-%d %H:%M:%S.%f").timestamp() > datetime.strptime(self.slaveObjects[obj[self.schema[self.target]['uniqueKey']].strip()]['modified'],"%Y-%m-%dT%H:%M:%S.%f").timestamp():
                    self.outOfDate.append(obj)
            except Exception as error:
                print('Exception raised for ' + obj[self.schema[self.target]['uniqueKey']] + ': New item or out of date; adding item to sync queue. (' + repr(error) + ')')
                if self.slaveObjects:
                    if not obj[self.schema[self.target]['uniqueKey']].strip() in self.slaveObjects.keys():
                        self.newItems.append(obj)
                    else:
                        self.outOfDate.append(obj)
                else:
                    self.newItems.append(obj)
                        
    def sync(self):
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
            doc = self.db.get(self.slaveObjects[item[self.schema[self.target]['uniqueKey']].strip()]['id'])
            if doc:
                for key in self.schema[self.target]['properties'].keys():
                    doc['data'][self.schema[self.target]['title']][key] = self.schema[self.target]['properties'][key]['fcn'](item)
            self.db.upsert(doc)
        self.outOfDate = []
        
    def __recursive__(self,obj,row):
        if obj['type'] == "array":
            result = obj['data'](row)
            return [self.__recursive__(obj['items'],line) for line in result] 
        elif obj['type'] == "object":
            out = {}
            for key in obj['properties'].keys():
                out[key] = self.__recursive__(obj['properties'][key],row)
            return out
        else:
            #print(obj['fcn'](row))
            return obj['fcn'](row)
                
              
    
    def sync2(self):
        self.__out_of_date__()
        out = []
        for item in self.newItems:
            doc = {
                    "_id":self.schema[self.target]['title'] + "_" + str(datetime.now().isoformat()),
                    "data":{self.schema[self.target]['title']:{}}
                    }
            it = {}
            print("New item: " + item[self.schema[self.target]['uniqueKey']])
            for key in self.schema[self.target]['properties'].keys():
                    it[key] = self.__recursive__(self.schema[self.target]['properties'][key],item)
                   
                    out.append(it)
            doc['data'][self.schema[self.target]['title']] = it
            self.db.upsert(doc)
        self.newItems = []
        for item in self.outOfDate:
            it = {}
            doc = self.db.get(self.slaveObjects[item[self.schema[self.target]['uniqueKey']].strip()]['id'])
            if doc:
                for key in self.schema[self.target]['properties'].keys():
                   it[key] = self.__recursive__(self.schema[self.target]['properties'][key],item)
                out.append(it)
            print("Out of date item: " + item[self.schema[self.target]['uniqueKey']])
            doc['data'][self.schema[self.target]['title']] = it
            self.db.upsert(doc)
        self.outOfDate = []
        return out
        
        
        
        
        
        
    ### --- ERP SPECIFIC FUNCTIONS --- ###
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
        result = self.__run_sql_query__(query)
        bom = []
        for line in result:
            if line['Qty']:
                # Append line to bom list
                bom.append({'article_number':line['PartCode'].strip(),'description':line['Description'],'quantity':line['Qty'],'active':True,'prodDossier':line['ProdHeaderDossierCode'].strip(),'spare_part':False})
        return bom