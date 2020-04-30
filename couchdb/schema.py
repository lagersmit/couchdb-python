# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 08:55:22 2020

@author: rmooijman
"""

schema = {
        'parts':{
                'type':'object',
                'couchdb':'part',
                'query':"SELECT T_Part.LastUpdatedOn, T_Part.PartCode AS part_code, T_Part.PurLocationCode AS location, T_VendorAddress.Name AS vendor, T_Part.VendPartCode AS vendor_part_code, T_Part.VendId AS vendor_id, T_Part.Description AS description, T_Quality.Description AS material, T_DocumentMain.DocPathName, T_DocumentMain.DocRefWords, T_DocumentMain.RevisionNr FROM T_Part LEFT JOIN T_DocumentMain ON T_Part.PartCode = T_DocumentMain.DocCode LEFT JOIN T_VendorAddress ON T_VendorAddress.VendId = T_Part.VendId LEFT JOIN T_Quality ON T_Quality.QualityCode = T_Part.QualityCode WHERE T_Part.LastUpdatedOn > '2015-01-01'",
                'properties':{
                        "part_code": {
                                'type':'string',
                                'fcn':lambda row: row['part_code']
                                },
                        "location": {
                                'type':'string',
                                'fcn':lambda row: row['location']
                                },
                        "vendor": {
                                'type':'string',
                                'fcn':lambda row: row['vendor']
                                },
                        "vendor_part_code": {
                                'type':'string',
                                'fcn':lambda row: row['vendor_part_code']
                                },
                        "vendor_id": {
                                'type':'string',
                                'fcn':lambda row: row['vendor_id']
                                },
                        "description": {
                                'type':'string',
                                'fcn':lambda row: row['description']
                                },
                        "material": {
                                'type':'string',
                                'fcn':lambda row: row['material']
                                },
                        "documents":{
                                'type':'array',
                                'fcn':lambda row: row['part_code']
                                },
                        'created':{
                                'type':'string',
                                'fcn':lambda row: row['part_code']
                                },
                        'modified':{
                                'type':'string',
                                'fcn':lambda row: row['part_code']
                                }
                        }
                }
        }