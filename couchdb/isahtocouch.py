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



"""
Object of necessary server connections.
"""
servers = {
            'LAG-ISAH': pyodbc.connect(
                        "Driver={SQL Server Native Client 11.0};"
                        "Server= LAG-ISAH\ISAHMSSQL;"
                        "Database=LagersmitDB;"
                        "uid=powerbi;pwd=Lagersmit#1"
                        )
            }

class Objects(couchdb.Document):
    def __init__(self):
        self.type = 'Document'