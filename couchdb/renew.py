# Copyright (c) 2019, Lagersmit B.V

"""
Lagersmit Seals and Vessels database package
"""
__version__ = '1.0'

"""
Package Objects
"""

from couchdb import Interpreter

dbs = ['sites','objects','parts','sales']



for db in dbs:
    interpreter = Interpreter(target=db,date_limit="2020-01-01",master='isah')
    interpreter.sync2()