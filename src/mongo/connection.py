from multiprocessing import connection
from pymongo import MongoClient   
from bson.binary import UuidRepresentation 
class ConnectionMongo:

    def __init__(self):
        #_ NAME DB
        db = "dbmonitors4"
        connection = MongoClient('mongodb://monitorSys:sys4log44sa831@67.205.164.216:27017/',uuidRepresentation='standard')
        self.con = connection[db]

    
