import pymongo
class MongoCli:
    def __init__( self , host = 'localhost' ,port = 27017  ,db = 'test'  , user = None , pwd = None):
        self.db = db
        if user == None :
            self.cli = pymongo.MongoClient( host,  )
        else:
            self.cli = pymongo.MongoClient(  [ host + ':'  +  str(  port ) ] ,
                               authSource= db  )
        self.db = self.cli[ db ]
        self.db.authenticate( user , pwd )
