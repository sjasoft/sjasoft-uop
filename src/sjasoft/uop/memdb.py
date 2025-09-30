from sjasoft.uop.db_collection import DBCollection
from sjasoft.uop.database import Database

class MemDBCollection(DBCollection):
    def __init__(self, name, collection: dict):
        self.name = name
        self.collection = collection

    def find(self, query=None):
        return self.collection.values()
    
    def find_one(self, query=None):
        return self.collection.values()[0]
    
    def get(self, id):
        return self.collection.get(id)
    
    def replace_one(self, id, data):
        self.collection[id] = data
        
        
    def insert(self, **kwargs):
        self.data.append(kwargs)
        
    def update(self, query, update):
        pass
        
        

class MemCollection(DBCollection):
    def __init__(self, collection: dict):   
        self._collection = collection
        super().__init__(collection)
        
    def insert(self, **kwargs):
        pass

    def update(self, query, update):
        pass

    def find(self, query=None):
        pass

    def find_one(self, query=None):
        pass

    def delete(self, query):
        pass

    def get(self, id):
        pass

    def drop(self):
        pass

class MemDB(Database):
    def __init__(self, on_disk=''):
        self._on_disk = on_disk
        self._mem_collections = dict(
            classes = {},
            attributes = {},
            roles = {},
            tags = {},
            groups = {},
            queries = {},
            related = {},
            changes = {},
            schemas = {},
        )
        self._collections = dict(
            classes = MemCollection(self._mem_collections['classes']),
            attributes = MemCollection(self._mem_collections['attributes']),
            roles = MemCollection(self._mem_collections['roles']),
            tags = MemCollection(self._mem_collections['tags']),
            groups = MemCollection(self._mem_collections['groups']),
            queries = MemCollection(self._mem_collections['queries']),
            related = MemCollection(self._mem_collections['related']),
            changes = MemCollection(self._mem_collections['changes']),
            schemas = MemCollection(self._mem_collections['schemas']),
        )
        pass

    def get_collection(self, name):
        coll = self._collections.get(name)
        if not coll:
            self._mem_collections[name] = {}
            coll = self._collections[name] = MemCollection(self._mem_collections[name])
        return coll

    def drop_database(self):
        pass

    def list_collection_names(self):
        pass
