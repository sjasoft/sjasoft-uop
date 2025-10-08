"""
    What the database, of whatever concrete kind, needs to do:
    1) provide standard metadata about its contents and the means to load and save that data;
    2) provide the means to find and load any object efficiently;
    3) provide teh manes to find and load related objects to an object, by role and by all roles, efficiently
    4) provide the means to update contents based on a changeset;
    5) provide support for class, relation and tag queries in any combination

    Do tags need to be handled separately?  Do tag hierarchies need any more precise handling than using some kind of
    separator between subparts?

    Future database idea:
     Perhaps the simplest functional database would store objects as json data.  One scheme would simply use a json
     dictionary of attribute names and values.  Another approach is to use a json list where the first N items are
     special.  Having data[:1] be class_id, object_id might be one choice.  There are many others with somewhat more
     indirection.  With any such scheme we have some indices per class to make search more efficient.  At minimum we
     have an index on object id.

     In a free form database it is useful to have grouping by such category as class in some efficient form such as
    linked blocks of either actual object data (clustering) or of object references.
"""

from sjasoft.uop import db_collection as db_coll
from sjasoft.uop.collections import uop_collection_names, per_tenant_kinds
from sjasoft.uop import changeset
from sjasoft.uopmeta.schemas import meta
from sjasoft.utils import decorations
from sjasoft.utils import cw_logging, index
import time
from sjasoft.utils.decorations import abstract
from collections import defaultdict

comment = defaultdict(set)
logger = cw_logging.getLogger('uop.database')


def id_dictionary(doclist):
    return dict([(x['_id'], x) for x in doclist])


def objects(doclist):
    return [x for x in doclist]


class Database(object):
    database_by_id = {}
    _meta_id_tree = None
    db_info_collection = 'uop_database'

    _index = index.Index('database', 48)

    @classmethod
    def make_test_database(cls):
        "create a randomly named test database of the appropriate type"
        msg = f'{cls.__name__} needs to implement make_test_database'
        raise Exception(msg)

    @classmethod
    @decorations.abstract
    def make_named_database(cls, name):
        "creates a new database with the given name"
        pass

    @classmethod
    def with_id(cls, idnum):
        return cls.database_by_id.get(idnum)

    @abstract
    def drop_database(self):
        pass

    @classmethod
    def existing_db_names(cls):
        return []

    def __init__(self, tenant_id=None, **dbcredentials):
        self.credentials = dbcredentials
        self._collections:db_coll.DatabaseCollections = None
        self._long_txn_start = 0
        self._tenants = None
        self._collections_complete = False
        self._tenant_id = tenant_id
        self._ensure_internal_collections()

    def _ensure_internal_collections(self):
        self._collections = db_coll.DatabaseCollections(self, col_map=dict(uop_collection_names))


    @contextmanager
    def changes(self, changeset=None):
        changes = self._changeset or changeset.ChangeSet()
        yield changes
        if not self._changeset:
            if self._cache:
                self._cache.apply_changes(changes)
            self._db.apply_changes(changes, self._db.collections)


    @property
    def in_long_transaction(self):
        return self._long_txn_start > 0

    def meta_context(self):
        data = self.collections.metadata()
        return meta.MetaContext.from_data(data)

    def random_collection_name(self):
        res = index.make_id(48)
        if not res[0].isalpha():
            res = 'x' + res
        return res

    def make_random_collection(self, schema=None):
        return self.get_managed_collection(self.random_collection_name(), schema)

    @property
    def collections(self):
        if not self._collections_complete:
            col_map = dict(uop_collection_names)
            if self._tenant_id: 
                tenant: meta.Tenant = self.get_tenant(self._tenant_id)
                if tenant:
                    self._collctions.update_tenant_collections(tenant.base_collections)
            self._collections_complete = True
        return self._collections

    def set_tenant_collections(self, tenant_id):
        collections = self.get_tenant_collections(tenant_id)
        self._collections = collections
        
    def merge_tenant_collections(self, tenant_id):
        collections = self.get_tenant_collections(tenant_id)
        self._collections._collections.update(collections._collections)

    def tenants(self):
        if not self._tenants:
            self._tenants = self._collections.get('tenants')
        return self._tenants

    def users(self):
        if not self._users:
            self._users = self.collections._collections.get('users')
        return self._users

    def applications(self):
        if not self._applications:
            self._applications = self.collections._collections.get('applications')
        return self._applications

    def schemas(self):
        if not self._schemas:
            self._schemas = self.collections._collections.get('schemas')
        return self._schemas
    
    def add_schema(self, a_schema: meta.Schema):
        """
        Adds a schema to the database.
        :param a_schema: a Schema
        :return: None
        """
        self.schemas().insert(**a_schema.dict())

    def get_tenant(self, tenant_id):
        tenants = self.tenants()
        return tenants.get(tenant_id)
    
    def get_user(self, user_id):
        users = self.users()
        return users.get(user_id)
    
    def add_user(self, user: meta.User, tenant_id: str):    
        users = self.users()
        user =users.insert(**user.dict())
        return user
    
    def add_tenant(self, tenant: meta.Tenant):
        tenants = self.tenants()
        tenant = tenants.insert(**tenant.dict())
        return tenant
    
    def add_tenant_user(self, tenant_id: str, user_id: str):
        role_id = self.roles.name_to_id['tenant_user']
        self.relate(tenant_id, role_id, user_id)
    
    def remove_tenant_user(self, tenant_id: str, user_id: str):
        role_id = self.roles.name_to_id['tenant_user']
        self.unrelate(tenant_id, role_id, user_id)
    

    def drop_tenant(self, tenant_id):
        """
        Drops the tenant from the database.  This version removes their data.
        :param tenant_id id of the tenant to remove
        """
        collections = self.get_tenant_collections(tenant_id)
        if collections:
            self.collections.drop_collections(collections)

    def create_tenannt(self, name = ''):
        tenant = meta.Tenant(name=name)
        for kind in per_tenant_kinds:
            tenant.base_collections[kind] = self.random_collection_name()
        self.add_tenant(tenant)
        return tenant
    
    def new_collection_name(self, baseName=None):
        return index.make_id(48)

    def ensure_indices(self, indices):
        pass

    def get_raw_collection(self, name):
        """
        A raw collection is whatever the underlying datastore uses, e.g., a table or
        document collection.
        :param name: name of the underlying
        :return: the raw collection or None
        """
        pass

    def get_managed_collection(self, name, schema=None):
        known = self.collections.get(name)
        if not known:
            raw = self.get_raw_collection(name, schema)
            known = self.wrap_raw_collecton(raw)
        return known

    def wrap_raw_collecton(self, raw):
        pass


    def start_long_transaction(self):
        pass

    def end_long_transaction(self):
        self._long_txn_start = 0


    def get_existing_collection(self, coll_name):
        return self.collections._collections.get(coll_name)

    def get_collection(self, collection_name):
        return self.collections.get(collection_name)
    
    def ensure_core_schema(self):
        core_schema: meta.Schema = meta.core_schema()
        if not self.schemas().find_one({'name': core_schema.name}):
            self.add_schema(core_schema)
        self.ensure_schema(meta.core_schema)
        
    def ensure_schema(self, a_schema: meta.Schema):
        if not self.schemas().find_one({'name': a_schema.name}):
            self.add_schema(a_schema)
        self.ensure_schema_installed(a_schema)


    def open_db(self, setup=None):
        self._collections = db_coll.DatabaseCollections(self)
        self._collections.ensure_collections(uop_collection_names)
        if self._tenant_id:
            tenant: meta.Tenant = self.get_tenant(self._tenant_id)
            if tenant:
                self._collections.ensure_collections(tenant.base_collections, override=True)
        self._collections.ensure_extensions()
        self._collections_complete = True

    def _db_has_collection(self, name):
        return False

    def log_changes(self, changeset, tenant_id=None):
        """ Log the changeset.
        We could log external to the main database but here we will presume that
        logging is local.
        """
        changes = meta.MetaChanges(timestamp=time.time(),
                                   changes=changeset.to_dict())
        coll = self.get_collection('changes')
        coll.insert(**changes.dict())

    def changes_since(self, epochtime, tenant_id, client_id=None):
        client_id = client_id or 0
        change_coll = self.get_managed_collection('changes')
        changesets = change_coll.find({'timestamp': {'$gt': epochtime}, 'client_id': {'$ne': client_id}},
                                      order_by=('timestamp',),
                                      only_cols=('changes',))
        return changeset.ChangeSet.combine_changes(*changesets)

    def begin_transaction(self):
        in_txn = self.in_long_transaction
        if not in_txn:
            self.ensure_extensions()
        self._long_txn_start += 1
        if not in_txn:
            self.start_long_transaction()

    def in_outer_transaction(self):
        return self._long_txn_start == 1

    def close_current_transaction(self):
        if self.in_long_transaction:
            self._long_txn_start -= 1


    def remove_collection(self, collection_name):
        pass

    def schema_changes(self, schema):
        meta = self.collections.metadata()


    def apply_changes(self, changeset, collections):
        self.begin_transaction()
        changeset.attributes.apply_to_db(collections)
        changeset.classes.apply_to_db(collections)
        changeset.roles.apply_to_db(collections)
        changeset.tags.apply_to_db(collections)
        changeset.groups.apply_to_db(collections)
        changeset.objects.apply_to_db(collections)
        changeset.related.apply_to_db(collections)
        changeset.queries.apply_to_db(collections)
        self.log_changes(changeset)
        self.commit()

    def really_commit(self):
        pass

    def abort(self):
        self.end_long_transaction()

    def commit(self):
        if self.in_outer_transaction():
            self.really_commit()
            self.end_long_transaction()
        self.close_current_transaction()

