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
from sjasoft.uopmeta.schemas.meta import MetaContext, BaseModel
from sjasoft.utils import decorations
from sjasoft.utils import logging, index
from sjasoft.utils.decorations import abstract
from sjasoft.uop.query import Q
from sjasoft.uopmeta import oid
from sjasoft.uop.exceptions import NoSuchObject
from collections import defaultdict
from functools import reduce
import time
from collections import defaultdict
from contextlib import contextmanager

import re

logger = logging.getLogger('uop.database')

def as_dict(data):
    if isinstance(data, BaseModel):
        return data.dict()
    return dict(data)


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

    def __init__(self, tenant_id=None, *schemas,**dbcredentials):
        self.credentials = dbcredentials
        self._collections:db_coll.DatabaseCollections = None
        self._long_txn_start = 0
        self._tenants = None
        self._collections_complete = False
        self._tenant_id = tenant_id
        self._context:meta.MetaContext = None
        self._changeset:changeset.ChangeSet = None
        self._mandatory_schemas = schemas
        self.open_db()


    @property
    def metacontext(self):
        return self._context
    
    def get_metadata(self):
        return self.collections.metadata()

    def reload_metacontext(self):
        coll_meta = self.get_metadata()
        self._context = MetaContext.from_data(coll_meta)
        

    @contextmanager
    def changes(self, changeset=None):
        changes = self._changeset or changeset.ChangeSet()
        yield changes
        if not self._changeset:
            self._db.apply_changes(changes, self._db.collections)

    def ensure_schema(self, a_schema):
        changes = changeset.meta_context_schema_diff(self.metacontext, a_schema)
        has_changes = changes.has_changes()
        if has_changes:
            self.apply_changes(changes)
            self.reload_metacontext()
        return has_changes, changes



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

    # Collections

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

    def classes(self):
        return self.collections.classes
    
    def changes(self):
        return self.collections.changes
    
    def attributes(self):
        return self.collections.attributes
    
    def queries(self):
        return self.collections.queries
    
    def related(self):
        return self.collections.related
    
    def roles(self):
        return self.collections.roles
    
    def tags(self):
        return self.collections.tags
    
    def groups(self):
        return self.collections.groups
    
    def tenants(self):
        if not self._tenants:
            self._tenants = self._collections.get('tenants')
        return self._tenants

    def users(self):
        return self.collections.users


    def schemas(self):
        return self.collections.schemas
    
    # These three methods are used to find/create managed collections wrapping underlying datastore collections
    # All database adaptors must implement gew_raw_collection and wrap_raw_collection
    def get_raw_collection(self, name, schema=None):
        """
        A raw collection is whatever the underlying datastore uses, e.g., a table or
        document collection.
        :param name: name of the underlying
        :return: the raw collection or None
        """
        pass

    def get_managed_collection(self, name, schema=None):
        """Gets an existing managed (subclass of DBCollection) collection by name. 
        If not found, creates it.

        Args:
            name (_type_): name of the collection which also be name on the underlying datastore
            schema (_type_, optional): schema of the collection for datastores that need it. 
            Will either be a some meta object class or a Metaclass instance. Defaults to None.

        Returns:
            DBCollection: the managed collection
        """
        known = self.collections.get(name)
        if not known:
            raw = self.get_raw_collection(name, schema)
            known = self.wrap_raw_collecton(raw)
        return known

    def wrap_raw_collecton(self, raw):
        """Wraps a raw collection in a managed collection.
        This is a subclass of DBCollection
        """
        pass

    # Meta Collections and useful functions on them. In memory meta-items managed by metacontext
    
    def meta_classes(self):
        return self.metacontext.classes
    
    def meta_attributes(self):
        return self.metacontext.attributes
    
    def meta_roles(self):
        return self.metacontext.roles
    
    def meta_tags(self):
        return self.metacontext.tags
    
    def meta_groups(self):
        return self.metacontext.groups
    
    def meta_queries(self):
        return self.metacontext.queries
    
    def meta_related(self):
        return self.metacontext.related
    
    def name_to_id(self, kind):
        return self.metacontext.name_to_id(kind)
    
    def id_to_name(self, kind):
        return self.metacontext.id_to_name(kind)
    
    def id_map(self, kind):
        return self.metacontext.id_map(kind)
    
    def name_map(self, kind):
        return self.metacontext.name_map(kind)
    
    def ids_to_names(self, kind):
        return self.metacontext.ids_to_names(kind)
    
    def names_to_ids(self, kind):
        return self.metacontext.names_to_ids(kind)

    def by_name(self, kind):
        return self.metacontext.by_name(kind)
    
    def by_id(self, kind):
        return self.metacontext.by_id(kind)
    
    def by_name_id(self, kind):
        return self.metacontext.by_name_id(kind)
    
    def by_id_name(self, kind):
        return self.metacontext.by_id_name(kind)
    
    def role_id(self, name):
        return self.metacontext.roles.name_to_id(name)
    


    # Schemas
    
    def add_schema(self, a_schema: meta.Schema):
        """
        Adds a schema to the database.
        :param a_schema: a Schema
        :return: None
        """
        self.schemas().insert(**a_schema.dict())
        
    # Tenants and Users

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
        self.relate(tenant_id, self.role_id['has_user'], user_id)

    
    def remove_tenant_user(self, tenant_id: str, user_id: str):
        self.unrelate(tenant_id, self.role_id['has_user'], user_id)


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

    # Transaction support
    
    @property
    def in_long_transaction(self):
        return self._long_txn_start > 0

    @contextmanager
    def perhaps_committing(self, commit=False):
        yield
        if commit:
            self.commit()

    def start_long_transaction(self):
        pass

    def end_long_transaction(self):
        self._long_txn_start = 0

    def begin_transaction(self):
        if not self._changeset:
            self._changeset = changeset.ChangeSet()
        in_txn = self.in_long_transaction
        self._long_txn_start += 1
        if not in_txn:
            self.start_long_transaction()
            

    def abort(self):
        self.end_transaction()

    def end_transaction(self):
        if self._changeset:
            self._changeset = None
            self.end_long_transaction()
    
    def commit(self):
        if self._changeset:
            self.apply_changes(self._changeset)
        self.end_transaction()
        self.reload_metacontext()
        

    def really_commit(self):
        pass


    def commit(self):
        if self.in_outer_transaction():
            self.really_commit()
            self.end_long_transaction()
        self.close_current_transaction()


    def in_outer_transaction(self):
        return self._long_txn_start == 1

    def close_current_transaction(self):
        if self.in_long_transaction:
            self._long_txn_start -= 1

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
        self.reload_metacontext()

    def _db_has_collection(self, name):
        return False
    
    # Changesets

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

    def remove_collection(self, collection_name):
        pass
        
    def apply_changes(self, changeset):
        self.begin_transaction()
        changeset.attributes.apply_to_db(self.collections)
        changeset.classes.apply_to_db(self.collections)
        changeset.roles.apply_to_db(self.collections)
        changeset.tags.apply_to_db(self.collections)
        changeset.groups.apply_to_db(self.collections)
        changeset.objects.apply_to_db(self.collections)
        changeset.related.apply_to_db(self.collections)
        changeset.queries.apply_to_db(self.collections)
        self.log_changes(changeset)
        self.commit()
        self.reload_metacontext()

