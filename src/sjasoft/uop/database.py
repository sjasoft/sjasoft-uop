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
from sjasoft.uop.collections import uop_collection_names
from sjasoft.uop import changeset
from sjasoft.uopmeta.schemas import meta
from sjasoft.utils import decorations
from sjasoft.utils import logging, index
import time
from sjasoft.utils.decorations import abstract
from collections import defaultdict

comment = defaultdict(set)
logger = logging.getLogger('uop.database')


def id_dictionary(doclist):
    return dict([(x['_id'], x) for x in doclist])


def objects(doclist):
    return [x for x in doclist]


class Database(object):
    database_by_id:dict = {}
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
    def with_id(cls, idnum:str):
        return cls.database_by_id.get(idnum)

    @abstract
    def drop_database(self):
        pass

    @classmethod
    def existing_db_names(cls):
        return []

    def __init__(self, index=None, collections=None,
                 tenancy='no_tenants', **dbcredentials):
        self.credentials = dbcredentials
        self._db_info = None
        self.types = meta.base_types
        self._id = index if index else self._index.next()
        self.database_by_id[self._id] = self._db = None
        self._collections = collections
        self._long_txn_start = 0
        self._tenancy = tenancy
        self._applications = None
        self._schemas = None
        self._tenants = None
        self._users = None
        self._tenant_map = {}
        self._base_collections_collected = False
        self.open_db()

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
        if not self._collections:
            self._collections = db_coll.DatabaseCollections(self)
        return self._collections

    def set_tenant_collections(self, tenant_id):
        collections = self.get_tenant_collections(tenant_id)
        self._collections = collections

    def tenants(self):
        if not self._tenants:
            self._tenants = self.collections._collections.get('tenants')
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

    def get_admined_application(self, tenant_id):
        """
        Returns application this tenant is admin for. This assumes
        that a particular tentant can be admin for at most one application.
        This is in keeping with application being available to one
        or more tenants or database wide.

        Args:
            tenant_id (id): ide of tentant

        Returns:
            application_id: id of application object or None
        """
        apps = self.applications()
        app = apps.find_one({'admin_user': tenant_id})
        return app['_id'] if app else None

    def get_tenant(self, tenant_id):
        tenants = self.tenants()
        return tenants.get(tenant_id)

    def drop_tenant(self, tenant_id):
        """
        Drops the tenant from the database.  This version removes their data.
        :param tenant_id id of the tenant to remove
        """
        collections = self.get_tenant_collections(tenant_id)
        if collections:
            self.collections.drop_collections(collections)

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
        return self.collections.get(name)

    def get_standard_collection(self, kind, tenant_modifer=None, name=''):
        coll_name = uop_collection_names[kind]
        pass

    def make_extension_collection(self, cls):
        coll_name = self.random_collection_name()


    def get_tenant_collection(self, name):
        return self.get_managed_collection(self.new_collection_name())

    def database_collection(self):
        return self.collections._collections.get('databases')

    def db_info(self):
        if not self._db_info:
            db = self.database_collection()
            self._db_info = db.get(self._id)
        return self._db_info

    def has_tenants(self):
        return self.db_info()['tenancy'] != 'no_tenants'

    def ensure_database_info(self):
        db_info = self.db_info()
        db = self.database_collection()
        if not db_info:
            db_info = meta.Database(tenancy=self._tenancy)
        return db_info

    def ensure_apps(self):
        pass

    def ensure_meta(self):
        pass

    def ensure_tenants(self):
        pass

    def ensure_setup(self):
        self.collections.ensure_basic_collections()
        self.ensure_database_info()

    def ensure_basic_collections(self):
        if not self._base_collections_collected:
            self.ensure_setup()
            self._base_collections_collected = True


    def get_tenant_collections(self, tenant_id=None):
        """
        Returns a db collections object for the given tenant_id if there
        is such a teannt
        :param tenant_id: id of the tenet
        :return: DBCollections instance or None
        """
        self.ensure_basic_collections()
        collections = self.collections
        if tenant_id:
            tenant = self.get_tenant(tenant_id)
            if tenant:
                collections = self._tenant_map.get(tenant_id)
                if not collections:
                    col_map = tenant.get('collections_map')
                    collections = db_coll.DatabaseCollections(self, tenant_id=tenant_id)
                    collections.ensure_basic_collections(col_map)
                    self._tenant_map[tenant_id] = collections
        return collections

    def all_types(self):
        return [t.persistent_fields() for t in self.types().values()]

    def ensure_extensions(self):
        """
        Ensure that the extensions are in place for the database.  Some databases
        have an issue if there is an attempt to change database structure (e.g.,
        creating a new table) while a transaction is in progress.  This method
        should be called before entering a long transaction to avoid this problem.
        :return:
        """
        self.collections.ensure_class_extensions()

    def start_long_transaction(self):
        pass

    def end_long_transaction(self):
        self._long_txn_start = 0


    def get_existing_collection(self, coll_name):
        return self.collections._collections.get(coll_name)

    def get_collection(self, collection_name):
        return self.collections.get(collection_name)

    def set_up_database(self):
        self._id = self._index.next()
        self.database_by_id[self._id] = self
        self.ensure_basic_collections()
        # TODO figure out whether this code was ever useful for anything and cleanup if so
        # user = User.create(f'owner_{self._id}')
        # self.users().insert(**user)
        # info = dict(
        #     id = self._id,
        #     owner = user.id
        # )
        # self.database_collection().update_one(self._id, {'owner': user.id})


    def open_db(self, setup=None):
        if self._database_new():
            self.set_up_database()

    def _db_has_collection(self, name):
        return False

    def _database_new(self):
        """
        database is new
        """
        return not self._db_has_collection('uop_classes')

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

