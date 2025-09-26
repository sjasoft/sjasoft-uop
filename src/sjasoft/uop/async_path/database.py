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

from collections import defaultdict

comment = defaultdict(set)
from sjasoft.utils.decorations import abstract
import time
from sjasoft.utils import cw_logging, index
from sjasoft.utils import decorations
from sjasoft.uop.async_path import changeset
from sjasoft.uopmeta import oid
from sjasoft.uop.async_path import db_collection as db_coll
from sjasoft.uop import interface as iface
from sjasoft.utils.index import make_id
import asyncio
from sjasoft.uop import database as base

logger = cw_logging.getLogger('uop.database')


def id_dictionary(doclist):
    return dict([(x['_id'], x) for x in doclist])


def objects(doclist):
    return [x for x in doclist]


class Database(base.Database):

    async def make_class_extension(self, cls_id):
        res = make_id(48)
        if not res[0].isalpha():
            res = 'x' + res
        return await self.get_managed_collection(res)

    async def get_admined_application(self, tenant_id):
        apps = await self.applications()
        app = await apps.find_one({'admin_tenant': tenant_id})
        return app['_id'] if app else None

    async def get_tenant(self, tenant_id):
        tenants = await self.tenants()
        return await tenants.get(tenant_id)

    async def make_random_collection(self):
        res = index.make_id(48)
        if not res[0].isalpha():
            res = 'x' + res
        return await self.get_managed_collection(res)

    async def drop_tenant(self, tenant_id):
        """
        Drops the tenant from the database.  This version removes their data.
        :param tenant_id id of the tenant to remove
        """
        collections = await self.get_tenant_collections(tenant_id)
        if collections:
            await collections.drop_collections(collections)

    async def ensure_indices(self, indices):
        pass

    async def gew_raw_collection(self, name):
        pass

    async def ensure_basic_collections(self):
        if not self._base_collections_collected:
            await self.ensure_setup()
            self._base_collections_collected = True
    async def ensure_setup(self):
        await self.collections.ensure_basic_collections()
        await self.ensure_database_info()

    @property
    def collections(self):
        if not self._collections:
            self._collections = db_coll.DatabaseCollections(self)
        return self._collections

    async def ensure_database_info(self):
        db_info = self.db_info()
        db = self.database_collection()
        if not db_info:
            db_info =  await db.insert(_id=self._id, tenancy=self._tenancy)
        return db_info

    async def db_info(self):
        if not self._db_info:
            db = self.database_collection()
            self._db_info = await db.get(self._id)
        return self._db_info

    async def get_tenant_collections(self, tenant_id=None):
        """
        Returns a db collections object for the given tenant_id if there
        is such a teannt
        :param tenant_id: id of the tenet
        :return: DBCollections instance or None
        """
        await self.ensure_basic_collections()
        collections = self.collections
        if tenant_id:
            tenant = await self.get_tenant(tenant_id)
            if tenant:
                collections = self._tenant_map.get(tenant_id)
                if not collections:
                    col_map = tenant.get('collections_map')
                    collections = db_coll.DatabaseCollections(self, tenant_id=tenant_id)
                    await collections.ensure_basic_collections(col_map)
                    self._tenant_map[tenant_id] = collections
        return collections

    async def log_changes(self, changeset, tenant_id=None):
        """ Log the changeset.
        We could log external to the main database but here we will presume that
        logging is local.
        """
        changes = changeset.to_dict()
        changes['timestamp'] = time.time()
        coll = self.get_collection('changes')
        await coll.insert(**changes)

    async def changes_since(self, epochtime, tenant_id, client_id=None):
        client_id = client_id or 0
        change_coll = await self.get_managed_collection('changes')
        changesets = await change_coll.find({'timestamp': {'$gt': epochtime}, 'client_id': {'$ne': client_id}},
                                      order_by=('epochtime',),
                                      only_cols=('changeset',))
        return await changeset.ChangeSet.combine_changes(*changesets)

    async def apply_changes(self, changeset, collections):
        self.begin_transaction()
        # premise is that changeset and dbs conjointly
        # no how to do this much much of the logic is
        # changeset logic
        await changeset.attributes.apply_to_db(collections)
        await changeset.classes.apply_to_db(collections)
        await changeset.roles.apply_to_db(collections)
        await changeset.tags.apply_to_db(collections)
        await changeset.groups.apply_to_db(collections)
        await changeset.objects.apply_to_db(collections)
        await changeset.tagged.apply_to_db(collections)
        await changeset.related.apply_to_db(collections)
        await changeset.grouped.apply_to_db(collections)
        await changeset.queries.apply_to_db(collections)
        await self.log_changes(changeset)
        self.commit()

    async def commit(self):
        await self._db.commit()
