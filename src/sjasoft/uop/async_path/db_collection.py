__author__ = 'samantha'

from sjasoft.utils.index import make_id
from functools import partial
from sjasoft.uop import interface as iface
from sjasoft.uop import db_collection as base
from sjasoft.uop.collections import uop_collection_names, meta_kinds, assoc_kinds, per_tenant_kinds
shared_collections = meta_kinds

default_collection_names = dict(
    tags='metatag',
    classes='metaclass',
    attributes='metattr',
    roles='metarole',
    groups='metagroup',
    queries='metaquery',
    tagged='uop_tagged',
    grouped='uop_grouped',
    related='uop_related',
    changes='changesets',
)



unique_field = lambda name: partial(base.UniqueField, name)


class DatabaseCollections(base.DatabaseCollections):

    async def metadata(self):
        return {k: await self._collections[k].find() for k in shared_collections}

    async def drop_collections(self, collections):
        for col in collections:
            await col.drop()

    async def class_extension(self, cls_id):
        cls = await self.classes.get(cls_id)
        return await self.get_class_extension(cls)

    async def get_class_extension(self, cls):
        cid = cls['id']
        known = self._extensions.get(cid)
        if not known:
            if not self._tenant_id:
                known = cls.get('extension')
            if not known:
                known = await self._db.make_random_collection()
                await self._save_class_extension(cls, known)
            self._extensions[cid] = known
        return known


    async def make_random_collection(self):
        res = index.make_id(48)
        if not res[0].isalpha():
            res = 'x' + res
        return await self.get_managed_collection(res)

    async def get(self, name):
        col = self._collections.get(name)
        if not col:
            col = await self._db.get_managed_collection(name, tenant_modifier=self._collection_tenant_condition(name))
            self._collections[name] = col
        return col

    async def _save_tenant_extensions(self, extensions):
        await self._db.tenants().update_one(self._tenant_id, {'extensions': extensions})

    async def _save_class_extension(self, cls, extension):
        cls['extension'] = extension
        cid = cls['id']
        name = extension.name
        if self._tenant_id:
            self._extensions[cid] = name
            extension_names = {k: v['name'] for k, v in self._extensions.items()}
            await self._save_tenant_extensions(extension_names)
        else:
            cls['extension_name'] = extension.name
            cls['extension'] = extension
            await self.classes.update_one(cls['id'], {'extension_name': cls['extension_name']})


    async def ensure_basic_collections(self, col_map=None):
        """
        set up the base collections on either default collection names or
        those passed in.  The col_map is only non-null when we have a tenant
        which has different collection names for some of the uop_collections
        """


        def get_col_name(name):
            col_name = name
            if name in self._collections:
                col_name = col_map[name]
            elif col_name in uop_collection_names:
                col_name = uop_collection_names[col_name]
            return col_name

        for name in shared_collections:
            if not self._collections.get(name):
                modifier = self._tenancy.with_tenant()
                self._collections[name] = await self._db.get_managed_collection(get_col_name(name), modifier)
        for name in (set(uop_collection_names) - set(shared_collections)):
            if not self._collections.get(name):
                col_name = get_col_name(name)
                col = await self._db.get_managed_collection(col_name)
                self._collections[name] = col

class DBCollection(base.DBCollection):
    """ Abstract collection base."""

    async def ensure_index(self, coll, *attr_order):
        pass

    async def distinct(self, key, criteria):
        pass


    async def update(self, selector, mods, partial=True):
        pass

    async def drop(self):
        cond = self._with_tenant({})
        if cond:
            await self.remove(cond)
        else:
            await self._coll.drop()

    async def insert(self, **fields):
        pass

    async def bulk_load(self, *ids):
        pass

    async def remove(self, dict_or_key):
        pass

    async def remove_instance(self, instance_id):
        return await self.remove(instance_id)

    async def find(self, criteria=None, only_cols=None,
                   order_by=None, limit=None, ids_only=False):
        return []


    async def all(self):
        return await self.find()

    async def ids_only(self, criteria=None):
        return await self.find(criteria=criteria, only_cols=['_id'])

    async def find_one(self, criteria, only_cols=None):
        res = await self.find(criteria, only_cols=only_cols,
                              limit=1)
        return res[0] if res else None

    async def exists(self, criteria):
        return await self.count(self._with_tenant(criteria))

    async def contains_id(self, an_id):
        if an_id not in self._by_id:
            return await self.exists({'_id': an_id})
        return True

    async def get(self, instance_id):
        data = None
        if self._indexed:
            data = self._by_id.get(instance_id)
        if not data:
            data = await self.find_one({'_id': instance_id})
        if data and self._indexed:
            self._index(data)
        return data

    async def get_all(self):
        """
        Returns a dictionary of mapping record ids to records for all
        records in the collection
        :return: the mapping
        """
        data = await self.find()
        return {x['_id']: x for x in data}

    async def instances(self):
        return await self.find()
