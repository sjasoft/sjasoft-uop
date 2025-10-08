__author__ = 'samantha'

from functools import partial
from sjasoft.uop import tenant
from sjasoft.uop.collections import uop_collection_names, meta_kinds, assoc_kinds, per_tenant_kinds, cls_extension_field
from sjasoft.uop.constraints import ConstraintViolation
from sjasoft.uopmeta.schemas.meta import kind_map
from collections import deque
import datetime
shared_collections = meta_kinds


class DatabaseCollections(object):

    def __getattr__(self, name):
        return self._collections[name]

    def __init__(self, db):
        self._collections = {}
        self._db = db
        self._extensions = {}

    def extension(self, cls):
        name = cls.get(cls_extension_field)
        if not name:
            name = self._db.new_collection_name()
            cls[cls_extension_field] = name
            self.classes.update_one(cls['id'], {cls_extension_field: name})
        return name

    def ensure_class_extensions(self):
        classes = self.classes.find()
        for cls in classes:
            if not cls['id'] in self._extensions:
                extension = self._db.get_managed_collection(self.extension(cls), cls)
                self._extensions[cls['id']] = extension

    def ensure_collections(self, col_map, override=False):
        for name in col_map:
            if override or not self._collections.get(name):
                schema = kind_map.get(name)
                self._collections[name] = self._db.get_manaded_collection(name, schema)

    def metadata(self):
        return {k: self._collections[k].find() for k in shared_collections}

    def drop_collections(self, collections):
        for col in collections:
            col.drop()

    def class_extension(self, cls_id):
        cls = self.classes.get(cls_id)
        return self._extensions.get[cls_id]

    def get(self, name, schema=None):
        col = self._collections.get(name)
        if not col:
            col = self._db.get_managed_collection(name, schema)
            self._collections[name] = col
        return col


class DBCollection(object):
    """ Abstract collection base."""
    ID_Field = 'id'

    @classmethod
    def ensure_criteria(cls, tenant_id=None):
        pass

    def __init__(self, collection, indexed=False,  *constraints):
        self._indexed = indexed  # Indexed in memory cache or not.
        self._by_id = {}
        self._by_name = {}
        self._coll = collection
        self._constraints = list(constraints)

    def ensure_index(self, coll, *attr_order):
        pass

    def standard_id(self, data):
        self.db_id(data)

    def db_id(self, data):
        pass

    def un_db_id(self, data):
        if not isinstance(data, dict):
            return data
        if self.ID_Field != 'id':
            if self.ID_Field in data:
                data['id'] = data.pop(self.ID_Field)
        return data


    @property
    def name(self):
        return self._coll.name

    def _index(self, json_object):
        pass

    def distinct(self, key, criteria):
        return set(self.find(criteria, only_cols=[key]))

    def _make_id_getter(self, key_name, the_dict):
        def get_by_index(value):
            obj = the_dict.get(value)
            if not obj:
                obj = self.find_one({key_name: value})
                if obj:
                    obj_id = obj['_id']
                    self._by_name[obj['name']] = obj
                    self._by_id[obj['_id']] = obj
                    return obj['_id']
            else:
                return obj['_id']

        return get_by_index

    def with_name(self, name):
        return self._by_name.get(name)

    def count(self, criteria):
        self.db_id(criteria)
        return self._coll.count(criteria)

    def add_constraints(self, *constraints):
        self._constraints.extend(constraints)

    def _filter_constraints(self, kind, is_admin):
        relevant = lambda constraint: kind in constraint.relevant_to
        not_admin_ok = lambda constraint: not (is_admin and constraint._admin_ok)
        return [x for x in self._constraints if relevant(x) and not_admin_ok(x)]

    def constrain_insert(self, data, is_admin=False, **other):
        for constrain in self._filter_constraints('insert', is_admin):
            constrain(data)

    def constrain_modify(self, criteria, mods, is_admin=False, **other):
        for constrain in self._filter_constraints('modify', is_admin):
            constrain(criteria=criteria, mods=mods)
        if not is_admin:
            if not isinstance(criteria, dict):
                criteria = {'_id': criteria}
            if not all(self.find(criteria, only_cols=['mutable'])):
                raise ConstraintViolation('not mutable', criteria=criteria, mods=mods)

    def constrain_delete(self, criteria, is_admin=False, **other):
        for constrain in self._filter_constraints('delete', is_admin):
            constrain(criteria=criteria)
        if not is_admin:
            obj = self.get(criteria)
            if obj and not obj.get('mutable'):
                raise ConstraintViolation('cannot delete', criteria)

    def update(self, selector, mods, partial=True):
        pass

    def replace_one(self, an_id, data):
        self._coll.replace_one({'_id': an_id}, data)
    
    def replace(self, object):
        id = object.pop('id')
        return self.replace_one(id, object)
        
    def drop(self):
        cond = {}
        if cond:
            self.remove(cond)
        else:
            self._coll.drop()

    def _unindex_id(self, an_id):
        item = self._by_id.pop(an_id, None)
        self._by_name.pop(item['name'], None)

    def _change_indexed(self, dict_or_id, change_fn):
        if not self._indexed: return
        if isinstance(dict_or_id, dict):
            ids = self.ids_only(dict_or_id)
            map(change_fn, ids)
        else:
            change_fn(dict_or_id)

    def _unindex(self, dict_or_id):
        self._change_indexed(dict_or_id, self._unindex_id)

    def insert(self, **fields):
        pass

    def bulk_load(self, *ids):
        pass

    def remove(self, dict_or_key):
        pass

    def remove_all(self):
        return self.remove({})

    def remove_instance(self, instance_id):
        return self.remove(instance_id)

    def modified_criteria(self, criteria):
        '''
        Some criteria types are a bit different than standard query, especially around property query.
        :param criteria: original criteria
        :return: modified criteria'''

        self.db_id(criteria)
        return criteria

    def find(self, criteria=None, only_cols=None,
                   order_by=None, limit=None, ids_only=False):
        return []

    def all(self):
        return self.find()

    def ids_only(self, criteria=None):
        return self.find(criteria=criteria, only_cols=[self.ID_Field])

    def find_one(self, criteria, only_cols=None):
        res = self.find(criteria, only_cols=only_cols,
                              limit=1)
        return res[0] if res else None

    def exists(self, criteria):
        return self.count(criteria)

    def contains_id(self, an_id):
        if an_id not in self._by_id:
            return self.exists({'_id': an_id})
        return True

    def get(self, instance_id):
        data = None
        if self._indexed:
            data = self._by_id.get(instance_id)
        if not data:
            data = self.find_one({'id': instance_id})
        if data and self._indexed:
            self._index(data)
        return data

    def get_all(self):
        """
        Returns a dictionary of mapping record ids to records for all
        records in the collection
        :return: the mapping
        """
        return {x['_id']: x for x in self.find()}

    def instances(self):
        return self.find()

