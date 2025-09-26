__author__ = 'samantha'

from functools import partial
from sjasoft.uop import tenant
from sjasoft.uop.collections import uop_collection_names, meta_kinds, assoc_kinds, per_tenant_kinds, cls_extension_field
from collections import deque
import datetime
shared_collections = meta_kinds

class ConstraintViolation(Exception):
    def __init__(self, constraint, data=None, criteria=None, mods=None):
        msg_fmt = "%s violated collection constraint %s"
        offending_change = dict(
            data=data, criteria=criteria, mods=mods
        )
        msg = msg_fmt % (offending_change, constraint)
        super(ConstraintViolation, self).__init__(msg)


class CollectionConstraint(object):
    """
    A constraint on a persistent collection of instances.
    e.g. uniqueness constraints on fields across instances.
    This is the abstract superclass.
    """

    def __init__(self, collection, relevant_to=None, admin_ok=False):
        """
        :param collection the colection the constraint is for
        :param relevant_to what operations on the collection the constraint needs to be ensured over
        :admin_ok whether an admin tenant can bypass the constraint such as a readonly or immutable by normal tenants collection
        """
        self._collection = collection
        self._relevant_to = relevant_to or []
        self._admin_ok = admin_ok

    @property
    def relevant_to(self):
        return self._relevant_to

    def __call__(self, data=None, criteria=None, mods=None):
        """
        Checks the constraint and raises a ConstraintViolation if it is
        violated.  Either data is a full instance suitable for the collection
        or criteria and mods specify criteria for selecting instances to have
        mods applied to.
        :param data optional full instance of the kind this collection expects
        :param criteria optional dict of criteria or an _id appropriate to the collection
        :param mods optional modification dict
        """
        pass


class UniqueField(CollectionConstraint):
    """
    SPecification for the circumstances under which a field should unique across instances containing that field. 
    """

    def __init__(self, name, collection, relevant_to=('insert', 'modify')):
        """
        :panam name - name of the field
        :param collection - the collection context for the uniqueness
        :param relevant_to what operations on the collection uniqueness needs to be ensured over
        """
        self._name = name
        super(UniqueField, self).__init__(collection)

    def __repr__(self):
        return 'unique_field(%s)' % self._name

    def __call__(self, data=None, criteria=None, mods=None):
        """
        Apply this uniqueness constraint.
        :param data info to be inserted in dict form
        :criteria used to limit the set of instances to consider. When
        None consider all instances
        :mods present or modifiaction case
        """

        def raise_exception():
            raise ConstraintViolation(self, data, criteria, mods)

        if criteria and mods:  # modification case
            if self._name in mods:
                matching = None
                if isinstance(criteria, dict):
                    matching_criteria = self._collection.ids_only(criteria)
                    if len(matching_criteria) > 1:
                        raise_exception()
                    elif len(matching_criteria) == 1:
                        matching = matching_criteria[0]
                else:
                    matching = criteria

                if matching:
                    name = self._name
                    matching_name = self._collection.ids_only({name: mods[name]})
                    if (len(matching_name) > 1) or \
                        (matching_name and matching != matching_name[0]):
                        raise_exception()

        elif data:  # insert case
            if self._collection.exists({'name': data['name']}):
                raise_exception()


unique_field = lambda name: partial(UniqueField, name)


class DatabaseCollections(object):

    def __getattr__(self, name):
        return self._collections[name]

    def __init__(self, db, tenancy_type='embedded', tenant_id=None):
        if not tenant_id:
            tenancy_type = 'no_tenants'
        self._tenant_id = tenant_id
        self._tenancy = tenant.get_tenancy(db, tenancy_type, tenant_id=tenant_id)
        self._collections = {}

        self._db = self._tenancy.database()
        self._tenant_condition = self._tenancy.with_tenant
        self._extensions = self._get_extensions()
        self._other = {}



    def extension(self, cls):
        return cls[cls_extension_field]

    def set_extension(self, cls, val):
        if self.extension(cls) != val:
            cls[cls_extension_field] = val

    def _set_class_extension(self, cls, extension):
        self.set_extension(cls, extension.name)
        self._extensions[cls['id']] = extension
        cls['extension'] = extension

    def _save_class_extension(self, cls, extension):
        cls['extension'] = extension
        if not self._tenant_id:
            cls[cls_extension_field] = extension.name
        cid = cls['id']
        current = self.extension(cls)
        name = extension.name
        renamed = current != name
        if self._tenant_id:
            self._extensions[cid] = name
            extension_names = {k: v['name'] for k, v in self._extensions.items()}
            self._save_tenant_extensions(extension_names)
        else:
            self._set_class_extension(cls, extension)
            if renamed:
                cls[cls_extension_field] = name
                self.classes.update_one(cls['id'], {cls_extension_field: name})

    def _save_tenant_extensions(self, extensions):
        self._db.tenants().update_one(self._tenant_id, {'extensions': extensions})

    def get_class_extension(self, cls, output=None):
        cid = cls['id']
        known = self._extensions.get(cid)
        if not known:
            if not self._tenant_id:
                known = cls.get('extension')
            if not known:
                known = self._db.get_instance_collection(self.expanded_class(cls))
                if output:
                    print(cls['name'], known.name, file=output)
                else:
                    with open('extra_extension.txt', 'a') as f:
                        print(cls['name'], known.name, file=f)

                self._save_class_extension(cls, known)
            self._extensions[cid] = known
        return known

    def _get_extensions(self):
        res = {}
        classes = self._collections.get('classes')
        if not classes:
            return res

        classes = classes.instances()

        changed = False
        if self._tenant_id:
            tenant = self._db.get_tenant(self._tenant_id)
            db_extensions = tenant.get('cls_extensions')
            changed = False
            for cls_id, coll_name in db_extensions.items():
                coll = self._db.get_managed_collection(coll_name)
                res[cls_id] = self._collections[coll_name] = coll
        for cls in classes:
            cid = cls['id']
            known = res.get(cid)
            if not known:
                res[cid] = self.get_class_extension(cls)
                changed = True
        if changed and self._tenant_id:
            self._save_tenant_extensions(res)
        return res

    def collection_name_map(self):
        col_names = list(uop_collection_names.keys())
        return {n: getattr(self, n).name for n in col_names}

    def ensure_class_extensions(self):
        classes = self.classes.find()
        with open('extensions.txt', 'a') as f:
            print([(c['id'], c['name']) for c in classes], file=f)
            print(datetime.datetime.now(), file=f)
            for cls in classes:
                if not cls['id'] in self._extensions:
                    self.get_class_extension(cls, output=f)
                    ext = self._extensions[cls['id']]
                    print(cls['name'], ext.name, file=f)
                    assert cls['id'] in self._extensions

    def ensure_basic_collections(self, col_map=None):
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
                modifier = self._tenancy.with_tenant(shared_table=True)
                col_name = uop_collection_names[name]
                self._collections[name] = self._db.get_standard_collection(name, modifier, name=col_name)
        for name in (set(uop_collection_names) - set(shared_collections)):
            if not self._collections.get(name):
                col_name = get_col_name(name)
                col = self._db.get_standard_collection(name, name=col_name)
                self._collections[name] = col

        self._extensions = self._get_extensions()

    def metadata(self):
        return {k: self._collections[k].find() for k in shared_collections}

    def all_collections(self):
        '''
        Returns all managed database collections for current tenant
        :return:
        '''
        cols = [getattr(self, n) for n in uop_collection_names.keys()]
        extension_names = [self.extension(c) for c in self.classes.find()]
        cols += [(self.get(e)) for e in extension_names if e]
        # TODO do we ever tenant others for things that don't belong to tenant?
        cols += list(self._other.values())
        return cols


    def save_collections(self, output_target):
        """This saves all of a tenants collections including changes and class
        extensions to the specified external store.  The external store must
        be a file-like object.
        """
        pass  # TODO (samantha) implement me before we have real tenants

    def drop_collections(self, collections):
        for col in collections:
            col.drop()

    def expanded_class(self, cls):
        by_name = {c['name']: c for c in self.classes.find()}
        attrs = {a['id']:a for a in self.attributes.find()}
        expand_attrs = deque(cls['attrs'])
        super = cls['superclass']
        if not super:
            return cls
        while super:
            s_cls = by_name[super]
            expand_attrs.extendleft(s_cls['attrs'])
            super = s_cls['superclass']
        res = dict(cls)
        res['attrs'] = list(expand_attrs)
        res['attributes'] = [attrs[a] for a in expand_attrs]
        return res


    def class_extension(self, cls_id):
        cls = self.classes.get(cls_id)
        return self.get_class_extension(cls)

    def _collection_tenant_condition(self, name):
        # TODO (sja) this cannot work for database per tenant
        if name in shared_collections:
            return self._tenant_condition
        return None

    def get(self, name):
        col = self._collections.get(name)
        if not col:
            col = self._db.get_managed_collection(name, tenant_modifier=self._collection_tenant_condition(name))
            self._collections[name] = col
        return col


class DBCollection(object):
    """ Abstract collection base."""
    ID_Field = 'id'

    @classmethod
    def ensure_criteria(cls, tenant_id=None):
        pass

    def __init__(self, collection, indexed=False, tenant_modifier=None, *constraints):
        self._indexed = indexed  # Indexed in memory cache or not.
        self._by_id = {}
        self._by_name = {}
        self._coll = collection
        self._constraints = list(constraints)
        self._with_tenant = tenant_modifier or (lambda x: x)

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
        return self._coll.count(self._with_tenant(criteria))

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
        cond = self._with_tenant({})
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
        return self._with_tenant(criteria)

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
        return self.count(self._with_tenant(criteria))

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

