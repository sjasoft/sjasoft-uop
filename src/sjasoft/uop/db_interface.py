
from sjasoft.uop import changeset
from sjasoft.utils.category import binary_partition, partition
from sjasoft.utils.tools import match_fields
from sjasoft.web.url import is_url
from sjasoft.utils.data import recurse_set
from sjasoft.uop import query as query_module
from sjasoft.uopmeta.schemas.meta import MetaContext, Grouped, Tagged, \
    Related, kind_map, BaseModel, MetaQuery, ClassComponent, AttributeComponent, AndQuery, OrQuery

from sjasoft.uopmeta.schemas import meta
from sjasoft.uop.database import Database
from sjasoft.uop.query import Q
from sjasoft.uopmeta import oid
from sjasoft.uop.exceptions import NoSuchObject
from collections import defaultdict
from functools import reduce

import re
import asyncio
from contextlib import contextmanager

def as_dict(data):
    if isinstance(data, BaseModel):
        return data.dict()
    return dict(data)

@contextmanager
def changes(obj):
    changes = obj._changeset or changeset.ChangeSet()
    yield changes
    if not obj._changeset:
        if obj._cache:
            obj._cache.apply_changes(changes)
        obj._db.apply_changes(changes, obj._db.collections)


def get_tenant_interface(db, tenant_id):
    """
    Creates a UserInterface and ensures its collections are mapped
    :param db:  The database instance
    :param tenant_id: the id of the tenant
    :return: the UserInterface
    """
    dbi = Interface(db, tenant_id=tenant_id)
    dbi.ensure_collections()

    return dbi


class Interface(object):
    """
    All the major externally facing functionality of UOP should be available here.
    Prequisite is to configure a database, an optional cache, and an optional user.
    On the database the two main design options are passing a previously configured database
    or a desired database class and parameters.  For most cases the first is clearly preferable.
    Passing a user works well with this choice in that the tenantDatabase wrapper allowing access
    to only the tenants data is set up around the database.  This is very convenient for servers
    handling requests for multiple tenants.
    Similarly a cache should be shared across requests to a process.
    """

    def __init__(self, db:Database, cache=None, tenant_id=None):
        self._db = db
        self._tenant = tenant_id
        self._cache = cache
        self._collections_ready = not tenant_id
        self._collections = None
        self._changeset = None
        self._metadata = None
        self._context = None

    @property
    def tenant_id(self):
        return self._tenant

    
    @property
    def metacontext(self):
        return self._context
    
    def get_metadata(self):
        return self.raw_db.collections.metadata()

    def reload_metacontext(self):
        coll_meta = self.get_metadata()
        self._context = MetaContext.from_data(coll_meta)

    def ensure_collections(self):
        if not self._collections:
            # here we should ensure collections correct for tenant
            self._collections = self._db.get_tenant_collections(self._tenant)
            self._collections_ready = True
            self.reload_metacontext()


    def ensure_schema(self, a_schema):
        changes = changeset.meta_context_schema_diff(self.metacontext, a_schema)
        has_changes = changes.has_changes()
        if has_changes:
            self.apply_changes(changes)
            self.reload_metacontext()
        return has_changes, changes


    @property
    def collections(self):
        return self._collections


    @contextmanager
    def perhaps_committing(self, commit=False):
        yield
        if commit:
            self.commit()

    def get_class(self, cls_id):
        return self.get_meta('classes', cls_id)

    def by_name(self, kind):
        return self.metacontext.by_name(kind)

    def get_meta(self, kind, an_id):
        return self.metacontext.get_meta(kind, an_id)

    def get_meta_named(self, kind, name):
        return self.metacontext.get_meta_named(kind, name)



    @property
    def raw_db(self):
        return self._db

    def update_metadata(self, metadata):
        """
        Modifies metadata, adding, modifying and deleting. The
        most common use is updating application standard metadata
        adding metadata for a new user.  INTERNAL
        :param metadata: Basically a changeset of updates.
        :return: None
        """
        self._db.apply_changes(metadata, self.collections)
        self.reload_metacontext()

    def begin_transaction(self):
        """starts a changeset that will not be applied unitl commit"""
        if not self._changeset:
            self._changeset = changeset.ChangeSet()
            self._db.begin_transaction()

    def abort(self):
        self.end_transaction()

    def end_transaction(self):
        if self._changeset:
            self._changeset = None
            self._db.end_long_transaction()
    
    def commit(self):
        if self._changeset:
            if self._cache:
                self._cache.apply_changes(self._changeset)
            self._db.apply_changes(self._changeset, self.collections)
        self.end_transaction()
        self.reload_metacontext()

    def apply_changes(self, changes):
        '''
        Applies the given changes to the current database possibly limited to a tenant.
        Optionally transforms ids from some other metadata set to the ones appropriate here.
        This transform is mainly only used for updating an application which is defined
        as a set of metadata instances only.  Transforming instance ids is not supported.
        :param changes:  the changeset of changes to apply
        :return: None
        '''
        self._db.apply_changes(changes, self.collections)
        self.reload_metacontext()

    def changes_until(self, a_time):
        changes = self._db.get_collection('changes')
        changesets = changes.find({'time': {'le': a_time}})
        if changesets:
            first = changeset.ChangeSet(**changesets[0])
            return first.add_changes(changesets[1:])
        return changeset.ChangeSet()

    @property
    def has_admin_user(self):
        # TODO fix this as it doesn't make sense currently
        if not hasattr(self, '_admin_user'):
            self._admin_user = True
            if self._tenant:
                user = self._db.get_user(self._tenant)

                if user:
                    self._admin_user = user['is_admin']
        return self._admin_user

    def get_object_data(self, uid):
        obj = self.get_object(uid)
        if not obj:
            raise NoSuchObject(uid)
        return obj

    def ensure_object(self, uuid):
        if not self.containing_collection(uuid).contains_id(uuid):
            raise NoSuchObject(uuid)

    def record(self, obj):
        return self.meta_insert(obj)

    def extension(self, cls_id):
        return self.collections.class_extension(cls_id)

    @property
    def related(self):
        return self.collections.related

    @property
    def tagged(self):
        return self.collections.tagged

    @property
    def grouped(self):
        return self.collections.grouped

    def by_id(self, kind):
        return getattr(self._context, kind).by_id

    @property
    def roles(self):
        return self.collections.roles

    @property
    def classes(self):
        return self.collections.classes

    @property
    def attributes(self):
        return self.collections.attributes

    @property
    def tags(self):
        return self.collections.tags

    @property
    def groups(self):
        return self.collections.groups

    @property
    def queries(self):
        return self.collections.queries

    def tag_ok(self, tag_id):
        return tag_id in self.tags

    def group_ok(self, group_id):
        return group_id in self.groups

    def role_ok(self, role_id):
        return role_id in self.roles

    def class_ok(self, cls_id):
        return cls_id in self.classes

    def object_ok(self, object_id):
        cls_id = oid.oid_class(object_id)
        if self.class_ok(cls_id):
            coll = self.extension(oid.oid_class(object_id))
            return coll.contains_id(object_id)
        return False

    def class_short_form(self, class_id):
        cls = self.get_class(class_id)
        if cls:
            return cls.short_form

    def is_uuid(self, str):
        return oid.has_uuid_form(str) and self.get_class(oid.oid_class(str))

    def object_for_url(self, url, record=False, **other_fields):
        '''
        Find WebURL type object by url.
        Always create object for url if we don't have one already
        :param url: the url
        :param record: whether to insert?
        :return: the object data for persistent WebURL
        '''

        results = self.instances_satisfying('WebURL', Q.eq('url', url))
        if results:
            return {'existing': True, 'object': results[0]}
        object = self.create_instance_of('WebURL', record=record, url=url, **other_fields)
        return {'existing': False, 'object': object}

    def get_by_objectRef(self, short_form, create_if_missing=False, recordNew=True):
        """
        Get an object by its short form values.
        @param short_form: of uuid form or className(objectSpec) where objectSpec is either uuid or
        comma separated list of attribute values of named class' short form attributes.
        @param create_if_missing: whether to create and object with the short fields if missing. Note
        that an url like string will also create a WebURL if missing.
        TODO: add path like strings and update documentations
        @return: the object if found (or created) else None.
        """
        if self.is_uuid(short_form):
            return self.get_object(short_form)
        
        urlstring = is_url(short_form)
        if urlstring:
            return self.object_for_url(short_form, record=recordNew)
        else:
            pat = re.compile(r'(?P<clsName>[^\(]+)\((?P<objectSpec>[^\)]+)\)')
            clsName, objSpec = match_fields(pat, short_form, 'clsName', 'objectSpec')
            if self.is_uuid(objSpec):
                return self.get_object(objSpec)
            if clsName and objSpec:
                the_class = self.metaclass_named(clsName)
                short_attrs = the_class.short_attributes()
                vals = [x.strip() for x in objSpec.split(',')]
                pairs = [(a.name, a.val_from_string(v)) for a, v in zip(short_attrs, vals)]
                query_parts = [Q.of_type('clsName')] + [Q.eq(p[0], p[1]) for p in pairs]
                query = Q.all(*query_parts)
                obj = self.query(query)
                if create_if_missing and not obj:
                    obj = self.create_instance_of(clsName, record=recordNew, **dict(pairs))
                    return {'existing': False, 'object': obj}
                return {'existing': True, 'object': obj}

    def oid_short_form(self, oid):
        obj = self.get_object(oid)
        if obj:
            return self.object_short_form(obj)

    def object_short_form(self, obj):
        '''
        Using the definition of the class of the object return
        a comma separated string of the values of its short form attributes.
    
        :param obj: object to return a short form for
        :return:  the short form object reference
        '''
        an_oid = obj['id']
        cls = oid.oid_class(an_oid)
        cls = self.get_class(cls)
        c_short = cls.short_form
        if c_short:
            return '%s(%s)' % (cls.name, ','.join([obj[x] for x in c_short]))
        else:
            return f'{cls.name}({an_oid}))'

    def get_object_roles(self, uuid):
        "returns all role_ids that the object is subject in"
        data = set(self.related.distinct('assoc_id', criteria=dict(subject_id=uuid)))
        data_rev = set(self.related.distinct('assoc_id', criteria=dict(object_id=uuid)))
        return data, data_rev  # return both forward and reverse applicable roles

    def get_role_related(self, role_id):
        forward = defaultdict(set)
        reversed = defaultdict(set)
        for data in self.related.find(dict(assoc_id=role_id)):
            subject, object = data['subject_id'], data['object_id']
            forward[subject].add(object)
            reversed[object].add(subject)
        return forward, reversed

    def get_object_relationships(self, uuid):
        """dictionary of role_id to object_id set"""
        roles, reverse_roles = self.get_object_roles(uuid)
        forward = dict([(r, self.get_roleset(uuid, r)) for r in roles])
        reverse = dict([(r, self.get_roleset(uuid, r, True)) for r in reverse_roles])
        return forward, reverse

    def get_related_objects(self, uuid):
        related, rev_related = self.get_object_relationships(uuid)
        res = reduce(lambda a, b: a | b, related.values(), set())
        res = reduce(lambda a, b: a | b, rev_related.values(), res)
        return res

    def get_related_by_name(self, uuid):
        related, rev_related = self.get_object_relationships(uuid)
        res = {}
        roles = self.by_id('roles')
        for r, oids in related.items():
            role = roles.get(r)
            res[role.name] = oids
        for r, oids in rev_related.items():
            role = roles.get(r)
            res[role.reverse_name] = oids
        return res

    def get_roleset(self, subject, role_id, reverse=False):
        key = role_id + ":" + subject
        res = self._cache and self._cache.get(key)
        if not res:
            role = self.roles.get(role_id)
            criteria = {'subject_id': subject, 'assoc_id': role_id}
            col = 'object_id'
            if reverse:
                criteria = {'object_id': subject, 'assoc_id': role_id}
                col = 'subject_id'
            res = set(self.related.find(criteria=criteria, only_cols=[col]))
            if self._cache:
                self._cache.set(key, res)
        return res

    def modify_associated_with_role(self, role_id, an_id, desired, reverse=False, do_replace=False):
        current = self.get_roleset(an_id, role_id, reverse=not reverse)
        related = lambda some_id: Related(subject_id=some_id, assoc_id=role_id, object_id=an_id)
        if reverse:
            related = lambda some_id: Related(subject_id=an_id, assoc_id=role_id, object_id=some_id)
            
        to_add = desired - current
        to_remove = current - desired
        with changes(self) as chng:
            for obj in to_add:
                chng.insert('related', related(obj))
            if do_replace:  
                for obj in to_remove:
                    chng.delete('related', related(obj))
        return desired
    
    def modify_object_tags(self, object_id, tag_ids, do_replace=False):
        role_id = self.roles.by_name['tag_applies']
        return self.modify_associated_with_role(role_id, object_id, tag_ids, do_replace=do_replace)
    
    def modify_object_groups(self, object_id, group_ids, do_replace=False):
        role_id = self.roles.by_name['group_contains']
        return self.modify_associated_with_role(role_id, object_id, group_ids, do_replace=do_replace)

    def modify_tag_objects(self, tag_id, object_ids, do_replace=False, reverse=True):
        role_id = self.roles.by_name['tag_applies']
        return self.modify_associated_with_role(role_id, tag_id, object_ids, do_replace=do_replace, reverse=reverse)
    
    def modify_group_objects(self, group_id, object_ids, do_replace=False, reverse=True):
        role_id = self.roles.by_name['group_contains']
        return self.modify_associated_with_role(role_id, group_id, object_ids, do_replace=do_replace, reverse=reverse)
    
    def modify_object_related(self, fixed_id, role_id, object_ids, do_replace=False, reverse=False):
        return self.modify_associated_with_role(role_id, fixed_id, object_ids, do_replace=do_replace, reverse=reverse)

        
    def groups_in_group(self, group_id):
        """ get groups contained in group using relations instead of directly
        """
        role_id = self.roles.by_name['contains_group']
        func = lambda gid: self.get_roleset(gid, role_id)
        return recurse_set(func(group_id), func)
    
    def groups_containing_group(self, group_id):
        """ get groups containing group using relations instead of directly
        """
        role_id = self.roles.by_name['contains_group']
        func = lambda gid: self.get_roleset(gid, role_id, reverse=True)
        return recurse_set(func(group_id), func)


    def group_item_check(self, item):
        return (self.group_ok(item)) or (self.object_ok(item))


    def get_all_related_by(self, role_id, reverse=False):
        '''
        Get map of subject to objects related by the given role.
        :param role_id: the role id
        :return: the mapping
        '''
        res = defaultdict(set)
        key, v_key = ['subject', 'object']
        if reverse:
            key, v_key = v_key, key

        for rec in self.related.find({'role': role_id}):
            res[rec[key]].add(rec[v_key])
        return res

    def get_subjects_related(self, role_id):
        '''
        Return just the subjects related by role_id
        '''
        return set(self.related.find({'role': role_id}, only_cols=['subject']))

    def get_all_related(self, uuid):
        """
        All objects directly related to uuid regardless of relationship role.
        :param uuid:  the object to find related objects for
        :return: set of object ids of related objects
        """
        res = set(self.related.find({'subject': uuid}, only_cols=['object_id']))
        res.update(self.related.find({'object_id': uuid}, only_cols=['subject']))
        return {r for r in res if oid.has_uuid_form(r)}


    def get_tagset(self, tag_id, recursive=False):
        role_id = self.roles.by_name['tag_applies']
        tags = set(tag_id)
        if recursive:
            tags.update(self.metacontext.subtags(tag_id))
        sets = [self.get_roleset(tid, role_id) for tid in tags]
        return reduce(lambda a, b: a | b, sets, set())

    def get_groupset(self, group_id, recursive=False):
        role_id = self.roles.by_name['group_contains']
        groups = set(group_id)
        if recursive:
            groups.update(self.groups_in_group(group_id))
        sets = [self.get_roleset(gid, role_id) for gid in groups]
        return reduce(lambda a, b: a | b, sets, set())


    def get_object_tags(self, uuid):
        role_id = self.roles.by_name['tag_applies']
        res = self.get_roleset(uuid, role_id, reverse=True)
        return res

    def get_object_groups(self, uuid, recursive=False):
        '''
        An object can directly be in various groups.  While
        these direct groups may be in other groups the object is only directly in
        the first set.
        :param uuid:
        :param recursive:
        :return:
        '''
        role_id = self.roles.by_name['group_contains']
        res = self.get_roleset(uuid, role_id, reverse=True)
        if recursive:
            return recurse_set(res, lambda gid: self.groups_containing_group(gid))
        return res

    def tagsets(self, tags):
        """
        Returns dict with tag_ids as keys and list objects having
        tag as value.
        """
        tagsets = [self.get_tagset(t) for t in tags]
        res = zip(tags, [list(ts) for ts in tagsets])
        return dict(res)

    def tag_neighbors(self, uuid):
        """
        returns tag_id -> tagset excluding uuid for objects related by
        tags to given object
        """
        tags = self.get_object_tags(uuid)
        if tags:
            return self.tagsets(tags)
        return {}

    def groupsets(self, groups):
        """
        Returns dict with group_ids as keys and list objects directly in group as value.
        """
        sets = [self.get_groupset(t) for t in groups]
        res = zip(groups, [list(items) for items in sets])
        return dict(res)

    def group_neighbors(self, uuid):
        """
        returns tag_id -> tagset excluding uuid for objects related by
        tags to given object
        """
        groups = self.get_object_groups(uuid)
        if groups:
            return self.groupsets(groups)
        return {}

    def objects_in_group(self, group_id, transitive=False):
        role_id = self.roles.by_name['group_contains']
        groups = set(group_id)
        if transitive:
            groups.update(self.groups_in_group(group_id))
        sets = [self.get_roleset(gid, role_id) for gid in groups]
        return reduce(lambda a, b: a | b, sets, set())


    def _ensure_dict(self, d):
        if isinstance(d, BaseModel):
            return d.dict()
        elif isinstance(d, meta.MetaQuery):
            return d.to_dict()
        return d

    def upsert(self, class_name, data):
        the_id = data.get('id')
        m_class = self.metaclass_named(class_name)
        m_id = m_class.id
        if the_id and m_id == oid.oid_class(the_id):
            self.extension(m_id).replace(data)
        else:
            data.pop('id', None)
            self.create_instance_of(class_name, **data)

    def create_query(self, data):
        query = meta.MetaQuery.from_dict(data)
        return self.meta_insert(query)

    def meta_insert(self, obj):
        with changes(self) as chng:
            data = self._ensure_dict(obj)
            kind = data.pop('kind', 'objects')
            chng.insert(kind, data)
        return obj

    def ensure_meta_named(self, kind, name):
        meta = self.get_meta_named(kind, name)
        if not meta:
            meta = self.insert(kind, name=name)
        return meta

    def meta_modify(self, kind, an_id, **data):
        with changes(self) as chng:
            res = chng.modify(kind, an_id, data)
        return res or getattr(self, kind,{}).get(an_id)

    def meta_delete(self, kind, id_or_data):
        with changes(self) as chng:
            if not isinstance(id_or_data, str):
                data = as_dict(id_or_data)
                data.pop('kind', None)
                id_or_data = data
            chng.delete(kind, id_or_data)

    def tag(self, oid, tag):
        role_id = self.roles.by_name['tag_applies']
        return self.relate(tag, role_id, oid)

    def untag(self, oid, tagid):
        role_id = self.roles.by_name['tag_applies']     
        return self.unrelate(tagid, role_id, oid)

    def relate(self, subject_oid, roleid, object_oid):
        data = Related(subject_id=subject_oid, assoc_id=roleid, object_id=object_oid)
        if not self.related.exists(data.without_kind()):
            return self.meta_insert(data)
        return data

    def unrelate(self, oid, roleid, other_oid):
        self.meta_delete('related', Related(subject_id=oid, assoc_id=roleid, object_id=other_oid))

    def group(self, oid, group_id):
        role_name = 'group_contains' if self.object_ok(oid) else 'contains_group'
        role_id = self.roles.by_name[role_name]
        return self.relate(group_id, role_id, oid)

    def ungroup(self, oid, group_id):
        role_name = 'group_contains' if self.object_ok(oid) else 'contains_group'
        role_id = self.roles.by_name[role_name]
        self.unrelate(group_id, role_id, oid)

    def _constrain(self, constrainer, data=None, criteria=None, mods=None):
        constrainer(data=data, criteria=criteria, mods=mods, is_admin=self.has_admin_user)

    def insert(self, kind, **spec):
        creator = kind_map[kind]
        coll = getattr(self, kind)
        data = creator(**spec)
        self._constrain(coll.constrain_insert, data=data.without_kind())
        return self.meta_insert(data)

    def modify(self, kind, an_id, mods):
        coll = getattr(self, kind)
        self._constrain(coll.constrain_modify, criteria=an_id, mods=mods)
        return self.meta_modify(kind, an_id, **mods)

    def delete(self, kind, an_id):
        coll = getattr(self, kind)
        self._constrain(coll.constrain_delete, criteria=an_id)
        return self.meta_delete(kind, an_id)

    def add_class(self, **class_spec):
        attributes = class_spec.pop('attributes', [])
        if attributes:
            class_spec['attrs'] = [x['id'] for x in attributes]
            for attribute in attributes:
                attribute.pop('kind', None)
                self.add_attribute(**attribute)
        cls = self.insert('classes', **class_spec)
        return cls

    def modify_class(self, cls_id, **mods):
        return self.modify('classes', cls_id, mods)

    def delete_class(self, clsid):
        return self.delete('classes', clsid)

    def add_attribute(self, **spec):
        return self.insert('attributes', **spec)

    def modify_attribute(self, attr_id, **mods):
        return self.modify('attributes', attr_id, mods)

    def delete_attribute(self, attrid):
        return self.delete('attributes', attrid)

    def add_role(self, **spec):
        return self.insert('roles', **spec)

    def modify_role(self, role_id, **mods):
        return self.modify('roles', role_id, mods)

    def delete_role(self, role_id):
        return self.delete('roles', role_id)

    def add_tag(self, **spec):
        return self.insert('tags', **spec)

    def modify_tag(self, tag_id, **mods):
        return self.modify('tags', tag_id, mods)

    def delete_tag(self, tag_id):
        self.meta_delete('tags', tag_id)

    def add_group(self, **spec):
        return self.insert('groups', **spec)

    def modify_group(self, group_id, **mods):
        return self.modify('groups', group_id, mods)

    def delete_group(self, group_id):
        self.delete('groups', group_id)

    def add_object(self, obj):
        return self.meta_insert(obj)

    def modify_object(self, uuid, mods):
        self.meta_modify('objects', uuid, **mods)

    def delete_object(self, uuid):
        self.meta_delete('objects', uuid)

    def meta_context(self):
        context = MetaContext()


    def metadata(self):
        return dict(
            classes = self.classes.all(),
            roles = self.roles.all(),
            attributes = self.attributes.all(),
            groups = self.groups.all(),
            tags = self.tags.all(),
            queries = self.queries.all()
        )

    def ensure_meta_id(self, kind, id_or_name):
        coll = getattr(self, kind)
        object = coll.find_one(
            {'$or': [{'_id': id_or_name}, {'name': id_or_name}]})
        return object['_id']

    def containing_collection(self, uuid):
        return self.extension(oid.oid_class(uuid))

    def metaclass_named(self, name):
        return self.get_meta_named('classes', name)

    def class_collection(self, name):
        cls = self.metaclass_named(name)
        return self.extension(cls.id)

    def class_instances(self, name):
        coll = self.class_collection(name)
        return coll.find()

    def instances_satisfying(self, name, criteria):
        return self.class_collection(name).find(criteria)

    def class_instance_ids(self, name):
        cls = self.metaclass_named(name)
        coll = self.extension(cls.id)
        return coll.ids_only()

    def create_instance_of(self, clsName, use_defaults=False, record=True, **data):
        '''
        creates and saves an instance of the class with the given name
        :param clsName: name of the class
        :param commit: whether to flush the new instance to database immediately
        :param data:  key,value dict of field values
        :return: the new saved object
        '''
        cls = self.get_meta_named('classes', clsName)
        if cls:
            try:
                obj = cls.make_instance(use_defaults=use_defaults, **data)
                if record:
                    return self.add_object(obj)
                return obj
            except Exception as e:
                raise e
        else:
            raise Exception(f'No class named {clsName}')

    def get_object(self, uuid):
        obj = None
        if self._cache:
            obj = self._cache.get(uuid)
        if not obj:
            coll = self.containing_collection(uuid)
            obj = coll.get(uuid)
        return obj

    def bulk_load(self, uuids, preserve_order=True):
        by_cls = partition(uuids, oid.oid_class)
        res = []
        for cls_id, ids in by_cls.items():
            coll = self.extension(cls_id)
            res.extend(coll.bulk_load(ids))
        if preserve_order:
            by_id = {x['_id']: x for x in res}
            res = [by_id[i] for i in uuids]
        return res


    async def query(self, query):
        """
        Run the meat of a query returning list of satisfying uuids.
        @param: query - query dict object with some single query type
        @param query: the body of the query (not entire query object)
        @returns list of uuids of objects satisfying the query
        """

        def normalized_query(q):
            res = {}
            if isinstance(q, dict):
                key, value = list(q.items())[0]
                if key.startswith('_'):
                    k = '$%s' % key[1:]
                    return {k: normalized_query(value)}
                else:
                    return q
            elif isinstance(q, list):
                return [normalized_query(i) for i in q]
            else:
                return q

        evaluator = query_module.QueryEvaluator2(normalized_query(query), self,
                                                self.metacontext)
        return await evaluator()
