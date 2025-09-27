from sjasoft.uop.async_path import changeset
from contextlib import contextmanager
from sjasoft.utils.category import binary_partition, partition
from sjasoft.utils.tools import  match_fields
from sjasoft.web.url import is_url
from sjasoft.uop.query import Q
from sjasoft.uopmeta.schemas import meta
from sjasoft.uopmeta.schemas.meta import MetaContext, Grouped, Tagged, \
    Related, kind_map, BaseModel
from sjasoft.uopmeta import oid
import re
import asyncio
from contextlib import asynccontextmanager
from sjasoft.uop import db_interface as base
from sjasoft.uop.exceptions import NoSuchObject

@asynccontextmanager
async def changes(obj):
    changes = obj._changeset or changeset.ChangeSet()
    yield changes
    if not obj._changeset:
        if obj._cache:
            obj._cache.apply_changes(changes)
        await obj._db.apply_changes(changes, obj._db.collections)


async def get_tenant_interface(db, tenant_id):
    """
    Creates a UserInterface and ensures its collections are mapped
    :param db:  The database instance
    :param user_id: the id of teh user
    :return: the UserInterface
    """
    dbi = Interface(db, tenant_id=tenant_id)
    await dbi.ensure_collections()
    return dbi


class Interface(base.Interface):
    """
    All the major externally facing functionality of UOP should be available here.
    Prequisite is to configure a database, an optional cache, and an optional user.
    On the database the two main design options are passing a previously configured database
    or a desired database class and parameters.  For most cases the first is clearly preferable.
    Passing a user works well with this choice in that the UserDatabase wrapper allowing access
    to only the users data is set up around the database.  This is very convenient for servers
    handling requests for multiple users.
    Similarly a cache should be shared across requests to a process.
    """
    _db = None
    _cache = None

    async def ensure_collections(self):
        if not self._collections:
            # here we should ensure collections correct for tenant
            self._collections = await self._db.get_tenant_collections(self._tenant)
            self._collections_ready = True
            await self.reload_metacontext()

    async def ensure_schema(self, a_schema):
        changes = changeset.meta_context_schema_diff(self.metacontext, a_schema)
        has_changes = changes.has_changes()  # TODO need changes to be async changes or equiv
        if has_changes:
            await self.apply_changes(changes)
        return has_changes, changes


    async def reload_metacontext(self):
        coll_meta = await self.raw_db.collections.metadata()
        self._context = MetaContext.from_data(coll_meta)

    async def update_metadata(self, metadata):
        """
        Modifies metadata, adding, modifying and deleting. The
        most common use is updating application standard metadata
        adding metadata for a new user.  INTERNAL
        :param metadata: Basically a changeset of updates.
        :return: None
        """
        await self._db.apply_changes(metadata, self._db.collections)
        self.reload_metacontext()

    async def commit(self):
        if self._changeset:
            if self._cache:
                self._cache.apply_changes(self._changeset)
            await self._db.apply_changes(self._changeset, self._db.collections)
            self._changeset = None

    async def apply_changes(self, changes):
        """
        Applies the given changes to the current database possibly limited to a tenant.
        Optionally transforms ids from some other metadata set to the ones appropriate here.
        This transform is mainly only used for updating an application which is defined
        as a set of metadata instances only.  Transforming instance ids is not supported.
        :param changes:  the changeset of changes to apply
        :param transform_relative: specification of source metadata so ids can be mapped
        :return: None
        """
        await self._db.apply_changes(changes, self._db.collections)
        await self.reload_metacontext()

    async def changes_until(self, a_time):
        changes = self._db.get_collection('changes')
        changesets = await changes.find({'time': {'le': a_time}})
        if changesets:
            first = changeset.ChangeSet(**changesets[0])
            return first.add_changes(changesets[1:])
        return changeset.ChangeSet()

    @property
    async def has_admin_user(self):
        if not hasattr(self, '_admin_tenant'):
            self._admin_tenant = True
            if self._tenant:
                tenant = await self._db.get_tenant(self._tenant)

                if tenant:
                    self._admin_tenant = tenant['is_admin']
        return self._admin_tenant

    async def get_object_data(self, uid):
        obj = await self.get_object(uid)
        if not obj:
            raise NoSuchObject(uid)
        return obj

    async def ensure_object(self, uuid):
        if not await self.containing_collection(uuid).contains_id(uuid):
            raise NoSuchObject(uuid)

    async def record(self, obj):
        return await self.meta_insert(obj)

    async def object_ok(self, object_id):
        cls_id = oid.oid_class(object_id)
        if self.class_ok(cls_id):
            coll = self.extension(oid.oid_class(object_id))
            coll = await coll
            return await coll.contains_id(object_id)
        return False

    def class_short_form(self, class_id):
        cls = self.get_class(class_id)
        if cls:
            return cls.short_form

    async def object_for_url(self, url, record=False):
        """
        Find WebURL type object by url.
        Always create object for url if we don't have one already
        :param url: the url
        :param record: whether to insert?
        :return: the object data for persistent WebURL
        """
        query = Q.all(Q.eq('url', url), Q.of_type('WebURL'))
        results = list(await self.query(query))
        return {'existing': True, 'object': await self.get_object(results[0])} if results else {'existing': False,
                                                                                                'object': await self.create_instance_of(
                                                                                                    'WebURL',
                                                                                                    record=record,
                                                                                                    url=url)}

    async def get_by_objectRef(self, short_form, create_if_missing=False, recordNew=True):
        """
        Get an object by its short form values.
        @param objectRef of uuid form or className(objectSpec) where objectSpec is either uuid or
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
            return await self.object_for_url(short_form, record=recordNew)
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

    def object_short_form(self, obj):
        '''
        Using the definition of the class of the object return
        a comma separated string of the values of its short form attributes.
    
        :param obj: object to return a short form for
        :return:  the short form object reference
        '''
        cid =  oid.oid_class(obj['_id'])
        cls = self.get_class(cid)
        c_short = cls.short_form
        return '%s(%s)' % (cls.name, ','.join([obj[x] for x in c_short]))


    async def get_object_roles(self, uuid):
        "returns all role_ids that the object is subject in"
        data = set(await self.related.distinct('role_id', criteria=dict(subject=uuid)))
        data_rev = set(await self.related.distinct('role_id', criteria=dict(object_id=uuid)))
        return data | data_rev

    async def get_object_relationships(self, uuid):
        """dictionary of role_id to object_id set"""
        roles = await self.get_object_roles(uuid)
        return dict([(r, await self.get_roleset(uuid, r)) for r in roles])

    async def get_roleset(self, subject, role_id):
        key = role_id + ":" + subject
        res = self._cache and self._cache.get(key)
        if not res:
            role = await self.roles.get(role_id)
            criteria = {'subject': subject, 'associated': role_id}
            col = 'object_id'
            if role['is_reversed']:
                criteria = {'object_id': subject, 'associated': role['reverse_id']}
                col = 'subject'
            res = set(await self.related.find(criteria=criteria, only_cols=[col]))
            if self._cache:
                self._cache.set(key, res)
        return res

    async def modify_associated(self, kind, current, future, constructor, do_replace=False):
        future = set(future)
        async with changes(self) as chng:
            insert = lambda x: chng.insert(kind, constructor(x))
            delete = lambda x: chng.delete(kind, constructor(x))
            map(insert, future - current)
            if do_replace:
                map(delete, current - future)
            return getattr(chng, kind).to_dict()

    def add_object_groups(self, object_id, group_ids):
        group_ids = filter(self.group_ok, group_ids)
        return self.modify_associated(
            'grouped', self.get_object_groups(object_id),
            group_ids, (lambda group_id: meta.Grouped(assoc_id=group_id, object_id=object_id)))

    def set_object_groups(self, object_id, group_ids):
        group_ids = filter(self.group_ok, group_ids)
        return self.modify_associated(
            'grouped', self.get_object_groups(object_id),
            group_ids, (lambda group_id: Grouped(group_id, object_id)), True)

    async def group_item_check(self, item):
        return (await self.group_ok(item)) or (await self.object_ok(item))

    async def add_group_objects(self, group_id, object_ids):

        object_ids = [o for o in object_ids if await self.group_item_check(o)]
        return await self.modify_associated(
            'grouped', self.objects_in_group(group_id),
            object_ids, (lambda object_id: Grouped(group_id, object_id)))

    async def set_group_objects(self, group_id, object_ids):
        object_ids = [o for o in object_ids if await self.group_item_check(o)]
        return self.modify_associated(
            'grouped', await self.objects_in_group(group_id),
            object_ids, (lambda object_id: Grouped(group_id, object_id)), True)

    async def add_object_tags(self, object_id, tag_ids):
        tag_ids = [t for t in tag_ids if await self.tag_ok(t)]
        return self.modify_associated(
            'tagged', self.get_object_tags(object_id),
            tag_ids, (lambda tag_id: Tagged(tag_id, object_id)))

    async def set_object_tags(self, object_id, tag_ids):
        tag_ids = [t for t in tag_ids if await self.tag_ok(t)]
        return self.modify_associated(
            'tagged', await self.get_object_tags(object_id),
            tag_ids, (lambda tag_id: Tagged(tag_id, object_id)), True)

    async def add_tag_objects(self, tag_id, object_ids):
        object_ids = [o for o in object_ids if await self.object_ok(o)]
        return self.modify_associated(
            'tagged', await self.get_tagset(tag_id),
            object_ids, (lambda object_id: Tagged(tag_id, object_id)))

    async def set_tag_objects(self, tag_id, object_ids):
        object_ids = filter(self.object_ok, object_ids)
        return self.modify_associated(
            'tagged', await self.get_tagset(tag_id),
            object_ids, (lambda object_id: Tagged(tag_id, object_id)))

    async def add_object_related(self, subject, role_id, object_ids):
        object_ids = [o for o in object_ids if await self.object_ok(o)]
        return await self.modify_associated(
            'related', await self.get_roleset(subject, role_id),
            object_ids,
            (lambda object_id: Related(subject, role_id, object_id)))

    async def set_object_related(self, subject, role_id, object_ids):
        object_ids = [o for o in object_ids if await self.object_ok(o)]
        return await self.modify_associated(
            'related', await self.get_roleset(subject, role_id),
            object_ids,
            (lambda object_id: Related(subject, role_id, object_id)),
            True)

    async def get_all_related_by(self, role_id):
        '''
        Get map of subject to objects related by the given role.
        :param role_id: the role id
        :return: the mapping
        '''
        res = defaultdict(list)
        for rec in await self.related.find({'role': role_id}):
            res[rec['subject']].append(rec['object'])
        return res

    async def get_subjects_related(self, role_id):
        '''
        Return just the subjects related by role_id
        '''
        return set(await self.related.find({'role': role_id}, only_cols=['subject']))

    async def get_all_related(self, uuid):
        """
        All objects related to uuid regardless of relationship role.
        :param uuid:  the object to find related objects for
        :return: set of object ids of related objects
        """
        res = set(await self.related.find({'subject': uuid}, only_cols=['object_id']))
        res.update(await self.related.find({'object_id': uuid}, only_cols=['subject']))
        return res

    async def get_assocset(self, coll, an_id):
        res = self._cache and self._cache.get(an_id)

        if not res:
            res = set(await coll.find({'associated': an_id}, only_cols=['object_id']))
            if self._cache:
                self._cache.set(an_id, res)
        return res

    async def get_tagset(self, tag_id):
        return await self.get_assocset(self.tagged, tag_id)

    async def get_groupset(self, group_id):
        return await self.get_assocset(self.grouped, group_id)

    async def get_object_tags(self, uuid):
        key = 'tags:%s' % uuid
        # TODO think on this hack and remember it on changes
        res = self._cache and self._cache.get(key)
        if not res:
            res = set(await self.tagged.find({'object_id': uuid}, only_cols=['associated']))
            if self._cache:
                self._cache.set(key, res)
        return res

    async def tagsets(self, tags):
        """
        Returns dict with tag_ids as keys and list objects having
        tag as value.
        """
        tagsets = await asyncio.gather(*[self.get_tagset(t) for t in tags])
        res = zip(tags, [list(ts) for ts in tagsets])
        return dict(res)

    async def tag_neighbors(self, uuid):
        """
        returns tag_id -> tagset excluding uuid for objects related by
        tags to given object
        """
        tags = await self.get_object_tags(uuid)
        if tags:
            return await self.tagsets(tags)
        return {}

    async def groupsets(self, groups):
        """
        Returns dict with group_ids as keys and list objects directly in group as value.
        """
        sets = await asyncio.gather(*[self.get_tagset(t) for t in groups])
        res = zip(groups, [list(items) for items in sets])
        return dict(res)

    async def group_neighbors(self, uuid):
        """
        returns tag_id -> tagset excluding uuid for objects related by
        tags to given object
        """
        groups = await self.get_object_groups(uuid)
        if groups:
            return await self.groupsets(groups)
        return {}

    async def objects_in_group(self, group_id, transitive=False):
        res = set()

        async def do_group(a_group):
            objs = await self.grouped.find({'associated': a_group})
            groups, objects = binary_partition(objs, lambda x: x['is_group'])
            objects = [x['object_id'] for x in objects]
            res.update(objects)
            if groups and transitive:
                for group in groups:
                    await do_group(group['object_id'])

        await do_group(group_id)
        return res

    async def get_object_groups(self, uuid, recursive=False):
        '''
        What does this mean? An object can directly be in various groups.  While
        these direct groups may be in other groups the object is only directly in
        the first set.
        :param uuid:
        :param recursive:
        :return:
        '''
        key = 'groups:%s' % uuid
        res = self._cache and self._cache.get(key)
        if not res:
            res = set()
            if recursive:
                return self.get_direct

            async def get_local_groups(item):
                return await self.grouped.find({'object_id': uuid}, only_cols=['associated'])

            groups = await get_local_groups(uuid)
            if groups:
                if self._cache:
                    self._cache.set(key, groups)
                res.update(groups)
                if recursive:
                    for group_id in groups:
                        await get_local_groups(group_id)
        return list(res)

    async def meta_insert(self, obj):
        async with changes(self) as chng:
            data = self._ensure_dict(obj)
            kind = data.pop('kind', 'objects')
            chng.insert(kind, data)
        return obj

    async def meta_modify(self, kind, an_id, **data):
        async with changes(self) as chng:
            res = chng.modify(kind, an_id, data)
        return res or getattr(self, kind).get(an_id)

    async def meta_delete(self, kind, id_or_data):
        async with changes(self) as chng:
            if not isinstance(id_or_data, str):
                data = id_or_data.dict()
                data.pop('kind', None)
                id_or_data = data
            chng.delete(kind, id_or_data)

    async def tag(self, oid, tag):
        data = Tagged(assoc_id=tag, object_id=oid)
        # TODO this db check for existence so many places seems expensive.
        if not await self.tagged.exists(data.without_kind()):
            return await self.meta_insert(data)
        return data

    async def untag(self, oid, tagid):
        await self.meta_delete('tagged', Tagged(assoc_id=tagid, object_id=oid).dict())

    async def relate(self, subject_oid, roleid, object_oid):
        data = Related(subject_id=subject_oid, assoc_id=roleid, object_id=object_oid)
        if not await self.related.exists(data.without_kind()):
            return await self.meta_insert(data)
        return data

    async def unrelate(self, oid, roleid, other_oid):
        await self.meta_delete('related', Related(subject_id=oid, assoc_id=roleid, object_id=other_oid))

    async def group(self, oid, group_id, is_group=False):
        data = Grouped(assoc_id=group_id, object_id=oid)
        if not await self.grouped.exists(data.without_kind()):
            return await self.meta_insert(data)
        return data

    async def ungroup(self, oid, group_id):
        await self.meta_delete('grouped', Grouped(assoc_id=group_id, object_id=oid))

    async def insert(self, kind, **spec):
        creator = meta.kind_map[kind]
        coll = getattr(self, kind)
        data = creator(**spec)
        self._constrain(coll.constrain_insert, data=data)
        return await self.meta_insert(data)

    async def modify(self, kind, an_id, mods):
        coll = getattr(self, kind)
        self._constrain(coll.constrain_modify, criteria=an_id, mods=mods)
        return await self.meta_modify(kind, an_id, **mods)

    async def delete(self, kind, an_id):
        coll = getattr(self, kind)
        self._constrain(coll.constrain_delete, criteria=an_id)
        return await self.meta_delete(kind, an_id)

    async def add_class(self, **class_spec):
        attributes = class_spec.pop('attributes', [])
        if attributes:
            class_spec['attrs'] = [x['id'] for x in attributes]
            for attribute in attributes:
                attribute.pop('kind', None)
                await self.add_attribute(**attribute)
        cls = await self.insert('classes', **class_spec)
        return cls

    async def modify_class(self, cls_id, **mods):
        return await self.modify('classes', cls_id, mods)

    async def delete_class(self, clsid):
        return await self.delete('classes', clsid)

    async def add_attribute(self, **spec):
        return await self.insert('attributes', **spec)

    async def modify_attribute(self, attr_id, **mods):
        return await self.modify('attributes', attr_id, mods)

    async def add_role(self, **spec):
        return await self.insert('roles', **spec)

    async def modify_role(self, role_id, **mods):
        return await self.modify('roles', role_id, mods)

    async def delete_role(self, role_id):
        return await self.delete('roles', role_id)

    async def add_tag(self, **spec):
        return await self.insert('tags', **spec)

    async def modify_tag(self, tag_id, **mods):
        return await self.modify('tags', tag_id, mods)

    async def delete_tag(self, tag_id):
        await self.meta_delete('tags', tag_id)

    async def add_group(self, **spec):
        return await self.insert('groups', **spec)

    async def modify_group(self, group_id, **mods):
        return await self.modify('groups', group_id, mods)

    async def delete_group(self, group_id):
        await self.delete('groups', group_id)

    async def add_object(self, obj):
        return await self.meta_insert(obj)

    async def modify_object(self, uuid, mods):
        await self.meta_modify('objects', uuid, **mods)

    async def delete_object(self, uuid):
        await self.meta_delete('objects', uuid)

    async def ensure_meta_id(self, kind, id_or_name):
        coll = getattr(self, kind)
        object = await coll.find_one(
            {'$or': [{'_id': id_or_name}, {'name': id_or_name}]})
        return object['_id']


    async def class_instances(self, name):
        cls = self.metaclass_named(name)
        coll = self.extension(cls['_id'])
        return await coll.find()

    async def class_instance_ids(self, name):
        cls = self.metaclass_named(name)
        coll = self.extension(cls['_id'])
        return await coll.ids_only()


    async def create_instance_of(self, clsName, record=True, **data):
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
                obj = cls.make_instance(**data)
                if record:
                    return await self.add_object(obj)
                return obj
            except Exception as e:
                raise e
        else:
            raise Exception(f'No class named {clsName}')

    async def get_object(self, uuid):
        obj = None
        if self._cache:
            obj = self._cache.get(uuid)
        if not obj:
            coll = self.containing_collection(uuid)
            obj = await coll.get(uuid)
        return obj

    async def bulk_load(self, uuids, preserve_order=True):
        by_cls = partition(uuids, oid.oid_class)
        res = []
        for cls_id, ids in by_cls.items():
            coll = await self.extension(cls_id)
            res.extend(await coll.bulk_load(ids))
        if preserve_order:
            by_id = {x['_id']: x for x in res}
            res = [by_id[i] for i in uuids]
        return res

