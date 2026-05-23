from collections import defaultdict
from contextlib import asynccontextmanager

comment = defaultdict(set)
from sjasoft.utils.decorations import abstract
import time
from sjasoft.utils.data import async_recurse_set
from sjasoft.utils import decorations
from uop.core import changeset
from uop.meta import oid
from uop.meta.schemas import meta
from uop.core import async_db_collection as db_coll
from uop.core import database as base

logger = base.logger


class Database(base.Database):
    async def get_metadata(self):
        return await self.collections.metadata()

    async def reload_metacontext(self):
        coll_meta = await self.get_metadata()
        self._context = meta.MetaContext.from_data(coll_meta)

    async def open_db(self, setup=None):
        self._collections = db_coll.DatabaseCollections(self)
        colmap = base.uop_collection_names
        if self._tenant_id:
            self._tenant = await self.get_tenant(self._tenant_id)
            if self._tenant:
                colmap.update(self._tenant.base_collections)
        await self._collections.ensure_collections(colmap)
        await self._collections.ensure_class_extensions()
        self._collections_complete = True

        await self.reload_metacontext()
        self._known_schemas = await self.db_schema_names()
        for schema in self._mandatory_schemas:
            await self.ensure_schema(schema)

    async def get_tenant(self, tenant_id):
        tenants = await self.tenants()
        return await tenants.get(tenant_id)

    async def make_random_collection(self):
        res = base.index.make_id(48)
        if not res[0].isalpha():
            res = "x" + res
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

    async def get_managed_collection(self, name, schema=None):
        known = self.collections.get(name)
        if not known:
            raw = await self.get_raw_collection(name, schema)
            known = self.wrap_raw_collection(raw)
        return known

    @property
    def collections(self):
        if not self._collections:
            self._collections = db_coll.DatabaseCollections(self)
        return self._collections

    async def ensure_database_info(self):
        db_info = self.db_info()
        db = self.database_collection()
        if not db_info:
            db_info = await db.insert(_id=self._id, tenancy=self._tenancy)
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
                    col_map = tenant.get("collections_map")
                    collections = db_coll.DatabaseCollections(self, tenant_id=tenant_id)
                    await collections.ensure_basic_collections(col_map)
                    self._tenant_map[tenant_id] = collections
        return collections

    async def log_changes(self, changeset, tenant_id=""):
        """Log the changeset.
        We could log external to the main database but here we will presume that
        logging is local.
        """
        changes = meta.MetaChanges(
            timestamp=time.time(),
            tenant_id=tenant_id or "",
            changes=changeset.to_dict(),
        )
        coll = self.collections.changes
        await coll.insert(**changes.dict())

    @asynccontextmanager
    async def changes(self):
        changes = self._changeset or changeset.ChangeSet()
        yield changes
        if not self._changeset:
            await self.apply_changes(changes)

    async def changes_since(self, epochtime, tenant_id, client_id=None):
        tenant_id = tenant_id or 0
        client_id = client_id or 0
        criteria = changeset.changes.criteria(epochtime, tenant_id, client_id)
        changesets = await self.collections.changes.find(
            criteria, order_by=("timestamp",), only_cols=("changes",)
        )
        return changeset.ChangeSet.combine_changes(*changesets)

    async def apply_changes(self, changeset):
        extensions_to_remove = []

        async def delete_class(cls_id):
            cls = self.metacontext.classes.by_id.get(cls_id)
            if cls:
                coll_name = getattr(cls, base.cls_extension_field)
                if coll_name:
                    coll = self.collections.get(coll_name)
                    if coll:
                        await coll.drop()
            criteria = changeset.classes.deletion_criteria(cls_id)
            await self.collections.related.remove(criteria)

        async def delete_attribute(attr_id):
            pass

        async def delete_role(role_id):
            criteria = changeset.roles.deletion_criteria(role_id)
            await self.collections.related.remove(criteria)

        async def delete_tag(tag_id):
            criteria = changeset.tags.deletion_criteria(
                tag_id, self.role_id("tag_applies")
            )
            await self.collections.related.remove(criteria)

        async def delete_group(group_id):
            containing_role_id = self.role_id("group_contains")
            contains_criteria = changeset.groups.containing_criteria(
                group_id, containing_role_id
            )
            await self.collections.related.remove(contains_criteria)
            contained_role_id = self.role_id("contains_group")
            contained_criteria = changeset.groups.contained_criteria(
                group_id, contained_role_id
            )
            await self.collections.related.remove(contained_criteria)

        async def delete_object(object_id):
            criteria = changeset.objects.deletion_criteria(object_id)
            await self.collections.related.remove(criteria)

        async def delete_query(query_id):
            pass

        delete_completions = dict(
            classes=delete_class,
            attributes=delete_attribute,
            roles=delete_role,
            tags=delete_tag,
            groups=delete_group,
            objects=delete_object,
            queries=delete_query,
        )

        async def apply_object_changes(changes):
            for k, v in changes.inserted.items():
                coll = await self.containing_collection(k)
                await coll.insert(**v)
            for k, v in changes.modified.items():
                coll = await self.containing_collection(k)
                await coll.update_one(k, v)
            for k in changes.deleted:
                coll = await self.containing_collection(k)
                await coll.remove(k)
                await delete_completions[changes.kind](k)

        async def apply_meta_changes(changes):
            coll = getattr(self.collections, changes.kind)
            for k, v in changes.inserted.items():
                await coll.insert(**v)
            for k, v in changes.modified.items():
                await coll.update_one(k, v)
            for k in changes.deleted:
                await coll.remove(k)
                await delete_completions[changes.kind](k)

        async def apply_related_changes(changes):
            for related in changes.inserted:
                await self.collections.related.insert(**related.without_kind())
            for related in changes.deleted:
                await self.collections.related.remove(related.without_kind())

        do_transaction = not self.in_long_transaction
        if do_transaction:
            await self.begin_transaction()
        for kind in base.crud_kinds:
            fn = apply_object_changes if kind == "objects" else apply_meta_changes
            await fn(getattr(changeset, kind))
        await apply_related_changes(changeset.related)

        for extension_name in extensions_to_remove:
            self.collections.drop_extension(extension_name)
        await self.log_changes(changeset, tenant_id=self._tenant_id)
        changeset.clear()
        self._changeset = None
        if do_transaction:
            await self.commit()

    async def ensure_core_schema(self):
        await self.ensure_schema(meta.core_schema)

    async def db_schema_names(self):
        schemas_coll = self._collections.schemas
        schema_data = await schemas_coll.find(only_cols=("name",))
        return set(schema_data) if schema_data else set()


    async def ensure_schema(self, schema_name:str):
        if schema_name not in self._known_schemas:
            a_schema = self._schema_store.schema_named(schema_name)
            if not a_schema:
                raise Exception(f"No such schema {schema_name} in schema store")    
            for dep_name in a_schema.requires_schemas:
                    await self.ensure_schema(dep_name)
            await self.ensure_schema_installed(a_schema)


    async def ensure_schema_installed(self, a_schema):
        changes = changeset.meta_context_schema_diff(self.metacontext, a_schema)
        has_changes = changes.has_changes()
        if has_changes:
            await self.apply_changes(changes)
            await self.reload_metacontext()
        await self.add_schema(a_schema)
        self._known_schemas.add(a_schema.name)


    async def object_ok(self, object_id):
        cls_id = oid.oid_class(object_id)
        if self.class_ok(cls_id):
            coll = await self.extension(cls_id)
            return await coll.contains_id(object_id)
        return False

    async def add_schema(self, a_schema: meta.Schema):
        """
        Adds a schema to the database.
        :param a_schema: a Schema
        :return: None
        """
        await self.collections.schemas.insert(**a_schema.dict())

    async def extension(self, cls_id):
        return await self.collections.class_extension(cls_id)

    async def add_tenant(self, tenant: meta.Tenant):
        tenant = self.collections.tenants.insert(**tenant.dict())
        return tenant

    async def add_tenant_user(self, tenant_id: str, user_id: str):
        await self.relate(tenant_id, self.role_id["has_user"], user_id)

    async def remove_tenant_user(self, tenant_id: str, user_id: str):
        await self.unrelate(tenant_id, self.role_id["has_user"], user_id)

    async def drop_tenant(self, tenant_id):
        """
        Drops the tenant from the database.  This version removes their data.
        :param tenant_id id of the tenant to remove
        """
        collections = self.get_tenant_collections(tenant_id)
        if collections:
            await self.collections.drop_collections(collections)

    async def get_tenant_collections(self, tenant_id):
        tenant = await self.get_tenant(tenant_id)
        return tenant.base_collections

    async def create_tenant(self, name=""):
        tenant = meta.Tenant(name=name)
        for kind in base.per_tenant_kinds:
            tenant.base_collections[kind] = self.random_collection_name()
        self.add_tenant(tenant)
        return tenant

    @base.contextmanager
    async def perhaps_committing(self, commit=False):
        yield
        if commit:
            await self.commit()

    async def start_long_transaction(self):
        self._changeset = changeset.ChangeSet()

    async def end_long_transaction(self):
        self._long_txn_start = 0
        self._changeset = None
        await self.reload_metacontext()

    async def begin_transaction(self):
        if not self.in_long_transaction:
            await self.start_long_transaction()
        self._long_txn_start += 1

    async def abort(self):
        await self.end_transaction()

    async def really_commit(self):
        if self._changeset:
            await self.apply_changes(self._changeset)
        await self.db_commit()
        await self.end_long_transaction()

    async def commit(self):
        if self.in_outer_transaction():
            await self.really_commit()
        else:
            self.close_current_transaction()

    async def db_commit(self):
        pass

    async def db_abort(self):
        pass

    # Basic CRUD

    async def insert(self, kind, **spec):
        creator = base.kind_map[kind]
        coll = getattr(self.collections, kind)
        data = creator(**spec)
        return await self.meta_insert(data)

    async def upsert(self, class_name, data):
        the_id = data.get("id")
        m_class = self.metaclass_named(class_name)
        m_id = m_class.id
        extension = await self.extension(m_id)
        if the_id and m_id == oid.oid_class(the_id):
            await extension.replace(data)
        else:
            data.pop("id", None)
            await self.create_instance_of(class_name, **data)

    async def modify(self, kind, an_id, mods):
        coll = getattr(self.collections, kind)
        return await self.meta_modify(kind, an_id, **mods)

    async def delete(self, kind, an_id):
        coll = getattr(self.collections, kind)
        return await self.meta_delete(kind, an_id)

    async def add_class(self, **spec):
        return await self.insert("classes", **spec)

    async def modify_class(self, cls_id, **mods):
        return await self.modify("classes", cls_id, mods)

    async def delete_class(self, clsid):
        return await self.delete("classes", clsid)

    async def add_attribute(self, **spec):
        return await self.insert("attributes", **spec)

    async def modify_attribute(self, attr_id, **mods):
        return await self.modify("attributes", attr_id, mods)

    async def delete_attribute(self, attrid):
        return await self.delete("attributes", attrid)

    async def modify_role(self, role_id, **mods):
        return await self.modify("roles", role_id, mods)

    async def add_role(self, **spec):
        return await self.insert("roles", **spec)

    async def delete_role(self, role_id):
        return await self.delete("roles", role_id)

    async def add_tag(self, **spec):
        return await self.insert("tags", **spec)

    async def modify_tag(self, tag_id, **mods):
        return await self.modify("tags", tag_id, mods)

    async def delete_tag(self, tag_id):
        return await self.delete("tags", tag_id)

    async def add_group(self, **spec):
        return await self.insert("groups", **spec)

    async def modify_group(self, group_id, **mods):
        return await self.modify("groups", group_id, mods)

    async def delete_group(self, group_id):
        return await self.delete("groups", group_id)

    async def add_object(self, obj):
        return await self.meta_insert(obj)

    async def modify_object(self, uuid, mods):
        return await self.meta_modify("objects", uuid, **mods)

    async def delete_object(self, uuid):
        return await self.meta_delete("objects", uuid)

    async def add_query(self, **spec):
        return await self.insert("queries", **spec)

    async def modify_query(self, query_id, **mods):
        return await self.modify("queries", query_id, mods)

    async def delete_query(self, query_id):
        return await self.delete("queries", query_id)

    # Classes

    async def containing_collection(self, uuid):
        return await self.extension(oid.oid_class(uuid))

    async def class_collection(self, name):
        cls = self.metaclass_named(name)
        return await self.extension(cls.id)

    async def create_instance_of(
        self, clsName, use_defaults=False, record=True, **data
    ):
        """
        creates and saves an instance of the class with the given name
        :param clsName: name of the class
        :param commit: whether to flush the new instance to database immediately
        :param data:  key,value dict of field values
        :return: the new saved object
        """
        cls = self.get_meta_named("classes", clsName)
        if cls:
            try:
                obj = cls.make_instance(use_defaults=use_defaults, **data)
                if record:
                    return await self.add_object(obj)
                return obj
            except Exception as e:
                raise e
        else:
            raise Exception(f"No class named {clsName}")

    # objects and their relationships

    async def get_object(self, uuid):
        coll = await self.containing_collection(uuid)
        obj = await coll.get(uuid)
        return obj

    async def bulk_load(self, uuids, preserve_order=True):
        by_cls = base.partition(uuids, oid.oid_class)
        res = []
        for cls_id, ids in by_cls.items():
            coll = await self.extension(cls_id)
            res.extend(await coll.bulk_load(ids))
        if preserve_order:
            by_id = {x["_id"]: x for x in res}
            res = [by_id[i] for i in uuids]
        return res

    async def oid_short_form(self, oid):
        obj = await self.get_object(oid)
        if obj:
            return self.object_short_form(obj)

    async def get_object_roles(self, uuid):
        "returns all role_ids that the object is subject in"
        data = set(
            await self.collections.related.distinct(
                "assoc_id", criteria=dict(subject_id=uuid)
            )
        )
        data_rev = set(
            await self.collections.related.distinct(
                "assoc_id", criteria=dict(object_id=uuid)
            )
        )
        return data, data_rev  # return both forward and reverse applicable roles

    async def object_for_url(self, url, record=False, **other_fields):
        """
        Find WebURL type object by url.
        Always create object for url if we don't have one already
        :param url: the url
        :param record: whether to insert?
        :return: the object data for persistent WebURL
        """

        results = await self.instances_satisfying("WebURL", base.Q.eq("url", url))
        if results:
            return {"existing": True, "object": results[0]}
        object = await self.create_instance_of(
            "WebURL", record=record, url=url, **other_fields
        )
        return {"existing": False, "object": object}

    async def get_by_objectRef(
        self, short_form, create_if_missing=False, recordNew=True
    ):
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
            return await self.get_object(short_form)

        urlstring = base.is_url(short_form)
        if urlstring:
            return await self.object_for_url(short_form, record=recordNew)
        else:
            pat = base.re.compile(r"(?P<clsName>[^\(]+)\((?P<objectSpec>[^\)]+)\)")
            clsName, objSpec = base.match_fields(
                pat, short_form, "clsName", "objectSpec"
            )
            if base.is_uuid(objSpec):
                return await self.get_object(objSpec)
            if clsName and objSpec:
                the_class = self.metaclass_named(clsName)
                short_attrs = the_class.short_attributes()
                vals = [x.strip() for x in objSpec.split(",")]
                pairs = [
                    (a.name, a.val_from_string(v)) for a, v in zip(short_attrs, vals)
                ]
                query_parts = [base.Q.of_type("clsName")] + [
                    base.Q.eq(p[0], p[1]) for p in pairs
                ]
                query = base.Q.all(*query_parts)
                obj = await self.query(query)
                if create_if_missing and not obj:
                    obj = await self.create_instance_of(
                        clsName, record=recordNew, **dict(pairs)
                    )
                    return {"existing": False, "object": obj}
                return {"existing": True, "object": obj}

    async def get_object_relationships(self, uuid):
        """dictionary of role_id to object_id set"""
        roles, reverse_roles = await self.get_object_roles(uuid)
        forward = dict([(r, await self.get_roleset(uuid, r)) for r in roles])
        reverse = dict(
            [(r, await self.get_roleset(uuid, r, True)) for r in reverse_roles]
        )
        return forward, reverse

    async def get_related_objects(self, uuid):
        related, rev_related = await self.get_object_relationships(uuid)
        res = base.reduce(lambda a, b: a | b, related.values(), set())
        res = base.reduce(lambda a, b: a | b, rev_related.values(), res)
        return res

    async def get_object_tags(self, uuid):
        role_id = self.roles.by_name["tag_applies"]
        res = await self.get_roleset(uuid, role_id, reverse=True)
        return res

    async def get_object_groups(self, uuid, recursive=False):
        """
        An object can directly be in various groups.  While
        these direct groups may be in other groups the object is only directly in
        the first set.
        :param uuid:
        :param recursive:
        :return:
        """
        role_id = self.roles.by_name["group_contains"]
        res = await self.get_roleset(uuid, role_id, reverse=True)
        if recursive:
            return await async_recurse_set(
                res, lambda gid: self.groups_containing_group(gid)
            )
        return res

    async def get_object_data(self, uid):
        obj = await self.get_object(uid)
        if not obj:
            raise base.NoSuchObject(uid)
        return obj

    async def ensure_object(self, uuid):
        if not await self.containing_collection(uuid).contains_id(uuid):
            raise base.NoSuchObject(uuid)

    async def modify_object_tags(self, object_id, tag_ids, do_replace=False):
        role_id = self.roles.by_name["tag_applies"]
        return await self.modify_associated_with_role(
            role_id, object_id, tag_ids, do_replace=do_replace
        )

    async def modify_object_groups(self, object_id, group_ids, do_replace=False):
        role_id = self.roles.by_name["group_contains"]
        return await self.modify_associated_with_role(
            role_id, object_id, group_ids, do_replace=do_replace
        )

    # Tags

    async def get_tagset(self, tag_id, recursive=False):
        role_id = self.roles.by_name["tag_applies"]
        tags = set(tag_id)
        if recursive:
            tags.update(self.metacontext.subtags(tag_id))
        sets = [await self.get_roleset(tid, role_id) for tid in tags]
        return base.reduce(lambda a, b: a | b, sets, set())

    async def modify_tag_objects(
        self, tag_id, object_ids, do_replace=False, reverse=True
    ):
        role_id = self.roles.by_name["tag_applies"]
        return await self.modify_associated_with_role(
            role_id, tag_id, object_ids, do_replace=do_replace, reverse=reverse
        )

    async def tagsets(self, tags):
        """
        Returns dict with tag_ids as keys and list objects having
        tag as value.
        """
        tagsets = [await self.get_tagset(t) for t in tags]
        res = base.zip(tags, [list(ts) for ts in tagsets])
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

    async def tag(self, oid, tag):
        role_id = self.role_id("tag_applies")
        return await self.relate(tag, role_id, oid)

    async def untag(self, oid, tagid):
        role_id = self.role_id("tag_applies")
        return await self.unrelate(tagid, role_id, oid)

    # Grousp

    async def get_groupset(self, group_id, recursive=False):
        role_id = self.role_id("group_contains")
        groups = set(group_id)
        if recursive:
            groups.update(await self.groups_in_group(group_id))
        sets = [await self.get_roleset(gid, role_id) for gid in groups]
        return base.reduce(lambda a, b: a | b, sets, set())

    async def modify_group_objects(
        self, group_id, object_ids, do_replace=False, reverse=True
    ):
        role_id = self.role_id("group_contains")
        return await self.modify_associated_with_role(
            role_id, group_id, object_ids, do_replace=do_replace, reverse=reverse
        )

    async def groups_in_group(self, group_id):
        """get groups contained in group using relations instead of directly"""
        role_id = self.roles.by_name["contains_group"]

        async def func(gid):
            return await self.get_roleset(gid, role_id)

        return await async_recurse_set(set(group_id), func)

    async def groups_containing_group(self, group_id):
        """get groups containing group using relations instead of directly"""
        role_id = self.roles.by_name["contains_group"]

        async def func(gid):
            return await self.get_roleset(gid, role_id, reverse=True)

        return await async_recurse_set(set(group_id), func)

    async def groupsets(self, groups):
        """
        Returns dict with group_ids as keys and list objects directly in group as value.
        """
        sets = [await self.get_groupset(t) for t in groups]
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
        role_id = self.name_to_id("roles", "group_contains")
        groups = set(group_id)
        if transitive:
            groups.update(await self.groups_in_group(group_id))
        sets = [await self.get_roleset(gid, role_id) for gid in groups]
        return base.reduce(lambda a, b: a | b, sets, set())

    async def group(self, oid, group_id):
        role_name = "group_contains" if await self.object_ok(oid) else "contains_group"
        role_id = self.role_id(role_name)
        return await self.relate(group_id, role_id, oid)

    async def ungroup(self, oid, group_id):
        role_name = "group_contains" if await self.object_ok(oid) else "contains_group"
        role_id = self.role_id(role_name)
        return await self.unrelate(group_id, role_id, oid)

    # Roles
    async def get_role_related(self, role_id):
        forward = defaultdict(set)
        reversed = defaultdict(set)
        async for data in self.collections.related.find(dict(assoc_id=role_id)):
            subject, object = data["subject_id"], data["object_id"]
            forward[subject].add(object)
            reversed[object].add(subject)
        return forward, reversed

    async def get_related_by_name(self, uuid):
        related, rev_related = await self.get_object_relationships(uuid)
        res = {}
        roles = self.by_id("roles")
        for r, oids in related.items():
            role = roles.get(r)
            res[role.name] = oids
        for r, oids in rev_related.items():
            role = roles.get(r)
            res[role.reverse_name] = oids
        return res

    async def get_roleset(self, subject, role_id, reverse=False):
        key = role_id + ":" + subject
        res = None
        if not res:
            criteria = {"subject_id": subject, "assoc_id": role_id}
            col = "object_id"
            if reverse:
                criteria = {"object_id": subject, "assoc_id": role_id}
                col = "subject_id"
            res = set(
                await self.collections.related.find(criteria=criteria, only_cols=[col])
            )
        return res

    async def modify_object_related(
        self, fixed_id, role_id, object_ids, do_replace=False, reverse=False
    ):
        return await self.modify_associated_with_role(
            role_id, fixed_id, object_ids, do_replace=do_replace, reverse=reverse
        )

    async def modify_associated_with_role(
        self, role_id, an_id, desired, reverse=False, do_replace=False
    ):
        current = await self.get_roleset(an_id, role_id, reverse=not reverse)
        related = lambda some_id: meta.Related(
            subject_id=some_id, assoc_id=role_id, object_id=an_id
        )

        if reverse:
            related = lambda some_id: meta.Related(
                subject_id=an_id, assoc_id=role_id, object_id=some_id
            )

        to_add = desired - current
        to_remove = current - desired
        async with self.changes() as chng:
            for obj in to_add:
                chng.insert("related", related(obj))
            if do_replace:
                for obj in to_remove:
                    chng.delete("related", related(obj))
        return desired

    async def get_all_related_by(self, role_id, reverse=False):
        """
        Get map of subject to objects related by the given role.
        :param role_id: the role id
        :return: the mapping
        """
        res = defaultdict(set)
        key, v_key = ["subject", "object"]
        if reverse:
            key, v_key = v_key, key

        async for rec in self.collections.related.find({"role": role_id}):
            res[rec[key]].add(rec[v_key])
        return res

    async def get_subjects_related(self, role_id):
        """
        Return just the subjects related by role_id
        """
        return set(
            await self.collections.related.find(
                {"role": role_id}, only_cols=["subject"]
            )
        )

    async def get_all_related(self, uuid):
        """
        All objects directly related to uuid regardless of relationship role.
        :param uuid:  the object to find related objects for
        :return: set of object ids of related objects
        """
        res = set(
            await self.collections.related.find(
                {"subject": uuid}, only_cols=["object_id"]
            )
        )
        res.update(
            await self.collections.related.find(
                {"object_id": uuid}, only_cols=["subject"]
            )
        )
        return {r for r in res if oid.has_uuid_form(r)}

    async def relate(self, subject_oid, roleid, object_oid):
        data = meta.Related(
            subject_id=subject_oid, assoc_id=roleid, object_id=object_oid
        )
        if not await self.collections.related.exists(data.without_kind()):
            return await self.meta_insert(data)
        return data

    async def unrelate(self, oid, roleid, other_oid):
        await self.meta_delete(
            "related",
            meta.Related(subject_id=oid, assoc_id=roleid, object_id=other_oid),
        )

    # Chngeset modifiers

    async def meta_insert(self, obj):
        async with self.changes() as chng:
            data = base.as_dict(obj)
            kind = data.pop("kind", "objects")
            chng.insert(kind, data)
        return obj

    async def ensure_meta_named(self, kind, name):
        meta = await self.get_meta_named(kind, name)
        if not meta:
            meta = await self.insert(kind, name=name)
        return meta

    async def meta_modify(self, kind, an_id, **data):
        async with self.changes() as chng:
            res = chng.modify(kind, an_id, data)
        return res

    async def meta_delete(self, kind, id_or_data):
        async with self.changes() as chng:
            if not isinstance(id_or_data, str):
                data = base.as_dict(id_or_data)
                data.pop("kind", None)
                id_or_data = data
            chng.delete(kind, id_or_data)

    # Queries

    async def create_query(self, data):
        query = meta.MetaQuery.from_dict(data)
        return await self.meta_insert(query)

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
                if key.startswith("_"):
                    k = "$%s" % key[1:]
                    return {k: normalized_query(value)}
                else:
                    return q
            elif isinstance(q, list):
                return [normalized_query(i) for i in q]
            else:
                return q

        evaluator = base.QueryEvaluator2(
            normalized_query(query), self, self.metacontext
        )
        return await evaluator()
