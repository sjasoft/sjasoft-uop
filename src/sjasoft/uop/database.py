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
from sjasoft.web.url import is_url
from sjasoft.utils.tools import match_fields
from sjasoft.utils.category import partition
from sjasoft.utils.data import recurse_sets
from sjasoft.uopmeta.schemas import meta
from sjasoft.uopmeta.schemas.meta import MetaContext, BaseModel, kind_map
from sjasoft.utils import decorations
from sjasoft.utils import logging, index
from sjasoft.utils.decorations import abstract
from sjasoft.uop.query import Q, QueryEvaluator2
from sjasoft.uopmeta import oid
from sjasoft.uop.exceptions import NoSuchObject
from collections import defaultdict
from functools import reduce
import time
from collections import defaultdict
from contextlib import contextmanager

import re

logger = logging.getLogger("uop.database")


def as_dict(data):
    if isinstance(data, BaseModel):
        return data.dict()
    return dict(data)


class Database(object):
    database_by_id = {}
    _meta_id_tree = None
    db_info_collection = "uop_database"

    _index = index.Index("database", 48)

    @classmethod
    def make_test_database(cls):
        "create a randomly named test database of the appropriate type"
        msg = f"{cls.__name__} needs to implement make_test_database"
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

    def __init__(self, tenant_id=None, *schemas, **dbcredentials):
        self.credentials = dbcredentials
        self._collections: db_coll.DatabaseCollections = None
        self._long_txn_start = 0
        self._tenants = None
        self._collections_complete = False
        self._tenant_id = tenant_id
        self._context: meta.MetaContext = None
        self._changeset: changeset.ChangeSet = None
        self._mandatory_schemas = schemas
        self.open_db()

    @property
    def metacontext(self):
        return self._context

    def metadata(self):
        return dict(
            classes=self.classes.all(),
            roles=self.roles.all(),
            attributes=self.attributes.all(),
            groups=self.groups.all(),
            tags=self.tags.all(),
            queries=self.queries.all(),
        )

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

    def random_collection_name(self):
        res = index.make_id(48)
        if not res[0].isalpha():
            res = "x" + res
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

    def tenants(self):
        if not self._tenants:
            self._tenants = self._collections.get("tenants")
        return self._tenants

    def users(self):
        return self._collections.users

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
            known = self.wrap_raw_collection(raw)
        return known

    def wrap_raw_collection(self, raw):
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

    def get_meta(self, kind, an_id):
        return self.metacontext.get_meta(kind, an_id)

    def get_meta_named(self, kind, name):
        return self.metacontext.get_meta_named(kind, name)

    def get_class(self, cls_id):
        return self.get_meta("classes", cls_id)

    def by_id(self, kind):
        return self.metacontext.by_id(kind)

    def by_name_id(self, kind):
        return self.metacontext.by_name_id(kind)

    def by_id_name(self, kind):
        return self.metacontext.by_id_name(kind)

    def role_id(self, name):
        return self.metacontext.roles.name_to_id(name)

    # Checks

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
        user = users.insert(**user.dict())
        return user

    def add_tenant(self, tenant: meta.Tenant):
        tenants = self.tenants()
        tenant = tenants.insert(**tenant.dict())
        return tenant

    def add_tenant_user(self, tenant_id: str, user_id: str):
        self.relate(tenant_id, self.role_id["has_user"], user_id)

    def remove_tenant_user(self, tenant_id: str, user_id: str):
        self.unrelate(tenant_id, self.role_id["has_user"], user_id)

    def drop_tenant(self, tenant_id):
        """
        Drops the tenant from the database.  This version removes their data.
        :param tenant_id id of the tenant to remove
        """
        collections = self.get_tenant_collections(tenant_id)
        if collections:
            self.collections.drop_collections(collections)

    def create_tenannt(self, name=""):
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

    def get_collection(self, collection_name):
        return self.collections.get(collection_name)

    def ensure_core_schema(self):
        core_schema: meta.Schema = meta.core_schema()
        if not self.schemas().find_one({"name": core_schema.name}):
            self.add_schema(core_schema)
        self.ensure_schema(meta.core_schema)

    def ensure_schema(self, a_schema: meta.Schema):
        if not self.schemas().find_one({"name": a_schema.name}):
            self.add_schema(a_schema)
        self.ensure_schema_installed(a_schema)

    def open_db(self, setup=None):
        self._collections = db_coll.DatabaseCollections(self)
        self._collections.ensure_collections(uop_collection_names)
        if self._tenant_id:
            tenant: meta.Tenant = self.get_tenant(self._tenant_id)
            if tenant:
                self._collections.ensure_collections(
                    tenant.base_collections, override=True
                )
        self._collections.ensure_extensions()
        self._collections_complete = True
        self.reload_metacontext()

    def _db_has_collection(self, name):
        return False

    # Changesets

    def log_changes(self, changeset, tenant_id=None):
        """Log the changeset.
        We could log external to the main database but here we will presume that
        logging is local.
        """
        changes = meta.MetaChanges(
            timestamp=time.time(), tenant_id=tenant_id, changes=changeset.to_dict()
        )
        coll = self.collections.changes
        coll.insert(**changes.dict())

    def changes_since(self, epochtime, tenant_id, device_id=None):
        """Get and return the aggregate changes since the given time made by others

        Args:
            epochtime (_type_): _description_
            tenant_id (_type_): _description_
            client_id (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        # TODO this is broken. as we have no concept of client_id implemented yet.  It is
        # really a device id in the first place.
        tenant_id = tenant_id or 0
        device_id = device_id or 0
        criteria = Q.all(Q.gt("timestamp", epochtime), Q.neq("device_id", device_id))
        changesets = self.collections.changes.find(
            criteria, order_by=("timestamp",), only_cols=("changes",)
        )
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

    # Basic CRUD
    def _constrain(self, constrainer, data=None, criteria=None, mods=None):
        constrainer(
            data=data, criteria=criteria, mods=mods, is_admin=self.has_admin_user
        )

    def insert(self, kind, **spec):
        creator = kind_map[kind]
        coll = getattr(self.collections, kind)
        data = creator(**spec)
        self._constrain(coll.constrain_insert, data=data.without_kind())
        return self.meta_insert(data)

    def upsert(self, class_name, data):
        the_id = data.get("id")
        m_class = self.metaclass_named(class_name)
        m_id = m_class.id
        extension = self.extension(m_id)
        if the_id and m_id == oid.oid_class(the_id):
            extension.replace(data)
        else:
            data.pop("id", None)
            self.create_instance_of(class_name, **data)
            extension.insert(data)

    def modify(self, kind, an_id, mods):
        coll = getattr(self.collections, kind)
        self._constrain(coll.constrain_modify, criteria=an_id, mods=mods)
        return self.meta_modify(kind, an_id, **mods)

    def delete(self, kind, an_id):
        coll = getattr(self.collections, kind)
        self._constrain(coll.constrain_delete, criteria=an_id)
        return self.meta_delete(kind, an_id)

    def add_class(self, **spec):
        return self.insert("classes", **spec)

    def modify_class(self, cls_id, **mods):
        return self.modify("classes", cls_id, mods)

    def delete_class(self, clsid):
        return self.delete("classes", clsid)

    def add_attribute(self, **spec):
        return self.insert("attributes", **spec)

    def modify_attribute(self, attr_id, **mods):
        return self.modify("attributes", attr_id, mods)

    def delete_attribute(self, attrid):
        return self.delete("attributes", attrid)

    def add_role(self, **spec):
        return self.insert("roles", **spec)

    def modify_role(self, role_id, **mods):
        return self.modify("roles", role_id, mods)

    def delete_class(self, clsid):
        return self.delete("classes", clsid)

    def add_attribute(self, **spec):
        return self.insert("attributes", **spec)

    def modify_attribute(self, attr_id, **mods):
        return self.modify("attributes", attr_id, mods)

    def delete_attribute(self, attrid):
        return self.delete("attributes", attrid)

    def add_role(self, **spec):
        return self.insert("roles", **spec)

    def modify_role(self, role_id, **mods):
        return self.modify("roles", role_id, mods)

    def delete_role(self, role_id):
        return self.delete("roles", role_id)

    def add_tag(self, **spec):
        return self.insert("tags", **spec)

    def modify_tag(self, tag_id, **mods):
        return self.modify("tags", tag_id, mods)

    def delete_tag(self, tag_id):
        self.meta_delete("tags", tag_id)

    def add_group(self, **spec):
        return self.insert("groups", **spec)

    def modify_group(self, group_id, **mods):
        return self.modify("groups", group_id, mods)

    def delete_group(self, group_id):
        self.delete("groups", group_id)

    def add_object(self, obj):
        return self.meta_insert(obj)

    def modify_object(self, uuid, mods):
        self.meta_modify("objects", uuid, **mods)

    def delete_object(self, uuid):
        self.meta_delete("objects", uuid)

    def delete_role(self, role_id):
        return self.delete("roles", role_id)

    def add_tag(self, **spec):
        return self.insert("tags", **spec)

    def modify_tag(self, tag_id, **mods):
        return self.modify("tags", tag_id, mods)

    def delete_tag(self, tag_id):
        self.meta_delete("tags", tag_id)

    def add_group(self, **spec):
        return self.insert("groups", **spec)

    def modify_group(self, group_id, **mods):
        return self.modify("groups", group_id, mods)

    def delete_group(self, group_id):
        self.delete("groups", group_id)

    def add_object(self, obj):
        return self.meta_insert(obj)

    def modify_object(self, uuid, mods):
        self.meta_modify("objects", uuid, **mods)

    def delete_object(self, uuid):
        self.meta_delete("objects", uuid)

    def add_query(self, **spec):
        return self.insert("queries", **spec)

    def modify_query(self, query_id, **mods):
        return self.modify("queries", query_id, mods)

    def delete_query(self, query_id):
        self.delete("queries", query_id)

    # Classes

    def extension(self, cls_id):
        return self.collections.class_extension(cls_id)

    def class_short_form(self, class_id):
        cls = self.get_class(class_id)
        if cls:
            return cls.short_form

    def containing_collection(self, uuid):
        return self.extension(oid.oid_class(uuid))

    def metaclass_named(self, name):
        return self.get_meta_named("classes", name)

    def class_collection(self, name):
        cls = self.metaclass_named(name)
        return self.extension(cls.id)

    def class_instances(self, name):
        cls = self.name_to_id("classes", name)
        return self.extension(cls).find()

    def instances_satisfying(self, name, criteria):
        return self.class_collection(name).find(criteria)

    def class_instance_ids(self, name):
        cls = self.metaclass_named(name)
        coll = self.extension(cls.id)
        return coll.ids_only()

    def create_instance_of(self, clsName, use_defaults=False, record=True, **data):
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
                    return self.add_object(obj)
                return obj
            except Exception as e:
                raise e
        else:
            raise Exception(f"No class named {clsName}")

    # objects and their relationships

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
            by_id = {x["_id"]: x for x in res}
            res = [by_id[i] for i in uuids]
        return res

    def oid_short_form(self, oid):
        obj = self.get_object(oid)
        if obj:
            return self.object_short_form(obj)

    def object_short_form(self, obj):
        """
        Using the definition of the class of the object return
        a comma separated string of the values of its short form attributes.

        :param obj: object to return a short form for
        :return:  the short form object reference
        """
        an_oid = obj["id"]
        cls = oid.oid_class(an_oid)
        cls = self.get_class(cls)
        c_short = cls.short_form
        if c_short:
            return "%s(%s)" % (cls.name, ",".join([obj[x] for x in c_short]))
        else:
            return f"{cls.name}({an_oid}))"

    def get_object_roles(self, uuid):
        "returns all role_ids that the object is subject in"
        data = set(self.related.distinct("assoc_id", criteria=dict(subject_id=uuid)))
        data_rev = set(self.related.distinct("assoc_id", criteria=dict(object_id=uuid)))
        return data, data_rev  # return both forward and reverse applicable roles

    def object_for_url(self, url, record=False, **other_fields):
        """
        Find WebURL type object by url.
        Always create object for url if we don't have one already
        :param url: the url
        :param record: whether to insert?
        :return: the object data for persistent WebURL
        """

        results = self.instances_satisfying("WebURL", Q.eq("url", url))
        if results:
            return {"existing": True, "object": results[0]}
        object = self.create_instance_of(
            "WebURL", record=record, url=url, **other_fields
        )
        return {"existing": False, "object": object}

    def is_uuid(self, str):
        return oid.has_uuid_form(str) and self.get_class(oid.oid_class(str))

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
            pat = re.compile(r"(?P<clsName>[^\(]+)\((?P<objectSpec>[^\)]+)\)")
            clsName, objSpec = match_fields(pat, short_form, "clsName", "objectSpec")
            if self.is_uuid(objSpec):
                return self.get_object(objSpec)
            if clsName and objSpec:
                the_class = self.metaclass_named(clsName)
                short_attrs = the_class.short_attributes()
                vals = [x.strip() for x in objSpec.split(",")]
                pairs = [
                    (a.name, a.val_from_string(v)) for a, v in zip(short_attrs, vals)
                ]
                query_parts = [Q.of_type("clsName")] + [Q.eq(p[0], p[1]) for p in pairs]
                query = Q.all(*query_parts)
                obj = self.query(query)
                if create_if_missing and not obj:
                    obj = self.create_instance_of(
                        clsName, record=recordNew, **dict(pairs)
                    )
                    return {"existing": False, "object": obj}
                return {"existing": True, "object": obj}

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

    def get_object_tags(self, uuid):
        role_id = self.roles.by_name["tag_applies"]
        res = self.get_roleset(uuid, role_id, reverse=True)
        return res

    def get_object_groups(self, uuid, recursive=False):
        """
        An object can directly be in various groups.  While
        these direct groups may be in other groups the object is only directly in
        the first set.
        :param uuid:
        :param recursive:
        :return:
        """
        role_id = self.roles.by_name["group_contains"]
        res = self.get_roleset(uuid, role_id, reverse=True)
        if recursive:
            return recurse_set(res, lambda gid: self.groups_containing_group(gid))
        return res

    def get_object_data(self, uid):
        obj = self.get_object(uid)
        if not obj:
            raise NoSuchObject(uid)
        return obj

    def ensure_object(self, uuid):
        if not self.containing_collection(uuid).contains_id(uuid):
            raise NoSuchObject(uuid)

    def modify_object_tags(self, object_id, tag_ids, do_replace=False):
        role_id = self.roles.by_name["tag_applies"]
        return self.modify_associated_with_role(
            role_id, object_id, tag_ids, do_replace=do_replace
        )

    def modify_object_groups(self, object_id, group_ids, do_replace=False):
        role_id = self.roles.by_name["group_contains"]
        return self.modify_associated_with_role(
            role_id, object_id, group_ids, do_replace=do_replace
        )

    # Tags

    def get_tagset(self, tag_id, recursive=False):
        role_id = self.roles.by_name["tag_applies"]
        tags = set(tag_id)
        if recursive:
            tags.update(self.metacontext.subtags(tag_id))
        sets = [self.get_roleset(tid, role_id) for tid in tags]
        return reduce(lambda a, b: a | b, sets, set())

    def modify_tag_objects(self, tag_id, object_ids, do_replace=False, reverse=True):
        role_id = self.roles.by_name["tag_applies"]
        return self.modify_associated_with_role(
            role_id, tag_id, object_ids, do_replace=do_replace, reverse=reverse
        )

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

    def tag(self, oid, tag):
        role_id = self.roles.by_name["tag_applies"]
        return self.relate(tag, role_id, oid)

    def untag(self, oid, tagid):
        role_id = self.roles.by_name["tag_applies"]
        return self.unrelate(tagid, role_id, oid)

    # Grousp

    def get_groupset(self, group_id, recursive=False):
        role_id = self.roles.by_name["group_contains"]
        groups = set(group_id)
        if recursive:
            groups.update(self.groups_in_group(group_id))
        sets = [self.get_roleset(gid, role_id) for gid in groups]
        return reduce(lambda a, b: a | b, sets, set())

    def modify_group_objects(
        self, group_id, object_ids, do_replace=False, reverse=True
    ):
        role_id = self.roles.by_name["group_contains"]
        return self.modify_associated_with_role(
            role_id, group_id, object_ids, do_replace=do_replace, reverse=reverse
        )

    def groups_in_group(self, group_id):
        """get groups contained in group using relations instead of directly"""
        role_id = self.roles.by_name["contains_group"]
        func = lambda gid: self.get_roleset(gid, role_id)
        return recurse_set(func(group_id), func)

    def groups_containing_group(self, group_id):
        """get groups containing group using relations instead of directly"""
        role_id = self.roles.by_name["contains_group"]
        func = lambda gid: self.get_roleset(gid, role_id, reverse=True)
        return recurse_set(func(group_id), func)

    def group_item_check(self, item):
        return (self.group_ok(item)) or (self.object_ok(item))

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
        role_id = self.roles.by_name["group_contains"]
        groups = set(group_id)
        if transitive:
            groups.update(self.groups_in_group(group_id))
        sets = [self.get_roleset(gid, role_id) for gid in groups]
        return reduce(lambda a, b: a | b, sets, set())

    def group(self, oid, group_id):
        role_name = "group_contains" if self.object_ok(oid) else "contains_group"
        role_id = self.roles.by_name[role_name]
        return self.relate(group_id, role_id, oid)

    def ungroup(self, oid, group_id):
        role_name = "group_contains" if self.object_ok(oid) else "contains_group"
        role_id = self.roles.by_name[role_name]
        self.unrelate(group_id, role_id, oid)

    # Roles
    def get_role_related(self, role_id):
        forward = defaultdict(set)
        reversed = defaultdict(set)
        for data in self.related.find(dict(assoc_id=role_id)):
            subject, object = data["subject_id"], data["object_id"]
            forward[subject].add(object)
            reversed[object].add(subject)
        return forward, reversed

    def get_related_by_name(self, uuid):
        related, rev_related = self.get_object_relationships(uuid)
        res = {}
        roles = self.by_id("roles")
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
            criteria = {"subject_id": subject, "assoc_id": role_id}
            col = "object_id"
            if reverse:
                criteria = {"object_id": subject, "assoc_id": role_id}
                col = "subject_id"
            res = set(self.related.find(criteria=criteria, only_cols=[col]))
            if self._cache:
                self._cache.set(key, res)
        return res

    def modify_object_related(
        self, fixed_id, role_id, object_ids, do_replace=False, reverse=False
    ):
        return self.modify_associated_with_role(
            role_id, fixed_id, object_ids, do_replace=do_replace, reverse=reverse
        )

    def modify_associated_with_role(
        self, role_id, an_id, desired, reverse=False, do_replace=False
    ):
        current = self.get_roleset(an_id, role_id, reverse=not reverse)
        related = lambda some_id: meta.Related(
            subject_id=some_id, assoc_id=role_id, object_id=an_id
        )

        if reverse:
            related = lambda some_id: meta.Related(
                subject_id=an_id, assoc_id=role_id, object_id=some_id
            )

        to_add = desired - current
        to_remove = current - desired
        with self.changes(self) as chng:
            for obj in to_add:
                chng.insert("related", related(obj))
            if do_replace:
                for obj in to_remove:
                    chng.delete("related", related(obj))
        return desired

    def get_all_related_by(self, role_id, reverse=False):
        """
        Get map of subject to objects related by the given role.
        :param role_id: the role id
        :return: the mapping
        """
        res = defaultdict(set)
        key, v_key = ["subject", "object"]
        if reverse:
            key, v_key = v_key, key

        for rec in self.related.find({"role": role_id}):
            res[rec[key]].add(rec[v_key])
        return res

    def get_subjects_related(self, role_id):
        """
        Return just the subjects related by role_id
        """
        return set(self.related.find({"role": role_id}, only_cols=["subject"]))

    def get_all_related(self, uuid):
        """
        All objects directly related to uuid regardless of relationship role.
        :param uuid:  the object to find related objects for
        :return: set of object ids of related objects
        """
        res = set(self.related.find({"subject": uuid}, only_cols=["object_id"]))
        res.update(self.related.find({"object_id": uuid}, only_cols=["subject"]))
        return {r for r in res if oid.has_uuid_form(r)}

    def relate(self, subject_oid, roleid, object_oid):
        data = meta.Related(
            subject_id=subject_oid, assoc_id=roleid, object_id=object_oid
        )
        if not self.related.exists(data.without_kind()):
            return self.meta_insert(data)
        return data

    def unrelate(self, oid, roleid, other_oid):
        self.meta_delete(
            "related",
            meta.Related(subject_id=oid, assoc_id=roleid, object_id=other_oid),
        )

    # Chngeset modifiers

    def meta_insert(self, obj):
        with self.changes(self) as chng:
            data = as_dict(obj)
            kind = data.pop("kind", "objects")
            chng.insert(kind, data)
        return obj

    def ensure_meta_named(self, kind, name):
        meta = self.get_meta_named(kind, name)
        if not meta:
            meta = self.insert(kind, name=name)
        return meta

    def meta_modify(self, kind, an_id, **data):
        with self.changes(self) as chng:
            res = chng.modify(kind, an_id, data)
        return res or getattr(self, kind, {}).get(an_id)

    def meta_delete(self, kind, id_or_data):
        with self.changes(self) as chng:
            if not isinstance(id_or_data, str):
                data = as_dict(id_or_data)
                data.pop("kind", None)
                id_or_data = data
            chng.delete(kind, id_or_data)

    # Queries

    def create_query(self, data):
        query = meta.MetaQuery.from_dict(data)
        return self.meta_insert(query)

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

        evaluator = query_module.QueryEvaluator2(
            normalized_query(query), self, self.metacontext
        )
        return await evaluator()
