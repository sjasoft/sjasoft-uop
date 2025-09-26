__author__ = 'samantha'

from sjasoft.uopmeta import oid
import asyncio
from sjasoft.uop import changeset as base

def oid_matches(to_check, oid):
    return to_check == oid

class NoModChanges(base.NoModChanges):

    async def db_not_dup(self, collection, data):
        return not await collection.exists(data)

    async def apply_to_db(self, collections):
        coll = self.user_collection(collections)
        for item in self.inserted:
            item = dict(item)
            if self.db_not_dup(coll, item):
                await coll.insert(**item)
        for item in self.deleted:
            item = dict(item)
            await coll.remove(item)
            self.on_db_delete(item, collections)

    @classmethod
    async def delete_object_references(cls, collection, objid):
        await collection.remove(cls._object_db_filter(objid))

    @classmethod
    async def delete_class_references(cls, collection, clsid):
        await collection.remove(cls._class_db_filter(clsid))

    @classmethod
    async def delete_association_references(cls, collection, an_id):
        await collection.remove(cls._association_db_filter(an_id))

class TaggedChanges(NoModChanges):
    association_type = 'tag'
    kind = 'tagged'


class RelatedChanges(NoModChanges):
    _object_fields = 'object_id', 'subject_id'
    _class_fields = 'obj_cls', 'subject_cls'
    kind = 'related'


class GroupedChanges(NoModChanges):
    _association_type = 'group'
    kind = 'grouped'


class CrudChanges(base.CrudChanges):

    def __init__(self, changeset, data=None):
        super().__init__(changeset, data=data)

    async def db_modify(self, collection, mods):
        for key, item_mods in mods.items():
            await collection.update_instance(key, **item_mods)

    async def db_not_dup(self, collection, data):
        checked_data = dict(name=data['name'])
        return not await collection.exists(checked_data)

    async def db_delete_others(self, collections, key):
        pass

    async def on_db_delete(self, uuid, collections):
        pass

    async def apply_to_db(self, collections):
        coll = getattr(collections, self.kind)
        for k, v in self.inserted.items():
            await coll.insert(**v)
        for k, v in self.modified.items():
            await coll.update_one(k, v)
        for k in self.deleted:
            await coll.remove(k)
            await self.on_db_delete(k, collections)

    async def delete_from_collections(self, collections, key):
        """
        :param collections: all crud and associated db collections.
        It is up to the subclass to apply the change only to the correct ones
        :pama key: identifier of item being deleted
        :returns: None
        """
        pass


class ObjectChanges(CrudChanges):
    kind = 'objects'

    def delete(self, identifier, in_changeset=None):
        CrudChanges.delete(self, identifier, in_changeset)

    async def apply_to_db(self, collections):

        colls = {}

        async def collection(uuid):
            cls_id = oid.oid_class(uuid)
            if not cls_id in colls:
                colls[cls_id] = await collections.class_extension(cls_id)
            return colls[cls_id]

        for k, v in self.inserted.items():
            coll = await collection(k)
            await coll.insert(**v)
        for k, v in self.modified.items():
            coll = await collection(k)
            await coll.update({'_id': k}, v)
        for k in self.deleted:
            coll = await collection(k)
            await coll.remove(k)
            await self.on_db_delete(k, collections)

    async def on_db_delete(self, uuid, collections):
        await asyncio.gather(
            collections.grouped.remove({'object_id': uuid}),
            collections.tagged.remove({'object_id': uuid}),
            collections.related.remove(
                {'$or': [
                    {'object_id': uuid},
                    {'subject_id': uuid}]}));

    async def db_delete_others(self, collections, key):
        await TaggedChanges.delete_object_references(collections.tagged, key)
        await RelatedChanges.delete_object_references(collections.related, key)
        await GroupedChanges.delete_object_references(collections.grouped, key)

    def handle_delete(self, identifier, in_changeset):
        in_changeset.tagged.delete_object(identifier)
        in_changeset.grouped.delete_object(identifier)
        in_changeset.related.delete_object(identifier)

    def delete_class(self, class_id):
        test_class = lambda uuid: uuid.split('.') != class_id
        self.inserted = dict([(k, v) for k, v in self.inserted.items() if test_class(k)])
        self.modified = dict([(k, v) for k, v in self.modified.items() if test_class(k)])
        self.deleted = {s for s in self.deleted if test_class(s)}

    async def delete_from_collections(self, collections, key):
        """
        :param collection: database collection for this type of CrudChange
        :pama key: identifier of item being deleted
        :returns: None
        """
        await super(ObjectChanges, self).delete_from_collections(collections, key)
        await base.RelatedChanges.user_collection(collections).delete_object_references(key)
        await base.TaggedChanges.user_collection(collections).delete_object_references(key)
        await GroupedChanges.user_collection(collections).delete_object_references(key)


class RoleChanges(CrudChanges):
    kind = 'roles'

    async def on_db_delete(self, key, collections):
        await collections.related.remove({'associated': key})

    async def delete(self, identifier, in_changeset=None):
        await super(RoleChanges, self).delete(identifier, in_changeset)
        in_changeset.related.delete_association(identifier)

    async def db_not_dup(self, collection, data):
        # TODO (samantha) think on whether this is enough more deeply
        checked_data = dict(name=data['name'], reverse_id=data['reverse_id'])
        return not await collection.exists(checked_data)


class TagChanges(CrudChanges):
    kind = 'tags'

    async def on_db_delete(self, key, collections):
        await collections.tagged.remove({'associated': key})

    async def delete(self, identifier, in_changeset=None):
        await super(TagChanges, self).delete(identifier, in_changeset)
        in_changeset.tagged.delete_association(identifier)


class GroupChanges(CrudChanges):
    kind = 'groups'

    async def on_db_delete(self, key, collections):
        await collections.grouped.remove({'associated': key})

    def delete(self, identifier, in_changeset=None):
        super(GroupChanges, self).delete(identifier, in_changeset)
        in_changeset.grouped.delete_association(identifier)


class QueryChanges(CrudChanges):
    kind = 'queries'
    pass


class ClassChanges(CrudChanges):
    kind = 'classes'


    async def on_db_delete(self, key, collections):
        import re
        regex = re.compile(f'_{key}$')
        cls_expr = {'$regex': regex}
        grouped = await collections.grouped.find({'object_id': cls_expr})
        await asyncio.gather(
            collections.grouped.remove({'object_id': cls_expr}),
            collections.tagged.remove({'object_id': cls_expr}),
            collections.related.remove({'$or': [
                {'object_id': cls_expr},
                {'subject_id': cls_expr}]}))




class AttributeChanges(CrudChanges):
    kind = 'attributes'
    pass

    async def db_not_dup(self, collection, data):
        checked_data = dict(name=data['name'], type_id=data['type_id'])
        return not await collection.exists(checked_data)


class ChangeSet(base.ChangeSet):
    change_types = dict(
        objects=ObjectChanges,
        roles=RoleChanges,
        tags=TagChanges,
        groups=GroupChanges,
        grouped=GroupedChanges,
        classes=ClassChanges,
        attributes=AttributeChanges,
        tagged=TaggedChanges,
        related=RelatedChanges,
        queries=QueryChanges
    )

    def __init__(self, **data):
        """
        Creates a changeset in internal form from a changeset in external form
        :param data: changeset with all sets made lists that is json compatible
        """
        self.objects = ObjectChanges(self, data.get('objects'))
        self.roles = RoleChanges(self, data.get('roles'))
        self.tags = TagChanges(self, data.get('tags'))
        self.groups = GroupChanges(self, data.get('groups'))
        self.grouped = GroupedChanges(self, data.get('grouped'))
        self.classes = ClassChanges(self, data.get('classes'))
        self.attributes = AttributeChanges(self, data.get('attributes'))
        self.tagged = TaggedChanges(self, data.get('tagged'))
        self.related = RelatedChanges(self, data.get('related'))
        self.queries = QueryChanges(self, data.get('queries'))


def meta_context_schema_diff(context, a_schema):
    changes = ChangeSet()
    context.gather_schema_changes(a_schema, changes)
    return changes