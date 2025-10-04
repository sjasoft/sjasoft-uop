__author__ = 'samantha'

from collections import defaultdict
from sjasoft.uopmeta import oid
from sjasoft.uopmeta import attr_info
from sjasoft.uopmeta.schemas.meta import (kind_map, MetaContext, Schema, as_dict as meta_dict,
                                  as_meta, as_tuple, dict_or_tuple)

from sjasoft.uopmeta.attr_info import assoc_kinds, meta_kinds, crud_kinds
from sjasoft.uopmeta.oid import id_field



get_id = lambda data: data[id_field]

def oid_matches(to_check, oid):
    return to_check == oid


class ChangeSetComponent(object):
    def __init__(self, changeset):
        self._changeset = changeset

    def on_db_delete(self, uuid, collections):
        pass

    def memory_filter(self, items, criteria):
        return []

    def adjusted_find(self, criteria, results, dbi):
        return results

    def expanded_changed(self):
        """
        Inserted objects and modifications apllied to existing objects

        Returns:
            List(dict): list of objects including objects modified with their
           x mods in place
        """
        return self.inserted

as_dict = dict_or_tuple

class NoModChanges(ChangeSetComponent):
    kind = '_'
    _object_fields = ('object_id',)
    _association_type = ''

    @classmethod
    def user_collection(cls, collections):
        return getattr(collections, cls.kind)

    def _data_tuple(self, data):
        fields = list(data['assoc_id'], data['object_id'])
        if 'subject_id' in data:
            fields.append(data['subject_id'])
        return tuple(fields)

    def adjusted_find(self, criteria, results):
        removed = (self._data_tuple(i) for i in self.deleted)
        def not_deleted(data):
            return self._data_tuple(data) not in removed
        results = [r for r in results if not deleted(r)]
        results.extend(self.momory_filter(self.expanded_changed, criteria))
        return results


    def db_not_dup(self, collection, data):
        return not collection.exists(data)

    def apply_to_db(self, collections):
        coll = self.user_collection(collections)
        for item in self.inserted:
            item = dict(item)
            if self.db_not_dup(coll, item):
                coll.insert(**item)
        for item in self.deleted:
            item = dict(item)
            coll.remove(item)
            self.on_db_delete(item, collections)

    def standardized(self, item):
        item = as_dict(item)
        if isinstance(item, dict):
            return tuple(item.items())
        return item

    def __init__(self, changeset, data=None):
        data = data or {}
        items = lambda key: data.get(key, [])
        self.inserted = {self.standardized(d) for d in items('inserted')}
        self.deleted = {self.standardized(d) for d in items('deleted')}
        ChangeSetComponent.__init__(self, changeset)

    def clear(self):
        self.inserted.clear()
        self.deleted.clear()

    def has_changes(self):
        return any([self.inserted, self.deleted])

    def add_changes(self, other):
        """
        Add subsequent changes to the existing changes
        :param other: the other changeset component
        :return: None
        """
        deleted_objects = self._changeset.objects.deleted | other._changeset.objects.deleted
        deleted_classes = self._changeset.classes.deleted | other._changeset.objects.deleted
        fn = lambda x: not x.contanins_deleted(deleted_objects, deleted_classes)
        to_insert = filter(fn, other.inserted)
        to_delete = filter(fn, other.deleted)
        self.inserted.update(to_insert)
        new_deletes = to_delete - self.inserted
        self.inserted -= to_delete
        self.deleted.update(new_deletes)

    def to_dict(self):
        dict_list = lambda data: [as_dict(d) for d in data]
        return dict(inserted= dict_list(self.inserted), deleted=dict_list(self.deleted))

    def _references_ok(self, data):
        for field in self._object_fields:
            oid = as_dict(data).get(field)
            if self._changeset.object_deleted(oid):
                return False
        return True

    def insert(self, data):
        if self._references_ok(data):
            self.inserted.add(self.standardized(data))

    def delete(self, data, unused_changset=None):
        if self._references_ok(data):
            data = self.standardized(data)
            if data in self.inserted:
                self.inserted.discard(data)
            else:
                self.deleted.add(data)

    def get_field(self, data, field):
        return data.get(field) if isinstance(data, dict) else getattr(data, field)

    def remove_by_obj_class(self, cls_id, fields):
        get_cls = lambda item, field: oid.oid_class(self.get_field(item, field))
        test = lambda item: all([(get_cls(item,k) != cls_id) for k in fields])

        self.inserted = {x for x in self.inserted if test(dict(x))}
        self.deleted = {x for x in self.deleted if test(dict(x))}

    def memory_filter(self, disallowed_id, fields, additional_extractor=None):
        is_obj = oid.oid_sep in disallowed_id
        def getter():
            simple_getter = lambda item, f: self.get_field(item, f)
            if additional_extractor:
                return lambda item, f: additional_extractor(simple_getter(item, f))
            return simple_getter
        get_fn = getter()
        obj_test = lambda obj: obj == disallowed_id
        cls_test = lambda obj: oid.oid_class(obj) == disallowed_id
        oid_test = obj_test if is_obj else cls_test
        test = lambda item: any([oid_test(get_fn(item, k)) for k in fields])
        self.inserted = {x for x in self.inserted if not test(dict(x))}
        self.deleted = {x for x in self.deleted if not test(dict(x))}

    def delete_object(self, object_id):
        self.memory_filter(object_id, self._object_fields)

    def delete_class(self, cls_id):
        self.memory_filter(cls_id, self._object_fields, additional_extractor=lambda x: oid.oid_class(x))

    def delete_association(self, assoc_id):
        self.memory_filter(assoc_id, ['assoc_id'])

    @classmethod
    def _db_ref_check(cls, an_id, flds):
        """builds db clause to check for equality in any of the given fields"""
        clauses = [{k: an_id} for k in flds]
        return {'$or': clauses} if len(flds) > 1 else clauses[0]

    @classmethod
    def _object_db_filter(cls, obj_id):
        return cls._db_ref_check(obj_id, cls._object_fields)

    @classmethod
    def _class_db_filter(cls, cls_id):
        flds = [('cls_%s' % f) for f in cls._object_fields]
        return cls._db_ref_check(cls_id, flds)

    @classmethod
    def _association_db_filter(cls, assoc_id):
        return {'assoc_id': {'$eq': assoc_id}}

    @classmethod
    def delete_object_references(cls, collection, objid):
        collection.remove(cls._object_db_filter(objid))

    @classmethod
    def delete_class_references(cls, collection, clsid):
        collection.remove(cls._class_db_filter(clsid))

    @classmethod
    def delete_association_references(cls, collection, an_id):
        collection.remove(cls._association_db_filter(an_id))



class RelatedChanges(NoModChanges):
    _object_fields = 'object_id', 'subject_id'
    kind = 'related'


class CrudChanges(ChangeSetComponent):
    kind = '_'

    # classmethod
    def user_collection(cls, collections):
        return collections[cls.kind]

    def __init__(self, changeset, data=None):
        data = data or {}
        self.inserted = data.get('inserted', defaultdict(dict))
        self.modified = data.get('modified', defaultdict(dict))
        self.deleted = set(data.get('deleted', []))
        ChangeSetComponent.__init__(self, changeset)

    def expand_changed(self, dbi):
        res = super().expand_changed()
        for oid, mods in self.modified.items():
            data = dbi.get_object(oid)
            if data:
                data.update(mods)
                res.append(data)
        return res

    def adjusted_find(self, criteria, results, dbi):
        questionable_ids = self.deleted | set(self.modified.keys())
        results = [r for r in results if r['id'] not in questionable_ids]
        items = self.expand_changed(dbi)
        results.extend(self.memory_filter(items, criteria))
        return results

    def has_changes(self):
        return any([self.inserted, self.modified, self.deleted])

    def __copy__(self):
        return self.__class__(self.kind, self.to_dict())

    def delete(self, identifier, in_changeset=None):
        """
        Rationalize deletes against insertions and modifications.
        If item is in the insert of the changes then remove it.
        Remove it if it is in the modified set otherwise.
        Add it to the deleted set if it was not in the inserted set
        modify other parts of the overall changeset for the kind
        of item being deleted.
        :param identifier: id of the item being deleted
        :param in_changeset: the containing changeset.
        :return: None
        """
        if not self.inserted.pop(identifier, None):
            self.modified.pop(identifier, None)
            self.deleted.add(identifier)
        self.handle_delete(identifier, in_changeset)

    def handle_delete(self, identifier, changeset):
        pass

    def modify(self, identifier, data):
        "Rationalize modifications"
        if identifier in self.inserted:
            self.inserted[identifier].update(data)
            return self.inserted[identifier]
        elif identifier in self.modified:
            self.modified[identifier].update(data)
        else:
            self.modified[identifier] = data
        return None

    def db_modify(self, collection, mods):
        for key, item_mods in mods.items():
            collection.update_instance(key, **item_mods)

    def db_not_dup(self, collection, data):
        checked_data = dict(name=data['name'])
        return not collection.exists(checked_data)

    def db_delete_others(self, collections, key):
        pass

    def insert(self, data):
        self.inserted[get_id(data)] = data

    def delete_cls_matching(self, clsid, changeset):
        test = oid.oid_class_matcher(clsid)
        self.inserted = dict([(k, v) for k, v in self.inserted.items if not test(k)])
        self.modified = dict([(k, v) for k, v in self.modified.items if not test(k)])
        self.deleted = set([k for k in self.deleted if not test(k)])

    def on_db_delete(self, uuid, collections):
        pass

    def apply_to_db(self, collections):
        coll = getattr(collections, self.kind)
        for k, v in self.inserted.items():
            coll.insert(**v)
        for k, v in self.modified.items():
            coll.update_one(k, v)
        for k in self.deleted:
            coll.remove(k)
            self.on_db_delete(k, collections)

    def delete_from_collections(self, collections, key):
        """
        :param collections: all crud and associated db collections.
        It is up to the subclass to apply the change only to the correct ones
        :pama key: identifier of item being deleted
        :returns: None
        """
        pass

    def to_dict(self):
        return dict(
            inserted=self.inserted,
            modified=self.modified,
            deleted=list(self.deleted)
        )

    def add_changes(self, other_changes, in_changeset):
        self.inserted.update(other_changes.inserted)
        for k, v in other_changes.modified.items():
            self.modify(k, v)
        for _id in other_changes.deleted:
            self.delete(_id, in_changeset)

    def clear(self):
        self.inserted.clear()
        self.modified.clear()
        self.deleted.clear()


class ObjectChanges(CrudChanges):
    kind = 'objects'

    def delete(self, identifier, in_changeset=None):
        super().delete(identifier, in_changeset)

    def handle_delete(self, identifier, in_changeset):
        in_changeset.related.delete_object(identifier)

    def apply_to_db(self, collections):

        colls = {}

        def collection(uuid):
            cls_id = oid.oid_class(uuid)
            if not cls_id in colls:
                colls[cls_id] = collections.class_extension(cls_id)
            return colls[cls_id]

        for k, v in self.inserted.items():
            coll = collection(k)
            coll.insert(**v)
        for k, v in self.modified.items():
            coll = collection(k)
            coll.update({'id': k}, v)
        for k in self.deleted:
            coll = collection(k)
            coll.remove(k)
            self.db_delete_others(collections, k)

    def on_db_delete(self, uuid, collections):
        collections.related.remove(
            {'$or': [
            {'object_id': uuid},
            {'subject_id': uuid}]})

    def db_delete_others(self, collections, key):
        RelatedChanges.delete_object_references(collections.related, key)

    def delete_class(self, class_id):

        test_class = lambda uuid: uuid.split('.') != class_id
        self.inserted = dict([(k, v) for k, v in self.inserted.items() if test_class(k)])
        self.modified = dict([(k, v) for k, v in self.modified.items() if test_class(k)])
        self.deleted = {s for s in self.deleted if test_class(s)}


    def delete_from_collections(self, collections, key):
        """
        :param collection: database collection for this type of CrudChange
        :pama key: identifier of item being deleted
        :returns: None
➜  uop git:(remove-application) gi
        """
        super(ObjectChanges, self).delete_from_collections(collections, key)
        RelatedChanges.user_collection(collections).delete_object_references(key)



class RoleChanges(CrudChanges):
    kind = 'roles'

    def on_db_delete(self, key, collections):
        collections.related.remove({'assoc_id': key})

    def delete(self, identifier, in_changeset=None):
        super().delete(identifier, in_changeset)
        in_changeset.related.delete_association(identifier)

    def db_not_dup(self, collection, data):
        # TODO (samantha) think on whether this is enough more deeply
        checked_data = dict(name=data['name'], reverse_id=data['reverse_id'])
        return not collection.exists(checked_data)


class TagChanges(CrudChanges):
    kind = 'tags'

    def on_db_delete(self, key, collections):
        role_id = collections.roles.by_name['tag_applies']
        collections.related.remove({'subject_id': key, 'assoc_id': role_id})



class GroupChanges(CrudChanges):
    kind = 'groups'

    def on_db_delete(self, key, collections):
        role_id = collections.roles.by_name['group_contains']
        rev_id = collections.roles.by_name['contains_group']
        collections.related.remove({'subject_id': key, 'assoc_id': role_id})
        collections.related.remove({'object_id': key, 'assoc_id': rev_id})

class QueryChanges(CrudChanges):
    kind = 'queries'
    pass


class ClassChanges(CrudChanges):
    kind = 'classes'

    def on_db_delete(self, key, collections):
        filter = lambda fld:{'$regex': {fld: f'^{key}\\.'}}
        obj_check = filter('object_id') 
        subject_check = filter('subject_id')
        collections.related.remove({'$or': [
            obj_check, subject_check]})

    def delete_from_collections(self, collections, key):
        """
        :param collection: database collection for this type of CrudChange
        :pama key: identifier of item being deleted
        :returns: None
        """
        ObjectChanges.user_collection(collections).delete_class(key)
        RelatedChanges.user_collection(collections).delete_class(key)

    def handle_delete(self, identifier, in_changeset):
        in_changeset.objects.delete_class(identifier)
        in_changeset.related.delete_class(identifier)

class AttributeChanges(CrudChanges):
    kind = 'attributes'
    pass

    def db_not_dup(self, collection, data):
        checked_data = dict(name=data['name'], type_id=data['type_id'])
        return not collection.exists(checked_data)


class ChangeSet(object):
    change_types = dict(
        objects=ObjectChanges,
        roles=RoleChanges,
        tags=TagChanges,
        groups=GroupChanges,
        classes=ClassChanges,
        attributes=AttributeChanges,
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
        self.classes = ClassChanges(self, data.get('classes'))
        self.attributes = AttributeChanges(self, data.get('attributes'))
        self.related = RelatedChanges(self, data.get('related'))
        self.queries = QueryChanges(self, data.get('queries'))

    def adjust_found(kind, criteria, results, dbi):
        target: ChangeSetComponent = getattr(self, kind)
        return target.adjusted_find(criteria, results, dbi)


    def usermap_translated(self, user_map, user_id):
        '''
        Creates and returns a new changeset in terms of another set of ids
        :param user_map: map, perhaps partial or even empty, of ids relevant to this changeset
        to some other user id space by kind of medatadata.
        :return: the new changeset corresponding changeset in terms of user_map ids
        :side-effect: updates to user_map
        '''
        changeset = self.__class__()

        def get_new_id(kind, old_id):
            kmap = user_map[kind]
            new_id = kmap.get(old_id)
            if not new_id:
                new_id = attr_info.make_meta_id()
                kmap[old_id] = new_id
                return new_id, True
            return new_id, False

        def handle_inserted(kind, obj):
            new_id, created = get_new_id(kind, obj['_id'])
            if created:
                new_obj = dict(obj)
                new_obj['_userId'] = user_id
                obj['_id'] = new_id
                changeset.insert(kind, obj)

        for attr in self.attributes.inserted.values():
            handle_inserted('attributes', attr)

        for cls in self.classes.inserted.values():
            handle_inserted('classes', cls)

        cls_mappings = user_map['classes']
        attr_mappings = user_map['attributes']

        for cls in changeset.classes.inserted.values():
            if (cls['superclass']):
                cls['superclass'] = cls_mappings[cls['superclass']]
            cls['attrs'] = [attr_mappings[a] for a in cls['attrs']]

        for kind in attr_info.meta_kinds:
            change_kind = getattr(changeset, kind)
            k_map = user_map[kind]
            crud_change = getattr(self, kind)
            for insert in crud_change.inserted.values():  # insert if not present
                if kind in ('attributes', 'classes'):
                    continue
                crud_id = insert['_id']
                new_id = k_map.get(crud_id)
                if not new_id:
                    new_id = attr_info.make_meta_id()
                    k_map[crud_id] = new_id
                if new_id not in change_kind.inserted:
                    d = dict(insert)
                    d['_id'] = new_id
                    changeset.insert(kind, d)

            for k, mods in crud_change.modified.items():
                u_id = k_map.get(k)
                if not u_id:
                    print('no mapping from %s in' % k, kind)
                if kind == 'classes':
                    u_map = user_map['attributes']
                    if 'attrs' in mods:
                        mods['attrs'] = [u_map[a] for a in mods['attrs']]
                    if 'short_form' in mods:
                        mods['short_form'] = [u_map[a] for a in mods['short_form']]

                changeset.modify(kind, u_id, mods)
            for d in crud_change.deleted:
                mapped = k_map.get(d)
                if mapped:
                    changeset.delete(kind, mapped)
                    k_map.pop(d, None)
        return changeset

    def has_changes(self):
        fields = ['objects', 'roles', 'tags', 'groups', 'grouped',
                       'classes', 'attributes', 'tagged', 'related']
        changes = {f: getattr(self, f).has_changes() for f in fields}
        return any(changes.values())


    def to_dict(self):
        return dict([(key, getattr(self, key).to_dict()) for key in self.change_types])

    def object_deleted(self, obj_id):
        cls_id = oid.oid_class(obj_id)
        return (cls_id in self.classes.deleted) or (obj_id in self.objects.deleted)

    def add_changes(self, other_changes):
        for kind in crud_kinds:
            data = getattr(other_changes, kind)
            for inserted in data.inserted.values():
                self.insert(kind, inserted)
            for k, v in data.modified.items():
                self.modify(kind, k, v)
            for k in data.deleted:
                self.delete(kind, k)
        deleted_objects = self.objects.deleted | other_changes.objects.deleted
        deleted_classes = self.classes.deleted | other_changes.classes.deleted
        ensure_refs = lambda x: not x.contains_deleted(deleted_objects, deleted_classes)
        for kind in assoc_kinds:
            data = getattr(other_changes, kind)
            as_metas = lambda items: [as_meta(kind, d) for d in items]
            for item in filter(ensure_refs, as_metas(data.inserted)):
                self.insert(kind, item)
            for item in filter(ensure_refs, as_metas(data.deleted)):
                self.delete(kind, item)

    @classmethod
    def combine_changes(cls, *changesets):
        """
        combines sequential changesets into one
        :param changesets: sequence of changeset in dict form
        :return: combined changeset
        """

        def as_changeset(changes):
            return changes if isinstance(changes, ChangeSet) else cls(**changes)

        def as_dict(changes):
            return changes.to_dict() if isinstance(changes, ChangeSet) else changes

        combined = cls(**as_dict(changesets[0]))  # copy of first changeset
        for cs in changesets[1:]:
            combined.add_changes(as_changeset(cs))
        return combined

    def clear(self):
        for kind in self.change_types:
            getattr(self, kind).clear()

    def insert(self, kind, data):
        try:
            coll = getattr(self, kind)
        except Exception as e:
            raise e
        coll.insert(data)

    def modify(self, kind, an_id, data):
        coll = getattr(self, kind)
        return coll.modify(an_id, data)

    def delete(self, kind, an_id):
        coll = getattr(self, kind)
        coll.delete(an_id, self)


def meta_context_as_changeset(context:MetaContext):
    """
    Builds a changeset matching the context. This is primarily used
    for storage of a new Application's metadata.
    """
    changes = ChangeSet()
    for kind in meta_kinds:
        instances = getattr(context, kind, [])
        for instance in instances:
            changes.insert(kind, instance.dict(exclude_none=True))
    return changes

def context_to_schema_changeset(context: MetaContext, schema:Schema):
    changes = ChangeSet()
    for kind in meta_kinds:
        s_instances = getattr(schema, kind, [])
        for inst in s_instances:
            pass

def meta_context_schema_diff(context: MetaContext, a_schema):
    changes = ChangeSet()
    context.gather_schema_changes(a_schema, changes)
    return changes

