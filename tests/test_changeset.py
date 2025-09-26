__author__ = 'samantha'

from sjautils.tools import lmap
from sjautils.dicts import diff as dict_diff
from uopmeta.oid import id_field
from uop import changeset
from uopmeta import attr_info
from uopmeta.schemas.predefined import pkm_schema
from uopmeta.schemas.meta import (WorkingContext, as_tuple, as_dict,
                                  as_meta, tuple_to_meta)

dataset = WorkingContext.from_schema(pkm_schema)
dataset.configure(num_assocs=6)
lmap2 = lambda fn, *args: [fn(a) for a in args]

def crud_insert(cs, kind, items):
    """
    Insert into (and test) CRUD kind of changeset item

    :param cs: a changeset to insert into
    :param kind:  name of type of CRUD-like component
    :param items: instances of that kind
    :return: the changeset
    """
    if not isinstance(items, list):
        items = [items]
    target = getattr(cs, kind)
    for item in items:
        cs.insert(kind, item)
        assert item[id_field] in target.inserted
    return cs

def crud_modify(cs, kind, modifications):
    """
    Modify (and test) CRUD kind of changeset component

    :param cs: a changeset to insert into
    :param kind:  name of type of CRUD-like component
    :param modifications: map(id->map(fld->value))
    :return: the changeset
    """
    def check_mods(changes, mods):
        for k, v in mods.items():
            assert changes.get(k) == v

    target = getattr(cs, kind)
    for key, mods in modifications.items():
        was_inserted = key in target.inserted
        cs.modify(kind, key, mods)
        sub_target = target.inserted if was_inserted else target.modified
        if was_inserted:
            assert key not in target.modified
        check_mods(sub_target[key], mods)
    return cs

def crud_delete(cs, kind, ids):
    """
    Delete from (and test) CRUD kind of changeset item

    :param cs: a changeset to insert into
    :param kind:  name of type of CRUD-like component
    :param ids: component ids to delete
    :return: the changeset
    """
    if not isinstance(ids, list):
        ids = [ids]
    target = getattr(cs, kind)
    for an_id in ids:
        was_inserted = an_id in target.inserted
        expect_in_deleted = not was_inserted
        was_modified = an_id in target.modified
        cs.delete(kind, an_id)
        if was_inserted:
            assert an_id not in target.inserted
        elif was_modified:
            assert an_id not in target.modified
        assert expect_in_deleted == (an_id in target.deleted)
    return changeset

def assoc_insert(cs, kind, *objects):
    """
    Insert into (and test) changest association component

    :param cs: a changeset to insert into
    :param kind:  name of type of association component
    :param objects: association instances of that kind
    :return: the changeset
    :return:
    """
    target = getattr(cs, kind)
    for data in objects:
        cs.insert(kind, data)
        t_data = as_tuple(data)
        if target._references_ok(data):
            assert t_data in target.inserted
        assert t_data not in target.deleted
    return cs

def assoc_delete(cs, kind, *tuples):
    """
    Delete from (and test) changest association component.

    :param cs: a changeset to delete from
    :param kind:  name of type of association component
    :param tuples: instances of that kind
    :return: the changeset
    :return:
    """
    target = getattr(cs, kind)
    for data in tuples:
        t_data = tuple(data.items())
        was_present = t_data in target.inserted
        cs.delete(kind, data)
        if was_present:
            assert t_data not in target.inserted
            assert t_data not in target.deleted
        else:
            if target._references_ok(data):
                assert t_data in target.deleted
    return cs

def kind_objects(kind):
    raw = getattr(dataset, kind)
    if not isinstance(raw, list):
        raw = raw.by_name.values()
    return [as_dict(r) for r in raw]

def kind_items(kind):
    return [as_dict(r) for r in kind_objects(kind)]


def full_changeset(starting_at=0):
    cs = changeset.ChangeSet()
    for kind in attr_info.meta_kinds:
        if kind == 'queries':
            continue
        data = kind_items(kind)
        if len(data) < 3:
            print(f'{kind} data has only {len(data)} items')
        first, second, third = data[starting_at: starting_at + 3]
        crud_insert(cs, kind, first)
        crud_modify(cs, kind, {second[id_field]: dict(description='new description')})
        crud_delete(cs, kind, third[id_field])
    for kind in changeset.assoc_kinds:
        objects = kind_objects(kind)
        first, second = objects[starting_at: starting_at + 2]
        assoc_insert(cs, kind, first)
        assoc_delete(cs, kind, second)
    return cs

def test_conversion():
    cs = full_changeset()
    ds = cs.to_dict()
    cs2 = changeset.ChangeSet(**ds)
    ds2 = cs2.to_dict()
    diff = dict_diff(ds, ds2)
    assert ds2 == ds

def test_modification():
    cs = changeset.ChangeSet()
    cls = dataset.random_class().dict()
    cid = cls[id_field]
    new_description = 'a new test_modification description'
    crud_insert(cs, 'classes', cls)
    crud_modify(cs, 'classes', {cid: {'description': new_description}})
    assert cs.classes.inserted[cid]['description'] == new_description

only_id = lambda o: o[id_field] if isinstance(o, dict) else o
tag_fn = lambda tag: lambda obj_id: dataset.random_tagged(tag.id, only_id(obj_id))
group_fn = lambda group: lambda obj_id: dataset.random_tagged(group.id, only_id(obj_id))
relate_fn = lambda role: lambda object_id, subject_id: dataset.random_related(role.id,
                                                                              only_id(object_id), only_id(subject_id))
def test_delete_class():
    # TODO finish fixing discrepancy between meta objects and dicts. do generically
    cs = changeset.ChangeSet()
    classes = list(dataset.classes.by_name.values())
    cls, cls2 = classes[:2]
    instance = cls.random_instance()
    instance2 = cls.random_instance()
    instance3 = cls2.random_instance()
    crud_insert(cs, 'objects', instance)
    crud_insert(cs, 'objects', instance2)
    tag = dataset.random_tag()
    group = dataset.random_group()
    role = dataset.random_role()
    tag_it = tag_fn(tag)
    group_it = group_fn(group)
    relate_it = relate_fn(role)
    tagged = lmap(tag_it, (instance, instance2))
    grouped = lmap(group_it, (instance, instance2))
    related = lmap(relate_it, (instance, instance3), (instance3, instance))
    for kind, data in [('tagged', list(tagged)), ('grouped', list(grouped)), ('related', list(related))]:
        data = [as_dict(d) for d in data]
        assoc_insert(cs, kind, data[0])
        assoc_delete(cs, kind, data[1])
    cs.delete('classes', cls.id)
    for kind, data in [('tagged', list(tagged)), ('grouped', list(grouped)), ('related', list(related))]:
        what = getattr(cs, kind)
        data = [as_tuple(d) for d in data]
        assert data[0] not in what.inserted
        assert data[1] not in what.deleted

def test_delete_object():
    cs = changeset.ChangeSet()
    objects = [dataset.random_instance() for _ in range(3)]
    oids = [o['id'] for o in objects]
    tag = dataset.random_tag()
    group = dataset.random_group()
    role = dataset.random_role()
    tag_it = tag_fn(tag)
    group_it = group_fn(group)
    relate_it = relate_fn(role)
    tagged = lmap(tag_it, oids[:2])
    grouped = lmap(group_it, oids[:2])
    related = lmap(relate_it, (oids[0], oids[1]), (oids[1], oids[2]))
    for kind, data in [('tagged', tagged), ('grouped', grouped), ('related', related)]:
        data = [as_dict(d) for d in data]
        assoc_insert(cs, kind, data[0])
        assoc_delete(cs, kind, data[1])
    cs.delete('objects', objects[0]['id'])
    for kind, data in [('tagged', list(tagged)), ('grouped', list(grouped)), ('related', list(related))]:
        data = [as_tuple(d) for d in data]
        assert data[0] not in getattr(cs, kind).inserted
        assert data[1] in getattr(cs, kind).deleted

def assertIn(a, b):
    assert a in b

def assertNotIn(a, b):
    assert a not in b

def test_delete_role():
    cs = changeset.ChangeSet()
    role = dataset.random_role()
    obj = dataset.random_instance()
    related = dataset.random_related(role.id, obj['id'], obj['id'])
    assoc_insert(cs, 'related', related)
    cs.delete('roles', role.id)
    assertNotIn(as_tuple(related), cs.related.inserted)

def test_delete_group():
    cs = changeset.ChangeSet()
    group = dataset.random_group()
    obj = dataset.random_instance()
    grouped = dataset.random_grouped(group.id, obj['id'])
    assoc_insert(cs, 'grouped', grouped)
    cs.delete('groups', group.id)
    assertNotIn(as_tuple(grouped), cs.grouped.inserted)

def test_delete_tag():
    cs = changeset.ChangeSet()
    tag = dataset.random_group()
    obj = dataset.random_instance()
    tagged = dataset.random_tagged(tag.id, obj['id'])
    assoc_insert(cs, 'tagged', tagged)
    crud_delete(cs, 'tags', tag.id)
    assertNotIn(as_tuple(tagged), cs.tagged.inserted)

def test_combination():
    cs = full_changeset()
    other = full_changeset(3)
    combined = changeset.ChangeSet.combine_changes(cs, other)

    get_id = lambda x: x.get('id') if isinstance(x, dict) else x.id
    for kind in attr_info.meta_kinds:
        if kind == 'queries':
            continue
        data = kind_objects(kind)
        cs_data = getattr(combined, kind)
        assertIn(get_id(data[0]), cs_data.inserted)
        assertIn(get_id(data[3]), cs_data.inserted)
        assertIn(get_id(data[1]), cs_data.modified)
        assertIn(get_id(data[4]), cs_data.modified)
        assertIn(get_id(data[2]), cs_data.deleted)
        assertIn(get_id(data[5]), cs_data.deleted)

    deleted_classes = combined.classes.deleted
    deleted_objects = combined.objects.deleted

    def check(assoc, container, opposite_container=None):
        should_be_in = not assoc.contains_deleted(
            deleted_objects, deleted_classes)
        if opposite_container:  # here to take care of dropped insert/delete
            if should_be_in:
                if assoc not in container:
                    should_be_in = assoc in opposite_container
        if should_be_in:
            if not assoc in container:
                print(assoc, 'should be in', container)
            assertIn(assoc, container)
        else:
            if assoc in container:
                print(assoc, 'should not be in', container)
            assertNotIn(assoc, container)

    for kind in changeset.assoc_kinds:
        data = [tuple_to_meta(d) for d in kind_objects(kind)]
        cs_data = getattr(combined, kind)
        inserted = {tuple_to_meta(i) for i in cs_data.inserted}
        deleted = {tuple_to_meta(d) for d in cs_data.deleted}
        check(data[0], inserted, deleted)
        check(data[3], inserted, deleted)
        check(data[1], deleted, inserted)
        check(data[4], deleted, inserted)
