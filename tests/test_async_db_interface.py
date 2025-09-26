__author__ = 'samantha'

from uop.test import testing
from uopmeta.schemas import meta
from uopmeta.schemas.predefined import pkm_schema
from uop.collections import crud_kinds, assoc_kinds, meta_kinds, uop_collection_names
from uop.utils import ca


def crud_names(base):
    return 'add_' + base, 'modify_' + base, 'delete_' + base


interface_methods = dict(
    classes=crud_names('class'),
    roles=crud_names('role'),
    attributes=crud_names('attribute'),
    tags=crud_names('tag'),
    groups=crud_names('group'),
    tagged=('tag', 'untag'),
    grouped=('group', 'ungroup'),
    related=('relate', 'unrelate'),
    objects=crud_names('object')
)

#credentials = dict(username='admin', password='password')
credentials = {}
context = testing.TestContext.fresh_context(**credentials, use_async=True)
sync_context = context._sync

def random_kind(kind):
    return getattr(random_data, f'random_{kind}')


def get_kind_collection(kind, a_context=context):
    return a_context.get_db_method(kind)

def get_methods(kind, a_context=context):
    """
    insert, mobify, delete methods against database intereface
    :param kind: type of meta item method is needed for
    :param a_context: testing tontext giving dbi etc.
    :return: the method name
    """
    method_names = interface_methods[kind]
    methods = [a_context.get_db_method(m) for m in method_names]
    kv = zip(['insert', 'modify', 'delete'], methods)
    return dict(kv)

async def object_exists(obj_id):
    return context.get_db_method('get_object')(obj_id)

def meta_item_exists(kind, an_id):
    return get_kind_collection(kind).get(an_id)

def check_collections():
    """just ensure collection existence, and separation across user and regular dbs"""
    for kind in meta.kind_map:
        # TODO maybe expand here to test tenant case
        collection = get_kind_collection(kind)
        assert collection

async def insert_and_check(random_data, db_tagged, db_grouped, db_related):
    for kind in crud_kinds:
        if kind in ['objects', 'queries']:
            continue
        cls = meta.kind_map[kind]
        inserter = get_methods(kind)['insert']
        coll = get_kind_collection(kind)
        data = random_data.all_of_kind(kind)
        for obj in data:
            id = obj.id
            present = await coll.get(id)
            if present:
                print('already in database', kind, id, obj)
            else:
                data = obj.without_kind()
                stored = await inserter(**data)
            from_db = await coll.get(id)
            if not from_db:
                print('%s(%s) not in db!' % (kind, id))
            assert from_db
    assoc_add = random_data.random_tagged()
    for kind in assoc_kinds:
        fn = getattr(random_data, f'random_{kind}')
        coll = get_kind_collection(kind)
        for i in range(5):
            obj = fn()
            obj = obj.without_kind()
            await coll.insert(**obj)
            found = await coll.find_one(obj)
            assert found

async def modify_and_check(random_data, db_tagged, db_grouped, db_related):
    desc = "this is the new description"
    for kind in crud_kinds:
        if kind in ['objects', 'queries']:
            continue
        cls = meta.kind_map[kind]
        modifier = get_methods(kind)['modify']
        coll = context.get_db_method(kind)
        for obj in random_data.all_of_kind(kind):
            id = obj.id
            await modifier(id, description=desc)
            from_db = await coll.get(id)
            if not from_db:
                print('%s(%s) no in db!' % (kind, id))
            assert from_db['description'] == desc
            
get_id = lambda obj: obj['id']

async def delete_and_check(random_data, db_tagged, db_grouped, db_related):
    a_class, other_class = random_data.distinct_pair('classes')
    random_class = random_data.random_new_class()
    add_class = context.get_db_method('add_class')
    added = await add_class(**random_class.without_kind())
    a_role, another_role = random_data.distinct_pair('roles')
    a_tag, another_tag = random_data.distinct_pair('tags')
    a_group, another_group = random_data.distinct_pair('groups')

    add_object = context.get_db_method('add_object')

    async def add_class_object(a_class):
        object = random_data.random_class_instance(a_class)
        insert = context.get_db_method('add_object')
        await insert(object)
        return object

    async def add_grouped(group, object)->meta.Grouped:
        assoc = meta.Grouped(assoc_id=group.id, object_id=get_id(object))
        db_group = context.get_db_method('group')
        await db_group(get_id(object), group.id)
        return assoc

    async def add_tagged(tag, object)->meta.Tagged:
        assoc = meta.Tagged(assoc_id=tag.id, object_id=get_id(object))
        db_tag = context.get_db_method('tag')
        await db_tag(get_id(object), tag.id)
        return assoc

    async def add_related(role, subject, object)->meta.Related:
        assoc = meta.Related(assoc_id=role.id, object_id=get_id(object), subject_id=get_id(subject))
        db_relate = context.get_db_method('relate')
        await db_relate(get_id(subject), role.id, get_id(object))
        return assoc

    async def assoc_exists(collection, assoc:meta.Associated):
        data = assoc.dict()
        data.pop('kind', None)
        return await collection.exists(data)

    obj1 = await add_class_object(a_class)
    assert await object_exists(obj1['id'])
    obj2 = await add_class_object(a_class)
    obj3 = await add_class_object(other_class)
    obj4 = await add_class_object(random_class)
    obj5 = await add_class_object(random_class)

    assert await object_exists(obj1['id'])

    grouped = await add_grouped(a_group, obj1)
    grouped2 = await add_grouped(a_group, obj2)
    grouped3 = await add_grouped(another_group, obj2)
    grouped4 = await add_grouped(another_group, obj4)

    tagged = await add_tagged(a_tag, obj1)
    tagged2 = await add_tagged(a_tag, obj2)
    tagged3 = await add_tagged(another_tag, obj2)
    tagged4 = await add_tagged(another_tag, obj4)

    related = await add_related(a_role, obj1, obj2)
    related2 = await add_related(a_role, obj2, obj2)
    related3 = await add_related(another_role, obj2, obj4)
    related4 = await add_related(another_role, obj3, obj4)

    obj_delete = get_methods('objects')['delete']
    await obj_delete(get_id(obj1))
    assert not await assoc_exists(db_grouped, grouped)
    assert await assoc_exists(db_grouped, grouped2)
    assert not await assoc_exists(db_tagged, tagged)
    assert await assoc_exists(db_tagged, tagged2)
    assert not await assoc_exists(db_related, related)
    assert await assoc_exists(db_related, related2)

    cls_delete = get_methods('classes')['delete']
    await cls_delete(random_class.id)
    assert not await assoc_exists(db_grouped, grouped4)
    assert await assoc_exists(db_grouped, grouped2)
    assert not await assoc_exists(db_tagged, tagged4)
    assert await  assoc_exists(db_tagged, tagged2)
    assert not await assoc_exists(db_related, related3)
    assert not await assoc_exists(db_related, related4)
    assert await assoc_exists(db_related, related2)

    group_delete = get_methods('groups')['delete']
    await group_delete(another_group.id)
    assert not await get_kind_collection('grouped').exists(grouped4.without_kind())
    tag_delete = get_methods('tags')['delete']
    await tag_delete(another_tag.id)
    assert not await get_kind_collection('tagged').exists(tagged4.without_kind())
    role_delete = get_methods('roles')['delete']
    await role_delete(another_role.id)
    assert not await get_kind_collection('related').exists(related4.without_kind())

async def complete_context():
    await context.service_and_db_class()

async def test_db():
    await complete_context()
    random_data = context.dataset()
    db_tagged = context.get_db_method('tagged')
    db_grouped = context.get_db_method('grouped')
    db_related = context.get_db_method('related')
    await insert_and_check(random_data, db_tagged, db_grouped, db_related)
    await modify_and_check(random_data, db_tagged, db_grouped, db_related)
    await delete_and_check(random_data, db_tagged, db_grouped, db_related)

