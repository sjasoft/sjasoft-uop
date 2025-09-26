__author__ = 'samantha'

from uop.test import testing
from uopmeta.schemas import meta
from uopmeta.schemas.predefined import pkm_schema
from uop.collections import crud_kinds, assoc_kinds, meta_kinds, uop_collection_names

def crud_names(base):
    return 'add_' + base, 'modify_' + base, 'delete_' + base

schemas = (pkm_schema,)

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


context:testing.TestContext = None

def set_context(**the_credentials):
    global context
    context = testing.TestContext.fresh_context(**the_credentials)


def get_kind_collection(kind, a_context=context):
    global context
    a_context = a_context or context
    return a_context.get_db_method(kind)

def get_methods(kind, a_context=context):
    """
    insert, mobify, delete methods against database intereface
    :param kind: type of meta item method is needed for
    :param a_context: testing tontext giving dbi etc.
    :return: the method name
    """
    global context
    a_context = a_context or context
    method_names = interface_methods[kind]
    methods = [a_context.get_db_method(m) for m in method_names]
    kv = zip(['insert', 'modify', 'delete'], methods)
    return dict(kv)

def object_exists(obj_id):
    global context
    return context.get_db_method('get_object')(obj_id)

def meta_item_exists(kind, an_id):
    return get_kind_collection(kind).get(an_id)

def check_collections():
    """just ensure collection existence, and separation across user and regular dbs"""
    for kind in meta.kind_map:
        # TODO maybe expand here to test tenant case
        collection = get_kind_collection(kind)
        assert collection

def get_unique(count, random_fn, known):
    """
    Get count unique items from random_fn, not in known
    :param count: number of items to get
    :param random_fn: function to get random items
    :param known: list of dicts  of known items
    :return: list of unique items
    """
    res = []
    def to_tuple(item):
        as_dict = item if isinstance(item, dict) else item.dict()
        return tuple(as_dict.values())
    known = set(map(to_tuple, known))
    while len(res) < count:
        item = random_fn()
        as_tuple = to_tuple(item)
        if as_tuple not in known:
            res.append(item)
            known.add(as_tuple)
    return res

def insert_and_check(random_data, db_tagged, db_grouped, db_related):
    for kind in crud_kinds:
        if kind in ['objects', 'queries']:
            continue
        cls = meta.kind_map[kind]
        inserter = get_methods(kind)['insert']
        coll = get_kind_collection(kind)
        data = random_data.all_of_kind(kind)
        for obj in data:
            id = obj.id
            present = coll.get(id)
            if present:
                print('already in database', kind, id, obj)
            else:
                data = obj.without_kind()
                inserter(**data)
            from_db = coll.get(id)
            if not from_db:
                print('%s(%s) not in db!' % (kind, id))
            assert from_db
    assoc_add = random_data.random_tagged()
    for kind in assoc_kinds:
        fn = getattr(random_data, f'random_{kind}')
        coll = get_kind_collection(kind)
        items = get_unique(5, fn, coll.find())
        for obj in items:
            obj = obj.without_kind()
            coll.insert(**obj)
            found = coll.find_one(obj)
            assert found


def modify_and_check(random_data, db_tagged, db_grouped, db_related):
    global context
    desc = "this is the new description"
    for kind in crud_kinds:
        if kind in ['objects', 'queries']:
            continue
        cls = meta.kind_map[kind]
        modifier = get_methods(kind)['modify']
        coll = context.get_db_method(kind)
        for obj in random_data.all_of_kind(kind):
            id = obj.id
            modifier(id, description=desc)
            from_db = coll.get(id)
            if not from_db:
                print('%s(%s) no in db!' % (kind, id))
            assert from_db['description'] == desc
            
get_id = lambda obj: obj['id']

def delete_and_check(random_data, db_tagged, db_grouped, db_related):
    global context
    a_class, other_class = random_data.distinct_pair('classes', lambda c: not c.is_abstract)
    random_class = random_data.random_new_class()
    context.get_db_method('add_class')(**random_class.without_kind())
    a_role, another_role = random_data.distinct_pair('roles')
    a_tag, another_tag = random_data.distinct_pair('tags')
    a_group, another_group = random_data.distinct_pair('groups')

    add_object = context.get_db_method('add_object')

    def add_class_object(a_class):
        global context
        object = random_data.random_class_instance(a_class)
        insert = context.get_db_method('add_object')
        insert(object)
        return object

    def add_grouped(group, object)->meta.Grouped:
        global context
        assoc = meta.Grouped(assoc_id=group.id, object_id=get_id(object))
        db_group = context.get_db_method('group')
        db_group(get_id(object), group.id)
        return assoc
    def add_tagged(tag, object)->meta.Tagged:
        global context
        assoc = meta.Tagged(assoc_id=tag.id, object_id=get_id(object))
        db_tag = context.get_db_method('tag')
        db_tag(get_id(object), tag.id)
        return assoc
    def add_related(role, subject, object)->meta.Related:
        global context
        assoc = meta.Related(assoc_id=role.id, object_id=get_id(object), subject_id=get_id(subject))
        db_relate = context.get_db_method('relate')
        db_relate(get_id(subject), role.id, get_id(object))
        return assoc

    def assoc_exists(collection, assoc:meta.Associated):
        data = assoc.dict()
        data.pop('kind', None)
        return collection.exists(data)

    obj1 = add_class_object(a_class)
    assert object_exists(obj1['id'])
    obj2 = add_class_object(a_class)
    obj3 = add_class_object(other_class)
    obj4 = add_class_object(random_class)
    obj5 = add_class_object(random_class)

    assert object_exists(obj1['id'])

    grouped = add_grouped(a_group, obj1)
    grouped2 = add_grouped(a_group, obj2)
    grouped3 = add_grouped(another_group, obj2)
    grouped4 = add_grouped(another_group, obj4)

    tagged = add_tagged(a_tag, obj1)
    tagged2 = add_tagged(a_tag, obj2)
    tagged3 = add_tagged(another_tag, obj2)
    tagged4 = add_tagged(another_tag, obj4)

    related = add_related(a_role, obj1, obj2)
    related2 = add_related(a_role, obj2, obj2)
    related3 = add_related(another_role, obj2, obj4)
    related4 = add_related(another_role, obj3, obj4)

    get_methods('objects')['delete'](get_id(obj1))
    assert not assoc_exists(db_grouped, grouped)
    assert assoc_exists(db_grouped, grouped2)
    assert not assoc_exists(db_tagged, tagged)
    assert assoc_exists(db_tagged, tagged2)
    assert not assoc_exists(db_related, related)
    assert assoc_exists(db_related, related2)

    get_methods('classes')['delete'](random_class.id)
    assert not assoc_exists(db_grouped, grouped4)
    assert assoc_exists(db_grouped, grouped2)
    assert not assoc_exists(db_tagged, tagged4)
    assert assoc_exists(db_tagged, tagged2)
    assert not assoc_exists(db_related, related3)
    assert not assoc_exists(db_related, related4)
    assert assoc_exists(db_related, related2)


    get_methods('groups')['delete'](another_group.id)
    assert not get_kind_collection('grouped').exists(grouped4.without_kind())
    get_methods('tags')['delete'](another_tag.id)
    assert not get_kind_collection('tagged').exists(tagged4.without_kind())
    get_methods('roles')['delete'](another_role.id)
    assert not get_kind_collection('related').exists(related4.without_kind())


async def complete_context():
    global context
    await context.service_and_db_class()

def check_extensions():
    global context
    pass


async def test_db():
    """
    This is the main test of UOP db_interface.  To use it for a particular interface
    first use set_context to set the inferface to use. Then call/await this function.

    :return:
    """
    global context, schemas
    await context.complete_context(schemas=schemas)
    random_data = context.dataset()
    db_tagged = context.get_db_method('tagged')
    db_grouped = context.get_db_method('grouped')
    db_related = context.get_db_method('related')
    insert_and_check(random_data, db_tagged, db_grouped, db_related)
    modify_and_check(random_data, db_tagged, db_grouped, db_related)
    delete_and_check(random_data, db_tagged, db_grouped, db_related)
