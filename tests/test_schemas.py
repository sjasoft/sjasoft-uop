from uopmeta.schemas import meta
from uop.test.testing import check_all_pass, TestContext
from uopmeta.schemas.predefined import pkm_schema as schema


def check_subs_pass(test, base):
    check_all_pass(test, base.requires_schemas)
    check_all_pass(test, base.uses_schemas)


def check_schema(a_schema):
    of_type = lambda a_type: lambda item: isinstance(item, a_type)
    assert a_schema
    assert a_schema.requires_schemas
    assert all
    check_subs_pass(of_type(meta.Schema), a_schema)
    names = [r.name for r in a_schema.requires_schemas]
    assert ('uop_core' in names)
    db_form = a_schema.db_form()
    check_subs_pass(of_type(str), db_form)
    all_schemas = [a_schema]
    all_schemas.extend(a_schema.sub_schemas().values())
    db_forms = [a.db_form().dict() for a in all_schemas]
    from_db = meta.Schema.schemas_from_db(db_forms)
    from_db_form = from_db[a_schema.name]
    assert from_db_form.dict() == a_schema.dict()

def check_ensure_schema(context, a_schema):
    """
    Given that we start in context with fresh db schema should not be there originally. But once it was
    ensured the next assured should have nothing to do.
    :param context: The test context which includes relatively empty database
    :param a_schema: The schema to ensure is installed
    :return: nothing. internal asserts succeed or fail.
    """
    has_work, changes =  context.ensure_schema(a_schema)
    assert has_work
    classes_to_add = [c['name'] for c in changes.classes.inserted.values()]
    assert 'PersistentObject' not in classes_to_add, f'PersistentObject should have been already installed'
    has_work, changes = context.ensure_schema(a_schema)
    assert not has_work

def check_db_deleted(db_class, name):
    assert name not in db_class.existing_db_names()

def test_pkm_schema():
    db_name = ''
    db_class = None
    with TestContext.fresh_context() as context:
        db_name =  context.db_name
        db_class = context.db_class
        check_schema(schema)
        check_ensure_schema(context, schema)
    # check_db_deleted(db_class, db_name)

def check_attr_completion(context):
    def cls_attrs(cls):
        return {a.id for a in cls.attributes}

    by_name = context.classes.by_name
    for cls in context.classes.by_id.values():
        attr_ids = cls_attrs(cls)
        scls = by_name[cls.superclass] if cls.superclass else None
        supers = cls_attrs(scls) if scls else set()
        assert not (supers - attr_ids)

def check_subclasses(context:meta.MetaContext):
    by_id = context.classes.by_id
    by_name = context.classes.by_name
    for cid, cls in context.classes.by_id.items():
        print(cid, cls.name)
        subs = context.subclasses(cid)
        assert cid in subs
        for sid in (subs - {cid}):
            scls = by_id[sid]
            if scls.superclass:
                ssid = by_name[scls.superclass].id
                assert ssid in subs

def check_config(context, num_assocs, num_instances):
    assert len(context.tagged) >= num_assocs
    assert len(context.grouped) >= num_assocs
    assert len(context.related) >= num_assocs
    assert len(context.instances) >= num_instances
    assert len(context.tags.by_name.values()) >= num_instances
    assert len(context.groups.by_name.values()) >= num_instances
    assert len(context.roles.by_name.values()) >= num_instances


# def test_meta():
#     context = dataset(num_assocs=3, num_instances=10)
#     check_config(context, 3, 10)
#     check_subclasses(context)
#     assert context


