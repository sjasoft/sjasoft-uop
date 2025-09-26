from uopmeta.schemas import meta


def check_persist(qc):
    d = qc.to_dict()
    qc2 = meta.qc_dict_to_component(d)
    assert qc == qc2

def test_persist():
    cls = meta.ClassComponent(cls_name='Person')
    check_persist(cls)
    attr = meta.AttributeComponent(attr_name='foo',
                                   operate='$gte',
                                   value=3)
    check_persist(attr)
    tags = meta.TagsComponent(names=['foo','bar'], application='all')
    groups = meta.GroupsComponent(names=['foo','bar'], application='all')
    check_persist(tags)
    check_persist(groups)
    related = meta.RelatedTo(obj_id='332302e_432efa', role='whatever')
    check_persist(related)
    and_all = meta.AndQuery(components=[
        tags, groups, related, attr, cls
    ])
    or_all = meta.OrQuery(components=[
        tags, groups, related, attr, cls
    ])
    and1 = meta.AndQuery(components=[
        tags, cls
    ])
    and2 = meta.AndQuery(components=[
        attr, cls
    ])
    or1 = meta.OrQuery(components=[
        tags, cls
    ])
    or2 = meta.OrQuery(components=[
        tags, cls
    ])
    and_m = meta.AndQuery(components=[
        or2, attr, and1
    ])
    or_m = meta.OrQuery(components=[
        tags, cls, and2, or1
    ])
    check_persist(and_m)
    check_persist(or_m)


