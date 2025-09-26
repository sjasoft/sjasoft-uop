cls_extension_field = 'instance_collection'

uop_collection_names = dict(
    tags='uop_tags',
    classes='uop_classes',
    attributes='uop_attrs',
    roles='uop_roles',
    groups='uop_groups',
    queries='uop_queries',
    related='uop_related',
    grouped='uop_grouped',
    tagged='uop_tagged',
    changes='uop_changes',
    databases='uop_database',
    tenants='uop_tenants',
    schemas='uop_schemas',
    users='uop_users',
)

crud_kinds = ['objects', 'classes', 'attributes', 'roles', 'tags',
              'groups', 'queries']
meta_kinds = crud_kinds[1:]  # TODO reconsider queries which are mixed!
internal_kinds = ['database', 'tenants', 'schemas', 'users', 'applications', 'application_tenants']
assoc_kinds = ['grouped', 'tagged', 'related']
per_tenant_kinds = assoc_kinds + ['changes']
kinds = crud_kinds + assoc_kinds
shared_collections = crud_kinds[1:]


