from sjasoft.utils.dicts import first_kv

class MultiTenancy:
    kind = 'no_tenants'
    def __init__(self, db, tenant_id=None):
        """
        Multi-tenancy base class. A multi-tenancy implementation
        may change what database is used or add information to
        collections for example specifying which tenant_id we want
        to specialize how to find per tenant collections or select by
        tenant_id for shared underlying tables/collections.
        """
        self._db = db
        self._tenant_id = tenant_id

    def with_tenant(self, shared_table=False):
        return lambda x: x

    def database(self):
        return self._db
    
class TenantFieldTenancy(MultiTenancy):
    kind = 'embedded'
    def with_tenant(self, shared_table=False):
        def shared_modifier(condition):
            tenant_equality = {'tenant_id': self._tenant_id}
            if (condition == tenant_equality) or not condition:
                return tenant_equality
            key, value = first_kv(condition)
            if key == '$and':
                if 'tenant_id' not in value:
                    return {key: value.extend([tenant_equality])}
                return condition
            else:
                return {'$and': [tenant_equality, condition]}

        return shared_modifier if shared_table else super().with_tenant()


class SeparateDBCollections(MultiTenancy):
    """
    This class is the bsse for multi-tenant systems that use
    separate database per tenant.
    """
    kind = 'separate'
    pass

class SchemaDBCollections(MultiTenancy):
    """
    Handles multi-tenant situation implemented by database such as
    postgresql which implement separate schemas in the same database.
    """
    kind = 'schema'
    pass


tenancy_types = {
    'no_tenants': MultiTenancy,
    'embedded': TenantFieldTenancy,
    'schema': SchemaDBCollections,
    'separate': SeparateDBCollections
}

def get_tenancy(db, tenant_type='no_tenants', tenant_id=None):
    return tenancy_types[tenant_type](db, tenant_id)
