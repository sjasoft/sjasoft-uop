__author__ = 'samantha'

from collections import defaultdict
from sjasoft.uop import db_interface
from sjasoft.uopmeta.schemas.meta import core_schema, Tenant, User


class Services(object):
    service_map = {}


    def __init__(self, db):
        self._db = db

    def ensure_base_schema(self):
        self.ensure_schema(core_schema)


    def schemas(self):
        return self._db.schemas()

    def tenants(self):
        return self._db.tenants()

    def get_tenant(self, tenant_id):
        tenants = self.tenants()
        return tenants.get(tenant_id)


    def tenant_user_collection(self):
        return self._db.collections.get('uop_tenant_users')

    def tenant_user_ids(self, tenant_id):
        return self.tenant_user_collection().find({'tenant_id': tenant_id}, only_cols=['user-id'])

    def active_tenants(self):
        tenants = self.tenants()
        return tenants.ids_only()
        
    def has_tenants(self):
        return self._db.has_tenants()

    def tenant_interface(self, tenant_id=None):
        # TODO likely need mechanism to reuse tenant_interface for same uuid?
        return db_interface.get_tenant_interface(self._db, tenant_id)

    def ensure_schema(self, a_schema):
        """
        Ensures that the meta objects in the schema are in the database
        :param a_schema: a Schema
        :return: None
        """
        dbi = self.tenant_interface()
        schema = self.schemas().find_one({'name': a_schema.name})
        if not schema:
            self.add_schema(a_schema)
        self.ensure_schema_installed(a_schema)

    def ensure_schema_installed(self, a_schema):
        """
        Ensure mataobjects defined in schema are what is in the database.
        """
        dbi = self.tenant_interface()
        dbi.ensure_schema(a_schema)

    def add_schema(self, a_schema):
        return self.schemas().insert(**a_schema.dict())

    def user_collection(self):
        return self._db.collections.get('uop_users')

    def get_user(self, user_id):
        return self.user_collection().get(user_id)



    def update_schema(self, schema):
        """
        Updates the metadata in the schemas wherever used (main metadata and per tenant
        :param schema: the updated schema
        :return: None
        """
        return self.ensure_schema(schema)


    def update_app_clients(self, app_id, new_schema):
        """
        The app has changed in its schema.  No other changes are possible.  So this is a
        schema evolution only that may need to be spread across tenants, if any.
        :param app_id: id of the app
        :param new_schema: updated schema
        :return: None
        """
        self.update_schema(new_schema)

    def register_tenant(self, tenantname, email, is_admin=False):
        tenant = Tenant(name=tenantname,
                          email=email, is_admin=is_admin)
        db_tenants = self._db.tenants()
        db_tenants.insert(**tenant.dict())
        return tenant

    def drop_tenant(self, tenant_id):
        """
        Drops the tenant from the database.  This version removes their data.
        :param tenant_id id of the tenant to remove
        """
        self._db.drop_tenant(tenant_id)

    def login_tenant(self, tenant_name, password):
        "returns tenant-id if credentials work"
        criteria = dict(username=tenant_name, password=password)
        db_tenants = self._db.tenants()
        tenant = db_tenants.find_one(criteria)
        if tenant:
            tenant.pop('password')
        return tenant


if __name__ == '__main__':
    from sjasoft.uop import db_service

    ser = db_service.get_service('mongo', 'pkm_app')
    u = ser.login_tenant('samantha5', 'g0dd3ss')
    dbi = ser.tenant_interface(u['_id'])
