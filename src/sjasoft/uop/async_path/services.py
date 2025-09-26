__author__ = 'samantha'

import asyncio
from collections import defaultdict
from sjasoft.uop.async_path import db_interface
from sjasoft.uop import services as base


class Services(base.Services):
    """
    Main externally visible API for defining tenants, applications and
    which tenants may use which applications
    """
    service_map = {}


    def __init__(self, db):
        self._db = db

    async def ensure_base_schema(self):
        await self.ensure_schema(base.core_schema)

    async def get_tenant(self, tenant_id):
        tenants = self.tenants()
        return await tenants.get(tenant_id)

    async def _application_from_db(self, app_info):
        if not app_info:
            raise Exception('Application not found')

        tenants = await self.tenants()
        apps = await self.applications()
        app_name = app_info['name']

        def cleaned(instance):
            ret = dict(instance)
            ret.pop('_tenantId', None)
            ret.pop('extension_name', None)
            return ret

        description = app_info.get('description')
        uid = app_info.get('admin_tenant')
        if not uid:
            app_tenant = await tenants.find_one({'tenantname': app_name})
            if app_tenant:
                uid = app_tenant['_id']
                await apps.update({'_id': app_info['_id']}, {'admin_tenant': uid})
            else:
                raise Exception('No admin tenant for application %s' % app_name)
        dbi = await self.tenant_interface(uid)
        metas = (await dbi.metadata()).instances()
        cleaned_meta = {k: [cleaned(i) for i in v] for k, v in metas.items()}
        return Application(app_name, description=description, _id=app_info['_id'], **cleaned_meta)

    async def get_application(self, app_id):
        apps = self.applications()
        app_info = await apps.get(app_id)
        return Application.from_db(app_info)

    async def get_admined_application(self, tenant_id):
        apps = await self.applications()
        app = await apps.find_one({'admin_tenant': tenant_id})
        return app['_id'] if app else None

    async def active_tenants(self):
        tenants = await self.tenants()
        return await tenants.ids_only()

    async def tenant_applications(self, tenant_id):
        app_tenants = await self.application_tenants_collection()
        return await app_tenants.find({'tenant_id': tenant_id}, only_cols=['app_id'])

    async def tenants_of_application(self, app_id):
        app_tenants = await self.application_tenants_collection()
        listed = await app_tenants.find({'app_id': app_id}, only_cols=['tenant_id'])
        active = await self.active_tenants()
        actual = set(listed) & set(active)
        return actual

    async def application_interface(self, app_id):
        return await db_interface.get_tenant_interface(self._db, tenant_id=app_id)

    async def tenant_interface(self, tenant_id=None):
        # TODO likely need mechanism to reuse tenant_interface for same uuid?
        return await db_interface.get_tenant_interface(self._db, tenant_id=tenant_id)

    async def add_application(self, application_spec):
        app_name = application_spec['name']
        apps = await self.applications()
        if (await apps.exists({'name': app_name})):
            raise Exception('Application named %s already exists' % app_name)
        data = {k: v for k, v in application_spec.items() if k != '_collection'}
        app_admin = await self.create_app_owner(app_name, application_spec)
        data['admin_tenant'] = app_admin['_id']
        await apps.insert(data)

    async def get_application_named(self, app_name):
        apps = await self.applications()
        app_info = await apps.find_one({'name': app_name})
        if not app_info:
            return None
        return await self._application_from_db(app_info)

    async def ensure_application(self, app):
        apps = self.applications()
        exists = await apps.exists({'name': app['name']})
        if not exists:
            await self.add_application(app)
        return await self.get_application_named(app.name)


    async def ensure_schema(self, a_schema):
        """
        Ensures that the meta objects in the schema are in the database
        :param a_schema: a Schema
        :return: None
        """
        await self.tenant_interface()
        schema = await self.schemas().find_one({'name': a_schema.name})
        if not schema:
            await self.add_schema(a_schema)
        await self.ensure_schema_installed(a_schema)

    async def add_schema(self, a_schema):
        return await self.schemas().insert(**a_schema.dict())

    async def ensure_schema_installed(self, a_schema):
        """
        Ensure mataobjects defined in schema are what is in the database.
        """
        dbi = await self.tenant_interface()
        await dbi.ensure_schema(a_schema)

    async def get_user(self, user_id):
        return await self.user_collection().get(user_id)

    async def add_app_to_tenant(self, tenant_id, app_id):
        '''
        Adds the application to the tenant if it is not present
        :param tenant_id: id of the tenant
        :param app_id: id of the app
        :return: None
        '''
        app = await self.get_application(app_id)
        tenants = await self.tenants()
        a_tenants = await self.tenants_of_application(app_id)
        tenant = await tenants.get(tenant_id)
        if tenant_id not in a_tenants:
            # add the apps classes, etc to the tenant
            interface = await self.tenant_interface(tenant_id)
            if 'id_mapping' not in tenant:
                tenant['id_mapping'] = defaultdict(dict)

            app_changes = application_as_changeset(app)
            changes = app_changes.tenantmap_translated(tenant['id_mapping'], tenant['_id'])
            await tenants.update({'_id': tenant_id}, {'id_mapping': tenant['id_mapping']})
            await interface.apply_changes(changes)
            meta = (await interface.metadata()).instances()
            app_tenants = await self.application_tenants_collection()
            await app_tenants.insert({'app_id': app_id, 'tenant_id': tenant_id})

    async def update_if_app_changes(self, tenant_id, changes):
        apps = await self.applications()
        the_app = await apps.find_one({'admin_tenant': tenant_id})
        if the_app:
            await self.update_app_clients(the_app['_id'], changes)

    async def update_app_clients(self, app_id, changes):
        tenants = await self.tenants_of_application(app_id)
        await asyncio.gather(*[self.update_app_tenant(u, changes) for u in tenants])

    async def update_app_tenant(self, tenant_id, changes):
        tenants = await self.tenants()
        tenant = await self.get_tenant(tenant_id)
        if 'id_mapping' not in tenant:
            tenant['id_mapping'] = defaultdict(dict)
        u_changes = changes.tenantmap_translated(tenant['id_mapping'], tenant['_id'])
        u_dbi = await self.tenant_interface(tenant_id)
        await u_dbi.apply_changes(u_changes)
        await tenants.update({'_id': tenant_id}, {'id_mapping': tenant['id_mapping']})

    async def register_tenant(self, tenantname, password, email, is_admin=False):
        tenant = tenantx.tenant(tenantname=tenantname, password=password,
                          email=email, isAdmin=is_admin)
        db_tenants = await self._db.tenants()
        await db_tenants.insert(tenant)
        tenant = dict(tenant)
        tenant.pop('password')
        return tenant

    async def create_app_owner(self, app_name, app):
        admin = await self.register_tenant(app_name, app_name, 'info@conceptwareinc.com', is_admin=True)
        dbi = await self.tenant_interface(admin['_id'])
        await dbi.apply_changes(app.as_changeset())
        return admin

    async def drop_tenant(self, tenant_id):
        """
        Drops the tenant from the database.  This version removes their data.
        :param tenant_id id of the tenant to remove
        """
        await self._db.drop_tenant(tenant_id)

    async def login_tenant(self, username, password):
        "returns tenant-id if credentials work"
        criteria = dict(username=username, password=password)
        db_tenants = await self._db.tenants()
        tenant = await db_tenants.find_one(criteria)
        if tenant:
            tenant.pop('password')
        return tenant

    async def ensure_tenant_has_app(self, tenant_id, app_id):
        tenant_apps = await self.tenant_applications(tenant_id)
        if app_id not in tenant_apps:
            await self.add_app_to_tenant(tenant_id, app_id)


if __name__ == '__main__':
    from sjasoft.uop import db_service

    async def test_service():
        ser = db_service.get_service('mongo', 'pkm_app', use_async=True, host='pop2')
        u = await ser.login_tenant('samantha5', 'g0dd3ss')
        return await ser.tenant_interface(u['_id'])


    loop = asyncio.get_event_loop()
    print(loop.run_until_complete(test_service()))

