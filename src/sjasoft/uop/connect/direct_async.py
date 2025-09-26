from sjasoft.uop.connect import direct
from sjasoft.uop import changeset
from sjasoft.uop import db_service

class AsyncDBClient(direct.DirectConnection):
    @classmethod
    def get_connection(cls, db_type, db_name, **db_params):
        service = db_service.get_service(db_type, db_name, use_async=True, **db_params)
        return cls(service)

    # TODO Consider making several parts of service synchronous only

    async def register_client(self, tenantName, password, email):
        return super().register_client(tenantName, password, email)


    async def login_tenant(self, tenant_name, password):
        self._tenant = await self._service.login_tenant(tenant_name, password)
        self._db = await self._service.get_tenant_interface(self._tenante['_id'])

    async def metadata(self):
        return await super().metadata()

    async def get_changes(self, until=None):
        return await super().changes_until(until)

    async def record_changes(self, changes):
        the_changes = changeset.ChangeSet(**changes)
        await self.dbi.apply_changes(the_changes)
        if self._tenant:
            await self._service.update_if_app_changes(self._tenant, **the_changes)

    async def get_object(self, obj_id):
        return await super().get_object(obj_id)

    async def get_object_groups(self, object_id):
        return super().get_object_groups(object_id)

    async def add_object_groups(self, obj_id, group_ids):
        return await self.dbi.add_object_groups(obj_id, group_ids)

    async def set_object_groups(self, obj_id, group_ids):
        return await self.dbi.set_object_groups(oid, group_ids)

    async def add_object_tags(self, obj_id, tag_ids):
        return await self.add_object_tags(obj_id, new_tags)            

    async def set_object_tags(self, obj_id, tag_ids):
        return await self.dbi.set_object_tags(obj_id, tag_ids)

    async def tag_object(self, object_id, tag_id):
        return await self.dbi.tag(object_id, tag_id)

    async def get_object_tags(self, object_id):
        
        return await self.dbi.get_object_tags(object_id)

    async def get_object_roles(self, object_id):
        """
        Returns list of role_ids for all relationships the object identified has.
        """
        return await self.dbi.get_object_roles(object_id)

    async def tag_neighbors(self, object_id):
        """
        Return map tag_id => [object id] of objects that are tagged with each tag
        that object_id is tagged with.
        """
        return await self.dbi.tag_neighbors(object_id)

    async def group_neighbors(self, object_id):
        """
        Return map group_id => [object id] for all groups the object is directly in to other objects 
        directly in each group.  
        """
        return await self.dbi.group_neighbors(object_id)

    async def role_neighbors(self, object_id):
        """
        Returns map role_id => [object id] for all objects the given object is related to
        """
        return await self.dbi.get_object_relationships(object_id)


    async def related_to_object(self, object_id, role_id):
        """
        Returns [object id] for all objects related to the given object by the specified role
        """
        return await self.dbi.get_roleset(object_id, role_id)

    async def add_related_objects(self, object_id, role_id, object_ids):
        """
        Relate if not previously so related object_ids to the give object by the given role
        """
        return await self.dbi.add_object_related(object_id, role_id, object_ids)

    async def set_related_objects(self, object_id, role_id, object_ids):
        return await self.dbi.set_object_related(object_id, role_id, object_ids)

    async def get_tagged(self, tag_id):
        return await self.dbi.get_tagset(tag_id)
    
    async def add_tagged(self, tag_id, object_ids):
        return await self.dbi.add_tag_objects(tag_id, object_ids)

    async def set_tagged(self, tag_id, object_ids):
        return await self.dbi.set_tag_objects(tag_id, object_ids)

    async def get_grouped(self, group_id):
        return await self.dbi.get_groupset(group_id)

    async def add_grouped(self, group_id, object_ids):
        return await self.dbi.add_group_objects(group_id, object_ids)
        
    async def set_grouped(self, group_id, object_ids):
        return await self.dbi.set_group_objects(group_id, object_ids)

    async def get_tags(self):
        return await self.dbi.tags.find()

    async def create_tag(self, tag_data):
        return await self.dbi.add_tag(**tag_data)

    async def modify_tag(self, tag_id, mods):
        return await self.dbi.modify.tag(tag_id, **mods)

    async def delete_tag(self, tag_id):
        return await self.dbi.delete_tag(tag_id)

    async def get_roles(self):
        return await self.dbi.related.find()

    async def create_role(self, data):
        return await self.dbi.add_role(**data)

    async def modify_role(self, role_id, mods):
        return await self.dbi.modify_role(role_id, **mods)

    async def delete_role(self, role_id):
        return await self.dbi.delete_role(role_id)

    async def get_classes(self):
        return await self.dbi.classes.find()

    async def create_class(self, data):
        return await self.dbi.add_class(**data)

    async def modify_class(self, class_id, mods):
        return await self.dbi.modify_class(class_id, **mods)
        
    async def delete_class(self, class_id):
        return await self.dbi.delete_class(class_id)

    async def get_queries(self):
        return await self.dbi.queries.find()

    async def create_query(self, data):
        return await self.dbi.add_query(**data)

    async def modify_query(self, query_id, mods):
        return await self.dbi.modify_query(query_id, **mods)

    async def delete_query(self, query_id):
        return await self.dbi.delete_query(query_id)

    async def get_groups(self):
        return await self.dbi.groups.find()

    async def create_group(self, group_data):
        return await self.dbi.add_group(**group_data)

    async def modify_group(self, group_id, mods):
        return await self.dbi.modify_group(group_id, **mods)

    async def delete_group(self, group_id):
        return await self.dbi.delete_group(group_id)

    async def get_attributes(self):
        return await self.dbi.attributes.find()

    async def create_attribute(self, data):
        return await self.dbi.add_attribute(**data)

    async def modify_attribute(self, attr_id, mods):
        return await self.dbi.modify_attribute(attr_id, **mods)

    async def delete_attribute(self, attr_id):
        return await self.dbi.delete_attribute(attr_id)

    async def run_query(self, query_id=None, query=None):
        the_query = (await self.dbi.queries.get(query_id)) if query_id else query
        return await self.dbi.query(query)

    async def bulk_load(self, ids, perserve_order=True):
        return await self.dbi.bulk_load(ids, perserve_order)

