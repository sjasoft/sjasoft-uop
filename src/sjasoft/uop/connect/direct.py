
from sjasoft.uop.connect import generic
from sjasoft.uop import db_service, changeset
import asyncio


class DirectConnection(generic.GenericConnection):
    @classmethod
    def get_connection(cls, db_type, db_name, tenant_id=None, **db_params):
        service, context = db_service.get_uop_service(db_type=db_type, db_name=db_name, use_async=False, tenant_id=tenant_id,  **db_params)
        return cls(service, context, tenant_id)

    @classmethod
    async def get_connection(cls, db_type, db_name, tenant_id=None, **db_params):
        service, context = await db_service.get_uop_service(db_type=db_type, db_name=db_name, use_async=False, tenant_id=tenant_id,  **db_params)
        return cls(service, context, tenant_id)

    @classmethod
    def connect(cls, db_type, db_name, tenant_id=None, **db_params):
        loop = asyncio.get_event_loop()
        service, context = loop.run_until_complete(db_service.get_uop_service(db_type=db_type, db_name=db_name,
                                                                              use_async=False, tenant_id=tenant_id,
                                                                              **db_params))
        return cls(service, context, tenant_id)

    def __init__(self, service, context, tenant_id=None):
        super().__init__()
        self._service = service
        self._context = context
        self._dbi = self._context.interface
        self._tenant_id = tenant_id


    @property
    def dbi(self):
        return self._dbi

    def __getattr__(self, name):
        return getattr(self._context, name, None) or getattr(self._dbi, name, None)

    def id_to_name(self, kind):
        return self._context.metacontext.id_to_name(kind)

    def name_to_id(self, kind):
        return self._context.metacontext.name_to_id(kind)

    def name_map(self, kind):
        return self._context.metacontext.name_map(kind)

    def id_map(self, kind):
        return self._context.metacontext.id_map(kind)

    def register_client(self, tenant_name, password, email):  # TODO change to JWT?
        return self._service.register_tenant(tenant_name, password, email)

    def logged_in(self):
        return True  # TODO make this better possibly also with JWT


    def login_tenant(self, tenant_name, password):  # TODO JWT
        self._tenant = self._service.login_tenant(tenant_name, password)
        self._db = self._service.get_tenant_interface(self._tenant['_id'])

    def metacontext(self):
        return self._context.metacontext

    def get_changes(self, since):
        return self.dbi.changes_since(since)

    def record_changes(self, changes):
        the_changes = changeset.ChangeSet(**changes)
        self.dbi.apply_changes(the_changes)


    def get_object(self, obj_id):
        return self.dbi.get_object(obj_id)

    def get_object_groups(self, object_id):
        return self.dbi.get_object_groups(object_id)

    def add_object_groups(self, obj_id, group_ids):
        new_groups = set(self.get_object_groups(obj_id)) | set(group_ids)
        self.set_object_groups(obj_id, new_groups)            

    def set_object_groups(self, obj_id, group_ids):
        self.dbi.set_object_groups(obj_id, group_ids)

    def add_object_tags(self, obj_id, tag_ids):
        new_tags = set(self.get_object_tags(obj_id)) | set(tag_ids)
        self.set_object_tags(obj_id, new_tags)            

    def set_object_tags(self, obj_id, tag_ids):
        self.dbi.set_object_tags(obj_id, tag_ids)

    def tag_object(self, object_id, tag_id):
        return self.dbi.tag(object_id, tag_id)

    def get_object_tags(self, object_id):
        return self.dbi.get_object_tags(object_id)

    def get_object_roles(self, object_id):
        """
        Returns list of role_ids for all relationships the object identified has.
        """
        return self.dbi.get_object_roles(object_id)

    def tag_neighbors(self, object_id):
        """
        Return map tag_id => [object id] of objects that are tagged with each tag
        that object_id is tagged with.
        """
        return self.dbi.tag_neighbors(object_id)

    def group_neighbors(self, object_id):
        """
        Return map group_id => [object id] for all groups the object is directly in to other objects 
        directly in each group.  
        """
        return self.dbi.group_neighbors(object_id)

    def role_neighbors(self, object_id):
        """
        Returns map role_id => [object id] for all objects the given object is related to
        """
        return self.dbi.get_object_relationships(object_id)


    def related_to_object(self, object_id, role_id):
        """
        Returns [object id] for all objects related to the given object by the specified role
        """
        return self.dbi.get_roleset(object_id, role_id)

    def add_related_objects(self, object_id, role_id, object_ids):
        """
        Relate if not previously so related object_ids to the give object by the given role
        """
        return self.dbi.add_object_related(object_id, role_id, object_ids)

    def set_related_objects(self, object_id, role_id, object_ids):
        return self.dbi.set_object_related(object_id, role_id, object_ids)

    def get_tagged(self, tag_id):
        return self.dbi.get_tagset(tag_id)
    
    def add_tagged(self, tag_id, object_ids):
        return self.dbi.add_tag_objects(tag_id, object_ids)

    def set_tagged(self, tag_id, object_ids):
        return self.dbi.set_tag_objects(tag_id, object_ids)

    def get_grouped(self, group_id):
        return self.dbi.get_groupset(group_id)

    def add_grouped(self, group_id, object_ids):
        return self.dbi.add_group_objects(group_id, object_ids)
        
    def set_grouped(self, group_id, object_ids):
        return self.dbi.set_group_objects(group_id, object_ids)

    def get_tags(self):
        return self.dbi.tags.find()

    def create_tag(self, tag_data):
        return self.dbi.add_tag(**tag_data)

    def modify_tag(self, tag_id, mods):
        return self.dbi.modify.tag(tag_id, **mods)

    def delete_tag(self, tag_id):
        return self.dbi.delete_tag(tag_id)

    def get_roles(self):
        return self.dbi.related.find()

    def create_role(self, data):
        return self.dbi.add_role(**data)

    def modify_role(self, role_id, mods):
        return self.dbi.modify_role(role_id, **mods)

    def delete_role(self, role_id):
        return self.dbi.delete_role(role_id)

    def get_classes(self):
        return self.dbi.classes.find()

    def create_class(self, data):
        return self.dbi.add_class(**data)

    def modify_class(self, class_id, mods):
        return self.dbi.modify_class(class_id, **mods)
        
    def delete_class(self, class_id):
        return self.dbi.delete_class(class_id)

    def get_queries(self):
        return self.dbi.queries.find()

    def create_query(self, data):
        return self.dbi.add_query(**data)

    def modify_query(self, query_id, mods):
        return self.dbi.modify_query(query_id, **mods)

    def delete_query(self, query_id):
        return self.dbi.delete_query(query_id)

    def get_groups(self):
        return self.dbi.groups.find()

    def create_group(self, group_data):
        return self.dbi.add_group(**group_data)

    def modify_group(self, group_id, mods):
        return self.dbi.modify_group(group_id, **mods)

    def delete_group(self, group_id):
        return self.dbi.delete_group(group_id)

    def get_attributes(self):
        return self.dbi.attributes.find()

    def create_attribute(self, data):
        return self.dbi.add_attribute(**data)

    def modify_attribute(self, attr_id, mods):
        return self.dbi.modify_attribute(attr_id, **mods)

    def delete_attribute(self, attr_id):
        return self.dbi.delete_attribute(attr_id)

    def run_query(self, query_id=None, query=None):
        the_query = self.dbi.queries.get(query_id) if query_id else query
        return self.dbi.query(query)

    def bulk_load(self, ids, perserve_order=True):
        return self.dbi.bulk_load(ids, perserve_order)

