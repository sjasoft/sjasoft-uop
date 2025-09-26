import time, uuid

class GenericConnection():

    def __init__(self):
        self._client_id = uuid.uuid4().hex
        self._context = None
        self._tenant = None

    def register_tenant(self, tenantname, password, email=''):
        pass

    @property
    def tenant(self):
        return getattr(self, '_tenant', None)

    @property
    def is_admin(self):
        return getattr(self, '_is_admin', getattr(self, '_tenant', None) and self._tenant['isAdmin'])

    @property
    def logged_in(self):
        return True

    def login_tenant(self, tenantname, password):
        pass

    def metadata(self):
        pass


    def get_changes(self, until=None):
        pass

    def record_changes(self, changes):
        pass

    def get_object(self, obj_id):
        pass

    def get_object_groups(self, object_id):
        pass

    def add_object_groups(self, obj_id, group_ids):
        pass

    def set_object_groups(self, obj_id, group_ids):
        pass

    def add_object_tags(self, obj_id, tag_ids):
        pass

    def set_object_tags(self, obj_id, tag_ids):
        pass

    def tag_object(self, object_id, tag_id):
        pass

    def get_object_tags(self, object_id):
        pass

    def get_object_roles(self, object_id):
        """
        Returns list of role_ids for all relationships the object identified has.
        """
        pass

    def tag_neighbors(self, object_id):
        """
        Return map tag_id => [object id] of objects that are tagged with each tag
        that object_id is tagged with.
        """
        pass

    def group_neighbors(self, object_id):
        """
        Return map group_id => [object id] for all groups the object is directly in to other objects 
        directly in each group.  
        """
        pass

    def role_neighbors(self, object_id):
        """
        Returns map role_id => [object id] for all objects the given object is related to
        """
        pass

    def related_to_object(self, object_id, role_id):
        """
        Returns [object id] for all objects related to the given object by the specified role
        """
        pass

    def add_related_objects(self, object_id, role_id, object_ids):
        """
        Relate if not previously so related object_ids to the give object by the given role
        """
        pass

    def set_related_objects(self, object_id, role_id, object_ids):
        pass

    def get_tagged(self, tag_id):
        pass
    
    def add_tagged(self, tag_id, object_ids):
        pass

    def set_tagged(self, tag_id, object_ids):
        pass

    def get_grouped(self, group_id):
        pass

    def add_grouped(self, group_id, object_ids):
        pass

    def set_grouped(self, group_id, object_ids):
        pass

    def get_tags(self):
        pass

    def create_tag(self, tag_data):
        pass

    def modify_tag(self, tag_id, mods):
        pass

    def delete_tag(self, tag_id):
        pass

    def get_roles(self):
        pass

    def create_role(self, data):
        pass

    def modify_role(self, role_id, mods):
        pass

    def delete_role(self, role_id):
        pass

    def get_classes(self):
        pass

    def create_class(self, data):
        pass

    def modify_class(self, class_id, mods):
        pass

    def delete_class(self, class_id):
        pass

    def get_queries(self):
        pass

    def create_query(self, data):
        pass

    def modify_query(self, query_id, mods):
        pass

    def delete_query(self, query_id):
        pass

    def get_groups(self):
        pass

    def create_group(self, group_data):
        pass

    def modify_group(self, group_id, mods):
        pass

    def delete_group(self, group_id):
        pass

    def get_attributes(self):
        pass

    def create_attribute(self, data):
        pass

    def modify_attribute(self, attr_id, mods):
        pass

    def delete_attribute(self, attr_id):
        pass

    def run_query(self, query_id=None, query=None):
        pass

    def bulk_load(self, ids, items_only=True):
        pass

    def id_to_name(self, kind):
        return {}

    def name_to_id(self, kind):
        return {}

    def name_map(self, kind):
        return {}

    def id_map(self, kind):
        return {}