from typing_extensions import ParamSpecArgs
from sjasoft.uop.connect import generic
import uuid, requests
import time


class WebConnection(generic.GenericConnection):
    @classmethod
    def as_tenant(cls, tenant_name, passwd, host='localhost', port='8080'):
        instance = cls(host=host, port=port)
        instance.login_tenant(tenant_name, passwd)
        return instance

    @classmethod
    def as_new_user(cls, username, password, host='localhost', port=8080):
        pass

    def __init__(self, url=None, host='localhost', port='8080'):
        super().__init__()
        self._session = requests.Session()
        # self._session.headers.update({'pkm-client': self._client_id, 'content_type': 'application/json'})
        if url:
            self._url_head = '%s/' % url if (not url.endswith('/')) else url
        elif host and port:
            self._url_head = 'http://%s:%s/' % (host, port)
        else:
            raise Exception('either an url or host and port must be specified')
    
    def _make_url(self, *parts):
        return self._url_head + '/'.join(list(parts))

    def get(self, *path, **params):
        res = self._session.get(self._make_url(*path))
        return res.json()

    def post(self, *path, data):
        res = self._session.post(self._make_url(*path), json=data)
        return res.json()

    def put(self, *path, data):
        res = self._session.put(self._make_url(*path), json=data)
        return res.json()

    def delete(self, *path):
        res = self._session.delete(self._make_url(*path))

    def _encrypt(self, original):
        return original

    @property
    def tenant(self):
        return getattr(self, '_tenant', None)

    @property
    def is_admin(self):
        return getattr(self, '_is_admin', getattr(self, '_user', None) and self._user['isAdmin'])

    @property
    def logged_in(self):
        raw = self.get('login')
        if raw.get('logged_in'):
            self._user = raw['user']
            self._is_admin = raw['isAdmin']
            return True
        else:
            return False

    def add_application(self, app_data):
        return self.post('applications', data=app_data)

    def login_tenant(self, username, password):
        self._user = self.post('login',
                               data=dict(username=username, password=self._encrypt(password)))
        self._is_admin = self._user['isAdmin']
        return self._user

    def register_tenant(self, username, password, email):
        self._user = self.post('register',
                               data=dict(username=username, password=self._encrypt(password), email=email))
        self._is_admin = self._user['isAdmin']
        return self._user

    def metadata(self):
        return self.get('metadata')

    def class_instances(self, cls_name):
        return self.post('run-query', data={'$and': {'$type': cls_name}})

    def get_changes(self, until=None):
        if not until:
            until = time.time()
        return self.get('changes', str(until))

    def record_changes(self, changes):
        return self.post('changes', data=changes)

    def get_object(self, obj_id):
        return self.get('objects', obj_id)

    def get_object_groups(self, object_id):
        return self.get('object-groups', object_id)

    def add_object_groups(self, obj_id, group_ids):
        return self.put('object-groups', data=group_ids)

    def set_object_groups(self, obj_id, group_ids):
        return self.post('object-groups', data=group_ids)
        
    def add_object_tags(self, obj_id, tag_ids):
        return self.put('object-tags', data=tag_ids)

    def set_object_tags(self, obj_id, tag_ids):
        return self.post('object-tags', data=tag_ids)

    def tag_object(self, object_id, tag_id):
        return self.post('object_tags', object_id, tag_id)

    def get_object_tags(self, object_id):
        return self.get('object-tags', object_id)

    def get_object_roles(self, object_id):
        return self.get('object-roles', object_id)

    def tag_neighbors(self, object_id):
        return self.get('tag-neighbors', object_id)

    def group_neighbors(self, object_id):
        return self.get('group-neighbors', object_id)

    def role_neighbors(self, object_id):
        return self.get('role-neighbors', object_id)

    def related_to_object(self, object_id, role_id):
        return self.get('related-objects', object_id, role_id)

    def add_related_objects(self, object_id, role_id, object_ids):
        return self.put('related-objects', object_id, role_id, data=object_ids)

    def set_related_objects(self, object_id, role_id, object_ids):
        return self.post('related-objects', object_id, role_id, data=object_ids)wvbm 

    def get_tagged(self, tag_id):
        return self.get('tagged', tag_id)

    def add_tagged(self, tag_id, object_ids):
        return self.put('tagged', tag_id, data=object_ids)

    def set_tagged(self, tag_id, object_ids):
        return self.post('tagged', tag_id, data=object_ids)

    def get_grouped(self, group_id):
        return self.get('grouped', group_id)

    def add_grouped(self, group_id, object_ids):
        return self.put('grouped', group_id, data=object_ids)

    def set_grouped(self, group_id, object_ids):
        return self.post('tagged', group_id, data=object_ids)

    def get_tags(self):
        return self.get('tags')

    def create_tag(self, tag_data):
        return self.post('tags', data=tag_data)

    def modify_tag(self, tag_id, mods):
        return self.put('tags', tag_id, data=mods)

    def delete_tag(self, tag_id):
        return self.delete('tags', tag_id)

    def get_roles(self):
        return self.get('tags')

    def create_role(self, data):
        return self.post('roles', data=data)

    def modify_role(self, role_id, mods):
        return self.put('roles', role_id, data=mods)

    def delete_role(self, role_id):
        return self.delete('roles', role_id)

    def get_classes(self):
        return self.get('classes')

    def create_class(self, data):
        return self.post('classes', data=data)

    def modify_class(self, class_id, mods):
        return self.put('classes', class_id, data=mods)

    def delete_class(self, class_id):
        return self.delete('classes', class_id)

    def get_queries(self):
        return self.get('queries')

    def create_query(self, data):
        return self.post('queries', data=data)

    def modify_query(self, query_id, mods):
        return self.put('queries', query_id, data=mods)

    def delete_query(self, query_id):
        return self.delete('queries', query_id)

    def get_groups(self):
        return self.get('groups')

    def create_group(self, tag_data):
        return self.post('groups', data=tag_data)

    def modify_group(self, group_id, mods):
        return self.put('groups', group_id, data=mods)

    def delete_group(self, group_id):
        return self.delete('groups', group_id)

    def get_attributes(self):
        return self.get('attributes')

    def create_attribute(self, data):
        return self.post('attributes', data=data)

    def modify_attribute(self, attr_id, mods):
        return self.put('attributes', attr_id, data=mods)

    def delete_attribute(self, attr_id):
        return self.delete('tags', attr_id)

    def run_query(self, query_id=None, query=None):
        args = ['run_query']
        if query_id:
            args.append(query_id)
            self.post(*args, data={})
        elif query:
            args.append(query)
            return self.post(*args, data=query)
        else:
            raise Exception('Either query_id or query must be specified')

    def bulk_load(self, ids):
        return self.post('bulk-load', data={'ids': ids})

