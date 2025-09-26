import uuid, requests
from sjautils.tools import plain2cipher


class UOPWebClient(object):
    def __init__(self, host='localhost', port='8080'):
        self._client_id = uuid.uuid4().hex
        self._session = requests.Session()
        # self._session.headers.update({'pkm-client': self._client_id, 'content_type': 'application/json'})
        self._url_head = 'http://%s:%s/' % (host, port)

    def _make_url(self, *parts):
        return self._url_head + '/'.join(list(parts))

    def get(self, *path, **params):
        res = self._session.get(self._make_url(*path))
        return res

    def get_json(self, *path, **params):
        res = self.get(*path, **params)
        assert res.ok
        return res.json()

    def post(self, *path, data):
        res = self._session.post(self._make_url(*path), json=data)
        return res

    def post_json(self, *path, data):
        res = self.post(*path, data=data)
        assert res.ok
        return res.json()

    def put(self, *path, **data):
        res = self._session.put(self._make_url(*path), json=data)
        return res

    def delete(self, *path):
        res = self._session.delete(self._make_url(*path))
        return res

    # def _encrypt(self, original):
    #   return plain2cipher(self._client_id, original)

    def _encrypt(self, original):
        return original

    def register_user(self, username, password, email):
        user_data = dict(
            username=username,
            password=self._encrypt(password),
            email=email)
        return self.post('register', data=user_data)

    def login_user(self, username, password):
        return self.post('login',
                         data=dict(username=username, password=self._encrypt(password)))

    def classInstances(self, clsName):
        return self.post('run-query', data={'$and': {'$type': clsName}})
