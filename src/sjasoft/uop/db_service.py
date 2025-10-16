import sjasoft.uop.services as s_services
import sjasoft.uop.async_path.services as a_services
from sjasoft.uop.utils import ca
from sjasoft.uopmeta.schemas import meta
from random import randint


class DatabaseClass:
    dbs = {"async": {}, "sync": {}}

    @classmethod
    def sync_type(cls, is_async):
        return "async" if is_async else "sync"

    @classmethod
    def get_db_class(cls, db_type, is_async):
        sync_key = cls.sync_type(is_async)
        db_map = cls.dbs.get(sync_key)
        db_cls = db_map.get(db_type)
        if not db_cls:
            raise Exception("no known %s db class of type %s" % (sync_key, db_type))
        return db_cls

    @classmethod
    def register_db(cls, the_class, db_type, is_async=False):
        cls.dbs[cls.sync_type(is_async)][db_type] = the_class

    def __init__(self, db_type="mongo", use_async=False):
        db_map = self.dbs[self.sync_type(use_async)]
        self._db_cls = self.get_db_class(db_type, use_async)

    def __call__(self, **db_args):
        return self._db_cls(**db_args)

    def test_database(self, **kwargs):
        return self._db_cls.make_test_database(**kwargs)


class UOPContext:
    def __init__(self, db_name, db_type="mongo", use_async=False, **kwargs):
        self._db_type = db_type
        self._db_name = db_name
        self._dbi = None
        self._kwargs = kwargs
        if use_async not in kwargs:
            kwargs["use_async"] = use_async
        self._service, self.db_class, self._dbi = None, None, None

    @property
    def metacontext(self):
        return self._dbi.metacontext

    def get_service_method(self, name):
        return getattr(self._service, name)

    def get_db_method(self, name):
        return getattr(self._dbi, name)

    def dataset(self, num_assocs=3, num_instances=10, persist_to=None):
        self.metacontext.complete()
        data = meta.WorkingContext.from_metadata(self.metacontext)
        data.configure(
            num_assocs=num_assocs, num_instances=num_instances, persist_to=persist_to
        )
        return data

    async def complete_context(self, tenant_id=None, schemas=None):
        schemas = schemas or []
        service = await self.complete_service(tenant_id=tenant_id)
        for schema in schemas:
            await ca(service.ensure_schema, schema)
        if schemas:
            self.interface.reload_metacontext()
        return service

    @classmethod
    def fresh_context(cls, db_type="mongo", **kwargs: object) -> object:
        name = f"testdb_{randint(10000, 99999)}"
        return cls(name, db_type, **kwargs)

    def __enter__(self, db_type="mongo"):
        return self.fresh_context(db_type)

    def __exit__(self, e_type, e_val, e_trace):
        self.db_class.drop_named_database(self._db_name)

    def ensure_schema(self, a_schema):
        return self._dbi.ensure_schema(a_schema)


async def get_uop_service(db_name, db_type="mongo", schemas=None, **kwargs):
    context = UOPContext(db_name, db_type=db_type, **kwargs)
    service = await context.complete_context(schemas=schemas)
    return service, context
