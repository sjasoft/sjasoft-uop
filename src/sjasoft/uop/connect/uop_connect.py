from sjasoft.uop.db_service import get_uop_service, DatabaseClass, UOPContext
from sjasoft.uop.connect import generic
from sjasoft.uopmeta import oid
from sjasoft.uopmeta.schemas import meta
from functools import reduce
from collections import defaultdict
import asyncio


def register_adaptor(db_class, db_type, is_async=False):
    DatabaseClass.register_db(db_class, db_type, is_async=is_async)


class ConnectionWrapper:

    def __init__(self, connect=None):
        self._connect = connect
        self._metacontext = None
        self.reset_context()

    def set_connection(self, connect: generic.GenericConnection):
        self._connect = connect
        self.reset_context()

    def all_names(self, kind):
        return list(self.name_to_id(kind).keys())

    def abort(self):
        self._connect.abort()
        self.reset_context()

    def attr_name_map(self, disambiguated=True):
        attrs = self.id_map('attributes').values()
        cid_map = self.id_map('classes')
        attr_classes = defaultdict(set)
        for cls in cid_map.values():
            name = cls.name
            for aid in cls.attrs:
                attr_classes[aid].add(name)
        by_name = {}
        for attr in attrs:
            name = attr.name
            type = attr.type
            prev = by_name.get(name)
            if prev:
                if type != prev.type:
                    if disambiguated:
                        classes = attr_classes[attr.id]
                        extra = classes[0] if classes else 'Unknown'
                        key = f'{name}({extra})'
                        by_name[key] = attr
            else:
                by_name[name] = attr

        return by_name

    def begin_transaction(self):
        self._connect.begin_transaction()


    def class_named(self, name):
        return self._metacontext.classes.by_name.get(name)

    def __getattr__(self, name):
        return getattr(self._connect, name, None)

    def create_instance(self, cls, **data):
        return self.create_instance_of(cls.name, use_defaults=True, **data)

    def commit(self):
        self._connect.commit()
        self.reset_context()

    def get_dataset(self, num_assocs=3, num_instances=10, persist_to=None):
        # assume metacontext is complete
        data = meta.WorkingContext.from_metadata(self._metacontext)
        data.configure(num_assocs=num_assocs, num_instances=num_instances, persist_to=persist_to)
        return data

    def dataset(self, num_assocs=3, num_instances=10, persist=None):
        persist_to = None
        if persist:
            self.dbi.begin_transaction()
            persist_to = self.dbi
        data = self.get_dataset(num_assocs=num_assocs, num_instances=num_instances, persist_to=persist_to)
        if persist:
            self.dbi.commit()
        return data

    def get_db_method(self, name):
        return getattr(self, name)

    def get_named_role(self, name):
        role = self.name_map('roles').get(name)
        if not role:
            for role in self.roles():
                if role.reverse_name == name:
                    return role
        return role

    def get_role_named(self, name):
        self._metacontext.get_meta_named('roles', name)

    def meta_map(self):
        data = self._metacontext.__dict__
        kinds = {k:v for k,v in data.items() if isinstance(v, meta.ByNameId)}
        return   {k: v.by_id for k,v in kinds.items()}

    def metacontext(self):
        return self._metacontext

    def non_abstract_classes(self):
        raw = self.by_name('classes')
        return {k:v for k,v in raw.items() if not v.is_abstract}

    def object_attributes(self, obj_id):
        _cls = self.object_class(obj_id)
        if _cls:
            return _cls.attributes
        raise Exception(f'No class for object {obj_id}')

    def object_class(self, obj_id):
        cid = oid.oid_class(obj_id)
        return self.id_map('classes').get(cid)

    def object_display_info(self, obj_id):
        cid = oid.oid_class(obj_id)
        cname = self.id_to_name('classes')(cid)
        short = self.dbi.oid_short_form(obj_id)
        return dict(
            short_form=short, class_name=cname
        )

    def reverse_relation(self, rel_assoc):
        oid, name, other = rel_assoc
        role = self.get_named_role(name)
        if not role:
            raise Exception(f'no role found for {rel_assoc}')
        if name == role.name:
            return other, role.reverse_name, oid
        else:
            return other, role.name, oid

    def reverse_role_names(self):
        return [r.reverse_name for r in self.roles()]

    def reset_context(self):
        if self._connect:
            self._metacontext = self._connect.metacontext()

    def roles(self):
        return self.name_map('roles').values()

    def rolesets(self, obj, rids):
        getter = self.roleset_getter(obj)
        return {r: getter(r) for r in rids}

    def subgroups(self, gid):
        return self._metacontext.subgroups(gid)

    def untag(self, oid, tag_id):
        self._connect.untag(oid, tag_id)

    def ungroup(self, oid, group_id):
        self._connect.ungroup(oid, group_id)

    def unrelate(self, subject, role_id, object_id):
        self._connect.unrelate(subject, role_id, object_id)

    def url_to_object(self, url):
        return self.dbi.object_for_url(url, True)


    def name_map(self, kind):
        return self._metacontext.by_name(kind)

    def id_map(self, kind):
        return self._metacontext.by_id(kind)

    def id_to_name(self, kind):
        return self._metacontext.id_to_name(kind)

    def names_from_ids(self, kind, *ids):
        return self._metacontext.names_to_ids(kind)(ids)


    def name_to_id(self, kind):
        return self._metacontext.name_to_id(kind)

    def neighbor_text_form(self, kind, neighbor_dict):
        """
        Internal neighbor form has id nodes(key) and list of object ids leaves.
        The corresponding text form has corresponding meta object name nodes and object short form leaves.
        """
        name_map = self.id_to_name(kind)
        unique_objects = reduce(lambda a,b: a & set(b), neighbor_dict.values(), set())
        short_map = {}
        for oid in unique_objects:
            short_map[oid] = self._connect.object_short_form(self.get_object(oid))

        def short_objs(oids):
            return [short_map[o] for o in oids]

        return {name_map[k]: short_objs(v) for k,v in neighbor_dict.items()}

class LocalDB:
    """
    Gives a fully set up direct database context using uop.db_service underneeath
    """

    @classmethod
    def db(cls, db_type='mongo', dbname='pkm_db', schemas=None, **kwargs):
        schemas = schemas or []
        instance  = cls(db_type, dbname, schemas=schemas, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(instance.setup())
        return instance

    def __init__(self, db_type='mongo', dbname='pkm_app', tenant_id=None, schemas=None, **kwargs):
        self._service = None
        self._context:UOPContext = None
        self._db_type = db_type
        self._dbname = dbname
        self._args = kwargs
        self._schemas = schemas or []

    async def setup(self):
        if self._context is None:
            self._service, self._context = await get_uop_service(
                self._dbname, self._db_type, schemas=self._schemas, **self._args)
        return self._context

    def dbi(self):
        return self._context.interface

    def dataset(self, *args, **kwargs):
        return self._context.dataset(*args, **kwargs)



class LocalPKM(LocalDB):
    Singleton = None
    ConnectArgs = {}

    def __init__(self, db_type='mongo', dbname='pkm_app', **kwargs):
        super().__init__(db_type, dbname, **kwargs)

    @property
    def is_setup(self):
        return self._context is not None

    @property
    def dbi(self):
        if self.is_setup:
            return self._context.interface
        raise Exception('LocalPKM has not been set up!')

    @property
    def metadata(self):
        if self.is_setup:
            return self._context.metadata



