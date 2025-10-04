from sjasoft.utils.category import binary_partition, identity_function as identity
from collections import defaultdict
from functools import reduce, partial
from sjasoft.uopmeta.schemas import meta
from sjasoft.uopmeta import oid
from sjasoft.uop import utils
from sjasoft.utils.cw_logging import getLogger

logger = getLogger(__file__)


def propVal(op, prop, val):
    return {op: {prop: val}}


class Q:
    '''
    Simple query builder trying to app part of what Django has.  It is primarily used 
    for building class instance queries without relationships.
    '''
    @staticmethod
    def query_function(query):
        """
        Returns a function that can be used to evaluate a query against a dictionary.
        @param query: the query to evaluate
        @return: a function that can be used to evaluate a query against a dictionary
        """
        def _and(functions: list[callable]):                
            return lambda x: all(fn(x) for fn in functions)
    
        def _or(functions: list[callable]):                
            return lambda x: any(fn(x) for fn in functions)

        def operator_function(key, value):
            
            ops = '$gt', '$lt', '$gte', '$lte', '$eq', '$neq', '$regex'
            if key != ops:
                temp = {key: value}
                key = '$eq'
                value = temp
            prop, val = first_kv(value)
            ops_map = {
                '$gt': lambda x: x[prop] > val,
                '$lt': lambda x: x[prop] < val,
                '$gte': lambda x: x[prop] >= val,
                '$lte': lambda x: x[prop] <= val,
                '$eq': lambda x: x[prop] == val,
                '$neq': lambda x: x[prop] != val,
                '$regex': lambda x: re.search(val, x[prop]),
            }
            return ops_map[key]
            
            
        key, value = first_kv(query)
        functions = [query_function(i) for i in value] if key in ('$and', '$or') else [operator_function(key, value)]
        if key == '$and':   
            return _and(functions)
        if key == '$or':
            return _or(functions)
        else:
            return functions[0]

    @staticmethod
    def gt(prop, val):
        return propVal('$gt', prop, val)

    @staticmethod
    def gte(prop, val):
        return propVal('$gte', prop, val)

    @staticmethod
    def lt(prop, val):
        return propVal('$lt', prop, val)

    @staticmethod
    def lte(prop, val):
        return propVal('$lte', prop, val)

    @staticmethod
    def eq(prop, val):
        return propVal('$eq', prop, val)

    @staticmethod
    def neq(prop, val):
        return propVal('$neq', prop, val)

    @staticmethod
    def of_type(clsName):
        return {'$type': clsName}

    @staticmethod
    def tagged(spec):
        return {'$tagged': spec}

    @staticmethod
    def grouped(spec):
        return {'$grouped': spec}

    @staticmethod
    def related(object, role=None):
        """
        Returns all objects related to the given object.  If role is
        specified then only the objects related to the object by role
        are returned.  Else all objects related by whatever role are
        returned
        @param object - the object to be related to
        @param role - optional specific role
        @return - list of related object uuids
        """
        return {'$related': (object, role)}

    @staticmethod
    def all(*clauses):
        return {'$and': clauses}

    @staticmethod
    def any(*clauses):
        return {'$or': clauses}


def split_clause(clause):
    keys = list(clause.keys())
    if len(keys) > 1:  # accept simple or compound single query
        raise Exception("bad query format: %s" % clause)
    key = keys[0]
    criteria = clause[key]
    return key, criteria


async def evaluate_classes(dbi, classes, to_filter=None):
    if to_filter:
        return {i for i in to_filter if oid.oid_class(i) in classes}
    else:
        async def cls_find(cid):

            coll = await dbi.extension(cid)
            return await coll.ids_only()

        return await utils.a_set_or(cls_find, classes)


property_operations = ('$gt', '$lt', '$gte', '$lte', '$neq', '$eq')


class NegatableSet(set):
    def __init__(self, items=None, negated=False):
        super().__init__(items or [])
        self._negated = negated

    def __and__(self, other_set):
        self_negated = self._negated
        other_negated = isinstance(other_set, NegatableSet) and other_set._negated
        not_both = self_negated ^ other_negated
        if not_both:
            if other_negated:
                return self - other_set
            else:
                return other_set - self
        else:
            raw = super().__and__(other_set)
            return self.__class__(raw, self_negated)

    def __or__(self, other_set):
        self_negated = self._negated
        other_negated = isinstance(other_set, NegatableSet) and other_set._negated
        not_both = self_negated ^ other_negated
        if not_both:
            # TODO think on whether there is a better way
            logger.warn('ignoring to expensive not clause in OR concext')
            return other_set if self._negated else self
        else:
            raw = super().__or__(other_set)
            return self.__class__(raw, self._negated)

    def filter(self, items, key=identity):
        keyed = {key(i): i for i in items}
        if self._negated:
            valid = set(keyed.keys()) - self
        else:
            valid = set(keyed.keys()) & self
        return {keyed[k] for k in valid}


class ComponentEvaluator:
    @classmethod
    def evaluator(cls, component, in_context, object_ids=None):
        return cls(component, in_context, object_ids)

    def __init__(self, component, in_context, object_ids=None, class_context=None):
        self._object_ids = object_ids
        self._class_filter = NegatableSet(class_context)
        self._in_context = in_context
        self._component = component
        self._class_context = class_context
        self.sub_eval = partial(self.evaluator, in_context=in_context)

    def object_filter(self):
        return NegatableSet(self._object_ids)

    @property
    def dbi(self):
        return self._in_context.dbi

    @property
    def metacontext(self):
        return self._in_context.metacontext

    def get_named(self, kind, names):
        by_name = getattr(self.metacontext, kind).by_name
        return {by_name[n].id for n in names}

    async def get_association(self, component: meta.AssociatedComponent):
        assoc_ids = set()
        assoc_objs = set()

        async def obj_assocs(obj):
            return set()

        if isinstance(component, meta.TagsComponent):
            assoc_ids = self.get_named('tags', component.names)
            associated = self.dbi.get_tagset
            obj_assocs = self.dbi.get_object_tags
        elif isinstance(component, meta.GroupsComponent):
            assoc_ids = self.get_named('groups', component.names)
            if component.include_subgroups:
                pass
            # TODO need group tree for subgroups in meta
            kinds = 'groups'
            associated = self.dbi.get_groupset
            obj_assocs = self.dbi.get_object_groups

        if self._object_ids:
            assoc_map = {oid: set(await obj_assocs(oid)) for oid in self._object_ids}
            if component.application == 'all':
                test = lambda s: (s & assoc_ids) == assoc_ids
                return {k for k, v in assoc_map.items() if test(v)}
            elif component.application == 'any':
                return {k for k, v in assoc_map.items() if v and assoc_ids}
            elif component.application == 'none':
                return {k for k, v in assoc_map.items() if not (v and assoc_ids)}
            return set()
        else:
            pass

    async def evaluate_tags(self, component: meta.TagsComponent):
        eval_tag = lambda tag: self.dbi.get_tagset(tag)
        tag_ids = [self.metacontext.tags.by_name[t][id] for t in component.names]
        raw = set()
        if component.application in ('any', 'none'):
            raw = await utils.a_set_or(eval_tag, tag_ids)
        elif component.application == 'all':
            raw = await utils.a_set_and(eval_tag, tag_ids)
        if component.application == 'none':
            return NegatableSet(raw, True)
        else:
            return raw

    async def evaluate_groups(self, component: meta.GroupsComponent):
        eval_tag = lambda tag: self.dbi.get_groupset(tag)
        group_ids = {self.metacontext.groups.by_name[t][id] for t in component.names}
        raw = set()
        if component.application in ('any', 'none'):
            group_ids = utils.set_or(self.metacontext.subgroups, group_ids)
            raw = await utils.a_set_or(eval_tag, group_ids)
        elif component.application == 'all':
            group_ids = utils.set_and(self.metacontext.subgroups, group_ids)
            raw = await utils.a_set_and(eval_tag, group_ids)
        if component.application == 'none':
            return NegatableSet(raw, True)
        else:
            return raw

    async def evaluate_related(self, component: meta.RelatedTo):
        fun = self.dbi.get_related_objects
        if component.role:
            rid = self.metacontext.roles.by_name[component.role].id
            fun = partial(self.dbi.get_roleset, role_id=rid)
        oids = await fun(component.obj_id)
        if component.negated:
            return NegatableSet(items=oids, negated=True)
        else:
            return oids

    async def evaluate_or(self, component: meta.OrQuery):
        evaluator = partial(self.sub_eval, class_context=self._class_context)
        fun = lambda child: evaluator(child)()
        return utils.a_set_or(fun, component.components)

    def _combine_classes(self, class_specs: meta.List[meta.ClassComponent], is_and):
        """
        Goal is to derive set of positive and negative class_ids. If the criteria
        are not conflicting the two sets are returned, positive classes to match first. If they are
        conflicting None, None is returned.
        NOTES:
        If is_and
        - If more than one positive spec is present there are 2 cases
          - there is a super-sub-class relationship such that once class specified includes the rest and include
          subclasses is on. Simplifies to the most general class specified
          - one or more classes in disparate class subtrees which is a conflict.
        - multiple negative specs are effectively ORed together
        Else
        - Take union of all positive class specs clid_id set

        :param class_specs: class components to combine
        :prama is_and: whether we are in and clause. False is for OR clause
        :return: set of classes allowed, set of classes not allowed
        """
        by_name = self._in_context.classes.by_name
        positive = set()
        negative = set()
        all_subs = self.metacontext.subclasses(by_name['PersistentObject'].id)
        pos_specs, neg_specs = binary_partition(class_specs, lambda cs: cs.positive)

        def clsids(spec):
            """
            All class_ids for spec.
            DECIDE: negative spec returs all other classes or not
            """
            cid = by_name[spec.cls_name].id
            res = {cid}
            if spec.include_subclasses:
                res.update(self.metacontext.subclasses(cid))
            return res

        res = set()
        if is_and:

            pos = [clsids(s) for s in pos_specs]
            res = clsids(pos_specs[0])
            pos = pos[1:]
            for spec in pos_specs[1:]:
                res &= clsids(spec)
                if not res:
                    return res
            if res and neg_specs:
                for np in neg_specs:
                    res -= clsids(np)
                    if not res:
                        return res
        else:
            res = reduce(lambda a, b: a | clsids(b), pos_specs, res)
            res = reduce(lambda a, b: a | all_subs - clsids(b), neg_specs, res)
            if res == all_subs:
                return None

        return res

    async def evaluate_and(self, component: meta.AndQuery):
        class_specs, non_class = binary_partition(component.components,
                                                  lambda x: isinstance(x, meta.ClassComponent))
        class_context = None
        if class_specs:
            class_context = self._combine_classes(class_specs)
            if class_context is not None:
                if not class_context:
                    return set()

        evaluator = partial(self.sub_eval, class_context=class_context)
        first = non_class[0]
        rest = non_class[1:]
        obj_ids = await evaluator(first)()
        if obj_ids:
            for child in rest:
                ids = await evaluator(child, object_ids=obj_ids)()
                obj_ids &= ids
                if not obj_ids:
                    return set()
        return obj_ids

    async def evaluate_attribute(self, component: meta.AttributeComponent):
        """
        An attribute criteria can only be evaluated on one or more classes.  There are
        three possible sources of classes:
        1) they are given by class specs in a compound clause the attribute criteria is within
        2) we have pevious satisfying object ids and classes may be drawn from those or the
        criteria directly evaluated as a filter
        3) we can scan metaclass info to see what classes have this attribute and use those.

        Another possibility arises when we support adding attribute values dynamically to objects.
        In this case we need keep some index of attribute names to object_ids that support a dynamic
        attirbute with that name. We can then pull those instances and filter by the attribute criteria.

        :param component:
        :return:
        """
        cls_by_id = self._in_context.classes.by_id

        def check_class(clsid):
            cls = cls_by_id(clsid)
            if not cls.is_abstract:
                return any(a.name == component.attr_name for a in cls.attributes)

        expr = {component.operate: {component.attr_name: component.value}}
        find_objects = lambda coll: coll.ids_only(expr)
        if self._object_ids:
            by_id = defaultdict(list)
            to_check = []
            for o in self._object_ids:
                by_id[oid.oid_class(o)].append(o)
            for cid, oids in by_id.items():
                to_check.extend(oids)
            fun = component.obj_eval()
            return utils.a_set_or(fun, to_check)
        else:
            classes = []
            test = lambda cls: check_class(cls.id) and not cls.is_abstract
            if self._class_context:
                cids = [check_class(cid) for cid in self._class_context]
            else:
                cids = [check_class(cid) for cid in self.metacontext.classes.by_id]
            colls = [self._in_context.dbi.extension(cid) for cid in cids]
            fun = lambda coll: coll.find(expr)
            return await utils.a_set_or(fun, colls)

    async def __call__(self):
        component = self._component
        component.simplify()
        if isinstance(component, meta.TagsComponent):
            return await self.evaluate_tags(component)
        elif isinstance(component, meta.GroupsComponent):
            return await self.evaluate_groups(component)
        elif isinstance(component, meta.RelatedTo):
            return await self.evaluate_related(component)
        elif isinstance(component, meta.AndQuery):
            return await self.evaluate_and(component)
        elif isinstance(component, meta.OrQuery):
            return await self.evaluate_or(component)
        elif isinstance(component, meta.AttributeComponent):
            return await self.evaluate_attribute(component)


class QueryEvaluator2:
    def __init__(self, query: meta.MetaQuery, dbi, metacontext: meta.MetaContext = None):
        self._object_ids = set()
        self._metacontext = metacontext or dbi.metacontext
        self._component = query.query
        self._dbi = dbi

    @property
    def metacontext(self):
        return self._metacontext

    @property
    def dbi(self):
        return self._dbi

    def __call__(self):
        evaluator = ComponentEvaluator(self._component, in_context=self)
        return evaluator()
