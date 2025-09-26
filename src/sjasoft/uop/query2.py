"""
Straightforward query handler from internal dict represpenstation
"""

q_gt = lambda property, value: {'$gt': {property: value}}
q_gte = lambda property, value: {'$gte': {property: value}}
q_lt = lambda property, value: {'$lt': {property: value}}
q_lte = lambda property, value: {'$lte': {property: value}}
q_eq = lambda property, value: {'$eq': {property: value}}
q_neq = lambda property, value: {'$neq': {property: value}}
q_class = lambda cls: {'$type': cls}
q_groups = lambda *items: {'$groups': items}
q_tags = lambda *items: {'$tags': items}
q_and = lambda *clauses: {'$and': list(clauses)}
q_or = lambda *clauses: {'$or': list(clauses)}
has_any = lambda *clauses: {'$or': list(clauses)}
has_all = lambda *clauses: {'$and': list(clauses)}
