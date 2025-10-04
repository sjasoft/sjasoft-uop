from functools import partial


class ConstraintViolation(Exception):
    def __init__(self, constraint, data=None, criteria=None, mods=None):
        msg_fmt = "%s violated collection constraint %s"
        offending_change = dict(
            data=data, criteria=criteria, mods=mods
        )
        msg = msg_fmt % (offending_change, constraint)
        super(ConstraintViolation, self).__init__(msg)


class CollectionConstraint(object):
    """
    A constraint on a persistent collection of instances.
    e.g. uniqueness constraints on fields across instances.
    This is the abstract superclass.
    """

    def __init__(self, collection, relevant_to=None, admin_ok=False):
        """
        :param collection the colection the constraint is for
        :param relevant_to what operations on the collection the constraint needs to be ensured over
        :admin_ok whether an admin tenant can bypass the constraint such as a readonly or immutable by normal tenants collection
        """
        self._collection = collection
        self._relevant_to = relevant_to or []
        self._admin_ok = admin_ok

    @property
    def relevant_to(self):
        return self._relevant_to

    def __call__(self, data=None, criteria=None, mods=None):
        """
        Checks the constraint and raises a ConstraintViolation if it is
        violated.  Either data is a full instance suitable for the collection
        or criteria and mods specify criteria for selecting instances to have
        mods applied to.
        :param data optional full instance of the kind this collection expects
        :param criteria optional dict of criteria or an _id appropriate to the collection
        :param mods optional modification dict
        """
        pass


class UniqueField(CollectionConstraint):
    """
    SPecification for the circumstances under which a field should unique across instances containing that field. 
    """

    def __init__(self, name, collection, relevant_to=('insert', 'modify')):
        """
        :panam name - name of the field
        :param collection - the collection context for the uniqueness
        :param relevant_to what operations on the collection uniqueness needs to be ensured over
        """
        self._name = name
        super(UniqueField, self).__init__(collection)

    def __repr__(self):
        return 'unique_field(%s)' % self._name

    def __call__(self, data=None, criteria=None, mods=None):
        """
        Apply this uniqueness constraint.
        :param data info to be inserted in dict form
        :criteria used to limit the set of instances to consider. When
        None consider all instances
        :mods present or modifiaction case
        """

        def raise_exception():
            raise ConstraintViolation(self, data, criteria, mods)

        if criteria and mods:  # modification case
            if self._name in mods:
                matching = None
                if isinstance(criteria, dict):
                    matching_criteria = self._collection.ids_only(criteria)
                    if len(matching_criteria) > 1:
                        raise_exception()
                    elif len(matching_criteria) == 1:
                        matching = matching_criteria[0]
                else:
                    matching = criteria

                if matching:
                    name = self._name
                    matching_name = self._collection.ids_only({name: mods[name]})
                    if (len(matching_name) > 1) or \
                        (matching_name and matching != matching_name[0]):
                        raise_exception()

        elif data:  # insert case
            if self._collection.exists({'name': data['name']}):
                raise_exception()


unique_field = lambda name: partial(UniqueField, name)

