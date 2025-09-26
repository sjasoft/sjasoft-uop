__author__ = 'samantha'


class SubclassResponsibility(Exception):
    def __init__(self, methodname):
        super(SubclassResponsibility, self).__init__('The method %s must be implemented by a subclass' % methodname)


def subclass_implement(methodname):
    raise SubclassResponsibility(methodname)
