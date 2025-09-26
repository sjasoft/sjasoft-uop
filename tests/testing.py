__author__ = 'samantha'

from uop import db_service

import random
from uop import db_service


class TestContext(db_service.UOPContext):
    pass

def check_all_pass(test, a_list):
    assert all(test(item) for item in a_list)


def test_context():
    context = TestContext.fresh_context()
    assert context

