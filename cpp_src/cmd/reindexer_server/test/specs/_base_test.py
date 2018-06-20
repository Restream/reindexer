import unittest

from specs.utils import get_swagger, get_users
from specs.mixins import ApiMixin, ValidateMixin, HelperMixin, RoleMixin


class BaseTest(unittest.TestCase, ApiMixin, ValidateMixin, HelperMixin, RoleMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.SWAGGER = get_swagger()
        self.USERS = get_users()

    @classmethod
    def setUpClass(cls):
        cls.role = cls.ROLE_DEFAULT

    def setUp(self):
        self.helper_update_timestamp()
        self.helper_update_testdata_entities()
