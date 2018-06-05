import unittest
import os
import yaml
import time
from jsonref import JsonRef
from jsonschema import validate

from specs.mixins import ApiMixin, ValidateMixin, HelperMixin


class BaseTest(unittest.TestCase, ApiMixin, ValidateMixin, HelperMixin):
    SCHEMA_PATH = os.path.dirname(__file__) + '/../../contrib/server.yml'

    @classmethod
    def setUpClass(cls):
        descriptor = open(cls.SCHEMA_PATH)
        schema = yaml.load(descriptor)
        cls.swagger = JsonRef.replace_refs(schema)
        descriptor.close()

        cls.api_base = cls.swagger['basePath']

        cls.class_timestamp = round(time.time() * 1000)

    def setUp(self):
        self._test_timestamp_update()

        self.update_db()
        self.update_ns()
        self.update_item()
        self.update_idx()

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def schema_validate_response(self, body, schemaName):
        schema = self._schema_get_schema(schemaName)
        validate(body, schema)

    # TODO rename test entities to uppercase when backend is ready

    def update_db(self):
        self.test_db = 'test_db_' + str(self.test_timestamp)

    def update_ns(self):
        self.test_ns = 'test_ns_' + str(self.test_timestamp)

    def update_item(self):
        self.test_item = 'test_item_' + str(self.test_timestamp)

    def update_idx(self):
        self.test_index = 'test_index' + str(self.test_timestamp)

    """private"""

    def _test_timestamp_update(self):
        time.sleep(0.001)
        self.test_timestamp = round(time.time() * 1000)

    def _api_call(self, method, url, body=None, headers={'Content-type': 'application/json'}, as_json=True):
        return self._api_request(method, self.api_base + url, body, headers, as_json)

    def _schema_get_schema(self, schemaName):
        return self.swagger['definitions'][schemaName]
