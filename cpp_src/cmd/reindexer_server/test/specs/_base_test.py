import unittest
import os
import shutil
import importlib


from specs.utils import get_swagger, get_users
from specs.mixins import ApiMixin, ValidateMixin, HelperMixin, RoleMixin

imported = {}


class BaseTest(unittest.TestCase, ApiMixin, ValidateMixin, HelperMixin, RoleMixin):
    schema = {
        'required': ['test_1', 'test_2', 'test_3', 'test_4', 'test_5'],
        'properties': {
            'test_1': {
                'type': 'integer'
            },
            'test_2': {
                'type': 'integer'
            },
            'test_3': {
                'type': 'integer'
            },
            'test_4': {
                'type': 'integer'
            },
            'test_5': {
                'type': 'integer'
            }
        },
        'additionalProperties': False,
        'type': 'object'
    }

    isProtobufAvailable = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.SWAGGER = get_swagger()
        self.USERS = get_users()

    def prepare_protobuf_schemas(self):
        """Should be able to get Protobuf parameters schemas"""

        status, body = self.api_put_namespace_schema(
            self.current_db, self.test_ns, self.schema)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespace_query_params_schema(
            self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        protoFolder = "proto/"
        if not os.path.exists(protoFolder):
            os.mkdir(protoFolder)

        pb2Suffix = '_pb2.py'
        moduleName = 'ns_send_params'
        protoExtension = '.proto'

        fileName = protoFolder + moduleName + protoExtension
        self.create_proto_schema(fileName, body['message'])
        generatedFile = protoFolder + moduleName + pb2Suffix
        self.isProtobufAvailable = os.path.exists(generatedFile)
        if self.isProtobufAvailable is True:
            shutil.copyfile(generatedFile, moduleName + pb2Suffix)
        return self.isProtobufAvailable

    def create_proto_schema(self, fileName, data):
        f = open(fileName, "w")
        f.write(str(data))
        f.close()
        os.system("protoc -I=. --python_out=. " + fileName)

    def instantiate_pb_object(self, objName, moduleName='ns_send_params_pb2'):
        if moduleName not in imported:
            imported[moduleName] = importlib.import_module(moduleName)
        else:
            importlib.reload(imported[moduleName])
        obj = getattr(imported[moduleName], objName)
        return obj()

    def dump_queryresults_object(self, queryresults):
        print("\n")
        print("items.size = %d" % len(queryresults.items))
        for item in queryresults.items:
            print(item)
        print("\n")

        print("cache_enabled = %d" % queryresults.cache_enabled)
        print("explain = %s" % queryresults.explain)
        print("total_items = %d" % queryresults.total_items)
        print("query_total_items = %d" % queryresults.query_total_items)
        print("columns.size = %d" % len(queryresults.columns))
        for column in queryresults.columns:
            print(column)
        print("\n")

        print("namespaces.size = %d" % len(queryresults.namespaces))
        for ns in queryresults.namespaces:
            print(ns)
        print("\n")

        print("aggregations.size = %d" % len(queryresults.aggregations))
        for aggregation in queryresults.aggregations:
            print(aggregation)
        print("\n")

    @classmethod
    def setUpClass(cls):
        cls.role = cls.ROLE_DEFAULT

    def setUp(self):
        self.helper_update_timestamp()
        self.helper_update_testdata_entities()
