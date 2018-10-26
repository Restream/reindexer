import unittest

import shutil
import pyreindexer

# TODO Add more tests. Just checking that builtin binding is installed for sure


class TestBuiltin(unittest.TestCase):
    def setUp(self):
        self.db_path = '/tmp/pyreindexer_builtin_test_db'
        self.namespace = 'test_namespace'

        self.db = pyreindexer.RxConnector('builtin://' + self.db_path)

        try:
            self.db.namespace_open(self.namespace)
        except Exception as ex:
            self.fail('open_namespace raised: ' + str(ex))

    def tearDown(self):
        self.db.namespace_close(self.namespace)
        self.db.close()

        shutil.rmtree(self.db_path, ignore_errors=True)

    def test_index_create(self):
        index_definition = dict(name='id', json_path='id', field_type='int',
                                index_type='hash', is_pk=True, is_array=False,
                                is_dense=False, is_sparse=False, collate_mode='none',
                                sort_order_letters='', config={})
        try:
            self.db.index_add(self.namespace, index_definition)
        except Exception as ex:
            self.fail('index_create raised: ' + str(ex))

        index_definition['field_type'] = 'int64'

        with self.assertRaises(Exception):
            self.db.index_add(self.namespace, index_definition)
