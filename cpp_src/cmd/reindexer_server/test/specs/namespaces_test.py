from specs import BaseTest


class NamespacesTest(BaseTest):
    def setUp(self):
        super().setUp()

        self.helper_namespaces_testdata_prepare()

    def test_get_namespaces(self):
        """Should be able to get namespaces list"""

        status, body = self.api_get_namespaces(self.current_db)
        self.validate_get_list_response(status, body, 'Namespaces')

    def test_get_current_namespace(self):
        """Should be able to get current namespace"""

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespaces(self.current_db)
        self.validate_get_list_response(status, body, 'Namespaces', True)

        status, body = self.api_get_namespace(self.current_db, self.test_ns)
        self.validate_get_namespace_response(status, body)

    def test_create_namespace(self):
        """Should be able to create a new namespace"""

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespaces(self.current_db)
        self.validate_get_list_response(status, body, 'Namespaces', True)

        status, body = self.api_get_namespace(self.current_db, self.test_ns)
        self.validate_get_namespace_response(status, body)

    def test_delete_namespace(self):
        """Should be able to delete a namespace"""

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespaces(self.current_db)
        self.validate_get_list_response(status, body, 'Namespaces', True)

        status, body = self.api_get_namespace(self.current_db, self.test_ns)
        self.validate_get_namespace_response(status, body)

        status, body = self.api_delete_namespace(self.test_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['not_found'], body)

    def test_truncate_namespace(self):
        """Should be able to delete all items from namespace"""

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        index_count = 5
        items_count = 10

        index_array_of_dicts = self.helper_index_array_construct(index_count)
        for i in range(0, index_count):
            status, body = self.api_create_index(
                self.current_db, self.test_ns, index_array_of_dicts[i])
            self.assertEqual(True, status == self.API_STATUS['success'], body)

        items = self.helper_item_array_construct(items_count)
        for item_body in items:
            status, body = self.api_create_item(
                self.current_db, self.test_ns, item_body)
            self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_items(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, len(body['items']) == items_count, body)

        status, body = self.api_truncate_namespace(
            self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_items(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, len(body['items']) == 0, body)

        status, body = self.api_delete_namespace(self.test_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

    def test_rename_namespace(self):
        """Should be able to rename a namespace"""
        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        ren_ns = "rename_namespace"
        status, body = self.api_rename_namespace(
            self.current_db, self.test_ns, ren_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespace(self.current_db, ren_ns)
        self.validate_get_namespace_response(status, body)

    def test_create_namespace_with_indexes(self):
        """Should be able to create a new namespace with valid indexes"""

        count = 5
        indexes_arr_of_dicts = self.helper_index_array_construct(count)

        status, body = self.api_create_namespace(
            self.current_db, self.test_ns, indexes_arr_of_dicts)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespaces(self.current_db)
        self.validate_get_list_response(status, body, 'Namespaces', True)

        status, body = self.api_get_namespace(self.current_db, self.test_ns)
        self.validate_get_namespace_response(
            status, body, indexes_arr_of_dicts)

    def test_sort_order_of_namespaces_empty_param(self):
        """Should be able to get non-sorted namespace list with empty sort_order param"""

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        self.helper_update_timestamp()
        self.helper_update_testdata_ns()

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_sorted_namespaces(self.current_db)
        self.validate_get_list_response(status, body, 'Namespaces', True)

        status_next, body_next = self.api_get_namespaces(self.current_db)
        self.validate_get_list_response(status, body, 'Namespaces', True)

        self.assertEqual(True, body == body_next)

    def test_sort_order_of_namespaces_asc(self):
        """Should be able to get asc-sorted namespace list"""

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        self.helper_update_timestamp()
        self.helper_update_testdata_ns()

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_sorted_namespaces(self.current_db, 'asc')
        self.validate_get_list_response(status, body, 'Namespaces', True)

        self.assertEqual(True, body['items'][-1]['name'] == self.test_ns)

    def test_sort_order_of_namespaces_desc(self):
        """Should be able to get desc-sorted namespace list"""

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        self.helper_update_timestamp()
        self.helper_update_testdata_ns()

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_sorted_namespaces(self.current_db, 'desc')
        self.validate_get_list_response(status, body, 'Namespaces', True)

        self.assertEqual(True, body['items'][0]['name'] == self.test_ns)

    def test_sort_order_of_namespaces_wrong_param(self):
        """Shouldn't be able to get sorted namespace list with wrong sort_order param"""

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        self.helper_update_timestamp()
        self.helper_update_testdata_ns()

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_sorted_namespaces(self.current_db, 'wrong')
        self.assertEqual(True, status == self.API_STATUS['bad_request'], body)

    def test_get_and_put_namespaces_meta_info(self):
        """Should be able to get and put namespace meta info"""

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        meta = self.helper_meta_construct('key1', 'value1')
        status, body = self.api_put_namespace_meta(
            self.current_db, self.test_ns, meta)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        meta = self.helper_meta_construct('key2', 'value2')
        status, body = self.api_put_namespace_meta(
            self.current_db, self.test_ns, meta)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespace_meta(
            self.current_db, self.test_ns, 'key1')
        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, body == self.helper_meta_construct(
            'key1', 'value1'), body)

        status, body = self.api_get_namespace_meta(
            self.current_db, self.test_ns, 'key2')
        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, body == self.helper_meta_construct(
            'key2', 'value2'), body)

    def test_get_namespaces_meta_info_list(self):
        """Should be able to get namespace meta info list"""

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        for x in range(100):
            meta = self.helper_meta_construct('key' + str(x), 'value' + str(x))
            status, body = self.api_put_namespace_meta(
                self.current_db, self.test_ns, meta)
            self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespace_meta_list(
            self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, body['total_items'] == 100, body)
        self.assertEqual(True, len(body['meta']) == 100, body)

        status, body = self.api_get_namespace_meta_list(
            self.current_db, self.test_ns, sort=self.SORT_ORDER['asc'], with_values=True, offset=10, limit=30)
        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, body['total_items'] == 100, body)
        self.assertEqual(True, len(body['meta']) == 30, body)
        indexes = []
        for i in range(50):
            indexes.append(str(i))
        indexes.sort()
        i = 10
        for x in body['meta']:
            self.assertEqual(True, x['key'] == 'key' + indexes[i], x)
            self.assertEqual(True, x['value'] == 'value' + indexes[i], x)
            i += 1

    def test_put_and_get_namespaces_schema(self):
        """Should be able to put and get namespace schema"""

        schema = {
            'required': ['Countries', 'nested'],
            'properties': {
                'Countries': {
                    'items': {
                        'type': 'string'
                    },
                    'type': 'array'
                },
                'nested': {
                    'required': ['Name'],
                    'properties': {
                        'Name': {
                            'type': 'string'
                        },
                    },
                    'additionalProperties': False,
                    'type': 'object',
                    'x-go-type': 'Nested'
                }
            },
            'additionalProperties': False,
            'type': 'object',
            'x-protobuf-ns-number': 100
        }

        status, body = self.api_create_namespace(self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_put_namespace_schema(
            self.current_db, self.test_ns, schema)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespace_schema(
            self.current_db, self.test_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, body == schema, body)
