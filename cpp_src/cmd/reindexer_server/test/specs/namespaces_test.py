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
