from specs import BaseTest


class IndexesTest(BaseTest):
    def setUp(self):
        super().setUp()

        self.helper_indexes_testdata_prepare()

    def test_get_indexes(self):
        """Should be able to get indexes list"""

        status, body = self.api_get_indexes(self.current_db, self.current_ns)
        self.validate_get_list_response(status, body, 'Indexes')

    def test_create_indexes(self):
        """Should be able to create indexes"""

        count = 5
        indexes_arr_of_dicts = self.helper_index_array_construct(count)

        for i in range(0, count):
            status, body = self.api_create_index(
                self.current_db, self.test_ns, indexes_arr_of_dicts[i])
            self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespace(self.current_db, self.test_ns)
        self.validate_get_namespace_response(
            status, body, indexes_arr_of_dicts)

    def test_delete_index(self):
        """Should be able to delete an index"""

        count = 5
        indexes_arr_of_dicts = self.helper_index_array_construct(count)

        for i in range(0, count):
            status, body = self.api_create_index(
                self.current_db, self.test_ns, indexes_arr_of_dicts[i])
            self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespace(self.current_db, self.test_ns)
        self.validate_get_namespace_response(
            status, body, indexes_arr_of_dicts)

        first_index = indexes_arr_of_dicts[0]
        first_index_name = first_index['name']
        status, body = self.api_delete_index(
            self.current_db, self.test_ns, first_index_name)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespace(self.current_db, self.test_ns)
        self.validate_get_namespace_response(
            status, body, indexes_arr_of_dicts[1:])

    def test_update_index(self):
        """Should be able to update an index"""

        index = self.helper_index_construct(
            name='id', field_type='int', index_type='hash', is_pk=True)

        status, body = self.api_create_index(
            self.current_db, self.test_ns, index)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        index_dict = []
        index_dict.append(index)
        status, body = self.api_get_namespace(self.current_db, self.test_ns)
        self.validate_get_namespace_response(
            status, body, index_dict)

        updated_index = self.helper_index_construct(
            name='id', field_type='string', index_type='hash', is_pk=True)
        updated_index_dict = []
        updated_index_dict.append(updated_index)

        status, body = self.api_update_index(
            self.current_db, self.test_ns, updated_index)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        status, body = self.api_get_namespace(self.current_db, self.test_ns)
        self.validate_get_namespace_response(status, body, updated_index_dict)
