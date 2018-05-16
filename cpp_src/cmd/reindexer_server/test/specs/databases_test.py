from specs import BaseTest


class DatabasesTest(BaseTest):
    def test_get_databases(self):
        """Should be able to get databases list"""

        status, body = self.api_get_dbs()
        self.validate_get_list_response(status, body, 'Databases')

    def test_create_databases(self):
        """Should be able to create a new database"""

        status, _ = self.api_create_db(self.test_db)
        self.assertEqual(True, status == 200, status)

        status, body = self.api_get_dbs()
        self.validate_get_list_response(status, body, 'Databases', True)

    def test_delete_database(self):
        """Should be able to delete a database"""

        status, _ = self.api_create_db(self.test_db)
        self.assertEqual(True, status == 200)

        status, body = self.api_get_dbs()
        self.validate_get_list_response(status, body, 'Databases', True)

        status, _ = self.api_delete_db(self.test_db)
        self.assertEqual(True, status == 200)

        status, body = self.api_get_dbs()
        self.validate_get_list_response(status, body, 'Databases')

    def test_sort_order_of_database_empty(self):
        """Should be able to get non-sorted database list with empty sort_order param"""

        status, _ = self.api_create_db(self.test_db)
        self.assertEqual(True, status == 200)

        self._test_timestamp_update()
        self.update_db()

        status, _ = self.api_create_db(self.test_db)
        self.assertEqual(True, status == 200)

        status, body = self.api_get_sorted_dbs()
        self.validate_get_list_response(status, body, 'Databases', True)

        status_next, body_next = self.api_get_dbs()
        self.validate_get_list_response(status, body, 'Databases', True)

        self.assertEqual(True, body == body_next)

    def test_sort_order_of_database_asc(self):
        """Should be able to get asc-sorted database list"""

        status, _ = self.api_create_db(self.test_db)
        self.assertEqual(True, status == 200)

        self._test_timestamp_update()
        self.update_db()

        status, _ = self.api_create_db(self.test_db)
        self.assertEqual(True, status == 200)

        status, body = self.api_get_sorted_dbs('asc')
        self.validate_get_list_response(status, body, 'Databases', True)

        self.assertEqual(True, body['items'][-1] == self.test_db)

    def test_sort_order_of_database_desc(self):
        """Should be able to get desc-sorted database list"""

        status, _ = self.api_create_db(self.test_db)
        self.assertEqual(True, status == 200)

        self._test_timestamp_update()
        self.update_db()

        status, body = self.api_create_db(self.test_db)
        self.assertEqual(True, status == 200, body)

        status, body = self.api_get_sorted_dbs('desc')
        self.validate_get_list_response(status, body, 'Databases', True)

        self.assertEqual(True, body['items'][0] == self.test_db)

    def test_sort_order_of_database_wrong_param(self):
        """Shouldn't be able to get sorted database list with wrong sort_order param"""

        status, _ = self.api_create_db(self.test_db)
        self.assertEqual(True, status == 200)

        self._test_timestamp_update()
        self.update_db()

        status, _ = self.api_create_db(self.test_db)
        self.assertEqual(True, status == 200)

        status, body = self.api_get_sorted_dbs('wrong')
        self.assertEqual(True, status == 400)
