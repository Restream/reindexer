import random
from specs import BaseTest


class AuthTest(BaseTest):
    def test_auth_api_check_status(self):
        """Should be able to have access to check api method for all roles except unauthorized"""

        self.role = 'none'
        status, _ = self.api_check()
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_check()
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

    def test_auth_api_db_get_list(self):
        """Should be able to get databases list for all roles except unauthorized"""

        self.role = 'none'
        status, _ = self.api_get_dbs()
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_get_dbs()
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

    def test_auth_api_db_create(self):
        """Should be able to create database for owner role only"""

        self.role = 'none'
        status, _ = self.api_create_db(self.test_db)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))
        self.helper_update_timestamp()
        self.helper_update_testdata_db()

        self.role = 'owner'
        status, _ = self.api_create_db(self.test_db)
        self.assertEqual(
            True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))
        self.helper_update_timestamp()
        self.helper_update_testdata_db()

        roles = ['data_read', 'data_write', 'db_admin']
        for role in roles:
            self.role = role
            status, _ = self.api_create_db(self.test_db)
            self.assertEqual(
                True, status == self.API_STATUS['forbidden'], self.helper_msg_role_status(status))
            self.helper_update_timestamp()
            self.helper_update_testdata_db()

    def test_auth_api_db_delete(self):
        """Should be able to delete database for owner role only"""

        self.role = 'none'
        self.helper_auth_create_test_db_as_owner()
        status, _ = self.api_delete_db(self.test_db)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        self.helper_update_timestamp()
        self.helper_update_testdata_db()

        self.role = 'owner'
        self.helper_auth_create_test_db_as_owner()
        status, _ = self.api_delete_db(self.test_db)
        self.assertEqual(
            True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

        self.helper_update_timestamp()
        self.helper_update_testdata_db()

        roles = ['data_read', 'data_write', 'db_admin']
        for role in roles:
            self.role = role
            self.helper_auth_create_test_db_as_owner()
            status, _ = self.api_delete_db(self.test_db)
            self.assertEqual(
                True, status == self.API_STATUS['forbidden'], self.helper_msg_role_status(status))

            self.helper_update_timestamp()
            self.helper_update_testdata_db()

    def test_auth_api_namespace_get_list(self):
        """Should be able to get namespaces list for all roles except unauthorized"""

        self.helper_auth_create_test_db_as_owner()

        self.role = 'none'
        status, _ = self.api_get_namespaces(self.test_db)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_get_namespaces(self.test_db)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

    def test_auth_api_namespace_get_current(self):
        """Should be able to get current namespace for all roles except unauthorized"""

        self.helper_auth_create_test_db_as_owner()
        self.helper_auth_create_test_namespace_as_owner()

        self.role = 'none'
        status, _ = self.api_get_namespace(self.test_db, self.test_ns)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_get_namespace(self.test_db, self.test_ns)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

    def test_auth_api_namespace_create(self):
        """Should be able to create a namespace for admin and owner roles only"""

        self.helper_auth_create_test_db_as_owner()

        self.role = 'none'
        status, _ = self.api_create_namespace(self.test_db, self.test_ns)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        self.helper_update_timestamp()
        self.helper_update_testdata_ns()

        roles = ['data_read', 'data_write']
        for role in roles:
            self.role = role
            status, _ = self.api_create_namespace(
                self.test_db, self.test_ns)
            self.assertEqual(
                True, status == self.API_STATUS['forbidden'], self.helper_msg_role_status(status))

            self.helper_update_timestamp()
            self.helper_update_testdata_ns()

        roles = ['db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_create_namespace(self.test_db, self.test_ns)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

            self.helper_update_timestamp()
            self.helper_update_testdata_ns()

    def test_auth_api_namespace_delete(self):
        """Should be able to delete a namespace for admin and owner roles only"""

        self.helper_auth_create_test_db_as_owner()
        self.helper_auth_create_test_namespace_as_owner()

        self.role = 'none'
        status, _ = self.api_delete_namespace(self.test_db, self.test_ns)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write']
        for role in roles:
            self.role = role
            status, _ = self.api_delete_namespace(
                self.test_db, self.test_ns)
            self.assertEqual(
                True, status == self.API_STATUS['forbidden'], self.helper_msg_role_status(status))

        roles = ['db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_delete_namespace(self.test_db, self.test_ns)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

            self.helper_update_timestamp()
            self.helper_update_testdata_ns()
            self.helper_auth_create_test_namespace_as_owner()

    def test_auth_api_item_get_list(self):
        """Should be able to get items list for all roles except unauthorized"""

        self.helper_auth_create_test_db_as_owner()
        self.helper_auth_create_test_namespace_as_owner()

        self.role = 'none'
        status, _ = self.api_get_items(self.test_db, self.test_ns)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_get_items(self.test_db, self.test_ns)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

    def test_auth_api_item_create(self):
        """Should be able to create an item for all roles except data_reader role and unauthorized"""

        self.helper_auth_create_test_db_as_owner()
        index_array_of_dicts = self.helper_index_array_construct()
        self.helper_auth_create_test_namespace_as_owner(index_array_of_dicts)
        item_body = self.helper_item_construct()

        self.role = 'none'
        status, _ = self.api_create_item(
            self.test_db, self.test_ns, item_body)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        self.role = 'data_read'
        status, _ = self.api_create_item(
            self.test_db, self.test_ns, item_body)
        self.assertEqual(
            True, status == self.API_STATUS['forbidden'], self.helper_msg_role_status(status))

        roles = ['data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_create_item(
                self.test_db, self.test_ns, item_body)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))
            self.helper_update_timestamp()
            self.helper_update_testdata_ns()
            self.helper_auth_create_test_namespace_as_owner(
                index_array_of_dicts)

    def test_auth_api_item_update(self):
        """Should be able to update an item for all roles except data_reader role and unauthorized"""

        self.helper_auth_create_test_db_as_owner()
        index_array_of_dicts = self.helper_index_array_construct()
        self.helper_auth_create_test_namespace_as_owner(index_array_of_dicts)
        _, _, written_item_body = self.helper_auth_create_test_item_as_owner()
        updated_key = sorted(written_item_body.keys())[-1]
        new_item_body = written_item_body
        new_item_body[updated_key] = random.random()

        self.role = 'none'
        status, _ = self.api_update_item(
            self.test_db, self.test_ns, new_item_body)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        self.role = 'data_read'
        status, _ = self.api_update_item(
            self.test_db, self.test_ns, new_item_body)
        self.assertEqual(
            True, status == self.API_STATUS['forbidden'], self.helper_msg_role_status(status))

        roles = ['db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_update_item(
                self.test_db, self.test_ns, new_item_body)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

            self.helper_update_timestamp()
            self.helper_update_testdata_ns()
            self.helper_auth_create_test_namespace_as_owner(
                index_array_of_dicts)
            _, _, written_item_body = self.helper_auth_create_test_item_as_owner()
            updated_key = sorted(written_item_body.keys())[-1]
            new_item_body = written_item_body
            new_item_body[updated_key] = random.random()

    def test_auth_api_item_delete(self):
        """Should be able to delete an item for all roles except data_reader role and unauthorized"""

        self.helper_auth_create_test_db_as_owner()
        index_array_of_dicts = self.helper_index_array_construct()
        self.helper_auth_create_test_namespace_as_owner(index_array_of_dicts)
        _, _, written_item_body = self.helper_auth_create_test_item_as_owner()

        self.role = 'none'
        status, _ = self.api_delete_item(
            self.test_db, self.test_ns, written_item_body)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        self.role = 'data_read'
        status, _ = self.api_delete_item(
            self.test_db, self.test_ns, written_item_body)
        self.assertEqual(
            True, status == self.API_STATUS['forbidden'], self.helper_msg_role_status(status))

        roles = ['db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_delete_item(
                self.test_db, self.test_ns, written_item_body)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

            self.helper_update_timestamp()
            self.helper_update_testdata_ns()
            self.helper_auth_create_test_db_as_owner()
            index_array_of_dicts = self.helper_index_array_construct()
            self.helper_auth_create_test_namespace_as_owner(
                index_array_of_dicts)
            _, _, written_item_body = self.helper_auth_create_test_item_as_owner()

    def test_auth_api_index_get_list(self):
        """Should be able to get indexes list for all roles except unauthorized"""

        self.helper_auth_create_test_db_as_owner()
        self.helper_auth_create_test_namespace_as_owner()

        self.role = 'none'
        status, _ = self.api_get_indexes(self.test_db, self.test_ns)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_get_indexes(self.test_db, self.test_ns)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

    def test_auth_api_index_create(self):
        """Should be able to create an index for admin and owner roles only"""

        self.helper_auth_create_test_db_as_owner()
        self.helper_auth_create_test_namespace_as_owner()

        index_name = 'MyIndex'
        index = self.helper_index_construct(index_name)

        self.role = 'none'
        status, _ = self.api_create_index(self.test_db, self.test_ns, index)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write']
        for role in roles:
            self.role = role
            status, _ = self.api_create_index(
                self.test_db, self.test_ns, index)
            self.assertEqual(
                True, status == self.API_STATUS['forbidden'], self.helper_msg_role_status(status))

        roles = ['db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_create_index(
                self.test_db, self.test_ns, index)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

            self.helper_update_timestamp()
            self.helper_update_testdata_ns()
            self.helper_auth_create_test_namespace_as_owner()

    def test_auth_api_index_delete(self):
        """Should be able to delete an index for admin and owner roles only"""

        self.helper_auth_create_test_db_as_owner()
        self.helper_auth_create_test_namespace_as_owner()
        _, _, index_name = self.helper_auth_create_test_index_as_owner()

        self.role = 'none'
        status, _ = self.api_delete_index(
            self.test_db, self.test_ns, index_name)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write']
        for role in roles:
            self.role = role
            status, _ = self.api_delete_index(
                self.test_db, self.test_ns, index_name)
            self.assertEqual(
                True, status == self.API_STATUS['forbidden'], self.helper_msg_role_status(status))

        roles = ['db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_delete_index(
                self.test_db, self.test_ns, index_name)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

            self.helper_update_timestamp()
            self.helper_update_testdata_ns()
            self.helper_auth_create_test_namespace_as_owner()
            _, _, index_name = self.helper_auth_create_test_index_as_owner()

    def test_auth_api_sqlquery_get(self):
        """Should be able to execute get sqlquery for all roles except unauthorized"""

        self.helper_auth_create_test_db_as_owner()
        self.helper_auth_create_test_namespace_as_owner()

        self.role = 'none'
        status, _ = self.api_sql_exec(
            self.test_db, 'SELECT * FROM ' + self.test_ns)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_sql_exec(
                self.test_db, 'SELECT * FROM ' + self.test_ns)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

            self.helper_update_timestamp()
            self.helper_update_testdata_ns()
            self.helper_auth_create_test_namespace_as_owner()

    def test_auth_api_sqlquery_post(self):
        """Should be able to execute post sqlquery for all roles except unauthorized"""

        self.helper_auth_create_test_db_as_owner()
        self.helper_auth_create_test_namespace_as_owner()

        self.role = 'none'
        status, _ = self.api_sql_post(
            self.test_db, 'SELECT * FROM ' + self.test_ns)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_sql_post(
                self.test_db, 'SELECT * FROM ' + self.test_ns)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

            self.helper_update_timestamp()
            self.helper_update_testdata_ns()
            self.helper_auth_create_test_namespace_as_owner()

    def test_auth_api_query_dsl(self):
        """Should be able to execute a query via dsl for all roles except unauthorized"""

        self.helper_auth_create_test_db_as_owner()
        self.helper_auth_create_test_namespace_as_owner()
        query_dsl = self.helper_query_dsl_construct(self.test_ns)

        self.role = 'none'
        status, _ = self.api_query_dsl(self.test_db, query_dsl)
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.api_query_dsl(self.test_db, query_dsl)
            self.assertEqual(
                True, status == self.API_STATUS['success'], self.helper_msg_role_status(status))

            self.helper_update_timestamp()
            self.helper_update_testdata_ns()
            self.helper_auth_create_test_namespace_as_owner()
            query_dsl = self.helper_query_dsl_construct(self.test_ns)

    def test_auth_web_face(self):
        """Should be able to use web face service for all roles except unauthorized"""

        self.role = 'none'
        status, _ = self.web_face()
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.web_face()
            self.assertEqual(
                True, status != self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

    def test_auth_web_facestaging(self):
        """Should be able to use web facestaging service for all roles except unauthorized"""

        self.role = 'none'
        status, _ = self.web_facestaging()
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.web_facestaging()
            self.assertEqual(
                True, status != self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

    def test_auth_web_swagger(self):
        """Should be able to use web swagger service for all roles except unauthorized"""

        self.role = 'none'
        status, _ = self.web_swagger()
        self.assertEqual(
            True, status == self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))

        roles = ['data_read', 'data_write', 'db_admin', 'owner']
        for role in roles:
            self.role = role
            status, _ = self.web_swagger()
            self.assertEqual(
                True, status != self.API_STATUS['unauthorized'], self.helper_msg_role_status(status))
