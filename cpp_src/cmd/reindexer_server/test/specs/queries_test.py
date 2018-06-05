import random
from specs import BaseTest


class QueriesTest(BaseTest):
    def setUp(self):
        super().setUp()

        self.current_db = self.test_db
        status, body = self.api_create_db(self.current_db)

        self.assertEqual(True, status == 200, body)

        self.current_ns = self.test_ns
        status, body = self.api_create_namespace(
            self.current_db, self.current_ns)

        self.assertEqual(True, status == 200, body)

        index_count = 2
        self.indexes = self.helper_index_array_construct(index_count)

        for i in range(0, index_count):
            status, body = self.api_create_index(
                self.current_db, self.current_ns, self.indexes[i])

            self.assertEqual(True, status == 200, body)

        self.items_count = 10
        self.items = self.helper_item_array_construct(self.items_count)

        for item_body in self.items:
            status, body = self.api_create_item(
                self.current_db, self.current_ns, item_body)
            self.assertEqual(True, status == 200, body)

    def test_query_sql(self):
        """Should be able to execute an sql query"""

        sql_query = 'SELECT * FROM ' + self.current_ns
        status, body = self.api_sql_exec(self.current_db, sql_query)

        self.assertEqual(True, status == 200, body)

    def test_query_sql_post(self):
        """Should be able to post an sql query"""

        query_body = 'SELECT * FROM ' + self.current_ns
        status, body = self.api_sql_post(self.current_db, query_body)

        self.assertEqual(True, status == 200, body)

    def test_query_dsl_(self):
        """Should be able to exec a dsl query"""

        query_dsl = self.helper_query_dsl_construct(self.current_ns)

        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == 200, body)

    def test_query_dsl_sort_asc(self):
        """Should be able to exec a dsl query and get asc-sorted item list"""

        sort_field = self.helper_items_first_key_of_item(self.items)
        sort_desc = False

        sort = self.helper_query_dsl_sort_construct(sort_field, sort_desc)
        query_dsl = self.helper_query_dsl_construct(self.current_ns, sort=sort)

        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == 200, body)
        self.assertEqual(True, body['items'][0][sort_field]
                         < body['items'][-1][sort_field], body)

    def test_query_dsl_sort_desc(self):
        """Should be able to exec a dsl query and get desc-sorted item list"""

        sort_field = self.helper_items_first_key_of_item(self.items)
        sort_desc = True

        sort = self.helper_query_dsl_sort_construct(sort_field, sort_desc)
        query_dsl = self.helper_query_dsl_construct(self.current_ns, sort=sort)

        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == 200, body)
        self.assertEqual(True, body['items'][0][sort_field]
                         > body['items'][-1][sort_field], body)

    def test_query_dsl_distinct(self):
        """Should be able to exec a dsl query and get distinct item list"""

        status, body = self.api_get_items(self.current_db, self.current_ns)
        self.assertEqual(True, status == 200, body)
        total_items = body['total_items']

        items = []
        items_count = 10
        distinct_field_value_random = random.randint(0x1FFFFFFF, 0x7FFFFFFF)

        items = self.helper_item_array_construct(items_count)
        pk_field_name = self.helper_items_first_key_of_item(items)
        test_field_name = self.helper_items_second_key_of_item(items)
        for i in range(0, items_count):
            items[i][pk_field_name] = i + 1000
            items[i][test_field_name] = distinct_field_value_random

        for item_body in items:
            status, body = self.api_create_item(
                self.current_db, self.current_ns, item_body)
            self.assertEqual(True, status == 200, body)

        distinct = self.helper_items_second_key_of_item(items)
        limit = total_items + items_count
        query_dsl = self.helper_query_dsl_construct(
            self.current_ns, distinct=distinct, limit=limit)
        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == 200, body)
        self.assertEqual(True, len(body['items']) == total_items + 1, body)

    def test_query_dsl_paginate(self):
        """Should be able to exec a dsl query and pagination works correct"""

        items = []
        items_count = 10

        items = self.helper_item_array_construct(items_count)
        pk_field_name = self.helper_items_first_key_of_item(items)
        for i in range(0, items_count):
            items[i][pk_field_name] = i + 1000

        limit = 1
        offset = self.items_count - 1
        query_dsl = self.helper_query_dsl_construct(
            self.current_ns, limit=limit, offset=offset)

        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == 200, body)
        self.assertEqual(True, self.items[-1] in body['items'], body)
        self.assertEqual(True, len(body['items']) == 1, body)

    def test_query_dsl_total(self):
        """Should be able to exec a dsl query and get total_items"""

        query_dsl = self.helper_query_dsl_construct(
            self.current_ns, req_total='enabled')

        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == 200, body)
        self.assertEqual(True, 'total_items' in body, body)
        self.assertEqual(True, body['total_items'] == self.items_count, body)

    def test_query_dsl_filter_eq(self):
        """Should be able to exec a dsl query with EQ filter"""

        test_field_name = self.helper_items_second_key_of_item(self.items)
        test_value = 2

        filter = self.helper_query_dsl_filter_construct(
            test_field_name, 'EQ', 'AND', test_value)

        filters = []
        filters.append(filter)

        query_dsl = self.helper_query_dsl_construct(
            self.current_ns, filters=filters)

        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == 200, body)
        self.assertEqual(True, self.items[0] in body['items'], body)
        self.assertEqual(True, len(body['items']) == 1, body)
