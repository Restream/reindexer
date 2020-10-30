import random
import msgpack
from specs import BaseTest


class QueriesTest(BaseTest):
    def setUp(self):
        super().setUp()

        self.helper_queries_testdata_prepare()

    def test_query_sql(self):
        """Should be able to execute an sql query"""

        sql_query = 'SELECT COUNT(*),* FROM ' + self.current_ns
        status, body = self.api_sql_exec(self.current_db, sql_query)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'items' in body, body)
        self.assertEqual(True, 'query_total_items' in body, body)

    def test_query_sql_msgpack(self):
        """Should be able to execute sql query and get response in msgpack format"""

        sql_query = 'explain SELECT COUNT(*),* FROM ' + self.current_ns
        status, body = self.api_sql_exec_with_columns(
            self.current_db, sql_query, self.EncodingType.MsgPack)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'message' in body, body)

        data = msgpack.loads(body.get('message'))
        self.assertEqual(True, 'namespaces' in data, data)
        self.assertEqual(True, 'items' in data, data)
        self.assertEqual(True, 'cache_enabled' in data, data)
        self.assertEqual(True, 'explain' in data, data)
        explainData = data['explain']
        self.assertEqual(True, 'total_us' in explainData, explainData)
        self.assertEqual(True, 'prepare_us' in explainData, explainData)
        self.assertEqual(True, 'indexes_us' in explainData, explainData)
        self.assertEqual(True, 'postprocess_us' in explainData, explainData)
        self.assertEqual(True, 'loop_us' in explainData, explainData)
        self.assertEqual(True, 'general_sort_us' in explainData, explainData)

        self.assertEqual(True, 'columns' in data, data)
        assert len(data['columns']) > 0
        for i in range(len(data['columns'])):
            columnData = data['columns'][i]
            self.assertEqual(True, 'max_chars' in columnData, columnData)
            self.assertEqual(True, 'width_chars' in columnData, columnData)
            self.assertEqual(True, 'width_percents' in columnData, columnData)
            self.assertEqual(True, 'name' in columnData, columnData)

    def test_aggregation_sql_query_msgpack(self):
        """Should be able to execute sql query with aggregate functions and get response in msgpack format"""

        sql_query = 'SELECT avg(test_1) FROM ' + self.current_ns
        status, body = self.api_sql_exec(
            self.current_db, sql_query, self.EncodingType.MsgPack)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'message' in body, body)

        data = msgpack.loads(body.get('message'))
        self.assertEqual(True, 'namespaces' in data, data)
        self.assertEqual(True, 'items' in data, data)
        self.assertEqual(True, 'cache_enabled' in data, data)
        self.assertEqual(True, 'aggregations' in data, data)

        aggregations = data.get('aggregations')
        self.assertEqual(True, len(aggregations) == 1, aggregations)
        aggregation = aggregations[0]
        self.assertEqual(True, 'value' in aggregation, aggregation)
        self.assertEqual(True, 'type' in aggregation, aggregation)
        self.assertEqual(True, aggregation['type'] == 'avg', aggregation)
        self.assertEqual(True, 'fields' in aggregation, aggregation)
        self.assertEqual(True, len(aggregation['fields']) == 1, aggregation)
        self.assertEqual(
            True, aggregation['fields'][0] == 'test_1', aggregation)

    def test_query_sql_protobuf(self):
        """Should be able to execute sql query and get response in protobuf format"""

        status, body = self.api_put_namespace_schema(
            self.current_db, self.test_ns, super().schema)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        if super().prepare_protobuf_schemas() is False:
            print("Protobuf is not installed")
            return

        sql_query = 'explain SELECT COUNT(*),* FROM ' + self.test_ns
        status, body = self.api_sql_exec_with_columns(
            self.current_db, sql_query, self.EncodingType.Protobuf)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'message' in body, body)

        queryresults = super().instantiate_pb_object('QueryResults')
        queryresults.ParseFromString(body.get('message'))

        self.assertEqual(True, len(queryresults.namespaces)
                         == 1, queryresults.namespaces)
        self.assertEqual(
            True, str(queryresults.namespaces[0]).startswith('test_ns_'), queryresults.namespaces)
        self.assertEqual(True, str(queryresults.explain).startswith('{"total_us":'),
                         queryresults.explain)
        self.assertEqual(True, queryresults.cache_enabled == 1,
                         queryresults.cache_enabled)
        self.assertEqual(True, len(queryresults.items)
                         == 10, queryresults.items)

        i = 0
        for it in queryresults.items:
            item = getattr(it, it.WhichOneof('item'))
            constVal = 10 * i
            self.assertEqual(True, item.test_1 == constVal + 1, item.test_1)
            self.assertEqual(True, item.test_2 == constVal + 2, item.test_2)
            self.assertEqual(True, item.test_3 == constVal + 3, item.test_3)
            self.assertEqual(True, item.test_4 == constVal + 4, item.test_4)
            self.assertEqual(True, item.test_5 == constVal + 5, item.test_5)
            i = i + 1

        self.assertEqual(True, len(queryresults.columns)
                         == 5, queryresults.columns)
        for column in queryresults.columns:
            self.assertEqual(True, column.max_chars == 6 and column.width_chars == 6 and str(
                column.name).startswith('test_') and column.width_percents != 0, column)

    def test_aggregation_sql_query_protobuf(self):
        """Should be able to execute sql query with aggregate functions and get response in protobuf format"""

        if super().prepare_protobuf_schemas() is False:
            print("Protobuf is not installed")
            return

        sql_query = 'SELECT avg(test_1) FROM ' + self.current_ns
        status, body = self.api_sql_exec(
            self.current_db, sql_query, self.EncodingType.Protobuf)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'message' in body, body)

        queryresults = super().instantiate_pb_object('QueryResults')
        queryresults.ParseFromString(body.get('message'))

        aggregations = queryresults.aggregations
        self.assertEqual(True, len(aggregations) == 1, aggregations)
        aggregation = aggregations[0]
        self.assertEqual(True, aggregation.type == 'avg', aggregation)
        self.assertEqual(True, aggregation.value == 46.0, aggregation)
        self.assertEqual(True, len(aggregation.fields) == 1, aggregation)
        self.assertEqual(True, aggregation.fields[0] == 'test_1', aggregation)

    def test_query_sql_post(self):
        """Should be able to post an sql query"""

        query_body = 'explain SELECT COUNT(*),* FROM ' + self.current_ns
        status, body = self.api_sql_post(
            self.current_db, query_body, self.EncodingType.PlainText)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'items' in body, body)
        self.assertEqual(True, 'query_total_items' in body, body)
        explainData = body['explain']
        self.assertEqual(True, 'total_us' in explainData, explainData)
        self.assertEqual(True, 'prepare_us' in explainData, explainData)
        self.assertEqual(True, 'indexes_us' in explainData, explainData)
        self.assertEqual(True, 'postprocess_us' in explainData, explainData)
        self.assertEqual(True, 'loop_us' in explainData, explainData)
        self.assertEqual(True, 'general_sort_us' in explainData, explainData)

    def test_query_sql_with_columns(self):
        """Should be able to get column parameters with sql query results"""

        sql_query = 'SELECT COUNT(*),* FROM ' + self.current_ns
        status, body = self.api_sql_exec_with_columns(
            self.current_db, sql_query)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'columns' in body, body)
        assert len(body['columns']) > 0

        widths = [None] * 10
        for item in body['items']:
            columnIdx = 0
            for key in item:
                value = item[key]
                widths[columnIdx] = max(len(key), len(str(value)))
                columnIdx += 1

        suppositiveScreenWidth = 100

        for i in range(len(body['columns'])):
            column_data = body['columns'][i]
            self.assertEqual(
                True, 'width_percents' in column_data, column_data)
            assert column_data['max_chars'] == widths[i]
            assert column_data['width_percents'] == (
                float(widths[i])/suppositiveScreenWidth)*100

    def test_query_dsl_(self):
        """Should be able to exec a dsl query"""

        query_dsl = self.helper_query_dsl_construct(self.current_ns)

        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'items' in body, body)

    def test_query_dsl_sort_asc(self):
        """Should be able to exec a dsl query and get asc-sorted item list"""

        sort_field = self.helper_items_first_key_of_item(self.items)
        sort_desc = False

        sort = self.helper_query_dsl_sort_construct(sort_field, sort_desc)
        query_dsl = self.helper_query_dsl_construct(self.current_ns, sort=sort)

        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'items' in body, body)
        self.assertEqual(True, body['items'][0][sort_field]
                         < body['items'][-1][sort_field], body)

    def test_query_dsl_sort_desc(self):
        """Should be able to exec a dsl query and get desc-sorted item list"""

        sort_field = self.helper_items_first_key_of_item(self.items)
        sort_desc = True

        sort = self.helper_query_dsl_sort_construct(sort_field, sort_desc)
        query_dsl = self.helper_query_dsl_construct(self.current_ns, sort=sort)

        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'items' in body, body)
        self.assertEqual(True, body['items'][0][sort_field]
                         > body['items'][-1][sort_field], body)

    def test_query_dsl_distinct(self):
        """Should be able to exec a dsl query and get distinct item list"""

        status, body = self.api_get_items(self.current_db, self.current_ns)
        self.assertEqual(True, status == self.API_STATUS['success'], body)
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
            self.assertEqual(True, status == self.API_STATUS['success'], body)

        distinct = self.helper_items_second_key_of_item(items)
        limit = total_items + items_count
        aggregations = self.helper_query_dsl_aggregation_construct(
            field=distinct, aggr_type='distinct')
        query_dsl = self.helper_query_dsl_construct(
            self.current_ns, aggregations=[aggregations], limit=limit, req_total="enabled")
        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'items' in body, body)
        self.assertEqual(True, 'query_total_items' in body, body)
        self.assertEqual(
            True, body['query_total_items'] == total_items + 1, body)

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

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'items' in body, body)
        self.assertEqual(True, self.items[-1] in body['items'], body)
        self.assertEqual(True, len(body['items']) == 1, body)

    def test_query_dsl_total(self):
        """Should be able to exec a dsl query and get total_items"""

        query_dsl = self.helper_query_dsl_construct(
            self.current_ns, req_total='enabled')

        status, body = self.api_query_dsl(self.current_db, query_dsl)

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'items' in body, body)
        self.assertEqual(True, 'query_total_items' in body, body)
        self.assertEqual(
            True, body['query_total_items'] == self.items_count, body)

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

        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'items' in body, body)
        self.assertEqual(True, self.items[0] in body['items'], body)
