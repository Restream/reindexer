import time


class HelperMixin(object):
    def helper_update_testdata_db(self):
        self.test_db = 'test_db_' + str(self.test_timestamp)

    def helper_update_testdata_ns(self):
        self.test_ns = 'test_ns_' + str(self.test_timestamp)

    def helper_update_testdata_item(self):
        self.test_item = 'test_item_' + str(self.test_timestamp)

    def helper_update_testdata_idx(self):
        self.test_index = 'test_index' + str(self.test_timestamp)

    def helper_update_testdata_entities(self):
        self.helper_update_testdata_db()
        self.helper_update_testdata_ns()
        self.helper_update_testdata_item()
        self.helper_update_testdata_idx()

    def helper_update_timestamp(self):
        time.sleep(0.001)
        self.test_timestamp = round(time.time() * 1000)

    def helper_namespaces_testdata_prepare(self):
        self.current_db = self.test_db
        status, body = self.api_create_db(self.current_db)

        self.assertEqual(True, status == self.API_STATUS['success'], body)

    def helper_items_testdata_prepare(self):
        self.current_db = self.test_db
        status, body = self.api_create_db(self.current_db)

        self.assertEqual(True, status == self.API_STATUS['success'], body)

        self.current_ns = self.test_ns
        status, body = self.api_create_namespace(
            self.current_db, self.current_ns)

        self.assertEqual(True, status == self.API_STATUS['success'], body)

    def helper_indexes_testdata_prepare(self):
        self.current_db = self.test_db
        status, body = self.api_create_db(self.current_db)

        self.assertEqual(True, status == self.API_STATUS['success'], body)

        self.current_ns = self.test_ns
        status, body = self.api_create_namespace(
            self.current_db, self.current_ns)

        self.assertEqual(True, status == self.API_STATUS['success'], body)

    def helper_queries_testdata_prepare(self):
        self.current_db = self.test_db
        status, body = self.api_create_db(self.current_db)

        self.assertEqual(True, status == self.API_STATUS['success'], body)

        self.current_ns = self.test_ns
        status, body = self.api_create_namespace(
            self.current_db, self.current_ns)

        self.assertEqual(True, status == self.API_STATUS['success'], body)

        index_count = 2
        self.indexes = self.helper_index_array_construct(index_count)

        for i in range(0, index_count):
            status, body = self.api_create_index(
                self.current_db, self.current_ns, self.indexes[i])

            self.assertEqual(True, status == self.API_STATUS['success'], body)

        self.items_count = 10
        self.items = self.helper_item_array_construct(self.items_count)

        for item_body in self.items:
            status, body = self.api_create_item(
                self.current_db, self.current_ns, item_body)
            self.assertEqual(True, status == self.API_STATUS['success'], body)

    def helper_index_construct(self, name, field_type='int', index_type='hash', is_pk=False):
        return {
            'name': name,
            'json_paths': [name],
            'field_type': field_type,
            'index_type': index_type,
            'is_pk': is_pk,
            'is_array': False,
            'is_dense': True,
            'is_sparse': False,
            'collate_mode': 'none',
            'sort_order_letters': '',
            'expire_after':0,
            'config': {}
        }

    def helper_meta_construct(self, key, value):
        return {
            'key': key,
            'value': value
        }

    def helper_meta_list_request_construct(self, sort = False, with_values = False, offset = 0, limit = 0):
        return {
            'sort': sort,
            'with_values': with_values,
            'offset': offset,
            'limit': limit
        }

    def helper_index_array_construct(self, count=2):
        indexes_arr_of_dicts = []

        for i in range(0, count):
            is_pk = False
            if i == 0:
                is_pk = True
            index_dict = self.helper_index_construct(
                'test_' + str(i + 1), is_pk=is_pk)
            indexes_arr_of_dicts.append(index_dict)

        return indexes_arr_of_dicts

    def helper_item_construct(self, fields_count=5, val_start=0, extra_array_count=0):
        item_dict = {}

        for i in range(0, fields_count):
            item_dict['test_' + str(i + 1)] = val_start + i + 1

        if extra_array_count != 0:
            test_arr = []
            for i in range(0, extra_array_count):
                test_arr.append(i)
            item_dict['test_arr'] = test_arr
        return item_dict

    def helper_item_array_construct(self, count=5):
        items_arr_of_dicts = []

        for i in range(0, count):
            item_dict = self.helper_item_construct(val_start=10 * i)
            items_arr_of_dicts.append(item_dict)

        return items_arr_of_dicts

    def helper_items_first_key_of_item(self, items):
        item_keys = items[0].keys()
        item_keys = sorted(item_keys)

        return item_keys[0]

    def helper_items_second_key_of_item(self, items):
        item_keys = items[1].keys()
        item_keys = sorted(item_keys)

        return item_keys[1]

    def helper_query_dsl_filter_construct(self, field, cond, op, value):
        return {
            'field': field,
            'cond': cond,
            'op': op,
            'value': value
        }

    def helper_query_dsl_sort_construct(self, field, desc=False, values=[]):
        return {
            'field': field,
            'values': values,
            'desc': desc
        }

    def helper_query_dsl_joined_on_construct(self, left_field, right_field, condition, op):
        return {
            'left_field': left_field,
            'right_field': right_field,
            'condition': condition,
            'op': op
        }

    def helper_query_dsl_aggregation_construct(self, field, aggr_type):
        return {
            'field': field,
            'type': aggr_type
        }

    def helper_query_dsl_joined_construct(self, namespace, join_type, op, limit=10, offset=0, filters=[], sort={}, on=[]):
        return {
            'namespace': namespace,
            'type': join_type,
            'op': op,
            'limit': limit,
            'offset': offset,
            'filters': filters,
            'sort': sort,
            'on': on
        }

    def helper_query_dsl_construct(self, namespace, limit=10, offset=0, distinct='',
                                   req_total='disabled', filters=[], sort={}, joined=[], merged=[],
                                   aggregations=[], select_filter=[], select_functions=[]):
        return {
            'namespace': namespace,
            'limit': limit,
            'offset': offset,
            'distinct': distinct,
            'req_total': req_total,
            'filters': filters,
            'sort': sort,
            'joined': joined,
            'merged': merged,
            'select_filter': select_filter,
            'select_functions': select_functions,
            'aggregations': aggregations
        }

    def helper_msg_role_status(self, status):
        msg = "Role: {role}. Status: {status}".format(
            role=self.role, status=status)

        return msg

    def helper_auth_create_test_db_as_owner(self):
        current_role = self.role

        self.role = 'owner'
        status, body = self.api_create_db(self.test_db)

        self.role = current_role

        return status, body

    def helper_auth_create_test_namespace_as_owner(self, index_array_of_dicts=[]):
        current_role = self.role

        self.role = 'owner'
        status, body = self.api_create_namespace(
            self.test_db, self.test_ns, index_array_of_dicts)

        self.role = current_role

        return status, body

    def helper_auth_create_test_item_as_owner(self):
        current_role = self.role

        item_body = self.helper_item_construct()

        self.role = 'owner'
        status, body = self.api_create_item(
            self.test_db, self.test_ns, item_body)

        self.role = current_role

        return status, body, item_body

    def helper_auth_create_test_index_as_owner(self):
        current_role = self.role

        index_name = 'MyIndex_' + str(self.test_timestamp)
        index = self.helper_index_construct(index_name)

        self.role = 'owner'
        status, body = self.api_create_index(
            self.test_db, self.test_ns, index)

        self.role = current_role

        return status, body, index_name
