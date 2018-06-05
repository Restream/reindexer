

class HelperMixin(object):
    def helper_index_construct(self, name, field_type='int', index_type='hash', is_pk=False):
        return {
            'name': name,
            'json_path': name,
            'field_type': field_type,
            'index_type': index_type,
            'is_pk': is_pk,
            'is_array': False,
            'is_dense': True,
            'is_appendable': True,
            'collate_mode': 'none',
            'sort_order_letters': ''
        }

    def helper_index_array_construct(self, count=5):
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
