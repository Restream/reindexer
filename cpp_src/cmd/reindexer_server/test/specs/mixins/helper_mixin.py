

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
            if i == 1:
                is_pk = True
            index_dict = self.helper_index_construct(
                'test_' + str(i + 1), is_pk=is_pk)
            indexes_arr_of_dicts.append(index_dict)

        return indexes_arr_of_dicts

    def helper_item_construct(self, fields_count=5, val_start=0):
        item_dict = {}

        for i in range(0, fields_count):
            item_dict['test_' + str(i + 1)] = val_start + i + 1

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
