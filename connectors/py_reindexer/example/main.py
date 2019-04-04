from pyreindexer import RxConnector


def create_index_example(db, namespace):
    index_definition = {
        'name': 'id',
        'json_paths': ['id'],
        'field_type': 'int',
        'index_type': 'hash',
        'is_pk': True,
        'is_array': False,
        'is_dense': False,
        'is_sparse': False,
        'collate_mode': 'none',
        'sort_order_letters': '',
        'expire_after':0,
        'config': {},
    }

    try:
        db.index_add(namespace, index_definition)
    except Exception:
        db.index_drop(namespace, 'id')
        db.index_add(namespace, index_definition)


def update_index_example(db, namespace):
    index_definition_modified = {
        'name': 'id',
        'json_paths': ['id'],
        'field_type': 'int64',
        'index_type': 'hash',
        'is_pk': True,
        'is_array': False,
        'is_dense': True,
        'is_sparse': False,
        'collate_mode': 'none',
        'sort_order_letters': '',
        'expire_after':0,
        'config': {},
    }
    db.index_update(namespace, index_definition_modified)


def create_items_example(db, namespace):
    ITEMS_COUNT = 10

    for i in range(0, ITEMS_COUNT):
        item = {'id': i + 1, 'name': 'item_' + str(i % 2)}
        db.item_upsert(namespace, item)


def select_item_query_example(db, namespace):
    item_name_for_lookup = 'item_0'

    return db.select('SELECT * FROM ' + namespace + ' WHERE name="' + item_name_for_lookup + '"')


def rx_example():
    db = RxConnector('builtin:///tmp/pyrx')
    # db = RxConnector('cproto://127.0.0.1:6534/pyrx')

    namespace = 'test_table'

    db.namespace_open(namespace)

    create_index_example(db, namespace)
    update_index_example(db, namespace)

    create_items_example(db, namespace)

    selected_items = select_item_query_example(db, namespace)

    res_count = selected_items.count()
    print('Results count: ', res_count)

    # disposable QueryResults iterator
    for item in selected_items:
        print('Item: ', item)

    # won't be iterated again
    for item in selected_items:
        print('Item: ', item)

    db.close()


if __name__ == "__main__":
    rx_example()
