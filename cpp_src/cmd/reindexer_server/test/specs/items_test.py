from specs import BaseTest


class ItemsTest(BaseTest):
    def check_update_msgpack_response(self, data, itemsCount, updatedItems=0):
        self.assertEqual(True, 'updated' in data, data)
        self.assertEqual(True, data['updated'] == itemsCount, data)
        self.assertEqual(True, 'success' in data, data)
        self.assertEqual(True, data['success'] == True, data)
        if updatedItems > 0:
            self.assertEqual(True, 'items' in data, data)
            self.assertEqual(
                True, len(data['items']) == updatedItems, data['items'])

    def check_update_protobuf_response(self, parameters, itemsCount, updatedItems=0):
        self.assertEqual(True, parameters.success == 1, parameters.success)
        self.assertEqual(True, parameters.updated ==
                         itemsCount, parameters.updated)
        if updatedItems > 0:
            self.assertEqual(True, len(parameters.items) ==
                             updatedItems, len(parameters.items))

    def setUp(self):
        super().setUp()

        self.helper_items_testdata_prepare()

    def test_update_item_protobuf(self):
        """Should be able to update item with protobuf"""

        status, body = self.api_put_namespace_schema(
            self.current_db, self.test_ns, super().schema)
        self.assertEqual(True, status == self.API_STATUS['success'], body)

        if super().prepare_protobuf_schemas() is False:
            print("Protobuf is not installed")
            return

        count = 10
        index_array_of_dicts = self.helper_index_array_construct(count)

        for i in range(0, count):
            status, body = self.api_create_index(
                self.current_db, self.test_ns, index_array_of_dicts[i])
            self.assertEqual(True, status == self.API_STATUS['success'], body)

        outputParameters = super().instantiate_pb_object(self.current_ns)
        for i in range(1, 11):
            item = super().instantiate_pb_object(self.current_ns)
            # item = getattr (outputParameters.items.add(),self.current_ns)
            item.test_1 = i*1
            item.test_2 = i*2
            item.test_3 = i*3
            item.test_4 = i*4
            item.test_5 = i*5
            status, body = self.api_create_item(
                self.current_db, self.current_ns, item.SerializeToString(), self.EncodingType.Protobuf)
            self.assertEqual(True, status == self.API_STATUS['success'], body)
            self.assertEqual(True, 'message' in body, body)

            inputParameters = super().instantiate_pb_object('ModifyResults')
            inputParameters.ParseFromString(body.get('message'))
            self.check_update_protobuf_response(inputParameters, 1)

        status, body = self.api_sql_exec(
            self.current_db, 'SELECT COUNT(*),* FROM ' + self.current_ns, self.EncodingType.Protobuf)
        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'message' in body, body)

        queryresults = super().instantiate_pb_object('QueryResults')
        queryresults.ParseFromString(body.get('message'))

        self.assertEqual(True, len(queryresults.namespaces)
                         == 1, queryresults.namespaces)
        self.assertEqual(
            True, queryresults.namespaces[0] == self.test_ns, queryresults.namespaces)
        self.assertEqual(True, queryresults.cache_enabled == 1,
                         queryresults.cache_enabled)
        self.assertEqual(True, len(queryresults.items)
                         == 10, queryresults.items)
        i = 0
        for it in queryresults.items:
            i += 1
            item = getattr(it, self.current_ns)
            self.assertEqual(True, item.test_1 == i*1, item.test_1)
            self.assertEqual(True, item.test_2 == i*2, item.test_2)
            self.assertEqual(True, item.test_3 == i*3, item.test_3)
            self.assertEqual(True, item.test_4 == i*4, item.test_4)
            self.assertEqual(True, item.test_5 == i*5, item.test_5)

        precepts = ['test_3=serial()']
        status, body = self.api_update_item(
            self.current_db, self.current_ns, getattr(queryresults.items[3], self.current_ns).SerializeToString(), precepts, self.EncodingType.Protobuf)
        self.assertEqual(True, status == self.API_STATUS['success'], body)
        self.assertEqual(True, 'message' in body, body)

        resultParameters = super().instantiate_pb_object('ModifyResults')
        resultParameters.ParseFromString(body.get('message'))
        self.check_update_protobuf_response(resultParameters, 1)
