

class ValidateMixin(object):
    def validate_get_list_response(self, status, body, schemaName, is_not_empty_check=False):
        self.assertEqual(True, status == 200)
        self.assertEqual(True, 'items' in body, body)
        self.assertEqual(True, 'total_items' in body)
        self.assertEqual(True, body['total_items'] >= 0)

        if is_not_empty_check:
            self.assertEqual(True, body['total_items'] > 0)
            self.assertEqual(True, len(body['items']) > 0)

        self.schema_validate_response(body, schemaName)

    def validate_get_namespace_response(self, status, body, sampleIndexesArrayOfDict=[]):
        self.assertEqual(True, status == 200)
        self.schema_validate_response(body, 'Namespace')

        if len(sampleIndexesArrayOfDict):
            receivedIndexes = body['indexes']
            for index in receivedIndexes:
                self.schema_validate_response(index, 'Index')
            self.assertEqual(True, len(sampleIndexesArrayOfDict)
                             == len(receivedIndexes))
            self.assertEqual(True, sampleIndexesArrayOfDict == receivedIndexes)

    def validate_get_indexes_response(self, status, body, sampleIndexesArrayOfDict=[]):
        self.assertEqual(True, status == 200)
        receivedIndexes = body['items']
        for index in receivedIndexes:
            self.schema_validate_response(index, 'Index')
        self.assertEqual(True, len(sampleIndexesArrayOfDict)
                         == len(receivedIndexes))
        self.assertEqual(True, sampleIndexesArrayOfDict == receivedIndexes)
