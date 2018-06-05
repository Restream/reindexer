import http.client
import json
from urllib.parse import urlencode


class ApiMixin(object):
    def _api_request(self, method, url, body=None, headers={}, as_json=True):
        self.api = http.client.HTTPConnection('127.0.0.1', 9088)

        if body is not None and as_json:
            body = json.dumps(body)

        self.api.request(method, url, body, headers)

        response = self.api.getresponse()
        content = response.read()

        content = content.decode()

        res_status = response.status

        try:
            res_body = json.loads(content)
        except:
            res_body = {'message': content}

        self.api.close()

        return res_status, res_body

    def api_get_dbs(self):
        return self._api_call('GET', '/db')

    def api_get_sorted_dbs(self, dir=''):
        order = '?sort_order=' + dir

        return self._api_call('GET', '/db' + order)

    def api_create_db(self, dbname):
        body = {'name': dbname}

        return self._api_call('POST', '/db', body)

    def api_delete_db(self, dbname):
        return self._api_call('DELETE', '/db/' + dbname)

    def api_get_namespaces(self, dbname):
        return self._api_call('GET', '/db/' + dbname + '/namespaces')

    def api_get_namespace(self, dbname, nsname):
        return self._api_call('GET', '/db/' + dbname + '/namespaces/' + nsname)

    def api_create_namespace(self, dbname, nsname, index_array_of_dicts=[]):
        body = {
            'name': nsname,
            'storage': {
                'enabled': True
            }
        }

        if len(index_array_of_dicts):
            body['indexes'] = index_array_of_dicts

        return self._api_call('POST', '/db/' + dbname + '/namespaces', body)

    def api_delete_namespace(self, dbname, nsname):
        return self._api_call('DELETE', '/db/' + dbname + '/namespaces/' + nsname)

    def api_get_sorted_namespaces(self, dbname, dir=''):
        order = '?sort_order=' + dir

        return self._api_call('GET', '/db/' + dbname + '/namespaces' + order)

    def api_get_indexes(self, dbname, nsname):
        return self._api_call('GET', '/db/' + dbname + '/namespaces/' + nsname + '/indexes')

    def api_create_index(self, dbname, nsname, body):
        return self._api_call('POST', '/db/' + dbname + '/namespaces/' + nsname + '/indexes', body)

    def api_delete_index(self, dbname, nsname, indexName):
        return self._api_call('DELETE', '/db/' + dbname + '/namespaces/' + nsname + '/indexes/' + indexName)

    def api_get_items(self, dbname, nsname):
        return self._api_call('GET', '/db/' + dbname + '/namespaces/' + nsname + '/items')

    def api_create_item(self, dbname, nsname, itemBody):
        return self._api_call('POST', '/db/' + dbname + '/namespaces/' + nsname + '/items', itemBody)

    def api_update_item(self, dbname, nsname, itemBody):
        return self._api_call('PUT', '/db/' + dbname + '/namespaces/' + nsname + '/items', itemBody)

    def api_delete_item(self, dbname, nsname, itemBody):
        return self._api_call('DELETE', '/db/' + dbname + '/namespaces/' + nsname + '/items', itemBody)

    def api_get_paginated_items(self, dbname, nsname, limit=10, offset=0):
        return self._api_call('GET', '/db/' + dbname + '/namespaces/' + nsname + '/items?'
                              + urlencode({'limit': limit, 'offset': offset}))

    def api_get_sorted_items(self, dbname, nsname, field='', direction=''):
        return self._api_call('GET', '/db/' + dbname + '/namespaces/' + nsname + '/items?'
                              + urlencode({'sort_field': field, 'sort_order': direction}))

    def api_sql_exec(self, dbname, sql_query=''):
        return self._api_call('GET', '/db/' + dbname + '/query?' + urlencode({'q': sql_query}))

    def api_sql_post(self, dbname, body):
        return self._api_call('POST', '/db/' + dbname + '/sqlquery', body, headers={'Content-type': 'text/plain'}, as_json=False)

    def api_query_dsl(self, dbname, body):
        return self._api_call('POST', '/db/' + dbname + '/query', body)
