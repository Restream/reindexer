import http.client
import json
from urllib.parse import urlencode
from urllib.parse import quote


class ApiMixin(object):
    API_STATUS = {
        'success': 200,
        'moved_permanently': 301,
        'bad_request': 400,
        'unauthorized': 401,
        'forbidden': 403,
        'not_found': 404
    }

    SORT_ORDER = {
        'desc': -1,
        'no_sort': 0,
        'asc': 1
    }

    def _server_request(self, method, url, body=None, headers={}, as_json=True):
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

    def _api_call(self, method, url, body=None, headers={}, as_json=True, with_basic_auth=True):
        api_base = self.SWAGGER['basePath']

        content_type = 'application/json' if as_json else 'text/plain'
        def_headers = {
            'Content-type': content_type,
        }

        if with_basic_auth:
            def_headers['Authorization'] = 'Basic ' + \
                self.role_token(self.role)

        req_headers = {**def_headers, **headers}

        return self._server_request(method, api_base + url, body, req_headers, as_json)

    def _web_call(self, url, with_basic_auth=True):
        headers = {}

        if with_basic_auth:
            headers['Authorization'] = 'Basic ' + self.role_token(self.role)

        return self._server_request('GET', url, headers=headers)

    def web_face_redirect(self):
        return self._web_call('/face')

    def web_face(self):
        return self._web_call('/face/')

    def web_facestaging_redirect(self):
        return self._web_call('/facestaging')

    def web_facestaging(self):
        return self._web_call('/facestaging/')

    def web_swagger_redirect(self):
        return self._web_call('/swagger')

    def web_swagger(self):
        return self._web_call('/swagger/')

    def api_check(self):
        return self._api_call('GET', '/check')

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

    def api_put_namespace_meta(self, dbname, nsname, body):
        return self._api_call('PUT', '/db/' + dbname + '/namespaces/' + nsname + '/metabykey', body)

    def api_get_namespace_meta(self, dbname, nsname, key):
        return self._api_call('GET', '/db/' + dbname + '/namespaces/' + nsname + '/metabykey/' + key)

    def api_get_namespace_meta_list(self, dbname, nsname, sort = SORT_ORDER['no_sort'], with_values = False, offset = 0, limit = 0):
        query = ''
        separator = '?'
        if sort == self.SORT_ORDER['asc']:
            query += '?sort_order=asc'
            separator = '&'
        elif sort == self.SORT_ORDER['desc']:
            query += '?sort_order=desc'
            separator = '&'

        if with_values:
            query += separator
            query += 'with_values=true'
            separator = '&'

        if offset > 0:
            query += separator
            query += 'offset='
            query += str(offset)
            separator = '&'

        if limit > 0:
            query += separator
            query += 'limit='
            query += str(limit)

        return self._api_call('GET', '/db/' + dbname + '/namespaces/' + nsname + '/metalist' + query)

    def api_get_indexes(self, dbname, nsname):
        return self._api_call('GET', '/db/' + dbname + '/namespaces/' + nsname + '/indexes')

    def api_create_index(self, dbname, nsname, body):
        return self._api_call('POST', '/db/' + dbname + '/namespaces/' + nsname + '/indexes', body)

    def api_update_index(self, dbname, nsname, body):
        return self._api_call('PUT', '/db/' + dbname + '/namespaces/' + nsname + '/indexes', body)

    def api_delete_index(self, dbname, nsname, index_name):
        return self._api_call('DELETE', '/db/' + dbname + '/namespaces/' + nsname + '/indexes/' + index_name)

    def api_get_items(self, dbname, nsname):
        return self._api_call('GET', '/db/' + dbname + '/namespaces/' + nsname + '/items')

    def api_create_item(self, dbname, nsname, item_body):
        return self._api_call('POST', '/db/' + dbname + '/namespaces/' + nsname + '/items', item_body)

    def api_update_item(self, dbname, nsname, item_body, precepts=[]):
        query = ''
        separator = '?'
        for precept in precepts:
            query += separator
            query += 'precepts='
            query += quote(precept)
            separator = '&'
        return self._api_call('PUT', '/db/' + dbname + '/namespaces/' + nsname + '/items' + query, item_body)

    def api_delete_item(self, dbname, nsname, item_body):
        return self._api_call('DELETE', '/db/' + dbname + '/namespaces/' + nsname + '/items', item_body)

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

    def api_get_filtered_items(self, dbname, nsname, filter=''):
        return self._api_call('GET', '/db/' + dbname + '/namespaces/' + nsname + '/items?'
                              + urlencode({'filter': filter}))
