import os
import yaml
import json
from jsonref import JsonRef


def get_swagger():
    schema_file_path = os.path.dirname(
        os.path.realpath(__file__)) + '/../../../contrib/server.yml'

    with open(schema_file_path) as f:
        swagger_data = yaml.load(f)
        swagger = JsonRef.replace_refs(swagger_data)
        f.close()

    return swagger


def get_users():
    users_file_path = os.path.dirname(
        os.path.realpath(__file__)) + '/../../mocks/users.json'

    with open(users_file_path) as f:
        users = json.load(f)
        f.close()

    return users
