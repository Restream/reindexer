import argparse
import re

import requests
from packaging.version import parse


URL = "http://repo.restream.ru/itv-api-ng/7/x86_64/"

parser = argparse.ArgumentParser(description='Version')
parser.add_argument('-v', '--version', type=int, choices=[3, 4, 5], default=5)
args = parser.parse_args()

version = args.version
if version == 4:
    name = ">reindexer-4-server-"
else:
    name = f">reindexer-server-{version}"

r = requests.get(URL)
res = r.text
res_list = re.findall(f'{name}.*.rpm', res)
versions_list = [(i[1:], parse(i[len(name):-11])) for i in res_list]
versions_list.sort(key=lambda x: x[1])

print(versions_list[-1][0])
