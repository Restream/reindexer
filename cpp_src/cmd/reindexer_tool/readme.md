# Reindexer dump tool
Reindexer dump tool is an utility to view the dump of the reindex persistent.

## Table of Content

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Examples](#examples)

## Features

- Dump whole namespaces into JSON format to console/file.
- Select items by query (like 'SQL-query')
- Delete items
- Insert/Update items ('Upsert')
- Dump meta info by KEY
- Dump a partial list of elements ('limit', 'offset' options)

## Installation

For installation you need to build app in the source directory.
Execute following command:

```sh
make
```
Utility will be put in the directory: `.build/`

## Usage

```sh
reindexer_tool [OPTION...]

  -h, --help  show this message

 '--dump' additional options:
      --limit INT   limit count of items
      --offset INT  offset from the beginning

 dest options:
      --out FILE  path to result output file (console by default)

 logging options:
      --log INT  log level (MAX = 5)

 source options:
      --db [=DIRECTORY(=)]  path to 'reindexer' cache

 start options:
      --dump [=arg(=)]       dump whole namespace (see also '--limit',
                             '--offset')
      --query [="QUERY"(=)]  dump by query (in quotes)
      --delete [="JSON"(=)]  delete item from namespace (JSON object in
                             quotes)
      --upsert [="JSON"(=)]  Insert or update item in namespace (JSON object
                             in quotes)
      --meta KEY             dump meta information by KEY from namespace
      --namespace NAMESPACE  needed for --dump, --delete, --upsert, --meta
                             actions
```

## Examples

Dump whole namespace into JSON file:
```sh
reindexer_tool --db /var/cache --dump sessions --out /home/user/sessions_dump.json --log 5
```


Print to console limited content of namespace by query:
```sh
reindexer_tool --db /var/cache --query "SELECT * FROM accounts WHERE age > 18" --offset 5 --limit 5
```

Upsert item into namespace:
```sh
reindexer_tool --db /var/cahce --upsert "{"id":3,"name":"horror"}" --namespace genres
```
