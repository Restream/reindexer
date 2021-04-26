# Reindexer command line tool
Reindexer command line tool is an client utility to work with database.

## Table of Content

- [Features](#features)
- [Usage](#usage)
- [Commands](#commands)
- [Examples](#examples)

## Features

- Backup whole database into text file or console.
- Make queries to database
- Modify documents and DB metadata
- Both standalone and embeded(builtin) modes are supported

## Usage

```
reindexer_tool {OPTIONS}

Options
  -d[DSN],      --dsn=[DSN]              DSN to 'reindexer', like 'cproto://127.0.0.1:6534/dbname' or 'builtin:///var/lib/reindexer/dbname'
  -f[FILENAME], --filename=[FILENAME]    execute commands from file, then exit
  -c[COMMAND],  --command=[COMMAND]      run only single command (SQL or internal) and exit
  -o[FILENAME], --output=[FILENAME]      send query results to file
  -l[INT=1..5], --log=[INT=1..5]         reindexer logging level
  -C[INT],      --connections=[INT]      Number of simulateonous connections to db
  -t[INT],      --threads=[INT]          Number of threads used by db connector (used only for bench)

```

## Commands

### Upsert document to namespace

*Syntax:*
```
\upsert <namespace> <document>
```

*Example:*
```
\upsert books {"id":5,"name":"xx"}
```

### Delete document from namespace

*Syntax:*
```
\delete <namespace> <document>
```

*Example:*
```
\delete books {"id":5}
```

### Backup database into text dump format

*Syntax:*
```
\dump [namespace1 [namespace2]...]
```

### Manipulate namespaces

*Syntax:*
```
\namespaces add <name> <definition>
```
Add new namespace
```
\namespaces list 
```
List available namespaces
```
\namespaces drop <namespace>
```
Drop namespace

### Working with databases
List of available databases
```
\databases list
```
Switching to existing database
```
\databases use <namespace>
```

### Manipulate metadata
*Syntax:*
```
\meta put <namespace> <key> <value>
Put metadata key value
\meta list
List all metadata in name
```

### Set output format
*Syntax:*
```
\set output <format>
```
Format can be one of the following:
- `json` Unformatted JSON
- `pretty` Pretty printed JSON
- `table` Table view (with lines and columns)

### Run simple benchmark
*Syntax:*
```
\bench <time>
Run benchmark for `<time>` seconds
```

### Subscribe to upstream WAL updates
*Syntax:*
```
\subscribe <on|off>
On or off subscrbibtion to WAL updates
```


## Examples

Backup whole database into single backup file:
```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --command '\dump' --output mydb.rxdump
```

Restore database from backup file:
```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --filename mydb.rxdump
```
