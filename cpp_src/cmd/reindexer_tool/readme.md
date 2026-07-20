# Reindexer command line tool
Reindexer command line tool is a client utility to work with the database.

## Table of contents

- [Features](#features)
- [Usage](#usage)
- [Commands](#commands)
- [Examples](#examples)

## Features

- Backup whole database into text file or console.
- Make queries to database
- Modify documents and DB metadata
- Both standalone and embedded (builtin) modes are supported

## Usage

```
reindexer_tool {OPTIONS}

Options
  -d[DSN],      --dsn=[DSN]              DSN to 'reindexer', like 'cproto://127.0.0.1:6534/dbname', 'cprotos://127.0.0.1:6535/dbname', 'builtin:///var/lib/reindexer/dbname' or `ucproto://user@password:/tmp/reindexer.sock:/dbname`
  -f[FILENAME], --filename=[FILENAME]    Execute commands from file, then exit
  -c[COMMAND],  --command=[COMMAND]      Run single command (SQL or internal) and exit
  -o[FILENAME], --output=[FILENAME]      Send query results to file
  -l[INT=1..5], --log=[INT=1..5]         Reindexer logging level
  -t[INT],      --threads=[INT]          Number of threads(connections) used by db connector
  -txs[INT],    --txsize=[INT]           Max transaction size used by db connector(0 - no transactions)
  --createdb                             Creates target database if it is missing
  --dry-run                              Validate dump file without changing target database. Applicable only with -f/--filename, incompatible with -c/--command
  --ignore-checksum-mismatch             Apply dump even if `-- __checksum` mismatches (warning is still printed)
  -a[Application name],
  --appname=[Application name]           Application name which will be used in login info
  --dump-mode=[DUMP_MODE]                Dump mode for sharded databases: 'full_node' (default), 'sharded_only', 'local_only'
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

### Dump modes

There are a few different dump modes to interact with sharded databases:
1. full_node - Default mode. Dumps all data of chosen node, including sharded namespaces. However, does not dump data from other shards.
2. sharded_only - Dumps data from sharded namespaces only (including the data from all other shards). May be useful for resharding.
3. local_only - Dumps data from local namespaces, excluding sharded namespaces.

Dump mode is saved in the output file and will be used during restoration process.

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
\meta delete <namespace> <key>
Delete metadata key value
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


## Examples

Backup whole database into single backup file:
```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --command '\dump' --output mydb.rxdump
```

The produced dump includes a checksum metadata line `-- __checksum: "<md5>"`. The MD5 digest covers every non-empty logical line that is not a comment (lines starting with `--`), concatenating line bodies without newline characters so the same file yields the same digest on Windows and Linux. Comment lines (including the checksum line itself, banners, `__dump_mode`, and `Dumping namespace ...` headers) are excluded from the digest.

Restore database from backup file:
```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --createdb --filename mydb.rxdump
```

Option `createdb` in example above allows to automatically create `mydb` if it does not exist. By default `reindexer_tool` requires an existing database to connect.

Validate a dump file without modifying the target database:
```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --filename mydb.rxdump --dry-run
```

#### What dry-run validates

- Optional `-- __checksum:` metadata: if present, the digest is recomputed from the same rules as `\dump` and must match.
- Format and order of dump commands (head comments, `\NAMESPACES ADD`, `\META PUT`, `\UPSERT`).
- JSON of `\NAMESPACES ADD` is parseable as a valid `NamespaceDef`, namespace name in the command matches the one in the JSON, and there are no duplicate `\NAMESPACES ADD` for the same namespace.
- JSON of every `\UPSERT` is parseable and conforms to the schema/types declared by the corresponding `\NAMESPACES ADD` (a temporary in-memory database is used as a schema validator). For `#config` items, validation skips entries with `type=action` and `type=sharding`, mirroring the behaviour of restore.
- `\META PUT` references a previously declared namespace.
- For every namespace present both in the dump and on the target, the full set of `IndexDef`s must match (name, json paths, index/field types, expire-after, options/params).

#### Output and exit code

The tool prints three sections (each with its own header):
1. dump errors with line numbers,
2. namespaces present both in the dump and on the target where the target side is non-empty (potential conflicts),
3. namespaces present on the target but absent from the dump.

If at least one error is reported, the tool exits with a non-zero code. Conflict and target-only listings are warnings — they are printed in any case, but do not affect the exit code. The `--ignore-checksum-mismatch` flag controls whether a checksum mismatch (`__checksum`) is treated as an error or as a warning.

#### Limitations

- `--dry-run` requires `-f/--filename` and is incompatible with `-c/--command`.
- `--createdb` is intentionally not applied in dry-run mode, so the target database must already exist (otherwise the tool fails before reaching validation).
- `--dry-run` never writes to the target database. The validator namespaces are created in-memory with storage explicitly disabled.
- During normal restore:
  - if dump contains `-- __checksum` and it mismatches, `reindexer_tool` prints warning `Dump checksum mismatch. Expected '...', Computed '...'` and aborts restore by default;
  - use `--ignore-checksum-mismatch` to continue restore despite mismatch (the warning is still printed).

Backup only sharded namespaces from all of the shards into single backup file:
```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --command '\dump' --dump-mode=sharded_only --output mydb.rxdump
```

