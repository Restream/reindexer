<h1 id="pyreindexer">pyreindexer</h1>


The pyreindexer module provides a connector and its auxiliary tools for interaction with Reindexer.

<h1 id="pyreindexer.rx_connector">pyreindexer.rx_connector</h1>


<h2 id="pyreindexer.rx_connector.RxConnector">RxConnector</h2>

```python
RxConnector(self, dsn)
```
RxConnector provides a binding to Reindexer upon two shared libraries (hereinafter - APIs): 'rawpyreindexerb.so' and 'rawpyreindexerc.so'.
The first one is aimed to a builtin way usage. That API embeds Reindexer so it could be used right in-place as is.
The second one acts as a lightweight client which establishes a connection to Reindexer server via RPC.
The APIs interfaces are completely the same.

__Attributes:__

    api (module): An API module loaded dynamically for Reindexer calls
    rx (int): A memory pointer to Reindexer instance
    err_code (int): the API error code
    err_msg (string): the API error message


<h3 id="pyreindexer.rx_connector.RxConnector.close">close</h3>

```python
RxConnector.close(self)
```
Closes an API instance with Reindexer resources freeing

__Returns:__

    None


<h3 id="pyreindexer.rx_connector.RxConnector.namespace_open">namespace_open</h3>

```python
RxConnector.namespace_open(self, namespace)
```
Opens a namespace specified or creates a namespace if it does not exist

__Arguments:__

    namespace (string): A name of a namespace

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.namespace_close">namespace_close</h3>

```python
RxConnector.namespace_close(self, namespace)
```
Closes a namespace specified

__Arguments:__

    namespace (string): A name of a namespace

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.namespace_drop">namespace_drop</h3>

```python
RxConnector.namespace_drop(self, namespace)
```
Drops a namespace specified

__Arguments:__

    namespace (string): A name of a namespace

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.namespaces_enum">namespaces_enum</h3>

```python
RxConnector.namespaces_enum(self, enum_not_opened=False)
```
Gets a list of namespaces available

__Arguments:__

    enum_not_opeden (bool, optional): An enumeration mode flag. If it is
        set then closed namespaces are in result list too. Defaults to False.

__Returns:__

    (:obj:`list` of :obj:`dict`): A list of dictionaries which describe each namespace.

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.index_add">index_add</h3>

```python
RxConnector.index_add(self, namespace, index_def)
```
Adds an index to the namespace specified

__Arguments:__

    namespace (string): A name of a namespace
    index_def (dict): A dictionary of index definiton

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.index_update">index_update</h3>

```python
RxConnector.index_update(self, namespace, index_def)
```
Updates an index in the namespace specified

__Arguments:__

    namespace (string): A name of a namespace
    index_def (dict): A dictionary of index definiton

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.index_drop">index_drop</h3>

```python
RxConnector.index_drop(self, namespace, index_name)
```
Drops an index from the namespace specified

__Arguments:__

    namespace (string): A name of a namespace
    index_name (string): A name of an index

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.item_insert">item_insert</h3>

```python
RxConnector.item_insert(self, namespace, item_def, precepts=[])
```
Inserts an item with its precepts to the namespace specified.

__Arguments:__

    namespace (string): A name of a namespace
    item_def (dict): A dictionary of item definiton
    precepts (:obj:`list` of :obj:`str`): A dictionary of index definiton

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.item_update">item_update</h3>

```python
RxConnector.item_update(self, namespace, item_def, precepts=[])
```
Updates an item with its precepts in the namespace specified.

__Arguments:__

    namespace (string): A name of a namespace
    item_def (dict): A dictionary of item definiton
    precepts (:obj:`list` of :obj:`str`): A dictionary of index definiton

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.item_upsert">item_upsert</h3>

```python
RxConnector.item_upsert(self, namespace, item_def, precepts=[])
```
Updates an item with its precepts in the namespace specified. Creates the item if it not exist.

__Arguments:__

    namespace (string): A name of a namespace
    item_def (dict): A dictionary of item definiton
    precepts (:obj:`list` of :obj:`str`): A dictionary of index definiton

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.item_delete">item_delete</h3>

```python
RxConnector.item_delete(self, namespace, item_def)
```
Deletes an item from the namespace specified.

__Arguments:__

    namespace (string): A name of a namespace
    item_def (dict): A dictionary of item definiton

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.commit">commit</h3>

```python
RxConnector.commit(self, namespace)
```
Flushes changes to a storage of Reindexer

__Arguments:__

    namespace (string): A name of a namespace

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.meta_put">meta_put</h3>

```python
RxConnector.meta_put(self, namespace, key, value)
```
Puts metadata to a storage of Reindexer by key

__Arguments:__

    namespace (string): A name of a namespace
    key (string): A key in a storage of Reindexer for metadata keeping
    value (string): A metadata for storage

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.meta_get">meta_get</h3>

```python
RxConnector.meta_get(self, namespace, key)
```
Gets metadata from a storage of Reindexer by key specified

__Arguments:__

    namespace (string): A name of a namespace
    key (string): A key in a storage of Reindexer where metadata is kept

__Returns:__

    string: A metadata value

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.meta_enum">meta_enum</h3>

```python
RxConnector.meta_enum(self, namespace)
```
Gets a list of metadata keys from a storage of Reindexer

__Arguments:__

    namespace (string): A name of a namespace

__Returns:__

    (:obj:`list` of :obj:`str`): A list of all metadata keys.

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h3 id="pyreindexer.rx_connector.RxConnector.select">select</h3>

```python
RxConnector.select(self, query)
```
Executes an SQL query and returns query results

__Arguments:__

    query (string): An SQL query

__Returns:__

    (:obj:`QueryResults`): A QueryResults iterator.

__Raises:__

    Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
    Exception: Raises with an error message of API return on non-zero error code.


<h1 id="pyreindexer.query_results">pyreindexer.query_results</h1>


<h2 id="pyreindexer.query_results.QueryResults">QueryResults</h2>

```python
QueryResults(self, api, qres_wrapper_ptr, qres_iter_count)
```
QueryResults is a disposable iterator of Reindexer results for such queries as SELECT and etc.
When the results are fetched the iterator closes and frees a memory of results buffer of Reindexer

__Attributes:__

    api (module): An API module for Reindexer calls
    err_code (int): the API error code
    err_msg (string): the API error message
    qres_wrapper_ptr (int): A memory pointer to Reindexer iterator object
    qres_iter_count (int): A count of results for iterations
    pos (int): The current result position in iterator

<h3 id="pyreindexer.query_results.QueryResults.count">count</h3>

```python
QueryResults.count(self)
```
Returns a count of results

__Returns__

`int`: A count of results


<h1 id="pyreindexer.index_definition">pyreindexer.index_definition</h1>


<h2 id="pyreindexer.index_definition.IndexDefinition">IndexDefinition</h2>

```python
IndexDefinition(self, /, *args, **kwargs)
```
IndexDefinition is a dictionary subclass which allows to construct and manage indexes more efficiently.
NOT IMPLEMENTED YET. USE FIELDS DESCRIPTION ONLY.

__Arguments:__

    name (str): An index name.
    json_paths (:obj:`list` of :obj:`str`): A name for mapping a value to a json field.
    field_type (str): A type of a field. Possible values are: `int`, `int64`, `double`, `string`, `bool`, `composite`.
    index_type (str): An index type. Possible values are: `hash`, `tree`, `text`, `-`.
    is_pk (bool): True if a field is a primary key.
    is_array (bool): True if an index is an array.
    is_dense (bool): True if an index is dense. reduce index size. Saves 8 bytes per unique key value for 'hash' and 'tree' index types.
        For '-' index type saves 4-8 bytes per each element. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity could
        significantly decrease update performance.
    is_sparse (bool): True if a value of an index may be not presented.
    collate_mode (str): Sets an order of values by collate mode. Possible values are: `none`, `ascii`, `utf8`, `numeric`, `custom`.
    sort_order_letters (str): Order for a sort sequence for a custom collate mode.
    config (dict): A config for a fulltext engine. [More](https://github.com/Restream/reindexer/blob/master/fulltext.md) .

