from pyreindexer.raiser_mixin import RaiserMixin
from pyreindexer.query_results import QueryResults


class RxConnector(RaiserMixin):
    """ RxConnector provides a binding to Reindexer upon two shared libraries (hereinafter - APIs): 'rawpyreindexerb.so' and 'rawpyreindexerc.so'.
    The first one is aimed to a builtin way usage. That API embeds Reindexer so it could be used right in-place as is.
    The second one acts as a lightweight client which establishes a connection to Reindexer server via RPC.
    The APIs interfaces are completely the same.

    # Attributes:
        api (module): An API module loaded dynamically for Reindexer calls
        rx (int): A memory pointer to Reindexer instance
        err_code (int): the API error code
        err_msg (string): the API error message

    """

    def __init__(self, dsn):
        """Constructs a new connector object.
        Initializes an error code and a Reindexer instance descriptor to zero.

        # Arguments:
            dsn (string): The connection string which contains a protocol. Examples: 'builtin:///tmp/pyrx', 'cproto://127.0.0.1:6534/pyrx

        """

        self.err_code = 0
        self.rx = 0
        self._api_import(dsn)
        self._api_init(dsn)

    def __del__(self):
        """Closes an API instance on a connecter object deletion if the API is initialized

        """

        if self.rx > 0:
            self._api_close()

    def close(self):
        """Closes an API instance with Reindexer resources freeing

        # Returns:
            None

        """

        self._api_close()

    def namespace_open(self, namespace):
        """Opens a namespace specified or creates a namespace if it does not exist

        # Arguments:
            namespace (string): A name of a namespace

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.namespace_open(
            self.rx, namespace)
        self.raise_on_error()

    def namespace_close(self, namespace):
        """Closes a namespace specified

        # Arguments:
            namespace (string): A name of a namespace

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.namespace_close(
            self.rx, namespace)
        self.raise_on_error()

    def namespace_drop(self, namespace):
        """Drops a namespace specified

        # Arguments:
            namespace (string): A name of a namespace

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.namespace_drop(
            self.rx, namespace)
        self.raise_on_error()

    def namespaces_enum(self, enum_not_opened=False):
        """Gets a list of namespaces available

        # Arguments:
            enum_not_opeden (bool, optional): An enumeration mode flag. If it is
                set then closed namespaces are in result list too. Defaults to False.

        # Returns:
            (:obj:`list` of :obj:`dict`): A list of dictionaries which describe each namespace.

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg, res = self.api.namespaces_enum(
            self.rx, enum_not_opened)
        self.raise_on_error()
        return res

    def index_add(self, namespace, index_def):
        """Adds an index to the namespace specified

        # Arguments:
            namespace (string): A name of a namespace
            index_def (dict): A dictionary of index definiton

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.index_add(
            self.rx, namespace, index_def)
        self.raise_on_error()

    def index_update(self, namespace, index_def):
        """Updates an index in the namespace specified

        # Arguments:
            namespace (string): A name of a namespace
            index_def (dict): A dictionary of index definiton

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.index_update(
            self.rx, namespace, index_def)
        self.raise_on_error()

    def index_drop(self, namespace, index_name):
        """Drops an index from the namespace specified

        # Arguments:
            namespace (string): A name of a namespace
            index_name (string): A name of an index

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.index_drop(
            self.rx, namespace, index_name)
        self.raise_on_error()

    def item_insert(self, namespace, item_def, precepts=[]):
        """Inserts an item with its precepts to the namespace specified.

        # Arguments:
            namespace (string): A name of a namespace
            item_def (dict): A dictionary of item definiton
            precepts (:obj:`list` of :obj:`str`): A dictionary of index definiton

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.item_insert(
            self.rx, namespace, item_def, precepts)
        self.raise_on_error()

    def item_update(self, namespace, item_def, precepts=[]):
        """Updates an item with its precepts in the namespace specified.

        # Arguments:
            namespace (string): A name of a namespace
            item_def (dict): A dictionary of item definiton
            precepts (:obj:`list` of :obj:`str`): A dictionary of index definiton

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.item_update(
            self.rx, namespace, item_def, precepts)
        self.raise_on_error()

    def item_upsert(self, namespace, item_def, precepts=[]):
        """Updates an item with its precepts in the namespace specified. Creates the item if it not exist.

        # Arguments:
            namespace (string): A name of a namespace
            item_def (dict): A dictionary of item definiton
            precepts (:obj:`list` of :obj:`str`): A dictionary of index definiton

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.item_upsert(
            self.rx, namespace, item_def, precepts)
        self.raise_on_error()

    def item_delete(self, namespace, item_def):
        """Deletes an item from the namespace specified.

        # Arguments:
            namespace (string): A name of a namespace
            item_def (dict): A dictionary of item definiton

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.item_delete(
            self.rx, namespace, item_def)
        self.raise_on_error()

    def commit(self, namespace):
        """Flushes changes to a storage of Reindexer

        # Arguments:
            namespace (string): A name of a namespace 

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.commit(
            self.rx, namespace)
        self.raise_on_error()

    def meta_put(self, namespace, key, value):
        """Puts metadata to a storage of Reindexer by key

        # Arguments:
            namespace (string): A name of a namespace
            key (string): A key in a storage of Reindexer for metadata keeping
            value (string): A metadata for storage

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.meta_put(
            self.rx, namespace, key, value)
        self.raise_on_error()

    def meta_get(self, namespace, key):
        """Gets metadata from a storage of Reindexer by key specified

        # Arguments:
            namespace (string): A name of a namespace
            key (string): A key in a storage of Reindexer where metadata is kept

        # Returns:
            string: A metadata value

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg, res = self.api.meta_get(
            self.rx, namespace, key)
        self.raise_on_error()
        return res

    def meta_enum(self, namespace):
        """Gets a list of metadata keys from a storage of Reindexer

        # Arguments:
            namespace (string): A name of a namespace

        # Returns:
            (:obj:`list` of :obj:`str`): A list of all metadata keys.

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg, res = self.api.meta_enum(
            self.rx, namespace)
        self.raise_on_error()
        return res

    def select(self, query):
        """Executes an SQL query and returns query results

        # Arguments:
            query (string): An SQL query

        # Returns:
            (:obj:`QueryResults`): A QueryResults iterator.

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.raise_on_not_init()
        self.err_code, self.err_msg, qres_wrapper_ptr, qres_iter_count = self.api.select(
            self.rx, query)
        self.raise_on_error()
        return QueryResults(self.api, qres_wrapper_ptr, qres_iter_count)

    def _api_import(self, dsn):
        """Imports an API dynamically depending on protocol specified in dsn.

        # Arguments:
            dsn (string): The connection string which contains a protocol.

        # Returns:
            None

        # Raises:
            Exception: Raises an exception if a connection protocol is unrecognized.

        """

        if dsn.startswith('builtin://'):
            self.api = __import__('rawpyreindexerb')
        elif dsn.startswith('cproto://'):
            self.api = __import__('rawpyreindexerc')
        else:
            raise Exception(
                "Unknown Reindexer connection protocol for dsn: ", dsn)

    def _api_init(self, dsn):
        """Initializes Reindexer instance and connects to a database specified in dsn.
        Obtains a pointer to Reindexer instance.

        # Arguments:
            dsn (string): The connection string which contains a protocol.

        # Returns:
            None

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.
            Exception: Raises with an error message of API return on non-zero error code.

        """

        self.rx = self.api.init()
        self.raise_on_not_init()
        self.err_code, self.err_msg = self.api.connect(self.rx, dsn)
        self.raise_on_error()

    def _api_close(self):
        """Desctructs Reindexer instance correctly and resets memory pointer.

        # Returns:
            None

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.

        """

        self.raise_on_not_init()
        self.api.destroy(self.rx)
        self.rx = 0
