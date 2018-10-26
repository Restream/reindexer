class QueryResults:
    """ QueryResults is a disposable iterator of Reindexer results for such queries as SELECT and etc.
    When the results are fetched the iterator closes and frees a memory of results buffer of Reindexer

    # Attributes:
        api (module): An API module for Reindexer calls
        err_code (int): the API error code
        err_msg (string): the API error message
        qres_wrapper_ptr (int): A memory pointer to Reindexer iterator object 
        qres_iter_count (int): A count of results for iterations
        pos (int): The current result position in iterator
    """

    def __init__(self, api, qres_wrapper_ptr, qres_iter_count):
        """Constructs a new Reindexer query results iterator object.

        # Arguments:
            api (module): An API module for Reindexer calls
            qres_wrapper_ptr (int): A memory pointer to Reindexer iterator object 
            qres_iter_count (int): A count of results for iterations

        """

        self.api = api
        self.qres_wrapper_ptr = qres_wrapper_ptr
        self.qres_iter_count = qres_iter_count
        self.pos = 0

    def __iter__(self):
        """Returns the current iteration result.

        """

        return self

    def __next__(self):
        """Returns the next iteration result.

        # Raises:
            StopIteration: Frees results on end of iterator and raises with iteration stop.

        """

        if self.pos < self.qres_iter_count:
            self.pos += 1
            self.err_code, self.err_msg, res = self.api.query_results_iterate(
                self.qres_wrapper_ptr)
            if (self.err_code):
                raise (self.err_msg)
            return res
        else:
            del(self)
            raise StopIteration

    def __del__(self):
        """Calls close iterator method on an iterator object deletion

        """

        self._close_iterator()

    def count(self):
        """Returns a count of results

        # Returns
            int: A count of results

        """

        return self.qres_iter_count

    def _close_iterator(self):
        """Frees query results for the current iterator

        """

        self.qres_iter_count = 0
        self.api.query_results_delete(self.qres_wrapper_ptr)
