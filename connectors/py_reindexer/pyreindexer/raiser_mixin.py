class RaiserMixin(object):
    """ RaiserMixin contains methods for checking some typical API bad events and raise if there is a necessity.

    """

    def raise_on_error(self):
        """Checks if there is an error code and raises with an error message.

        # Raises:
            Exception: Raises with an error message of API return on non-zero error code.

        """

        if self.err_code:
            raise Exception(self.err_msg)

    def raise_on_not_init(self):
        """Checks if there is an error code and raises with an error message.

        # Raises:
            Exception: Raises with an error message of API return if Reindexer instance is not initialized yet.

        """

        if self.rx <= 0:
            raise Exception("Connection is not initialized")
