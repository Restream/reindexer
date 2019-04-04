# TODO NOT IMPLEMENTED YET
# TODO dynamic setters which return self. e.g.: indexDef.name('test_name').is_pk().is_dense()
# TODO check types for each attrs of index definition
# TODO check possible values for attrs field_type, index_type, collate_mode


class IndexDefinition(dict):
    """ IndexDefinition is a dictionary subclass which allows to construct and manage indexes more efficiently.
    NOT IMPLEMENTED YET. USE FIELDS DESCRIPTION ONLY.

    # Arguments:
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
    """

    def __getitem__(self, attr):
        self._raise_if_key_error(attr)
        return super(IndexDefinition, self).get(attr)

    def __setitem__(self, attr, value):
        self._raise_if_key_error(attr)
        super(IndexDefinition, self).update({attr: value})
        return self

    def update(self, dict_part={}):
        raise NotImplementedError(
            'Bulk update is not implemented for IndexDefinition instance')

    def _get_known_attrs(self):
        return ['name', 'json_paths', 'field_type', 'index_type', 'is_pk',
                'is_array', 'is_dense', 'is_sparse', 'collate_mode', 'sort_order_letters', 'expire_after', 'config']

    def _raise_if_key_error(self, attr):
        known_attrs = self._get_known_attrs()
        if attr not in known_attrs:
            raise KeyError("Invalid key '{0}'. Known keys are: '{1}'".format(
                attr, ', '.join(known_attrs)))
