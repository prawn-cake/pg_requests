# -*- coding: utf-8 -*-
from collections import OrderedDict, Iterable
import copy
from pg_requests.operators import JOIN

from pg_requests.tokens import Token, CommaValue, StringValue, \
    FilterValue, NullValue, TupleValue, DictValue, CommaDictValue


class QueryBuilder(object):
    """Basic query builder implementation class"""

    # OrderedDict must use here, define tokens and templates for each new query
    # class

    # NOTE: tokens order is important
    TOKENS = OrderedDict([
        # Format: {token_key}, pg_requests.tokens.Token instance, example:
        # ('SELECT', Token(template='FROM {}', value_type=StringValue)),
    ])

    def __init__(self):
        # Init tokens with templates
        # NOTE: tokens order is important
        self.tokens = copy.deepcopy(self.TOKENS)

    def _set_token_value(self, token_name, value):
        """Token value setter

        :param token_name: str: name of token, i.e 'SELECT', 'FROM', etc
        :param value: value of token
        """
        token = self._get_token(token_name)
        token.value = value

    def _get_token(self, token_name):
        """Token getter

        :param token_name: str: token name
        :return: Token instance
        """
        return self.tokens[token_name]

    def _reset_tokens(self, exclude=None):
        """Reset all token values excluding given

        :param exclude: Iterable: keys of exclude tokens
        """
        for key, token in self.tokens.items():
            if exclude and isinstance(exclude, Iterable):
                if key in exclude:
                    continue
            token.reset()

    def _set_table_name(self, token_name, value):
        """Table name setter. It has different logic due to manual
        implementation of substitution of table name

        :param token_name: str
        :param value: str
        """
        # Format template with sanitized value
        self._set_token_value(token_name, self._sanitize_table_name(value))

    @staticmethod
    def _sanitize_table_name(name):
        """Due to the fact that psycopg2 doesn't allow to parametrize table
        name in requests (it must be hardcoded by default) add this possibility
        manually. This method sanitize table name against SQL injections

        :param name: str: table name string value
        :return: table_name_str
        :raise ValueError: if value is wrong
        """
        if not isinstance(name, str):
            raise ValueError(
                'Wrong table name string data type, must be basestring, '
                'given: {}'.format(type(name)))

        tokens = name.split(' ')
        if len(tokens) > 1:
            raise ValueError(
                "Suspicious table name '%s'. Looks like injection" % name)
        return name

    @staticmethod
    def _build_query(tokens):
        """Build query tuple

        :param tokens:
        :rtype : tuple
        :return: tuple query data

        Return value example:
            "INSERT INTO test (num, data) VALUES (%s, %s)", (42, 'bar')

        """
        sql_str_parts, values = [], ()
        for key, token in tokens.items():
            # Skip tokens which are not set
            if not token.is_set:
                continue

            # Can be either evaluated string or tuple where first element is
            # sql string and second is substitution value
            eval_result = token.eval()
            if isinstance(eval_result, str):
                # sql string
                sql_str_parts.append(eval_result)
            elif isinstance(eval_result, tuple):
                # sql string + tuple of values
                sql_str_parts.append(eval_result[0])

                # merge values tuple
                values += tuple(eval_result[1])

        query = (' '.join(sql_str_parts), values)
        return query

    def get_raw(self):
        """Get raw built sql

        :rtype : tuple
        :return: raw build result tuple
        Example:
            "INSERT INTO test (num, data) VALUES (%s, %s)", (42, 'bar')

        """
        return self._build_query(self.tokens)

    def execute(self, cursor):
        """Build queryset and execute it

        :param cursor: connection.cursor: instance
        :return: cursor: connection.cursor: cursor.execute

        Trick:
             result = qf.select('MyTable')\
                        .fields('id', 'user')\
                        .limit(10)\
                        .execute(cur)\
                        .fetchall()
        """
        cursor.execute(self.mogrify(cursor))
        return cursor

    def mogrify(self, cursor):
        """Return a query string after arguments binding

        :param cursor:
        :return:
        """
        return cursor.mogrify(*self.get_raw())


class SelectQuery(QueryBuilder):
    """Select query builder.
    The idea is to use builder pattern to make select query to database with
    query tokens.

    Example:
    >>> from pg_requests import query_facade as qf
    >>> qf.select('MyTable').fields('a', 'b').filter(score__gt=0).order_by('a').desc()
    """
    TOKENS = OrderedDict([
        ('SELECT', Token(template='SELECT {}', value_type=CommaValue,
                         required=True)),
        # Table tokens
        ('FROM', Token(template='FROM {}', value_type=StringValue)),
        ('FROM__ALIAS', Token(template="AS '{}' ", value_type=StringValue)),

        # User-function tokens
        # Use sub-token to glue token str value + sub-token value without space
        ('FROM__FN', Token(template='FROM {}',
                           value_type=StringValue,
                           subtoken=Token(template='({})',
                                          value_type=TupleValue))),
        ('FROM__FN_NAME', Token(template='FROM {}', value_type=StringValue)),
        ('FROM__FN_ARGS', Token(template='({})', value_type=TupleValue)),

        ('JOIN', Token(template='{join_type} {table_name}',
                       value_type=DictValue)),
        # NOTE: JOIN__ON and JOIN__USING mutual exclusive
        # FIXME: JOIN__ON must be conditional type
        ('JOIN__ON', Token(template='ON ({})', value_type=StringValue)),
        ('JOIN__USING', Token(template='USING ({})', value_type=CommaValue)),

        # NOTE: here is quite complex logic, see ConditionalValue imp
        ('WHERE', Token(template='WHERE {}', value_type=FilterValue)),

        ('GROUP_BY', Token(template='GROUP BY {}', value_type=CommaValue)),

        # TODO: add tests for having
        ('GROUP_BY__HAVING', Token(template='HAVING {}', 
                                   value_type=FilterValue)),

        ('ORDER_BY', Token(template='ORDER BY {}', value_type=CommaValue)),
        ('DESC', Token(template='DESC', value_type=NullValue)),
        ('LIMIT', Token(template='LIMIT {}', value_type=StringValue)),
        ('OFFSET', Token(template='OFFSET {}', value_type=StringValue)),
    ])

    def fields(self, *fields):
        """Select fields to fetch

        :param fields: list of str
        :return: self
        """

        # Filter False parameters
        fields = list(filter(None, fields))
        if fields:
            # Substitute as SELECT %s, %s...
            self._set_token_value('SELECT', fields)
        else:
            self._set_token_value('SELECT', '*')
        return self

    def call_fn(self, fn_name, args):
        """Call user-defined function, like
        SELECT * FROM my_function('param1', 2, True)

        :param fn_name: str: function name
        :param args: tuple: function arguments tuple
        :return: function result
        """
        self.fields('*')._set_token_value('FROM__FN', fn_name)
        # Custom setter for sub-token
        token = self._get_token('FROM__FN')
        # NOTE: subtoken value will be glued with the main token value without
        # a space
        token.subtoken.value = args
        return self

    def select(self, table_name, alias=None):
        """Select from a table. It means SQL FROM operator.

        :param table_name: str: table name
        :param alias: alias for a table - 'AS' keyword
        """
        # TODO: add sub-query feature + custom frontend function

        # Set default selection fields as '*'
        sanitized_tn = self._sanitize_table_name(table_name)
        self.fields('*')._set_token_value('FROM', sanitized_tn)

        # NOTE: SELECT * FROM <table_name> AS <alias>
        if alias is not None:
            self._set_token_value('FROM__ALIAS', alias)
        return self

    def join(self, table_name, join_type=JOIN.INNER, on=None, using=None):
        if join_type not in JOIN:
            raise ValueError(
                "Wrong join type '%r', must be '%r'" % (join_type, JOIN))
        self._set_token_value(
            'JOIN', dict(join_type=join_type, table_name=table_name))

        # FIXME: JOIN__ON must be conditional type or not ???
        if on is not None:
            self._set_token_value('JOIN__ON', on)
        elif using is not None:
            self._set_token_value('JOIN__USING', using)

        return self

    def filter(self, *args, **kwargs):
        token = self._get_token('WHERE')
        new_value = None
        if args:
            # In case of QueryOperators
            new_value = args[0]
        elif kwargs:
            # In case of simple key-value filters
            new_value = kwargs

        # Stub to prevent errors
        if new_value is None:
            return self

        if token.value:
            token.value.update(new_value)
        else:
            self._set_token_value('WHERE', new_value)
        return self

    # NOTE: python 3 syntax only
    # def order_by(self, *args, desc=False):
    def order_by(self, *args, **kwargs):
        args = list(filter(None, args))
        if args:
            self._set_token_value('ORDER_BY', args)

        # Descending order option
        if kwargs.get('desc'):
            self._set_token_value('DESC', True)
        return self

    def desc(self):
        self._set_token_value('DESC', True)
        return self

    def limit(self, value):
        self._set_token_value('LIMIT', int(value))
        return self

    def offset(self, value):
        self._set_token_value('OFFSET', int(value))
        return self

    def group_by(self, *args):
        args = list(filter(None, args))
        if args:
            self._set_token_value('GROUP_BY', args)
        return self

    def having(self, *args, **kwargs):
        if args:
            self._set_token_value('GROUP_BY__HAVING', args[0])
        elif kwargs:
            self._set_token_value('GROUP_BY__HAVING', kwargs)
        return self


class InsertQuery(QueryBuilder):
    """Insert query builder.

    Query example:

    >>> insert('users')\
        .data(name='Alex', gender='M')\
        .returning('id')\
        .execute(cursor)

    """

    TOKENS = OrderedDict([
        ('INSERT', Token(template='INSERT INTO {}', value_type=StringValue)),
        ('DEFAULT', Token(template='DEFAULT VALUES', value_type=NullValue)),

        # part of: INSERT INTO table ({fields})
        ('fields', Token(template='({})', value_type=CommaValue)),
        ('VALUES', Token(template='VALUES ({})', value_type=TupleValue)),
        # ('values_multi', Token(template='VALUES %s')),  # for multiple rows
        ('RETURNING', Token(template='RETURNING {}', value_type=CommaValue))
    ])

    def insert(self, table_name):
        self._set_table_name('INSERT', table_name)
        return self

    def data(self, **kwargs):
        """Insert values data

        :return:
        """
        self._set_token_value('fields', tuple(kwargs.keys()))
        self._set_token_value('VALUES', tuple(kwargs.values()))
        return self

    def values_multi(self, list_of_values):
        """Method allows to build multiple rows insert

        :param list_of_values: Iterable: list of multiple rows values
        For example:
            [('Alex', 'M'), ('Jane', 'F')]

        """
        if isinstance(list_of_values, Iterable):
            prepared_values = [', '.join(item) for item in list_of_values]
            self._set_token_value(
                'values_multi',
                # prepare rows by join of prepared_values
                ', '.join(['({})'.format(item) for item in prepared_values])
            )
        return self

    def defaults(self):
        """Allows to insert row with all defaults values
        Simulate the following: INSERT INTO {table_name} DEFAULT VALUES'
        """
        # reset tokens exclude table name
        self._reset_tokens(exclude=('INSERT', ))
        self._set_token_value('DEFAULT', True)
        return self

    def returning(self, *fields):
        fields = list(filter(None, fields))
        self._set_token_value('RETURNING', fields)
        return self


class UpdateQuery(QueryBuilder):
    TOKENS = OrderedDict([
        ('UPDATE', Token(template='UPDATE {}', value_type=StringValue,
                         required=True)),
        ('SET', Token(template='SET {}', value_type=CommaDictValue,
                      required=True)),
        ('FROM', Token(template='FROM {}', value_type=StringValue)),
        ('WHERE', Token(template='WHERE {}', value_type=FilterValue)),
    ])

    # NOTE: this is a full copy from SelectQuery
    def filter(self, *args, **kwargs):
        token = self._get_token('WHERE')
        new_value = None
        if args:
            # In case of QueryOperators
            new_value = args[0]
        elif kwargs:
            # In case of simple key-value filters
            new_value = kwargs

        # Stub to prevent errors
        if new_value is None:
            return self

        if token.value:
            token.value.update(new_value)
        else:
            self._set_token_value('WHERE', new_value)
        return self

    def update(self, table_name):
        """Update a table

        :param table_name: str
        :return: self
        """
        sanitized_tn = self._sanitize_table_name(table_name)
        self._set_token_value('UPDATE', sanitized_tn)
        return self

    def data(self, **kwargs):
        self._set_token_value('SET', kwargs)
        return self

    def _from(self, table_name):
        """Update FROM (JOIN in fact) postgres syntax

        SQL representation:
            UPDATE accounts
                SET contact_first_name = first_name,
                    contact_last_name = last_name
            FROM salesmen
            WHERE salesmen.id = accounts.sales_id;

        :param table_name: str
        :return: self
        """
        sanitized_tn = self._sanitize_table_name(table_name)
        self._set_token_value('FROM', sanitized_tn)
        return self


class DeleteQuery(QueryBuilder):
    pass


class QueryFacade(object):
    """Query facade. Combine all queries into the one facade

    Usage:
    >>> from pg_requests import query_facade as qf

    >>> qs = qf.select('MyTable').fields('id', 'name').execute(cursor)
    >>> result_set = qs.fetchall()
    """

    @staticmethod
    def select(table_name, alias=None):
        return SelectQuery().select(table_name, alias=alias)

    @staticmethod
    def call_fn(fn_name, args):
        return SelectQuery().call_fn(fn_name, args=args)

    @staticmethod
    def insert(table_name):
        return InsertQuery().insert(table_name)

    @staticmethod
    def update(table_name):
        return UpdateQuery().update(table_name)

    @staticmethod
    def delete(table_name):
        raise NotImplementedError('Not implemented yet')