# -*- coding: utf-8 -*-
import unittest
from pg_requests import query_facade as qf
from pg_requests.functions import fn
from pg_requests.operators import And, Q, JOIN


class BaseQueryBuilderTest(unittest.TestCase):
    pass


class SelectQueryTest(unittest.TestCase):
    def test_simple_select(self):
        sql_tpl = qf.select('MyTable').get_raw()
        expected_calls = ('SELECT * FROM MyTable', ())
        self.assertEqual(sql_tpl, expected_calls)

    def test_select_with_params(self):
        sql_tpl = qf.select('MyTable').fields('id', 'name').get_raw()
        expected_calls = ('SELECT id, name FROM MyTable', (),)
        self.assertEqual(sql_tpl, expected_calls)

    def test_select_with_all_params(self):
        sql_tpl = qf.select('MyTable')\
            .fields('id', 'name', 'visits')\
            .filter(name='John', visits__gte=5)\
            .group_by('visits')\
            .order_by('id', 'name').desc().limit(10)\
            .offset(5)\
            .get_raw()

        # Expect different order of parameters, but still correct
        expected_calls = (
            ('SELECT id, name, visits FROM MyTable WHERE ( name = %s AND '
             'visits >= %s ) GROUP BY visits ORDER BY id, name DESC LIMIT 10 '
             'OFFSET 5',
             ('John', 5)),

            ('SELECT id, name, visits FROM MyTable WHERE ( visits >= %s AND '
             'name = %s ) GROUP BY visits ORDER BY id, name DESC LIMIT 10 '
             'OFFSET 5',
             (5, 'John'))
        )
        self.assertIn(sql_tpl, expected_calls)

    def test_select_user_function(self):
        """Test query like SELECT * FROM my_fn('arg1', 'arg2')

        """
        result = qf.call_fn('my_fn', args=('a', 1, True)).get_raw()
        expected = (
            ('SELECT * FROM my_fn(%s, %s, %s)', ('a', 1, True, ))
        )
        self.assertEqual(result, expected)

    def test_filter_with_condition_operator(self):
        result = qf.select('MyTable')\
            .fields('a', 'b')\
            .filter(And(a__in=['val1', 'val2'])).get_raw()

        expected_result = (
            'SELECT a, b FROM MyTable WHERE ( a IN %s )',
            (['val1', 'val2'],))
        self.assertEqual(result, expected_result)

    def test_join_with_using_keyword(self):
        sql, values = qf.select('users')\
            .join('customers', using=('id', 'name')).get_raw()
        self.assertEqual(
            sql, 'SELECT * FROM users INNER JOIN customers USING (id, name)')

    def test_with_different_join_types(self):
        query = qf.select('users')\
            .join('customers', join_type=JOIN.RIGHT_OUTER, using=('id', ))\
            .filter(users__name='Mr.Robot').get_raw()
        expected = ('SELECT * FROM users RIGHT OUTER JOIN customers USING (id)'
                    ' WHERE ( users.name = %s )',
                    ('Mr.Robot',))
        self.assertEqual(query, expected)

    def test_select_with_agg_functions(self):
        raw_query = qf.select('users')\
            .fields(fn.COUNT('*'))\
            .filter(name='Mr.Robot').get_raw()
        self.assertEqual(
            raw_query,
            ('SELECT COUNT(*) FROM users WHERE ( name = %s )', ('Mr.Robot',)))

    def test_select_with_multiple_filter_calls(self):
        """Test the corner case when .filter() method is being called multiple
        times. Query builder concatenate it with AND operator
        """
        # With kwargs
        query = qf.select('users')\
            .filter(name='Mr.Robot')\
            .filter(login='anonymous')\
            .get_raw()
        self.assertEqual(
            query,
            ('SELECT * FROM users WHERE ( ( name = %s ) AND ( login = %s ) )',
             ('Mr.Robot', 'anonymous'))
        )

        # With Q objects
        query = qf.select('users')\
            .filter(Q(name='Mr.Robot') | Q(login='anonymous'))\
            .filter(Q(name='John'))\
            .get_raw()

        expected_sets = (
            ('SELECT * FROM users WHERE ( ( ( name = %s ) OR ( login = %s ) ) '
             'AND ( name = %s ) )',
             ('Mr.Robot', 'anonymous', 'John')),

            ('SELECT * FROM users WHERE ( ( ( login = %s ) OR ( name = %s ) ) '
             'AND ( name = %s ) )',
             ('anonymous', 'Mr.Robot', 'John')),
        )
        self.assertIn(query, expected_sets)

    def test_select_with_having_clause(self):
        query = qf.select('users')\
            .fields(fn.COUNT('*', alias='cnt'))\
            .filter(name='Mr.Robot')\
            .having(cnt__gte=4).get_raw()
        expected = (
            "SELECT COUNT(*) AS 'cnt' FROM users WHERE ( name = %s ) "
            "HAVING ( cnt >= %s )", ('Mr.Robot', 4,))
        self.assertEqual(query, expected)


class InsertQueryTest(unittest.TestCase):
    def test_insert_single_row(self):
        sql_tpl = qf.insert('MyTable')\
            .data(name='Alex', gender='M')\
            .get_raw()

        # Expect return values in different, but still correct order
        expected_calls = (
            ('INSERT INTO MyTable (name, gender) VALUES (%s, %s)',
             ('Alex', 'M')),

            ('INSERT INTO MyTable (gender, name) VALUES (%s, %s)',
             ('M', 'Alex')),
        )
        self.assertIn(sql_tpl, expected_calls)

    def test_insert_single_row_with_returning_value(self):
        sql_tpl = qf.insert('MyTable')\
            .data(name='Alex', gender='M')\
            .returning('id')\
            .get_raw()

        # Expect return values in different order
        expected_calls = (
            ('INSERT INTO MyTable (name, gender) VALUES (%s, %s) RETURNING id',
             ('Alex', 'M')),

            ('INSERT INTO MyTable (gender, name) VALUES (%s, %s) RETURNING id',
             ('M', 'Alex')),
        )
        self.assertIn(sql_tpl, expected_calls)

    def test_insert_row_with_all_defaults_values(self):
        sql_tpl = qf.insert('MyTable').defaults().get_raw()

        expected_calls = (
            'INSERT INTO MyTable DEFAULT VALUES', ())
        self.assertEqual(sql_tpl, expected_calls)

    @unittest.skip('fix it later')
    def test_insert_multiple_rows(self):
        values = [('Alex', 'M'), ('Jane', 'F')]
        sql_tpl = qf.insert('MyTable')\
            .values_multi(values)\
            .get_raw()

        expected_calls = (
            'INSERT INTO MyTable VALUES %s', ('(Alex, M), (Jane, F)', )
        )
        self.assertEqual(sql_tpl, expected_calls)


class UpdateQueryTest(unittest.TestCase):
    def test_simple_update(self):
        query = qf.update('users').filter(name='Mr.Robot')\
            .data(balance='balance + 100').get_raw()
        expected = ('UPDATE users SET balance = %s WHERE ( name = %s )',
                    ('balance + 100', 'Mr.Robot'))
        self.assertEqual(query, expected)

    def test_update_from_multiple_tables(self):
        query = qf.update('users')._from('customers')\
            .data(users__value='customers.value')\
            .filter(users__id='customers.id').get_raw()
        expected = (
            'UPDATE users SET users.value = %s '
            'FROM customers WHERE ( users.id = %s )',
            ('customers.value', 'customers.id'))
        self.assertEqual(query, expected)
