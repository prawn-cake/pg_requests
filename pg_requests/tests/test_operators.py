# -*- coding: utf-8 -*-
import unittest
from pg_requests.operators import Or, And, Q, F


class OperatorsTest(unittest.TestCase):
    def test_and_operator(self):
        condition = And({'a': 1, 'b__gt': 2, 'c__lt': 3, 'd__gte': 4,
                         'e__lte': 5, 'f__eq': 'test_eq', 'g__neq': 'test_neq',
                         'h__in': ['p1', 2], 'i__is': True, 'j__is_not': None,
                         'k__like': 'name_%', 'l__similar_to': '%(b|d)%',
                         'm__ilike': 'name_i%'})
        parts, values = condition.eval()
        expected_parts = (
            'j IS NOT %s',
            'e <= %s',
            'd >= %s',
            'a = %s',
            'h IN %s',
            'f = %s',
            'b > %s',
            'c < %s',
            'i IS %s',
            'g != %s',
            'k LIKE %s',
            'l SIMILAR TO %s',
            'm ILIKE %s',)
        expected_values = (None, 5, 4, 1, ['p1', 2], 'test_eq', 2, 3, True,
                           'test_neq', 'name_%', '%(b|d)%', 'name_i%')
        for p in expected_parts:
            self.assertIn(p, parts)

        for v in expected_values:
            self.assertIn(v, values)

    def test_and_operator_multiple_values(self):
        cond_1 = And({'a': 1}, {'b': 2})
        cond_2 = Or({'a__lt': 10}, {'a__gt': 1})
        val_1 = cond_1.eval()
        val_2 = cond_2.eval()
        expected_1 = (
            ('( a = %s AND b = %s )', (1, 2)),
            ('( b = %s AND a = %s )', (2, 1))
        )
        expected_2 = (
            ('( a < %s OR a > %s )', (10, 1)),
            ('( a > %s OR a < %s )', (1, 10))
        )
        self.assertIn(val_1, expected_1)
        self.assertIn(val_2, expected_2)

    def test_or_operator(self):
        condition = Or({'a': 'test', 'b__gt': 2})
        expected = (
            ('( a = %s OR b > %s )', ('test', 2)),
            ('( b > %s OR a = %s )', (2, 'test'))
        )
        self.assertIn(condition.eval(), expected)

    def test_operators_with_one_arg(self):
        # Check cornerstone cases
        or_cond, and_cond = Or({'a': 1}), And({'a': 1})
        value = and_cond.eval()
        self.assertTrue(
            or_cond.eval() == value == ('( a = %s )', (1, )), value)

    def test_or_and_combinations_1(self):
        condition = Or(And({'a1__in': [1, 'test'], 'a2__lte': 2}),
                       And({'b1__is': True, 'b2': 'test_b2'}))
        sql_str, values = condition.eval()
        expected_parts = (
            ('a1 IN %s AND a2 <= %s', 'a2 <= %s AND a1 IN %s'),
            ('b1 IS %s AND b2 = %s', 'b2 = %s AND b1 IS %s')
        )
        expected_values = ([1, 'test'], 2, True, 'test_b2',)
        for p1, p2 in expected_parts:
            debug_str = '\n'.join([p1, p2, sql_str])
            self.assertTrue(p1 in sql_str or p2 in sql_str, debug_str)
        for val in expected_values:
            self.assertIn(val, values)

    def test_or_and_combination_2(self):
        # Stupid case, but anyway..Or here is not needed, check that parser
        # works correctly
        condition = And(Or(And({'a': 1, 'b__gt': 2})), {'c': 3})
        sql_str, values = condition.eval()

        # Expect something like: ( ( ( b > %s AND a = %s ) ) AND c = %s )
        expected_parts = (
            ('( b > %s AND a = %s )', '( a = %s AND b > %s )'),
            'AND c = %s )')
        expected_values = (1, 2, 3, )
        for p in expected_parts:
            if isinstance(p, tuple):
                self.assertTrue(p[0] in sql_str or p[1] in sql_str)
            else:
                self.assertIn(p, sql_str)
        for v in expected_values:
            self.assertIn(v, expected_values)


class QueryObjectTest(unittest.TestCase):
    def test_or(self):
        op1 = Q(a=1, b=2)
        op2 = Q(a=3, b=4)
        res = (op1 | op2).eval()
        expected = (
            ('( ( a = %s AND b = %s ) OR ( a = %s AND b = %s ) )',
             (1, 2, 3, 4)),

            ('( ( b = %s AND a = %s ) OR ( b = %s AND a = %s ) )',
             (2, 1, 4, 3))
        )
        self.assertIn(res, expected)

    def test_and(self):
        op1 = Q(a=1, b=2)
        op2 = Q(a=3, b=4)
        res = (op1 & op2).eval()
        expected = (
            ('( ( a = %s AND b = %s ) AND ( a = %s AND b = %s ) )',
             (1, 2, 3, 4)),

            ('( ( b = %s AND a = %s ) AND ( b = %s AND a = %s ) )',
             (2, 1, 4, 3))
        )
        self.assertIn(res, expected)


class FieldExpressionTest(unittest.TestCase):
    def test_basic(self):
        res = F('total').eval()
        self.assertEqual(res, 'total')

    def test_arithmetic(self):
        res = F('total') + 1
        self.assertIsInstance(res, F)
        self.assertEqual(res.eval(), 'total + 1')

        res = F('total') - 1
        self.assertEqual(res.eval(), 'total - 1')

        res = F('total') * 10
        self.assertEqual(res.eval(), 'total * 10')

        res = F('total') / 10
        self.assertEqual(res.eval(), 'total / 10')

