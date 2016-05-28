# -*- coding: utf-8 -*-
import unittest
from pg_query.exceptions import TokenError
from pg_query.operators import And, JOIN
from pg_query.tokens import Token, TupleValue, CommaValue, StringValue, \
    NullValue, ConditionalValue, DictValue


class TokensTest(unittest.TestCase):
    def test_token_with_tuple_value_type(self):
        token = Token(template='VALUES ({})', value_type=TupleValue)
        with self.assertRaises(ValueError) as err:
            token.value = 'a'
            self.assertIn('must be list or tuple', str(err))

        token.value = ('a', 3)
        self.assertEqual(token.eval(), ("VALUES (%s, %s)", ('a', 3)),)

    def test_token_with_iterable_value(self):
        token = Token(template='({})', value_type=CommaValue)
        with self.assertRaises(ValueError) as err:
            token.value = 'a'
            self.assertIn('must be list or tuple', str(err))
        token.value = ['a', 'b', 'c']
        self.assertEqual(token.eval(), "(a, b, c)")

    def test_token_with_string_value(self):
        token = Token(template='INSERT INTO {}', value_type=StringValue)
        token.value = 'MyTable'
        self.assertIsInstance(token.value, StringValue, token.value)
        self.assertEqual(token.eval(), "INSERT INTO MyTable")

    def test_token_with_dict_value_and_join_types(self):
        token = Token(template='{join_type} {table_name}',
                      value_type=DictValue)
        # Check correctness of all join types
        for join_type in ('INNER', 'CROSS', 'LEFT_OUTER', 'RIGHT_OUTER',
                          'FULL_OUTER'):
            token.value = dict(join_type=getattr(JOIN, join_type),
                               table_name='MyTable')
            self.assertIsInstance(token.value, DictValue, token.value)
            self.assertEqual(
                token.eval(), "{} MyTable".format(getattr(JOIN, join_type)))

    def test_token_with_null_value(self):
        token = Token(template='DEFAULT VALUES', value_type=NullValue)
        token.value = "any value shouldn't be appeared"
        self.assertIsInstance(token.value, NullValue)
        self.assertIsInstance(token.value, NullValue)
        self.assertEqual(token.eval(), "DEFAULT VALUES")

    def test_token_with_conditional_value(self):
        token = Token(template='WHERE {}', value_type=ConditionalValue)
        token.value = {'a': 1, 'b__gt': 2, 'c__lt': 3, 'd__gte': 4,
                       'e__lte': 5, 'f__eq': 'test_eq', 'g__neq': 'test_neq',
                       'h__in': ['p1', 2], 'i__is': True, 'j__is_not': None}

        self.assertIsInstance(token.value.value, And)
        sql_template, values = token.eval()
        expected_parts = (
            "WHERE",
            "AND",
            "a = %s",
            "b > %s",
            "c < %s",
            "d >= %s",
            "e <= %s",
            "f = %s",
            "g != %s",
            "h IN %s",
            "i IS %s",
            "j IS NOT %s",
        )
        expected_values = (
            1, 2, 3, 4, 5, 'test_eq', 'test_neq', ['p1', 2], True, None)
        for part in expected_parts:
            self.assertIn(part, sql_template,
                          '%s is not found in\n%s' % (part, sql_template))
        for val in expected_values:
            self.assertIn(val, values,
                          '%s is not found in\n%s' % (val, expected_values))

    def test_token_required(self):
        token = Token(template='INSERT INTO {}', value_type=StringValue,
                      required=True)
        with self.assertRaises(TokenError) as err:
            token.eval()
            self.assertIn('is not set and required', str(err))

        token.value = 'reports'
        val = token.eval()
        self.assertIsInstance(val, str)

    def test_subtoken_eval(self):
        t = Token(template='FROM {}', value_type=StringValue,
                  subtoken=Token(template='({})', value_type=TupleValue))
        t.value = 'my_fn'
        t.subtoken.value = ('test', 2, True, )
        result = t.eval()
        self.assertEqual(result, ('FROM my_fn(%s, %s, %s)', ('test', 2, True)))

    def test_subtoken_eval_with_multiple_tuple_values(self):
        """Check correctness of composing tuple str and tuple values
        This is not a real case
        """
        t = Token(template='({})', value_type=TupleValue,
                  subtoken=Token(template='({})', value_type=TupleValue))
        t.value = ('val', 1, False, )
        t.subtoken.value = ('subtoken_val', 2, True, )
        result = t.eval()
        expected = ('(%s, %s, %s)(%s, %s, %s)',
                    ('val', 1, False, 'subtoken_val', 2, True,))
        self.assertEqual(result, expected)


class TokenValuesTest(unittest.TestCase):
    def test_string_value(self):
        t_val = StringValue('test_value')
        self.assertIsInstance(t_val.value, str)
        self.assertEqual(t_val.eval(), 'test_value')

    def test_null_value(self):
        t_val = NullValue('test_value')
        self.assertEqual(t_val.eval(), None)

    def test_iterable_value(self):
        t_val = CommaValue(['id', 'name'])
        self.assertIsInstance(t_val.value, list)
        self.assertEqual(t_val.eval(), 'id, name')

    def test_tuple_value(self):
        pass

    def test_conditional_value(self):
        pass

    def test_dict_value(self):
        t_val = DictValue(dict(a=1, b=2))
        self.assertIsInstance(t_val.value, dict)
        self.assertEqual(t_val.eval(), dict(a=1, b=2))

        with self.assertRaises(ValueError) as err:
            t_val = DictValue(1)
            self.assertIsNone(t_val)
            self.assertIn('must be dict', str(err))
