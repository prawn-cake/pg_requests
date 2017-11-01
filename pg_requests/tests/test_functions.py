# -*- coding: utf-8 -*-
import unittest
from pg_requests.functions import fn, Function


class FunctionFactoryTest(unittest.TestCase):
    def test_get_new_function(self):
        f = fn.COUNT
        self.assertIsInstance(f, Function)
        self.assertEqual(f.name, 'COUNT')
        self.assertEqual(f('*'), 'COUNT(*)')
        self.assertEqual(fn.COUNT('x'), 'COUNT(x)')

    def test_function_with_alias(self):
        self.assertEqual(fn.MAX('x', alias='max_x'), "MAX(x) AS 'max_x'")
        self.assertEqual(fn.MAX('x', alias='alias with space'),
                         "MAX(x) AS 'alias with space'")
