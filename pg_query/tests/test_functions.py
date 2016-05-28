# -*- coding: utf-8 -*-
import unittest
from pg_query.functions import fn, Function


class FunctionFactoryTest(unittest.TestCase):
    def test_get_new_function(self):
        f = fn.COUNT
        self.assertIsInstance(f, Function)
        self.assertEqual(f.name, 'COUNT')
        self.assertEqual(f('*'), 'COUNT(*)')
