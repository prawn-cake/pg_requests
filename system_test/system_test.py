# -*- coding: utf-8 -*-
"""Integration system test for database manager"""

import os
import time
import psycopg2
import unittest
import logging
from pg_query import query_facade as qf


USERS = (
    ('Mr.Robot', 'anonymous'),
    ('John', 'john'),
)


class PostgresClientSystemTest(unittest.TestCase):
    DB_USER = 'postgres'
    DB_PASSWORD = 'test'
    DB_NAME = 'test'
    DB_PORT = os.environ.get('POSTGRES_PORT', 5432)
    TABLE_NAME = 'users'

    def setUp(self):
        self.dsn = 'user={} password={} dbname={} host=localhost port={}'\
            .format(self.DB_USER, self.DB_PASSWORD, self.DB_NAME, self.DB_PORT)
        self.conn = self.get_conn(dsn=self.dsn)
        try:
            self._create_table()
        except psycopg2.Error:
            self._drop_table()
            self._create_table()

        # Load fixtures
        self._load_data(self.conn)

    @staticmethod
    def get_conn(dsn, attempts=20, timeout=1):
        for i in range(attempts):
            try:
                conn = psycopg2.connect(dsn)
            except psycopg2.OperationalError as err:
                logging.error(str(err))
                logging.warning(
                    'Check that postgres docker container is started. '
                    'Check README for more information')
                time.sleep(timeout)
                continue
            else:
                return conn

    @staticmethod
    def _load_data(conn):
        with conn.cursor() as cur:
            for item in USERS:
                qf.insert('users')\
                    .data(name=item[0], login=item[1]).execute(cur)

    def _create_table(self):
        # Init database with test data
        with self.conn.get_cursor() as cursor:
            cursor.execute(
                "CREATE TABLE {} "
                "(id SERIAL, "
                "name VARCHAR NOT NULL, "
                "login VARCHAR NOT NULL,);".format(self.TABLE_NAME))
        logging.info('Table {} has been created'.format(self.TABLE_NAME))

    def _drop_table(self):
        with self.conn.get_cursor() as cursor:
            cursor.execute('DROP TABLE {}'.format(self.TABLE_NAME))
        print('Table {} has been dropped'.format(self.TABLE_NAME))

    def tearDown(self):
        self._drop_table()
