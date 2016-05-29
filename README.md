PG Query
========
[![Build Status](https://travis-ci.org/prawn-cake/pg_query.svg?branch=master)](https://travis-ci.org/prawn-cake/pg_query)
[![Coverage Status](https://coveralls.io/repos/github/prawn-cake/pg_query/badge.svg?branch=master)](https://coveralls.io/github/prawn-cake/pg_query?branch=master)
![PythonVersions](https://www.dropbox.com/s/ck0nc28ttga2pw9/python-2.7_3.4-blue.svg?dl=1)

PostgreSQL-specific query builder.

## Querying

The library provides flexible way to make queries. All queries are executed in a safe manner, 
i.e every query is a tuple of sql template string and tuple of parameters, e.g 
    
    ('SELECT * FROM users WHERE ( name = %s )', ('Mr.Robot',))
    
And this is already prepared query for `psycopg2.cursor` execution, rely on that it also excludes sql-injection chances. 

**Starting point**
    
    from pg_query import query_facade as qf

### Select
    
    qf.select(<table_name>)\
      .fields(<field args>)\
      .filter(<django-style filter conditions>)

#### Example
    
    result_set = qf.select('users')\
        .fields('id', 'name')\
        .filter(created_at__gt=datetime.now() - timedelta(days=30))\
        .order_by('created_at').desc()\
        .limit(10)\
        .offset(20)\
        .execute(cursor).fetchall()


#### Complex conditions

#### Join
    
    # USING syntax
    qf.select('users').join('customers', using=('id', )).execute(cursor)
    
    # The following SQL will be executed
    SELECT * FROM users INNER JOIN customers USING (id)

#### Functions

##### Aggregation
    
    ...
    from pg_query.functions import fn
    
    qf.select('users').fields(fn.COUNT('*')).filter(name='Mr.Robot')
    
    # Query tuple
    ('SELECT COUNT(*) FROM users WHERE ( name = %s )', ('Mr.Robot',))

##### Calling stored procedures (user-defined database functions)

    qf.call_fn('my_user_function', args=(1, 'str value', False))    
    
    # Query tuple
    ('SELECT * FROM my_user_function(%s, %s, %s)', (1, 'str value', False))
    
### Insert

#### Example

    qf.insert('users')\
        .data(name='x', login='y')\
        .returning('id')\
        .execute(cursor)
    
    
### Update

Not implemented


### Delete

Not implemented