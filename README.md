pg_requests
========
[![Build Status](https://travis-ci.org/prawn-cake/pg_requests.svg?branch=master)](https://travis-ci.org/prawn-cake/pg_requests)
[![Coverage Status](https://coveralls.io/repos/github/prawn-cake/pg_requests/badge.svg?branch=master)](https://coveralls.io/github/prawn-cake/pg_requests?branch=master)

PostgreSQL-specific query builder.

## Why?

In some cases using ORM or well-known frameworks is not applicable when we are 
forced to work with database and plain sql directly. 

It's quite easy to write simple sql queries until it becomes cumbersome: 
long multi-line sql queries, wrong arguments evaluation creates risks for sql injections, 
building up sql depends on input parameters and extra conditions make manual way unmaintainable and barely checkable

The library provides handy way to build sql queries for PostgreSQL

## Requirements

* python2.7, python3.4+


## Querying

All queries are executed in a safe manner, 
i.e every query is a tuple of sql template string and tuple of parameters, e.g 
    
    ('SELECT * FROM users WHERE ( name = %s )', ('Mr.Robot',))
    
And this is already prepared query for `psycopg2.cursor` execution, rely on that it also excludes sql-injection chances. 

**Starting point**
    
    from pg_requests import query_facade as qf

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
    
    # Using different join types
    from pg_requests.operators import JOIN
    
    qf.select('users')\
        .join('customers', join_type=JOIN.RIGHT_OUTER, using=('id', ))\
        .filter(users__name='Mr.Robot').execute(cursor)
    
    # Query value
    ('SELECT * FROM users RIGHT OUTER JOIN customers USING (id) WHERE ( users.name = %s )', ('Mr.Robot',))

###### {table}.{field} Key evaluation

To use explicit form *{table}.{field}* you have to use the syntax:

    qf.select('users')\
        .join('customers', using=('id', )).filter(users__name='Mr.Robot').execute(cursor)

#### Functions

##### Aggregation
    
    ...
    from pg_requests.functions import fn
    
    qf.select('users').fields(fn.COUNT('*')).filter(name='Mr.Robot')
    
    # Query tuple
    ('SELECT COUNT(*) FROM users WHERE ( name = %s )', ('Mr.Robot',))

##### Calling stored procedures (user-defined database functions)

    qf.call_fn('my_user_function', args=(1, 'str value', False))    
    
    # Query tuple
    ('SELECT * FROM my_user_function(%s, %s, %s)', (1, 'str value', False))


### Insert

#### Example

    qf.insert('users').data(name='x', login='y').returning('id').execute(cursor)
    
    
### Update

#### Example
    
    # Basic example
    qf.update('users').data(users='Mr.Robot').filter(id=1).execute(cursor)
    
    # Update from foreign table (with JOIN)
    qf.update('users')._from('customers').data(users__value='customers.value')\
        .filter(users__id='customers.id').execute(cursor)
        

**NOTE:** In the last example we use {table}__{field} syntax key evaluation
    


### Delete

Not implemented