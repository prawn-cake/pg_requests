# PG Query

PostgreSQL-specific sql query builder sql-injection safe implementation.

## Querying

Starting point
    
    from pg_query import query_facade as qf
    
### Select
    
    qf.select('MyTable')

#### Simple
    
    result_set = qf.select('Users')\
        .fields('id', 'name')\
        .filter(created_at__gt=datetime.now() - timedelta(days=30))\
        .order_by('created_at').desc()\
        .limit(10)\
        .execute(cursor).fetchall()


#### Complex conditions

#### Join


#### Functions


### Insert

### Update

Not implemented


### Delete

Not implemented