Changelog
=========

0.1.0+1 (UNRELEASED)
--------------------
## TODO:
* [Feature] Sub-queries, for example: SELECT * FROM (SELECT * FROM MyTable)
* [Feature] SELECT {Function} support: COUNT(*), AVG(*)
* [Feature] Support multiple .filter() clauses, currently the value will be overwritten on every .filter() call
* [Feature] JOIN ON functionality. Currently it supports JOIN USING syntax only
* [Feature] Tests for HAVING clause
### END OF TODO

* [Feature] SelectQuery: Basic join support with USING keyword
* [Feature] SelectQuery: Offset option
* [Feature] User-function calls: SELECT * FROM my_custom_function(%s, %s, %s)


0.1.0 [2016-03-11]
--------------------
* Initial release
* [Feature] very basic SelectQuery and InsertQuery functionality