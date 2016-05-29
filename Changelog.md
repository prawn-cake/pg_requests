Changelog
=========

0.1.0+1 (UNRELEASED)
--------------------
## TODO:
* [Feature] Sub-queries, for example: SELECT * FROM (SELECT * FROM MyTable)
* [Feature] JOIN ON functionality. Currently it supports JOIN USING syntax only
* [Improvement] Add optional validation for operators (e.g validate IN value args) 

### END OF TODO

* [Improvement] Experimental support of pattern matching 'LIKE', 'ILIKE', 'SIMILAR TO' operator
* [Feature] Support multiple .filter() clauses, concatenate it with AND operator
* [Feature] SELECT {AggFunction} support: COUNT(*), AVG(*)
* [Feature] SelectQuery: Basic join support with USING keyword
* [Feature] SelectQuery: Offset option
* [Feature] User-function calls: SELECT * FROM my_custom_function(%s, %s, %s)


0.1.0 [2016-03-11]
--------------------
* Initial release
* [Feature] very basic SelectQuery and InsertQuery functionality