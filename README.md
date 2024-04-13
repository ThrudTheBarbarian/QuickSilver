# QuickSilver

QuickSilver was written because I wanted to do something with Swift to get more familiar with the language, and it had to be something "substantial" for me to get my teeth into.

The project implements an object-relational model for accessing a SQLite database. The idea is for the framework to take away persistence and just let you issue queries to get 'model' objects, which of course are just table-rows.

Some of the features of QuickSilver are:

- It implements a cache, and uses that cache to inflate objects as much as possible. That means less read/write to the database layer
- It implements a background-write thread, so you're not stuck waiting on the database when you make changes. The general process is to call the 'setXXX' methods in your model, which will update the local cache and schedule the background write to the database
- It handles coherency, so if you issue a bunch of write-requests, then a read, it will wait until those write requests are complete before satisfying the read
- It uses bindings throughout, so there's no chance of SQL injection throwing spanners into the works
- For each table, you only need to implement an Entity class (which handles saving and loading your specific fields in your table) and a Model class (which handles properties of your class, and anything else you want it to do).

The goal is for you to be able to write something like:

`if let models = entity.models(where: "salary>100000 AND salary<1000000")` or whichever query you want, and receive back a set of models, one per row, ready to be iterated over, sorted, searched, etc.

There's a lot of work in the base QSModel and QSEntity class to make things as easy as possible at the higher levels. QuickSilver does require a table layout that looks like:
```
tsql> select * from jobs ;
+--------------------------------------+-------------------+-------------------+-------+-----------+-----------+
|                 uuid                 |      created      |     modified      | title | minsalary | maxsalary |
+--------------------------------------+-------------------+-------------------+-------+-----------+-----------+
| 600DBA4C-C3B1-4075-9CC7-DA2F0BE574A4 | 1713048426.435542 | 1713048426.435544 | CEO   | 100000    | 1000000   |
+--------------------------------------+-------------------+-------------------+-------+-----------+-----------+
Time taken: 0.001636 secs, rows=1

```
where the first 3 columns are required (and maintained by QuickSilver), being:

- `uuid` is a unique identifier for the row in this table
- `created` is the creation-date of the record
- `modified` was the last time the record was modified

Anything else is free-form for you to do as you please. In the above, there's a minimal "Job" schema, which is used in the tests

The QuickSilver tests ought to give an idea of how the system works, since they form a basic test suite using the Job{Entity,Model} class.

Enjoy.
