# Honey SQL for HBase Phoenix

[![Build Status](https://travis-ci.org/juvenn/honeysql-phoenix.svg?branch=master)](https://travis-ci.org/juvenn/honeysql-phoenix)
[![Clojars Project](https://img.shields.io/clojars/v/walkingcloud/honeysql-phoenix.svg)](https://clojars.org/walkingcloud/honeysql-phoenix)

Apache [Phoenix](http://phoenix.apache.org) provides a low latency SQL
query engine over HBase, that enables clients query and write HBase
with ease.

The library extends [Honey SQL](https://github.com/jkk/honeysql) with
additional constructs to support Phoenix-specific queries, such as
upsert, dynamic columns, etc. It facilitates building SQL queries to
query Phoenix over HBase.

## Usage

```clj
(require '[honeysql.core :as sql]
         '[honeysql.helpers :refer :all]
         '[phoenix.honeysql :refer :all])
```

Select with map, or keywords, or helpers:

```clj
;; map
(-> {:select [:a :b]
     :from [:table]
     :where [:= :a 101]}
     sql/format)
=> ["SELECT a, b FROM table WHERE a = ?" 101]
;; build with keywords
(-> (sql/build :select [:a :b]
               :from :table
               :where [:= :a 101])
    sql/format)
=> ["SELECT a, b FROM table WHERE a = ?" 101]
;; with helpers
(-> (select :a :b)
    (from :table)
    (where [:= :a 101])
    sql/format)
=> ["SELECT a, b FROM table WHERE a = ?" 101]
```

Select dynamic columns:

```clj
(-> (select :a :b)
    (from :table)
    (columns [[:var_a :int] [:var_b "char(8)"] [:var_created :time]])
    (where [:= :a 101])
    sql/format)
=> ["SELECT a, b FROM table (var_a int var_b char(8)) WHERE a = ?" 101]
```

Upsert dynamic columns:

```clj
(-> (upsert-into :table)
    (columns :a :b [:var_a :int] [:var_b "CHAR(8)"] [:var_created :time])
    (values [[1 2 101 "hello" :%current_time]])
    sql/format)
=> ["UPSERT INTO table (a, b, var_a int, var_b CHAR(8), var_created time) VALUES (?, ?, ?, ?, current_time())" 1 2 101 "hello"]
```

Atomic update with `ON DUPLICATE KEY`:

```clj
(-> (upsert-into :table)
    (columns :a [:b :varchar] [:c "char(10)"] [:d "char[5]"] [:e :time])
    (values [[1 "b" "c" "d" :%current_time]])
    (on-duplicate-key {:a 2 :b "b+" :e :%current_time})
    sql/format)
=> ["UPSERT INTO table (a, b varchar, c char(10), d char[5], e time) VALUES (?, ?, ?, ?, current_time()) ON DUPLICATE KEY UPDATE a = ?, b = ?, e = current_time()" 1 "b" "c" "d" 2 "b+"]
```

Or ignore on duplicate key:

```clj
(-> (upsert-into :table)
    (on-duplicate-key :ignore)
    (columns :a [:b :varchar])
    (values [[1 "b"]])
    sql/format)
=> ["UPSERT INTO table (a, b varchar) VALUES (?, ?) ON DUPLICATE KEY IGNORE" 1 "b"]
```

See [honeysql](https://github.com/jkk/honeysql) for more examples.

## License

Copyright © 2017, Juvenn Woo.

Distributed under the Eclipse Public License version 1.0.
