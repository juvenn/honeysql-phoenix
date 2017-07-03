# Honey SQL for HBase Phoenix

[![Build Status](https://travis-ci.org/juvenn/honeysql-phoenix.svg?branch=master)](https://travis-ci.org/juvenn/honeysql-phoenix)
[![Clojars Project](https://img.shields.io/clojars/v/walkingcloud/honeysql-phoenix.svg)](https://clojars.org/walkingcloud/honeysql-phoenix)

Apache [Phoenix](http://phoenix.apache.org) provides a low latency SQL
query engine over HBase, that enables clients query and write HBase
with ease.

This library extends [Honey SQL](https://github.com/jkk/honeysql) with
additional constructs to support Phoenix-specific queries, such as
upsert, dynamic columns, etc. It facilitates building SQL queries
with clojure data structure.

## Features

* Atomic update with `on-duplicate-key`
* Automatically type-annotate dynamic columns

## Install

```clj
[phoenix-sql "0.2.0"]
```

In addition to that, a (compatible) phoenix client should be provided:

```clj
[org.apache.hbase/hbase-client "1.2.2"]
[org.apache.phoenix/phoenix-core "4.10.0-HBase-1.2"]
```

For maximum compatibility (and production deployment) though, it is
recommended to use the client jar accompanied with server jar that had
been deployed in cluster. Put the client jar on the classpath, one of
the ways is to put under `resources/` and then define in
`project.clj`:

```clj
:resource-paths ["resources/phoenix-{version}-client.jar"]
```

The client jar should include hbase-client, phoenix jdbc driver with
ensured compatibility to the server jar.

## Examples and Usage

```clj
(require '[honeysql.core :as sql]
         '[honeysql.helpers :refer :all]
         '[phoenix.db :refer [defdb deftable]]
         '[phoenix.honeysql :refer :all])
```

First of all, define db connection and table we would like to connect to
and query over.

```clj
(defdb my-db
  {:quorum "127.0.0.1,127.0.0.2:2181"
   :zk-path "/hbase"})

(deftable test-table
  {:db my-db
   :table :test_table
   ;; columns created with table
   :columns [:a :b :c]
   ;; dynamic typed columns
   :dynamic {:x :INTEGER :y "DECIMAL(10,2)"}})
```

To insert rows:

```clj
(upsert-into! test-table
              (values [{:a 1 :b "b1" :c "c1" :x 42 :y 3.14}])
              (on-duplicate-key {:x 43}))

;; translate to sql as
["UPSERT INTO test_table (a, b, c, x INTEGER, y DECIMAL(10,2))
  VALUES (?, ?, ?, ?, ?)
  ON DUPLICATE KEY UPDATE x=?"
  1 "b1" "c1" 42 3.14 43]
```

To query rows:

```clj
(select! :tt.a :tt.b :x :tt.y
         (from [[test-table :tt]])
         (where [:> :tt.a 42])
         (limit 5))
["SELECT tt.a, tt.b, x, tt.y
  FROM test_table (x INTEGER, y DECIMAL(10,2)) tt
  WHERE tt.a > ? LIMIT ?"
  42 5]
```

To delete rows:

```clj
(delete-from! test-table
              (where [:> :a 42]))
["DELETE FROM test_table WHERE a > ?" 42]
```

They all execute the query and return result. There are non-banged versions
as well which just build the query:

```clj
(-> (select :tt.a :tt.b :x :tt.y)
    (from [[test-table :tt]])
    (where [:> :tt.a 42])
    (limit 5)
    sql/format)
=> ["SELECT tt.a, tt.b, x, tt.y
     FROM test_table (x INTEGER, y DECIMAL(10,2)) tt
     WHERE tt.a > ? LIMIT ?"
     42 5]
```

For more examples, please refer to [Honeysql](https://github.com/jkk/honeysql).

## License

Copyright © 2017, Juvenn Woo.

Distributed under the Eclipse Public License version 1.0.
