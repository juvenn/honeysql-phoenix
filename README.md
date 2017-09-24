# Clojure SQL for HBase Phoenix

Clojure SQL for HBase [Phoenix](http://phoenix.apache.org). This
library extends [honeysql](https://github.com/jkk/honeysql) with
additional constructs to support Phoenix-specific queries, such as
upsert, dynamic columns, etc. It facilitates building SQL queries with
clojure data structure.

## Build

[![Build Status](https://travis-ci.org/juvenn/honeysql-phoenix.svg?branch=master)](https://travis-ci.org/juvenn/honeysql-phoenix)
[![Clojars Project](https://img.shields.io/clojars/v/honeysql-phoenix.svg)](https://clojars.org/honeysql-phoenix)

## Getting started

Specify dependencies:

```clj
:dependencies [
  [honeysql-phoenix "0.2.0"]
]
```

In addition to that, a (compatible) phoenix client
(e.g. `phoenix-{version}-client.jar`) should be added to classpath.

## Examples

```clj
(:require [phoenix.db :refer [defdb deftable phoenix] :as db]
          [phoenix.honeysql :refer :all])
```

First of all, define db connection:

```clj
(defdb my-db
  (phoenix {:zk-quorum "127.0.0.1,127.0.0.2:2181"}))
```

Then define table(s) with optional dynamic typed columns:

```clj
(deftable user
  (db/db* my-db)
  (db/table* :user)
  ;; define dynamic columns with its type
  (db/types* :referrer    "VARCHAR(64)"
             :landing_url "VARCHAR(64)"))
```

To insert a row:

```clj
(-> (upsert-into user)
    (values [{:username "jack" :email "jack@example.net"
              :referrer "google.com"}])
    db/exec)
```

In place of `db/exec`, we could invoke `as-sql` to render it as sql
string:

```clj
(-> (upsert-into user)
    (values [{:username "jack" :email "jack@example.net"
              :referrer "google.com"}])
    as-sql)
["UPSERT INTO user (username, email, referrer VARCHAR(64)) VALUES (?, ?, ?)"
 "jack" "jack@example.net" "google.com"]
```

Note that `referrer` is type annotated.

To query rows:

```clj
(-> (select :username :email :referrer :landing_url)
    (from user)
    (where [:= :email "jack@example.net"])
    (limit 1)
    as-sql)
;; manually formatted for ease of reading
["SELECT username, email, referrer, landing_url
  FROM user (referrer VARCHAR(64), landing_url VARCHAR(64))
  WHERE email = ? LIMIT ?"
"jack@example.net" 1]
```

To delete rows:

```clj
(-> (delete-from user)
    (where [:= :email "jack@example.net"])
    as-sql)
["DELETE FROM user WHERE email = ?"
 "jack@example.net"]
```

Atomic update:

```clj
(-> (upsert-into user)
    (values [{:username "jack" :email "jack@example.net"
              :referrer "google.com"}])
    (on-duplicate-key :ignore)
    as-sql)
["UPSERT INTO user (username, email, referrer VARCHAR(64)) VALUES (?, ?, ?) ON DUPLICATE KEY IGNORE"
 "jack" "jack@example.net" "google.com"]

(-> (upsert-into user)
    (values [{:username "jack" :email "jack@example.net"
              :referrer "google.com"}])
    (on-duplicate-key {:referrer "google.com"})
    as-sql)
["UPSERT INTO user (username, email, referrer VARCHAR(64)) VALUES (?, ?, ?)
   ON DUPLICATE KEY UPDATE referrer = ?"
"jack" "jack@example.net" "google.com" "google.com"]
```

For more examples, please refer to [honeysql](https://github.com/jkk/honeysql).

## License

Copyright © 2017, Juvenn Woo.

Distributed under the Eclipse Public License version 1.0.
