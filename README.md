rlite
=====

[![Build Status](https://travis-ci.org/seppo0010/rlite.svg?branch=master)](https://travis-ci.org/seppo0010/rlite)

rlite is a software library that implements a self-contained, serverless, zero-configuration, transactional Redis-compatible database engine. rlite is to Redis what SQLite is to SQL.

Use Cases
---------

This is a list of possible use cases where you might want to use rlite.

- Replace Redis in development stack. By being embedded, rlite does not need a
separate database process, and since it is compatible with Redis you can use it
while developing, even if you use Redis instead in production.

- Replace Redis in tests. The test stack can use rlite instead of Redis if you
use the latter in production. It can simplify the CI stack, and the
distribution of fixtures by being in binary form, and it will not require to
load completely in memory to run each test.

- Slave of Redis. You can run [rlite-server](https://github.com/seppo0010/rlite-server)
as a slave of a master Redis instance. It works as third alternative to Redis's
snapshot and append-only file.

- Mobile. If you are used to Redis data structure, and it is better suited for
your mobile application than sqlite, you can use rlite as a database.

- Store client-side application data. Alternatively to a propetary format or
sqlite, command line or simple applications can store its data using rlite.

Current Status and Roadmap
--------------------------

- [x] string (set, get...)
- [x] list (lpush, lpop...)
- [x] set (sadd, sismember...)
- [x] zset (zadd, zrank...)
- [x] hash (hset, hdel...)
- [x] key management (del, exists...)
- [x] transactions (multi, exec...)
- [ ] rlite-server pass all Redis tests
  - [ ] unit/scan
  - [ ] integration/replication
  - [x] unit/scripting
  - [x] unit/introspection
- [ ] pubsub
- [ ] brpop/brpoplpush
- [ ] multi process safe
- [ ] multi thread safe
- [ ] write ahead log
- [x] python bindings
- [x] ruby bindings
- [x] objective-c bindings

Installation
------------

rlite has no dependencies, just run `make all`.

Bindings
-------

- [rlite-py](https://github.com/seppo0010/rlite-py)
- [rlite-rb](https://github.com/seppo0010/rlite-rb)
- [objc-rlite](https://github.com/seppo0010/objc-rlite)
