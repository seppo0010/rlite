rlite
=====

[![Build Status](https://travis-ci.org/seppo0010/rlite.svg?branch=master)](https://travis-ci.org/seppo0010/rlite)

rlite is a software library that implements a self-contained, serverless, zero-configuration, transactional redis-compatible database engine. rlite is to redis what SQLite is to SQL.

Use Cases
---------

This is a list of possible use cases where you might want to use rlite.

- Replace redis in development stack. By being embedded, rlite does not need a
separate database process, and since it is compatible with redis you can use it
while developing, even if you use redis instead in production.

- Replace redis in tests. The test stack can use rlite instead of redis if you
use the latter in production. It can simplify the CI stack, and the
distribution of fixtures by being in binary form, and it will not require to
load completely in memory to run each test.

- Slave of redis. You can run [rlite-server](https://github.com/seppo0010/rlite-server)
as a slave of a master redis instance. It works as third alternative to redis's
snapshot and append-only file.

- Mobile. If you are used to redis data structure, and it is better suited for
your mobile application than sqlite, you can use rlite as a database.

Current Status and Roadmap
--------------------------

- [x] string (set, get...)
- [x] list (lpush, lpop...)
- [x] set (sadd, sismember...)
- [x] zset (zadd, zrank...)
- [x] hash (hset, hdel...)
- [x] key management (del, exists...)
- [x] transactions (multi, exec...)
- [ ] rlite-server pass all redis tests
  - [ ] unit/scan
  - [ ] integration/replication
  - [ ] unit/scripting
  - [ ] unit/introspection
- [ ] pubsub
- [ ] brpop/brpoplpush
- [ ] multi process safe
- [ ] multi thread safe
- [ ] write ahead log
- [x] python bindings
- [ ] ruby bindings

Installation
------------

rlite has no dependencies, just run `make all`.
