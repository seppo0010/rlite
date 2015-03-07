# rlite

[![Build Status](https://travis-ci.org/seppo0010/rlite.svg?branch=master)](https://travis-ci.org/seppo0010/rlite)

self-contained, serverless, zero-configuration, transactional Redis-compatible database engine. rlite is to Redis what SQLite is to SQL.

## Example

```ruby
require "redis"
require "hirlite/connection"

redis = Redis.new(host: ":memory:", driver: Rlite::Connection::Hirlite)

redis.set "key", "value"

puts redis.get "key"
# => "value"
```

[Ruby](https://github.com/seppo0010/rlite-rb#usage) bindings example. See
[Node.js](//github.com/seppo0010/rlite-js#usage),
[Python](https://github.com/seppo0010/rlite-py#usage),
[Objective-C](https://github.com/seppo0010/objc-rlite#api) and
[PHP](https://github.com/seppo0010/rlite-php#usage).

### Example in C
```c
#include <hirlite.h>

// ...

rliteContext *context = rliteConnect(":memory:", 0);

rliteReply* reply;
int argc_set = 3;
char *argv_set[] = {"SET", "key", "value"};
size_t argvlen_set[] = {3, 3, 5};
reply = rliteCommandArgv(context, argc_set, argv_set, argvlen_set);
rliteFreeReplyObject(reply);

int argc_get = 3;
char *argv_get[] = {"SET", "key", "value"};
size_t argvlen_get[] = {3, 3, 5};
reply = rliteCommandArgv(context, argc_get, argv_get, argvlen_get);
if (reply->type == RLITE_REPLY_STRING) {
	// reply->str is "value", reply->len is 5
}
rliteFreeReplyObject(reply);
```

## Use Cases

This is a list of possible use cases where you might want to use rlite.

- **Mobile**. If you are used to Redis data structure, and it is better suited for
your mobile application than sqlite, you can use rlite as a database.

- **Replace Redis in development stack**. By being embedded, rlite does not need a
separate database process, and since it is compatible with Redis you can use it
while developing, even if you use Redis instead in production.

- **Replace Redis in tests**. The test stack can use rlite instead of Redis if you
use the latter in production. It can simplify the CI stack, and the
distribution of fixtures by being in binary form, and it will not require to
load completely in memory to run each test.

- **Slave of Redis**. You can run [rlite-server](https://github.com/seppo0010/rlite-server)
as a slave of a master Redis instance. It works as third alternative to Redis's
snapshot and append-only file.

- **Store client-side application data**. Alternatively to a propetary format or
sqlite, command line or simple applications can store its data using rlite.

## Storage

All rlite data is stored in a single file using its own format. The format is
not rdb or aof since those are optimized for fast reading the whole content
and not for random access.

If file system persistence is no needed, use the magic file path ":memory:".

For more information about the file format check out
[its documentation](doc/rld-format.md).

## Current Status and Roadmap

- [x] string (set, get...)
- [x] list (lpush, lpop...)
- [x] set (sadd, sismember...)
- [x] zset (zadd, zrank...)
- [x] hash (hset, hdel...)
- [x] key management (del, exists...)
- [x] transactions (multi, exec...)
- [x] lua scripting (eval, evalsha, ...)
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

## Installation

rlite has no dependencies, just run `make all`.

## Bindings

- [rlite-py](https://github.com/seppo0010/rlite-py)
- [rlite-rb](https://github.com/seppo0010/rlite-rb)
- [rlite-js](https://github.com/seppo0010/rlite-js)
- [objc-rlite](https://github.com/seppo0010/objc-rlite)
- [rlite-php](https://github.com/seppo0010/rlite-php)

### Command-line interface

- [rlite-cli](https://github.com/seppo0010/rlite-cli)

## License

Copyright (c) 2014, Sebastian Waisbrot
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
