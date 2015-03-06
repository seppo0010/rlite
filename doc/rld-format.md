# rld file format

An rlite database file is divided in pages. By default, their size is 1024
bytes. The minimum size is 276 bytes.

## General considerations

Integer numbers are stored as Big Endian unless stated otherwise.

## Header page

The first page contains general information about the database.

```
72 6c 69 74 65 30 2e 30       # "rlite0.0" magic string
00 00 04 00                   # page size
00 00 06 70                   # next empty page
00 00 06 70                   # number of pages in the file
00 00 00 10                   # number of databases in the file (e.g.: "SELECT 15")

00 00 00 05                   # metadata of the first database (empty db if 0)
00 00 00 00                   # metadata of the second database
...
00 00 00 00                   # metadata of the Nth database
00 00 00 00                   # metadata of the scripts database
...                           # padding
```

Deleted pages are cleared and a reference one of them  stored in the header
file to reuse later in "next empty page". If no page was deleted or all were
already recycled, this value matches the "number of pages in the database".

The "number of databases in the file" enumerates the number of integers that
follow. Each of those is 0 if the database contains no key, or an integer
where to go to look for the key btree metadata.

The "scripts" database is a database formatted like the others but where the
user has no access. It is used internally to save the lua scripts.
The key of the lua scripts is the sha1 of the hex digest sha1 of the script.

## Key btree metadata page

```
00 00 00 04                   # key btree root
00 00 00 01                   # height of the btree
00 00 00 0e                   # maximum number of elements in a node
00 00 00 05                   # total number of element in the tree
...                           # padding
```

Metadata about the btree. The "key btree root" is the page where to look up
the root of the btree. The height is the maximum distance from the root to any
node in the tree.

## Key btree node page
```
00 00 00 05                   # number of elements in this node

                              # start block element
10 73 ab 6c da 4b 99 1c d2 9f 9e 83 a3 07 f3 40 04 ae 93 27
                              # sha1 of the key name
54                            # value type
00 00 00 07                   # key string page
00 00 00 02                   # value page
00 00 00 00 00 00 00 00       # expiration time
00 00 41 a8                   # key version
00 00 00 00                   # child key btree node page
...                           # repeats "number of elements" times

00 00 00 00                   # child key btree node page
...                           # padding
```

This page contains a list of elements. Each element has a 20 bytes sha-1 of its
key that is used to compare with other keys (using memcmp). A sha-1 collision
will be treated as if the two keys are the same.

The "value type" indicates what kind of object is stored in the key:

* 54: string
* 4c: list
* 53: set
* 5a: sorted set
* 48: hash

The "key string page" points to a multi page string page with the name of
the key.

The "value page" points to a page but its type depends on the previous
"value type". A key with a string value will point to a multi page string, and
all other types have their own special page.

"expiration time" is 0 if the key does not expire. Otherwise, it is the
number of milliseconds since January 1st, 1970 in Greenwich until the key is
supposed to expire.

The "key version" is a number to identify the current version of the key. This
is used for the "WATCH" command. The version does not have to start in 0, but
when a value is modified the key version should also be changed.

The "child key btree node page" is the page to continue when looking for a
sha-1 that's lower than the element's one.

Last, after all elements, there's a last key btree node page for hashes that
are higher.

## Multi page string page

The multi string page is a list metadata page with a list whose first element
is the length of the string, and the following are the string pages.

## List metadata page

The list metadata page contains general information about a list

```
00 00 00 01                   # left most list node page
00 00 00 01                   # right most list node page
00 00 00 7e                   # maximum number of elements per page
00 00 00 02                   # number of elements in the list
...                           # padding
```

## List node page

```
00 00 00 02                   # number of elements in this page
00 00 00 00                   # node page immediately to the left of this one (0 if first)
00 00 00 00                   # node page immediately to the right of this one (0 if last)
00 00 00 06                   # first value
00 00 00 03                   # second value
...                           # repeat
...                           # padding
```

## String page

This page stores an array of bytes up to the page size.

## Linked list metadata page

This page acts like a "list metadata page". Its values point to pages with
multi page strings with the string element from the list.

## Set metadata page

Btree like "key btree metadata page", using the member sha1 as a key and its
string page as a value.

## Sorted Set metadata page

This page behaves like a "list metadata page", but it always has two values.
The first one is a sorted set hashmap metadata and the second one is a
skiplist metadata.

## Sorted set hashmap metadata page

Btree like "key btree metadata page" using the member sha1 as a key,
and an 8 bytes representation of the score as a value, using IEEE 754 64-bit.

## Skiplist metadata page

00 00 00 1f                   # first element in the skiplist
00 00 06 69                   # last element in the skiplist
00 00 00 64                   # number of elements in the skiplist
00 00 00 04                   # number of levels in the skiplist
...                           # padding

## Skiplist node page

Each skiplist node contains exactly one element of the skiplist.

00 00 00 11                   # multi page string with the element value
40 00 00 00 00 00 00 00       # 8 bytes double with the element score
00 00 00 0f                   # immediately previous element in the skiplist
00 00 00 02                   # number of levels in the node
00 00 00 17                   # immediately next element in the skiplist
00 00 00 01                   # always 1
                              # block
00 00 00 1f                   # some skiplist node page with higher rank
00 00 00 03                   # distance to the node referenced
                              # repeat per number of levels in the node

## Hash metadata page

Same metadata as "key btree metadata page". The values are explained next.

## Hash node page

00 00 00 0c                   # number of elements in the page
                              # start block element
02 86 dd 55 2c 9b ea 9a 69 ec b3 75 9e 7b 94 77 76 35 51 4b
                              # sha1 of the field name
00 00 02 d2                   # multi page string with the field name
00 00 02 d5                   # multi page string with the field value
00 00 00 00                   # child key btree node page
...                           # repeat block
00 00 00 00                   # child key btree node page
...                           # padding

# Deleted page

00 00 00 04                   # next deleted page, if any
...                           # padding

Deleted pages work as a linked list. One is linked from the header, and it
links to the next one, and so on.
