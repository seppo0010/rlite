# wal file format

A wal file contains a transaction data. The file starts with a sha1 of its
contents. If it matches the content, it means the transaction can be applied.
Otherwise, it should be discarded.

## Format


```
72 6c 00 00 00 30 2e 30       # "rlwal0.0" magic string
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
                              # sha1 digest of the following content
00 00 00 02                   # number of pages
                              # page starts
00 00 00 00                   # number of page to write
00 00 00 00 00 00 ... 00 00   # page data
                              # repeat "number of pages" times
