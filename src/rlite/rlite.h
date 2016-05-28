#ifndef _RLITE_H
#define _RLITE_H

#include <stdio.h>
#include "status.h"
#include "page_btree.h"
#include "page_key.h"
#include "type_hash.h"
#include "type_zset.h"
#include "type_set.h"
#include "type_list.h"
#include "type_string.h"
#include "sort.h"
#include "restore.h"
#include "dump.h"
#include "util.h"

#define REDIS_RDB_VERSION 6

#define REDIS_RDB_6BITLEN 0
#define REDIS_RDB_14BITLEN 1
#define REDIS_RDB_32BITLEN 2
#define REDIS_RDB_ENCVAL 3

#define REDIS_RDB_OPCODE_EXPIRETIME_MS 252
#define REDIS_RDB_OPCODE_EXPIRETIME 253
#define REDIS_RDB_OPCODE_SELECTDB 254
#define REDIS_RDB_OPCODE_EOF 255

#define REDIS_RDB_TYPE_STRING 0
#define REDIS_RDB_TYPE_LIST 1
#define REDIS_RDB_TYPE_SET 2
#define REDIS_RDB_TYPE_ZSET 3
#define REDIS_RDB_TYPE_HASH 4
#define REDIS_RDB_TYPE_HASH_ZIPMAP 9
#define REDIS_RDB_TYPE_LIST_ZIPLIST 10
#define REDIS_RDB_TYPE_SET_INTSET 11
#define REDIS_RDB_TYPE_ZSET_ZIPLIST 12
#define REDIS_RDB_TYPE_HASH_ZIPLIST 13

#define REDIS_RDB_ENC_INT8 0
#define REDIS_RDB_ENC_INT16 1
#define REDIS_RDB_ENC_INT32 2
#define REDIS_RDB_ENC_LZF 3

#define RL_MEMORY_DRIVER 0
#define RL_FILE_DRIVER 1

#define RLITE_OPEN_READONLY  0x00000001
#define RLITE_OPEN_READWRITE 0x00000002
#define RLITE_OPEN_CREATE    0x00000004

#define RLITE_FLOCK_SH 1
#define RLITE_FLOCK_EX 2
#define RLITE_FLOCK_UN 3

#define RLITE_INTERNAL_DB_COUNT 6
#define RLITE_INTERNAL_DB_NO 0
#define RLITE_INTERNAL_DB_LUA 1
// the following two databases might look confusing, bear with me
// subscriber->channels map a subscriber_id to a set of channels it is subscribed
// channel->subscribers maps a channel to the subscribers id
// any operation on one of them should keep the other one in sync
// same premise applies to patterns instead of channels
#define RLITE_INTERNAL_DB_SUBSCRIBER_CHANNELS 2
#define RLITE_INTERNAL_DB_CHANNEL_SUBSCRIBERS 3
#define RLITE_INTERNAL_DB_SUBSCRIBER_PATTERNS 4
#define RLITE_INTERNAL_DB_PATTERN_SUBSCRIBERS 5
#define RLITE_INTERNAL_DB_SUBSCRIBER_MESSAGES 6

struct rlite;
struct rl_btree;

typedef struct rl_data_type {
	const char *name;
	int (*serialize)(struct rlite *db, void *obj, unsigned char *data);
	int (*deserialize)(struct rlite *db, void **obj, void *context, unsigned char *data);
	int (*destroy)(struct rlite *db, void *obj);
} rl_data_type;

typedef struct {
	FILE *fp;
	char *filename;
	int mode;
} rl_file_driver;

typedef struct {
	char *data;
	long datalen;
} rl_memory_driver;

typedef struct {
	long page_number;
	rl_data_type *type;
	void *obj;
#ifdef RL_DEBUG
	unsigned char *serialized_data;
#endif
} rl_page;

typedef struct rlite {
	// these four properties can change during a transaction
	// we need to record their original values to use when
	// checking watched keys
	long initial_next_empty_page;
	long initial_number_of_pages;
	int initial_number_of_databases;
	long *initial_databases;

	long number_of_pages;
	long next_empty_page;
	long page_size;
	void *driver;
	int driver_type;
	int selected_internal;
	int selected_database;
	int number_of_databases;
	long *databases;
	long read_pages_alloc;
	long read_pages_len;
	rl_page **read_pages;
	long write_pages_alloc;
	long write_pages_len;
	rl_page **write_pages;

	char *subscriber_id;
	char *subscriber_lock_filename;
	FILE *subscriber_lock_fp;
} rlite;

typedef struct watched_key {
	unsigned char digest[20];
	long version;
	int database;
} watched_key;

int rl_open(const char *filename, rlite **db, int flags);
int rl_refresh(rlite *db);
int rl_close(rlite *db);

int rl_ensure_pages(rlite *db);
int rl_read_header(rlite *db);
int rl_header_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data);
int rl_read(struct rlite *db, rl_data_type *type, long page, void *context, void **obj, int cache);
int rl_get_key_btree(rlite *db, struct rl_btree **btree, int create);
int rl_alloc_page_number(rlite *db, long *page_number);
int rl_write(struct rlite *db, rl_data_type *type, long page, void *obj);
int rl_purge_cache(struct rlite *db, long page);
int rl_delete(struct rlite *db, long page);
int rl_dirty_hash(struct rlite *db, unsigned char **hash);
int rl_commit(struct rlite *db);
int rl_discard(struct rlite *db);
int rl_is_balanced(struct rlite *db);
int rl_get_selected_db(struct rlite *db);
int rl_select(struct rlite *db, int selected_database);
int rl_select_internal(struct rlite *db, int internal);
int rl_move(struct rlite *db, unsigned char *key, long keylen, int database);
int rl_rename(struct rlite *db, const unsigned char *src, long srclen, const unsigned char *target, long targetlen, int overwrite);
int rl_dbsize(struct rlite *db, long *size);
int rl_keys(struct rlite *db, unsigned char *pattern, long patternlen, long *size, unsigned char ***result, long **resultlen);
int rl_randomkey(struct rlite *db, unsigned char **key, long *keylen);
int rl_flushall(struct rlite *db);
int rl_flushdb(struct rlite *db);

extern rl_data_type rl_data_type_header;
extern rl_data_type rl_data_type_btree_hash_sha1_hashkey;
extern rl_data_type rl_data_type_btree_hash_sha1_key;
extern rl_data_type rl_data_type_btree_node_hash_sha1_key;
extern rl_data_type rl_data_type_btree_node_hash_sha1_hashkey;
extern rl_data_type rl_data_type_btree_set_long;
extern rl_data_type rl_data_type_btree_node_set_long;
extern rl_data_type rl_data_type_btree_hash_long_long;
extern rl_data_type rl_data_type_btree_node_hash_long_long;
extern rl_data_type rl_data_type_btree_hash_double_long;
extern rl_data_type rl_data_type_btree_node_hash_double_long;
extern rl_data_type rl_data_type_btree_hash_sha1_double;
extern rl_data_type rl_data_type_btree_node_hash_sha1_double;
extern rl_data_type rl_data_type_btree_hash_sha1_long;
extern rl_data_type rl_data_type_btree_node_hash_sha1_long;
extern rl_data_type rl_data_type_list_long;
extern rl_data_type rl_data_type_list_node_long;
extern rl_data_type rl_data_type_list_node_key;
extern rl_data_type rl_data_type_string;
extern rl_data_type rl_data_type_long;
extern rl_data_type rl_data_type_skiplist;
extern rl_data_type rl_data_type_skiplist_node;

#endif
