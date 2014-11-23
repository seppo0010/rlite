#ifndef _RLITE_H
#define _RLITE_H

#include <stdio.h>
#include "status.h"
#include "page_btree.h"
#include "page_key.h"

#define RL_MEMORY_DRIVER 0
#define RL_FILE_DRIVER 1

#define RLITE_OPEN_READONLY  0x00000001
#define RLITE_OPEN_READWRITE 0x00000002
#define RLITE_OPEN_CREATE    0x00000004

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
	long page_number;
	rl_data_type *type;
	void *obj;
#ifdef DEBUG
	unsigned char *serialized_data;
#endif
} rl_page;

typedef struct rlite {
	long number_of_pages;
	long next_empty_page;
	long page_size;
	void *driver;
	int driver_type;
	int selected_database;
	int number_of_databases;
	long *databases;
	long read_pages_alloc;
	long read_pages_len;
	rl_page **read_pages;
	long write_pages_alloc;
	long write_pages_len;
	rl_page **write_pages;
} rlite;

int rl_open(const char *filename, rlite **db, int flags);
int rl_close(rlite *db);

int rl_read_header(rlite *db);
int rl_read(struct rlite *db, rl_data_type *type, long page, void *context, void **obj, int cache);
int rl_get_key_btree(rlite *db, struct rl_btree **btree);
int rl_alloc_page_number(rlite *db, long *page_number);
int rl_write(struct rlite *db, rl_data_type *type, long page, void *obj);
int rl_purge_cache(struct rlite *db, long page);
int rl_delete(struct rlite *db, long page);
int rl_commit(struct rlite *db);
int rl_discard(struct rlite *db);
int rl_is_balanced(struct rlite *db);
int rl_select(struct rlite *db, int selected_database);
int rl_move(struct rlite *db, unsigned char *key, long keylen, int database);
int rl_rename(struct rlite *db, const unsigned char *src, long srclen, const unsigned char *target, long targetlen, int overwrite);

extern rl_data_type rl_data_type_header;
extern rl_data_type rl_data_type_btree_hash_sha1_key;
extern rl_data_type rl_data_type_btree_node_hash_sha1_key;
extern rl_data_type rl_data_type_btree_set_long;
extern rl_data_type rl_data_type_btree_node_set_long;
extern rl_data_type rl_data_type_btree_hash_long_long;
extern rl_data_type rl_data_type_btree_node_hash_long_long;
extern rl_data_type rl_data_type_btree_hash_double_long;
extern rl_data_type rl_data_type_btree_node_hash_double_long;
extern rl_data_type rl_data_type_btree_hash_sha1_double;
extern rl_data_type rl_data_type_btree_node_hash_sha1_double;
extern rl_data_type rl_data_type_list_long;
extern rl_data_type rl_data_type_list_node_long;
extern rl_data_type rl_data_type_list_node_key;
extern rl_data_type rl_data_type_string;
extern rl_data_type rl_data_type_long;
extern rl_data_type rl_data_type_skiplist;
extern rl_data_type rl_data_type_skiplist_node;

#endif
