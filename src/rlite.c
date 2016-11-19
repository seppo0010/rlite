#include <sys/file.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "rlite/page_btree.h"
#include "rlite/page_list.h"
#include "rlite/page_long.h"
#include "rlite/page_string.h"
#include "rlite/page_skiplist.h"
#include "rlite/page_multi_string.h"
#include "rlite/type_string.h"
#include "rlite/type_zset.h"
#include "rlite/type_hash.h"
#include "rlite/rlite.h"
#include "rlite/util.h"
#include "rlite/sha1.h"
#include "rlite/flock.h"
#include "rlite/pubsub.h"
#include "rlite/wal.h"
#ifdef RL_DEBUG
#include <valgrind/valgrind.h>
#endif

#define DEFAULT_READ_PAGES_LEN 16
#define DEFAULT_WRITE_PAGES_LEN 8
#define DEFAULT_PAGE_SIZE 1024
#define HEADER_SIZE 200

int rl_header_serialize(struct rlite *db, void *obj, unsigned char *data);
int rl_has_flag(rlite *db, int flag);

rl_data_type rl_data_type_btree_hash_sha1_long = {
	"rl_data_type_btree_hash_sha1_long",
	rl_btree_serialize,
	rl_btree_deserialize,
	rl_btree_destroy,
};
rl_data_type rl_data_type_btree_node_hash_sha1_long = {
	"rl_data_type_btree_node_hash_sha1_long",
	rl_btree_node_serialize_hash_sha1_long,
	rl_btree_node_deserialize_hash_sha1_long,
	rl_btree_node_destroy,
};
rl_data_type rl_data_type_btree_hash_sha1_double = {
	"rl_data_type_btree_hash_sha1_double",
	rl_btree_serialize,
	rl_btree_deserialize,
	rl_btree_destroy,
};
rl_data_type rl_data_type_btree_node_hash_sha1_double = {
	"rl_data_type_btree_node_hash_sha1_double",
	rl_btree_node_serialize_hash_sha1_double,
	rl_btree_node_deserialize_hash_sha1_double,
	rl_btree_node_destroy,
};
rl_data_type rl_data_type_btree_hash_sha1_key = {
	"rl_data_type_btree_hash_sha1_key",
	rl_btree_serialize,
	rl_btree_deserialize,
	rl_btree_destroy,
};
rl_data_type rl_data_type_btree_node_hash_sha1_key = {
	"rl_data_type_btree_node_hash_sha1_key",
	rl_btree_node_serialize_hash_sha1_key,
	rl_btree_node_deserialize_hash_sha1_key,
	rl_btree_node_destroy,
};
rl_data_type rl_data_type_btree_hash_sha1_hashkey = {
	"rl_data_type_btree_hash_sha1_hashkey",
	rl_btree_serialize,
	rl_btree_deserialize,
	rl_btree_destroy,
};
rl_data_type rl_data_type_btree_node_hash_sha1_hashkey = {
	"rl_data_type_btree_node_hash_sha1_hashkey",
	rl_btree_node_serialize_hash_sha1_hashkey,
	rl_btree_node_deserialize_hash_sha1_hashkey,
	rl_btree_node_destroy,
};
rl_data_type rl_data_type_header = {
	"rl_data_type_header",
	rl_header_serialize,
	rl_header_deserialize,
	NULL
};
rl_data_type rl_data_type_btree_hash_long_long = {
	"rl_data_type_btree_hash_long_long",
	rl_btree_serialize,
	rl_btree_deserialize,
	rl_btree_destroy,
};
rl_data_type rl_data_type_btree_node_hash_long_long = {
	"rl_data_type_btree_node_hash_long_long",
	rl_btree_node_serialize_hash_long_long,
	rl_btree_node_deserialize_hash_long_long,
	rl_btree_node_destroy,
};
rl_data_type rl_data_type_btree_hash_double_long = {
	"rl_data_type_btree_hash_double_long",
	rl_btree_serialize,
	rl_btree_deserialize,
	rl_btree_destroy,
};
rl_data_type rl_data_type_list_long = {
	"rl_data_type_list_long",
	rl_list_serialize,
	rl_list_deserialize,
	rl_list_destroy,
};
rl_data_type rl_data_type_list_node_long = {
	"rl_data_type_list_node_long",
	rl_list_node_serialize_long,
	rl_list_node_deserialize_long,
	rl_list_node_destroy,
};
rl_data_type rl_data_type_string = {
	"rl_data_type_string",
	rl_string_serialize,
	rl_string_deserialize,
	rl_string_destroy,
};

rl_data_type rl_data_type_skiplist = {
	"rl_data_type_skiplist",
	rl_skiplist_serialize,
	rl_skiplist_deserialize,
	rl_skiplist_destroy,
};

rl_data_type rl_data_type_skiplist_node = {
	"rl_data_type_skiplist_node",
	rl_skiplist_node_serialize,
	rl_skiplist_node_deserialize,
	rl_skiplist_node_destroy,
};
rl_data_type rl_data_type_long = {
	"rl_data_type_long",
	rl_long_serialize,
	rl_long_deserialize,
	rl_long_destroy,
};

rl_data_type rl_data_type_skiplist_node;

static const unsigned char *identifier = (unsigned char *)"rlite0.0";

static int file_driver_fp(rlite *db)
{
	int retval = RL_OK;
	rl_file_driver *driver = db->driver;
	if (driver->fp == NULL) {
		char *mode;
		if (access(driver->filename, F_OK) == 0) {
			mode = "r+";
		}
		else {
			if ((driver->mode & RLITE_OPEN_READWRITE) == 0) {
				fprintf(stderr, "Opening unexisting file in readonly mode\n");
				retval = RL_INVALID_PARAMETERS;
				goto cleanup;
			}
			mode = "w+";
		}
		driver->fp = fopen(driver->filename, mode);
		if (driver->fp == NULL) {
			fprintf(stderr, "Cannot open file %s, errno %d, mode %s\n", driver->filename, errno, mode);
			perror(NULL);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		retval = rl_flock(driver->fp, (driver->mode & RLITE_OPEN_READWRITE) ? RLITE_FLOCK_EX : RLITE_FLOCK_SH);
		if (retval != RL_OK) {
			fclose(driver->fp);
			driver->fp = NULL;
			goto cleanup;
		}
	}
cleanup:
	return retval;
}

int rl_header_serialize(struct rlite *db, void *UNUSED(obj), unsigned char *data)
{
	int identifier_len = strlen((char *)identifier);
	memcpy(data, identifier, identifier_len);
	put_4bytes(&data[identifier_len], db->page_size);
	put_4bytes(&data[identifier_len + 4], db->next_empty_page);
	put_4bytes(&data[identifier_len + 8], db->number_of_pages);
	put_4bytes(&data[identifier_len + 12], db->number_of_databases);
	long i, pos = identifier_len + 16;
	for (i = 0; i < db->number_of_databases + RLITE_INTERNAL_DB_COUNT; i++) {
		if (db->databases[i] != 0) {
			put_4bytes(&data[pos], db->databases[i]);
		}
		pos += 4;
	}
	return RL_OK;
}

int rl_header_deserialize(struct rlite *db, void **UNUSED(obj), void *UNUSED(context), unsigned char *data)
{
	int retval = RL_OK;
	int identifier_len = strlen((char *)identifier);
	if (memcmp(data, identifier, identifier_len) != 0) {
		fprintf(stderr, "Unexpected header, expecting %s\n", identifier);
		return RL_INVALID_STATE;
	}
	db->page_size = get_4bytes(&data[identifier_len]);
	db->initial_next_empty_page =
	db->next_empty_page = get_4bytes(&data[identifier_len + 4]);
	db->initial_number_of_pages =
	db->number_of_pages = get_4bytes(&data[identifier_len + 8]);
	db->initial_number_of_databases =
	db->number_of_databases = get_4bytes(&data[identifier_len + 12]);
	rl_free(db->databases);
	rl_free(db->initial_databases);
	RL_MALLOC(db->databases, sizeof(long) * (db->number_of_databases + RLITE_INTERNAL_DB_COUNT));
	RL_MALLOC(db->initial_databases, sizeof(long) * (db->number_of_databases + RLITE_INTERNAL_DB_COUNT));

	long i, pos = identifier_len + 16;
	for (i = 0; i < db->number_of_databases + RLITE_INTERNAL_DB_COUNT; i++) {
		db->initial_databases[i] =
		db->databases[i] = get_4bytes(&data[pos]);
		pos += 4;
	}
cleanup:
	return retval;
}

int rl_ensure_pages(rlite *db)
{
	int retval = RL_OK;
	void *tmp;
	if (rl_has_flag(db, RLITE_OPEN_READWRITE)) {
		if (db->write_pages_len == db->write_pages_alloc) {
			RL_REALLOC(db->write_pages, sizeof(rl_page *) * db->write_pages_alloc * 2)
			db->write_pages_alloc *= 2;
		}
	}
	if (db->read_pages_len == db->read_pages_alloc) {
		RL_REALLOC(db->read_pages, sizeof(rl_page *) * db->read_pages_alloc * 2)
		db->read_pages_alloc *= 2;
	}
cleanup:
	return retval;
}

int rl_open(const char *filename, rlite **_db, int flags)
{
	int retval = RL_OK;
	rlite *db;
	RL_MALLOC(db, sizeof(*db));

	db->subscriber_lock_filename = NULL;
	db->subscriber_id = NULL;
	db->databases = NULL;
	db->initial_databases = NULL;
	db->selected_database = 0;
	db->selected_internal = RLITE_INTERNAL_DB_NO;
	db->page_size = DEFAULT_PAGE_SIZE;
	db->read_pages = db->write_pages = NULL;
	db->read_pages_alloc = db->read_pages_len = db->write_pages_len = db->write_pages_alloc = 0;
	db->initial_number_of_pages = db->number_of_pages = 0;
	db->initial_number_of_databases =
	db->number_of_databases = 0;
	db->driver = NULL;
	db->driver_type = -1;

	RL_MALLOC(db->read_pages, sizeof(rl_page *) * DEFAULT_READ_PAGES_LEN)
	db->read_pages_len = 0;
	db->read_pages_alloc = DEFAULT_READ_PAGES_LEN;
	db->write_pages_len = 0;
	if ((flags & RLITE_OPEN_READWRITE) > 0) {
		db->write_pages_alloc = DEFAULT_WRITE_PAGES_LEN;
		RL_MALLOC(db->write_pages, sizeof(rl_page *) * DEFAULT_WRITE_PAGES_LEN);
	}
	else {
		db->write_pages_alloc = 0;
	}

	if (strcmp(filename, ":memory:") == 0) {
		rl_memory_driver *driver;
		RL_MALLOC(driver, sizeof(*driver));
		db->driver_type = RL_MEMORY_DRIVER;
		db->driver = driver;
		driver->data = NULL;
		driver->datalen = 0;
	}
	else {
		if ((flags & RLITE_OPEN_CREATE) == 0) {
			int _access_flags;
			if ((flags & RLITE_OPEN_READWRITE) != 0) {
				_access_flags = W_OK;
			}
			else {
				_access_flags = R_OK | F_OK;
			}
			if (access(filename, _access_flags) != 0) {
				retval = RL_INVALID_PARAMETERS;
				goto cleanup;
			}
		}

		rl_file_driver *driver;
		RL_MALLOC(driver, sizeof(*driver));
		driver->fp = NULL;
		driver->filename = rl_malloc(sizeof(char) * (strlen(filename) + 1));
		if (!driver->filename) {
			rl_free(driver);
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		strcpy(driver->filename, filename);
		driver->mode = flags;
		db->driver = driver;
		db->driver_type = RL_FILE_DRIVER;
	}

	RL_CALL(rl_read_header, RL_OK, db);

	*_db = db;
cleanup:
	if (retval != RL_OK && db != NULL) {
		rl_close(db);
	}
	return retval;
}

int rl_refresh(rlite *db)
{
	int retval = RL_OK;
	if (db->driver_type == RL_FILE_DRIVER) {
		RL_CALL(rl_discard, RL_OK, db);
		RL_CALL(rl_read_header, RL_OK, db);
	}
cleanup:
	return retval;
}

int rl_close(rlite *db)
{
	if (!db) {
		return RL_OK;
	}

	if (db->driver_type == RL_FILE_DRIVER) {
		rl_unsubscribe_all(db);
	}
	// discard before removing the driver, since we need to release locks
	rl_discard(db);
	if (db->driver_type == RL_FILE_DRIVER) {
		rl_file_driver *driver = db->driver;
		rl_free(driver->filename);
	}
	else if (db->driver_type == RL_MEMORY_DRIVER) {
		rl_memory_driver* driver = db->driver;
		rl_free(driver->data);
	}
	if (db->subscriber_lock_filename) {
		remove(db->subscriber_lock_filename);
		rl_free(db->subscriber_lock_filename);
	}
	rl_free(db->driver);
	rl_free(db->subscriber_id);
	rl_free(db->read_pages);
	rl_free(db->write_pages);
	rl_free(db->databases);
	rl_free(db->initial_databases);
	rl_free(db);
	return RL_OK;
}

int rl_has_flag(rlite *db, int flag)
{
	if (db->driver_type == RL_FILE_DRIVER) {
		return (((rl_file_driver *)db->driver)->mode & flag) > 0;
	}
	else if (db->driver_type == RL_MEMORY_DRIVER) {
		return 1;
	}
	fprintf(stderr, "Unknown driver_type %d\n", db->driver_type);
	return RL_UNEXPECTED;
}

int rl_create_db(rlite *db)
{
	int retval, i;
	RL_CALL(rl_ensure_pages, RL_OK, db);
	db->initial_next_empty_page =
	db->next_empty_page = 1;
	db->initial_number_of_pages =
	db->number_of_pages = 1;
	db->selected_database = 0;
	db->selected_internal = RLITE_INTERNAL_DB_NO;
	db->initial_number_of_databases =
	db->number_of_databases = 16;
	RL_MALLOC(db->databases, sizeof(long) * (db->number_of_databases + RLITE_INTERNAL_DB_COUNT));
	RL_MALLOC(db->initial_databases, sizeof(long) * (db->number_of_databases + RLITE_INTERNAL_DB_COUNT));
	for (i = 0; i < db->number_of_databases + RLITE_INTERNAL_DB_COUNT; i++) {
		db->initial_databases[i] =
		db->databases[i] = 0;
	}
cleanup:
	return retval;
}

int rl_get_selected_db(rlite *db) {
	if (db->selected_internal == RLITE_INTERNAL_DB_NO) {
		return db->selected_database;
	} else {
		return db->number_of_databases + db->selected_internal - 1;
	}
}

int rl_get_key_btree(rlite *db, rl_btree **retbtree, int create)
{
	void *_btree;
	int retval;
	int selected_database = rl_get_selected_db(db);;
	if (!db->databases[selected_database]) {
		if (!create) {
			return RL_NOT_FOUND;
		}
		rl_btree *btree;
		RL_CALL(rl_btree_create, RL_OK, db, &btree, &rl_btree_type_hash_sha1_key);
		db->databases[selected_database] = db->next_empty_page;
		RL_CALL(rl_write, RL_OK, db, &rl_data_type_btree_hash_sha1_key, db->databases[selected_database], btree);
	}
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_btree_hash_sha1_key, db->databases[selected_database], &rl_btree_type_hash_sha1_key, &_btree, 1);
	*retbtree = _btree;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_read_header(rlite *db)
{
	db->page_size = HEADER_SIZE;
	int retval;
	if (db->driver_type == RL_MEMORY_DRIVER) {
		db->page_size = DEFAULT_PAGE_SIZE;
		RL_CALL(rl_create_db, RL_OK, db);
	}
	else if (db->driver_type == RL_FILE_DRIVER) {
		RL_CALL(file_driver_fp, RL_OK, db);
		RL_CALL(rl_apply_wal, RL_OK, db);
		retval = rl_read(db, &rl_data_type_header, 0, NULL, NULL, 1);
		if (retval == RL_NOT_FOUND && rl_has_flag(db, RLITE_OPEN_CREATE)) {
			db->page_size = DEFAULT_PAGE_SIZE;
			RL_CALL(rl_create_db, RL_OK, db);
			RL_CALL(rl_write, RL_OK, db, &rl_data_type_header, 0, NULL);
		}
		else if (retval != RL_FOUND) {
			goto cleanup;
		}
	} else {
		fprintf(stderr, "Unknown driver type %d\n", db->driver_type);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	retval = RL_OK;
cleanup:
	return retval;
}

#ifdef RL_DEBUG
void print_cache(rlite *db)
{
	printf("Cache read pages:");
	long i;
	rl_page *page;
	for (i = 0; i < db->read_pages_len; i++) {
		page = db->read_pages[i];
		printf("%ld, ", page->page_number);
	}
	printf("\nCache write pages:");
	for (i = 0; i < db->write_pages_len; i++) {
		page = db->write_pages[i];
		printf("%ld, ", page->page_number);
	}
	printf("\n");
}
#endif

#ifdef RL_DEBUG
int rl_search_cache(rlite *db, rl_data_type *type, long page_number, void **obj, long *position, void *context, rl_page **pages, long page_len)
#else
static int rl_search_cache(rlite *db, rl_data_type *type, long page_number, void **obj, long *position, void *context, rl_page **pages, long page_len)
#endif
{
	long pos, min = 0, max = page_len - 1;
	rl_page *page;
	if (max >= 0) {
		do {
			pos = min + (max - min) / 2;
			page = pages[pos];
			if (page->page_number == page_number) {
				if (obj) {
					if (page->type == NULL) {
						// This happens when we are in read-only mode, and have a wal file
						unsigned char *serialize_data = page->obj;
						int retval = type->deserialize(db, &page->obj, context, serialize_data);
						if (retval != RL_OK) {
							return retval;
						}
						page->type = type;
						rl_free(serialize_data);
					}
					*obj = page->obj;
#ifdef RL_DEBUG
				if (page->type != &rl_data_type_long && type != &rl_data_type_long && type != NULL && page->type != type) {
					fprintf(stderr, "Type of page in cache (%s) doesn't match the asked one (%s)\n", page->type->name, type->name);
					return RL_UNEXPECTED;
				}
#endif
				}
				if (position) {
					*position = pos;
				}
				return RL_FOUND;
			}
			else if (page->page_number > page_number) {
				max = pos - 1;
			}
			else {
				min = pos + 1;
			}
		}
		while (max >= min);
		if (position) {
			if (pages[pos]->page_number > page_number && pos > 0) {
				pos--;
			}
			if (pages[pos]->page_number < page_number && pos < page_len) {
				pos++;
			}
#ifdef RL_DEBUG
			long i, prev;
			rl_page *p;
			for (i = 0; i < db->read_pages_len; i++) {
				p = db->read_pages[i];
				if (i > 0) {
					if (prev >= p->page_number) {
						fprintf(stderr, "Reading cache is broken\n");
						print_cache(db);
						return RL_UNEXPECTED;
					}
				}
				prev = p->page_number;
			}
			for (i = 0; i < db->write_pages_len; i++) {
				p = db->write_pages[i];
				if (i > 0) {
					if (prev >= p->page_number) {
						fprintf(stderr, "Writing cache is broken\n");
						print_cache(db);
						return RL_UNEXPECTED;
					}
				}
				prev = p->page_number;
			}
#endif
			*position = pos;
		}
	}
	else {
		if (position) {
			*position = 0;
		}
	}
	return RL_NOT_FOUND;
}

int rl_read_from_cache(rlite *db, rl_data_type *type, long page_number, void *context, void **obj)
{
	int retval = rl_search_cache(db, type, page_number, obj, NULL, context, db->write_pages, db->write_pages_len);
	if (retval == RL_NOT_FOUND) {
		retval = rl_search_cache(db, type, page_number, obj, NULL, context, db->read_pages, db->read_pages_len);
	}
	return retval;
}

int rl_read(rlite *db, rl_data_type *type, long page, void *context, void **obj, int cache)
{
	// fprintf(stderr, "r %ld %s\n", page, type->name);
#ifdef RL_DEBUG
	int keep = 0;
	long initial_page_size = db->page_size;
	if (page == 0 && type != &rl_data_type_header) {
		VALGRIND_PRINTF_BACKTRACE("Unexpected");
		return RL_UNEXPECTED;
	}
#endif
	unsigned char *data = NULL;
	int retval;
	unsigned char *serialize_data;
	retval = rl_read_from_cache(db, type, page, context, obj);
	if (retval != RL_NOT_FOUND) {
		if (!cache) {
			RL_MALLOC(serialize_data, db->page_size * sizeof(unsigned char));
			retval = type->serialize(db, *obj, serialize_data);
			if (retval != RL_OK) {
				rl_free(serialize_data);
				return retval;
			}
			retval = type->deserialize(db, obj, context, serialize_data);
			rl_free(serialize_data);
			if (retval != RL_OK) {
				return retval;
			}
			retval = RL_FOUND;
		}
		return retval;
	}
	RL_MALLOC(data, db->page_size * sizeof(unsigned char));
	if (db->driver_type == RL_FILE_DRIVER) {
		rl_file_driver *driver = db->driver;
		RL_CALL(file_driver_fp, RL_OK, db);
		fseek(driver->fp, page * db->page_size, SEEK_SET);
		size_t read = fread(data, sizeof(unsigned char), db->page_size, driver->fp);
		if (read != (size_t)db->page_size) {
			if (page > 0) {
#ifdef RL_DEBUG
				print_cache(db);
#endif
				fprintf(stderr, "Unable to read page %ld on line %d\n", page, __LINE__);
				perror(NULL);
			}
			retval = RL_NOT_FOUND;
			goto cleanup;
		}
	}
	else if (db->driver_type == RL_MEMORY_DRIVER) {
		rl_memory_driver *driver = db->driver;
		if ((page + 1) * db->page_size > driver->datalen) {
			fprintf(stderr, "Unable to read page %ld on line %d\n", page, __LINE__);
			retval = RL_NOT_FOUND;
			goto cleanup;
		}
		memcpy(data, &driver->data[page * db->page_size], sizeof(unsigned char) * db->page_size);
	}
	else {
		fprintf(stderr, "Unexpected driver %d when asking for page %ld\n", db->driver_type, page);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	long pos;
	retval = rl_search_cache(db, type, page, NULL, &pos, context, db->read_pages, db->read_pages_len);
	if (retval != RL_NOT_FOUND) {
		fprintf(stderr, "Unexpectedly found page in cache\n");
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	retval = type->deserialize(db, obj, context ? context : type, data);
	if (retval != RL_OK) {
		goto cleanup;
	}

	if (cache) {
		rl_ensure_pages(db);
		rl_page *page_obj;
		page_obj = rl_malloc(sizeof(*page_obj));
		if (!page_obj) {
			if (obj) {
				if (type->destroy && *obj) {
					type->destroy(db, *obj);
				}
				*obj = NULL;
			}
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		page_obj->page_number = page;
		page_obj->type = type;
		page_obj->obj = obj ? *obj : NULL;
#ifdef RL_DEBUG
		keep = 1;
		if (initial_page_size != db->page_size) {
			page_obj->serialized_data = rl_realloc(data, db->page_size * sizeof(unsigned char));
			if (page_obj->serialized_data == NULL) {
				rl_free(page_obj);
				retval = RL_OUT_OF_MEMORY;
				goto cleanup;
			}
			data = page_obj->serialized_data;
			if (db->page_size > initial_page_size) {
				memset(&data[initial_page_size], 0, db->page_size - initial_page_size);
			}
		}
		else {
			page_obj->serialized_data = data;
		}

		serialize_data = calloc(db->page_size, sizeof(unsigned char));
		if (!serialize_data) {
			rl_free(page_obj->serialized_data);
			rl_free(page_obj);
		}
		retval = type->serialize(db, obj ? *obj : NULL, serialize_data);
		if (retval != RL_OK) {
			goto cleanup;
		}
		if (memcmp(data, serialize_data, db->page_size) != 0) {
			fprintf(stderr, "serialize unserialized data mismatch\n");
			long i;
			for (i = 0; i < db->page_size; i++) {
				if (serialize_data[i] != data[i]) {
					fprintf(stderr, "at position %ld expected %d, got %d\n", i, serialize_data[i], data[i]);
				}
			}
		}
		rl_free(serialize_data);
#endif
		if (pos < db->read_pages_len) {
			memmove(&db->read_pages[pos + 1], &db->read_pages[pos], sizeof(rl_page *) * (db->read_pages_len - pos));
		}
		db->read_pages[pos] = page_obj;
		db->read_pages_len++;
	}
	if (retval == RL_OK) {
		retval = RL_FOUND;
	}
cleanup:
#ifdef RL_DEBUG
	if (retval != RL_FOUND || !keep) {
		rl_free(data);
	}
#endif
#ifndef RL_DEBUG
	rl_free(data);
#endif
	return retval;
}

int rl_alloc_page_number(rlite *db, long *_page_number)
{
	int retval = RL_OK;
	long page_number = db->next_empty_page;
	if (page_number == db->number_of_pages) {
		db->next_empty_page++;
		db->number_of_pages++;
	}
	else {
		RL_CALL(rl_long_get, RL_OK, db, &db->next_empty_page, db->next_empty_page);
	}
	if (_page_number) {
		*_page_number = page_number;
	}
cleanup:
	return retval;
}

int rl_write(struct rlite *db, rl_data_type *type, long page_number, void *obj)
{
	// fprintf(stderr, "w %ld %s\n", page_number, type->name);
	rl_page *page = NULL;
	long pos;
	int retval;

	if (page_number == db->next_empty_page) {
		RL_CALL(rl_alloc_page_number, RL_OK, db, NULL);
		retval = rl_write(db, &rl_data_type_header, 0, NULL);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}

	retval = rl_search_cache(db, type, page_number, NULL, &pos, NULL, db->write_pages, db->write_pages_len);
	if (retval == RL_FOUND) {
		if (obj != db->write_pages[pos]->obj) {
			if (db->write_pages[pos]->obj) {
				db->write_pages[pos]->type->destroy(db, db->write_pages[pos]->obj);
			}
			db->write_pages[pos]->obj = obj;
			db->write_pages[pos]->type = type;
		}
		retval = RL_OK;
	}
	else if (retval == RL_NOT_FOUND) {
		if (db->driver_type == RL_FILE_DRIVER) {
			RL_CALL(file_driver_fp, RL_OK, db);
		}
		rl_ensure_pages(db);
		RL_MALLOC(page, sizeof(*page));
#ifdef RL_DEBUG
		page->serialized_data = NULL;
#endif
		page->page_number = page_number;
		page->type = type;
		page->obj = obj;
		if (pos < db->write_pages_len) {
			memmove(&db->write_pages[pos + 1], &db->write_pages[pos], sizeof(rl_page *) * (db->write_pages_len - pos));
		}
		db->write_pages[pos] = page;
		db->write_pages_len++;

		retval = rl_search_cache(db, type, page_number, NULL, &pos, NULL, db->read_pages, db->read_pages_len);
		if (retval == RL_FOUND) {
#ifdef RL_DEBUG
			rl_free(db->read_pages[pos]->serialized_data);
#endif
			if (db->read_pages[pos]->obj != obj) {
				db->read_pages[pos]->type->destroy(db, db->read_pages[pos]->obj);
			}
			rl_free(db->read_pages[pos]);
			memmove(&db->read_pages[pos], &db->read_pages[pos + 1], sizeof(rl_page *) * (db->read_pages_len - pos));
			db->read_pages_len--;
			retval = RL_OK;
		}
		else if (retval != RL_NOT_FOUND) {
			goto cleanup;
		}
		retval = RL_OK;
	}

cleanup:
	if (retval != RL_OK) {
		if (obj) {
			type->destroy(db, obj);
			rl_purge_cache(db, page_number);
		}
		if (page_number != 0) {
			rl_discard(db);
		}
	}
	return retval;
}

int rl_purge_cache(struct rlite *db, long page_number)
{
	long pos;
	int retval;
	retval = rl_search_cache(db, NULL, page_number, NULL, &pos, NULL, db->write_pages, db->write_pages_len);
	if (retval == RL_FOUND) {
		db->write_pages[pos]->obj = NULL;
	}
	else if (retval != RL_NOT_FOUND) {
		goto cleanup;
	}
	retval = rl_search_cache(db, NULL, page_number, NULL, &pos, NULL, db->read_pages, db->read_pages_len);
	if (retval == RL_FOUND) {
		db->read_pages[pos]->obj = NULL;
	}
	else if (retval != RL_NOT_FOUND) {
		goto cleanup;
	}
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_delete(struct rlite *db, long page_number)
{
	int retval, i;
	for (i = 0; i < db->number_of_databases + RLITE_INTERNAL_DB_COUNT; i++) {
		if (db->databases[i] == page_number) {
			db->databases[i] = 0;
			RL_CALL(rl_write, RL_OK, db, &rl_data_type_header, 0, NULL);
		}
	}
	RL_CALL(rl_long_set, RL_OK, db, db->next_empty_page, page_number);
	db->next_empty_page = page_number;
cleanup:
	return retval;
}

int rl_dirty_hash(struct rlite *db, unsigned char **hash)
{
	long i;
	int retval = RL_OK;
	rl_page *page;
	SHA1_CTX sha;
	unsigned char *data = NULL;

	if (db->write_pages_len == 0) {
		*hash = NULL;
		goto cleanup;
	}

	RL_MALLOC(data, db->page_size * sizeof(unsigned char));
	RL_MALLOC(*hash, sizeof(unsigned char) * 20);
	SHA1Init(&sha);
	for (i = 0; i < db->write_pages_len; i++) {
		page = db->write_pages[i];
		memset(data, 0, db->page_size);
		if (page->type) {
			retval = page->type->serialize(db, page->obj, data);
		}
		SHA1Update(&sha, data, db->page_size);
	}
	SHA1Final(*hash, &sha);
cleanup:
	rl_free(data);
	if (retval != RL_OK) {
		rl_free(*hash);
		*hash = NULL;
	}
	return retval;
}

int rl_commit(struct rlite *db)
{
	int retval;
	RL_CALL(rl_write_apply_wal, RL_OK, db);
	db->initial_next_empty_page = db->next_empty_page;
	db->initial_number_of_pages = db->number_of_pages;
	db->initial_number_of_databases = db->number_of_databases;
	rl_free(db->initial_databases);
	RL_MALLOC(db->initial_databases, sizeof(long) * (db->number_of_databases + RLITE_INTERNAL_DB_COUNT));
	memcpy(db->initial_databases, db->databases, sizeof(long) * (db->number_of_databases + RLITE_INTERNAL_DB_COUNT));
	rl_discard(db);
cleanup:
	return retval;
}

int rl_discard(struct rlite *db)
{
	long i;
	void *tmp;
	int retval = RL_OK;

	rl_page *page;

	if (db->driver_type == RL_FILE_DRIVER) {
		rl_file_driver *driver = db->driver;
		if (driver->fp) {
			RL_CALL(rl_flock, RL_OK, driver->fp, RLITE_FLOCK_UN);
			fclose(driver->fp);
			driver->fp = NULL;
		}
	}

	for (i = 0; i < db->read_pages_len; i++) {
		page = db->read_pages[i];
		if (page->type == NULL) {
			// read only, from wal
			rl_free(page->obj);
		} else if (page->type->destroy && page->obj) {
			page->type->destroy(db, page->obj);
		}
#ifdef RL_DEBUG
		rl_free(page->serialized_data);
#endif
		rl_free(page);
	}
	for (i = 0; i < db->write_pages_len; i++) {
		page = db->write_pages[i];
		if (page->type && page->type->destroy && page->obj) {
			page->type->destroy(db, page->obj);
		}
#ifdef RL_DEBUG
		rl_free(page->serialized_data);
#endif
		rl_free(page);
	}
	db->read_pages_len = 0;
	db->write_pages_len = 0;

	db->next_empty_page = db->initial_next_empty_page;
	db->number_of_pages = db->initial_number_of_pages;
	db->number_of_databases = db->initial_number_of_databases;
	rl_free(db->databases);
	RL_MALLOC(db->databases, sizeof(long) * (db->number_of_databases + RLITE_INTERNAL_DB_COUNT)); // ?
	if (db->initial_databases) {
		memcpy(db->databases, db->initial_databases, sizeof(long) *  (db->number_of_databases + RLITE_INTERNAL_DB_COUNT));
	}

	if (db->read_pages_alloc != DEFAULT_READ_PAGES_LEN) {
		tmp = rl_realloc(db->read_pages, sizeof(rl_page *) * DEFAULT_READ_PAGES_LEN);
		if (!tmp) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		db->read_pages = tmp;
		db->read_pages_alloc = DEFAULT_READ_PAGES_LEN;
	}
	if (db->write_pages_alloc > 0) {
		if (db->write_pages_alloc != DEFAULT_WRITE_PAGES_LEN) {
			db->write_pages_alloc = DEFAULT_WRITE_PAGES_LEN;
			tmp = rl_realloc(db->write_pages, sizeof(rl_page *) * DEFAULT_WRITE_PAGES_LEN);
			if (!tmp) {
				retval = RL_OUT_OF_MEMORY;
				goto cleanup;
			}
			db->write_pages = tmp;
		}
	}
cleanup:
	return retval;
}

int rl_database_is_balanced(rlite *db, short *pages)
{
	int retval;
	void *tmp = NULL;
	rl_key *key;
	rl_btree *btree;
	rl_btree_iterator *iterator = NULL;
	retval = rl_get_key_btree(db, &btree, 0);
	if (retval == RL_NOT_FOUND) {
		retval = RL_OK;
		goto cleanup;
	}
	else if (retval != RL_OK) {
		goto cleanup;
	}

	RL_CALL(rl_btree_pages, RL_OK, db, btree, pages);
	RL_CALL(rl_btree_iterator_create, RL_OK, db, btree, &iterator);

	while ((retval = rl_btree_iterator_next(iterator, NULL, &tmp)) == RL_OK) {
		key = tmp;
		pages[key->string_page] = 1;
		pages[key->value_page] = 1;
		RL_CALL(rl_multi_string_pages, RL_OK, db, key->string_page, pages);
		if (key->type == RL_TYPE_ZSET) {
			retval = rl_zset_pages(db, key->value_page, pages);
		}
		else if (key->type == RL_TYPE_HASH) {
			retval = rl_hash_pages(db, key->value_page, pages);
		}
		else if (key->type == RL_TYPE_SET) {
			retval = rl_set_pages(db, key->value_page, pages);
		}
		else if (key->type == RL_TYPE_LIST) {
			retval = rl_llist_pages(db, key->value_page, pages);
		}
		else if (key->type == RL_TYPE_STRING) {
			retval = rl_string_pages(db, key->value_page, pages);
		}
		else {
			fprintf(stderr, "Unknown type %d\n", key->type);
			goto cleanup;
		}
		if (retval != RL_OK) {
			goto cleanup;
		}
		rl_free(tmp);
	}
	tmp = NULL;
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	retval = RL_OK;

cleanup:
	rl_free(tmp);
	rl_btree_iterator_destroy(iterator);
	return retval;
}

int rl_is_balanced(rlite *db)
{
	int retval;
	long i, selected_database = db->selected_database;
	short *pages = NULL;
	long missing_pages = 0;
	RL_MALLOC(pages, sizeof(short) * db->number_of_pages);

	for (i = 1; i < db->number_of_pages; i++) {
		pages[i] = 0;
	}

	for (i = 0; i < db->number_of_databases + RLITE_INTERNAL_DB_COUNT; i++) {
		if (db->databases[i] == 0) {
			continue;
		}
		pages[db->databases[i]] = 1;
		RL_CALL(rl_select, RL_OK, db, i);
		RL_CALL(rl_database_is_balanced, RL_OK, db, pages);
	}

	RL_CALL(rl_select, RL_OK, db, selected_database);

	long page_number = db->next_empty_page;
	while (page_number != db->number_of_pages) {
		pages[page_number] = 1;
		RL_CALL(rl_long_get, RL_OK, db, &page_number, page_number);
	}

	for (i = 1; i < db->number_of_pages; i++) {
		if (pages[i] == 0) {
			fprintf(stderr, "Found orphan page %ld\n", i);
			missing_pages++;
		}
	}

	if (missing_pages) {
		fprintf(stderr, "Missing %ld pages\n", missing_pages);
		retval = RL_UNEXPECTED;
	}
cleanup:
	rl_free(pages);
	return retval;
}

int rl_select_internal(struct rlite *db, int internal)
{
	db->selected_internal = internal;
	return RL_OK;
}

int rl_select(struct rlite *db, int selected_database)
{
	if (selected_database < 0 || selected_database >= db->number_of_databases) {
		return RL_INVALID_PARAMETERS;
	}
	db->selected_database = selected_database;
	return RL_OK;
}

int rl_move(struct rlite *db, unsigned char *key, long keylen, int database)
{
	int retval;
	int olddb = db->selected_database;
	unsigned char type;
	unsigned long long expires;
	long value_page;
	// this could be more efficient, if we don't delete the value page
	RL_CALL(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &value_page, &expires, NULL);
	RL_CALL(rl_select, RL_OK, db, database);
	RL_CALL(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);
	RL_CALL(rl_select, RL_OK, db, olddb);
	RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
	RL_CALL(rl_select, RL_OK, db, database);
	RL_CALL(rl_key_set, RL_OK, db, key, keylen, type, value_page, expires, 0);
	retval = RL_OK;
cleanup:
	rl_select(db, olddb);
	return retval;
}

int rl_rename(struct rlite *db, const unsigned char *src, long srclen, const unsigned char *target, long targetlen, int overwrite)
{
	int retval;
	unsigned char type;
	unsigned long long expires;
	long value_page;
	long version = 0;
	if (overwrite) {
		RL_CALL2(rl_key_get, RL_FOUND, RL_NOT_FOUND, db, target, targetlen, NULL, NULL, NULL, NULL, &version);
		if (retval == RL_FOUND) {
			RL_CALL(rl_key_delete_with_value, RL_OK, db, target, targetlen);
			version++;
		}
	}
	else {
		RL_CALL(rl_key_get, RL_NOT_FOUND, db, target, targetlen, NULL, NULL, NULL, NULL, NULL);
	}
	// this could be more efficient, if we don't delete the value page
	RL_CALL(rl_key_get, RL_FOUND, db, src, srclen, &type, NULL, &value_page, &expires, NULL);
	RL_CALL(rl_key_delete, RL_OK, db, src, srclen);
	RL_CALL(rl_key_set, RL_OK, db, target, targetlen, type, value_page, expires, version);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_dbsize(struct rlite *db, long *size)
{
	int retval;
	rl_btree *btree;
	retval = rl_get_key_btree(db, &btree, 0);
	if (retval == RL_NOT_FOUND) {
		*size = 0;
		retval = RL_OK;
		goto cleanup;
	}
	else if (retval != RL_OK) {
		goto cleanup;
	}
	*size = btree->number_of_elements;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_keys(struct rlite *db, unsigned char *pattern, long patternlen, long *_len, unsigned char ***_result, long **_resultlen)
{
	int retval;
	rl_btree *btree;
	rl_btree_iterator *iterator;
	rl_key *key;
	void *tmp;
	long alloc, len;
	unsigned char **result = NULL, *keystr;
	long *resultlen = NULL, keystrlen;
	retval = rl_get_key_btree(db, &btree, 0);
	if (retval == RL_NOT_FOUND) {
		*_len = 0;
		*_result = NULL;
		*_resultlen = NULL;
		retval = RL_OK;
		goto cleanup;
	}
	else if (retval != RL_OK) {
		goto cleanup;
	}

	RL_CALL(rl_btree_iterator_create, RL_OK, db, btree, &iterator);

	len = 0;
	alloc = 16;
	RL_MALLOC(result, sizeof(unsigned char *) * alloc);
	RL_MALLOC(resultlen, sizeof(long) * alloc);
	int allkeys = patternlen == 1 && pattern[0] == '*';
	while ((retval = rl_btree_iterator_next(iterator, NULL, &tmp)) == RL_OK) {
		key = tmp;
		RL_CALL(rl_multi_string_get, RL_OK, db, key->string_page, &keystr, &keystrlen);
		if (allkeys || rl_stringmatchlen((char *)pattern, patternlen, (char *)keystr, keystrlen, 0)) {
			if (len + 1 == alloc) {
				RL_REALLOC(result, sizeof(unsigned char *) * alloc * 2)
				RL_REALLOC(resultlen, sizeof(long) * alloc * 2)
				alloc *= 2;
			}
			result[len] = keystr;
			resultlen[len] = keystrlen;
			len++;
		}
		else {
			rl_free(keystr);
		}
		rl_free(key);
	}

	if (retval != RL_END) {
		goto cleanup;
	}

	// TODO: should we realloc to shrink the result?
	*_len = len;
	*_result = result;
	*_resultlen = resultlen;

	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_free(result);
		rl_free(resultlen);
	}
	return retval;
}

int rl_randomkey(struct rlite *db, unsigned char **key, long *keylen)
{
	int retval;
	rl_btree *btree;
	rl_key *key_obj;
	RL_CALL(rl_get_key_btree, RL_OK, db, &btree, 0);
	RL_CALL(rl_btree_random_element, RL_OK, db, btree, NULL, (void **)&key_obj);
	RL_CALL(rl_multi_string_get, RL_OK, db, key_obj->string_page, key, keylen);
cleanup:
	return retval;
}

int rl_flushdb(struct rlite *db)
{
	int retval;
	rl_btree *btree;
	rl_btree_iterator *iterator = NULL;
	rl_key *key;
	void *tmp;
	RL_CALL2(rl_get_key_btree, RL_OK, RL_NOT_FOUND, db, &btree, 0);
	if (retval == RL_NOT_FOUND) {
		retval = RL_OK;
		goto cleanup;
	}

	RL_CALL(rl_btree_iterator_create, RL_OK, db, btree, &iterator);

	while ((retval = rl_btree_iterator_next(iterator, NULL, &tmp)) == RL_OK) {
		key = tmp;
		RL_CALL(rl_key_delete_value, RL_OK, db, key->type, key->value_page);
		RL_CALL(rl_multi_string_delete, RL_OK, db, key->string_page);
		rl_free(key);
	}
	if (retval != RL_END) {
		goto cleanup;
	}
	RL_CALL(rl_btree_delete, RL_OK, db, btree);
	RL_CALL(rl_delete, RL_OK, db, db->databases[db->selected_database]);
	db->databases[db->selected_database] = 0;
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_header, 0, NULL);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_flushall(struct rlite *db)
{
	int retval, i;
	int selected_database = db->selected_database;

	for (i = 0; i < db->number_of_databases + RLITE_INTERNAL_DB_COUNT; i++) {
		db->selected_database = i;
		RL_CALL(rl_flushdb, RL_OK, db);
	}
	db->selected_database = selected_database;
cleanup:
	return retval;
}
