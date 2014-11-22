#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "page_btree.h"
#include "page_list.h"
#include "page_long.h"
#include "page_string.h"
#include "page_skiplist.h"
#include "page_multi_string.h"
#include "type_zset.h"
#include "rlite.h"
#include "util.h"
#ifdef DEBUG
#include <valgrind/valgrind.h>
#endif

#define DEFAULT_READ_PAGES_LEN 16
#define DEFAULT_WRITE_PAGES_LEN 8
#define DEFAULT_PAGE_SIZE 1024
#define HEADER_SIZE 100
#define GLOBAL_KEY_BTREE 1

int rl_header_serialize(struct rlite *db, void *obj, unsigned char *data);
int rl_header_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data);
int rl_has_flag(rlite *db, int flag);

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
rl_data_type rl_data_type_header = {
	"rl_data_type_header",
	rl_header_serialize,
	rl_header_deserialize,
	NULL
};
rl_data_type rl_data_type_btree_set_long = {
	"rl_data_type_btree_set_long",
	rl_btree_serialize,
	rl_btree_deserialize,
	rl_btree_destroy,
};
rl_data_type rl_data_type_btree_node_set_long = {
	"rl_data_type_btree_node_set_long",
	rl_btree_node_serialize_set_long,
	rl_btree_node_deserialize_set_long,
	rl_btree_node_destroy,
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

static int file_driver_fp(rlite *db, int readonly)
{
	int retval = RL_OK;
	rl_file_driver *driver = db->driver;
	if (driver->fp == NULL) {
		if (!readonly && (driver->mode & RLITE_OPEN_READWRITE) == 0) {
			fprintf(stderr, "Trying to write but open as readonly\n");
			retval = RL_INVALID_PARAMETERS;
			goto cleanup;
		}
		char *mode;
		if (access(driver->filename, F_OK) == 0) {
			if (readonly) {
				mode = "r";
			}
			else {
				mode = "r+";
			}
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
	return RL_OK;
}

int rl_header_deserialize(struct rlite *db, void **UNUSED(obj), void *UNUSED(context), unsigned char *data)
{
	int identifier_len = strlen((char *)identifier);
	if (memcmp(data, identifier, identifier_len) != 0) {
		fprintf(stderr, "Unexpected header, expecting %s\n", identifier);
		return RL_INVALID_STATE;
	}
	db->page_size = get_4bytes(&data[identifier_len]);
	db->next_empty_page = get_4bytes(&data[identifier_len + 4]);
	db->number_of_pages = get_4bytes(&data[identifier_len + 8]);
	return RL_OK;
}

static int rl_ensure_pages(rlite *db)
{
	void *tmp;
	if (rl_has_flag(db, RLITE_OPEN_READWRITE)) {
		if (db->write_pages_len == db->write_pages_alloc) {
			tmp = realloc(db->write_pages, sizeof(rl_page *) * db->write_pages_alloc * 2);
			if (!tmp) {
				return RL_OUT_OF_MEMORY;
			}
			db->write_pages = tmp;
			db->write_pages_alloc *= 2;
		}
	}
	if (db->read_pages_len == db->read_pages_alloc) {
		tmp = realloc(db->read_pages, sizeof(rl_page *) * db->read_pages_alloc * 2);
		if (!tmp) {
			return RL_OUT_OF_MEMORY;
		}
		db->read_pages = tmp;
		db->read_pages_alloc *= 2;
	}
	return RL_OK;
}

int rl_open(const char *filename, rlite **_db, int flags)
{
	rl_btree_init();
	rl_list_init();
	int retval = RL_OK;
	rlite *db;
	RL_MALLOC(db, sizeof(*db));

	db->page_size = DEFAULT_PAGE_SIZE;
	db->read_pages = db->write_pages = NULL;
	db->driver = NULL;
	db->driver_type = RL_MEMORY_DRIVER;
	db->read_pages_alloc = db->read_pages_len = db->write_pages_len = db->write_pages_alloc = 0;

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

	if (memcmp(filename, ":memory:", 9) == 0) {
		db->driver_type = RL_MEMORY_DRIVER;
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

int rl_close(rlite *db)
{
	if (!db) {
		return RL_OK;
	}
	if (db->driver_type == RL_FILE_DRIVER) {
		rl_file_driver *driver = db->driver;
		if (driver->fp) {
			fclose(driver->fp);
		}
		rl_free(driver->filename);
		rl_free(db->driver);
	}
	rl_discard(db);
	rl_free(db->read_pages);
	rl_free(db->write_pages);
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
	int retval;
	RL_CALL(rl_ensure_pages, RL_OK, db);
	db->next_empty_page = 2;
	db->number_of_pages = 2;
	rl_btree *btree;
	long max_node_size = (db->page_size - 8) / 8; // TODO: this should be in the type
	RL_CALL(rl_btree_create, RL_OK, db, &btree, &rl_btree_type_hash_sha1_key, max_node_size);
	retval = rl_write(db, &rl_data_type_btree_hash_sha1_key, GLOBAL_KEY_BTREE, btree);
	if (retval != RL_OK) {
		rl_discard(db);
		goto cleanup;
	}
cleanup:
	return retval;
}

int rl_get_key_btree(rlite *db, rl_btree **btree)
{
	void *_btree;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_btree_hash_sha1_key, GLOBAL_KEY_BTREE, &rl_btree_type_hash_sha1_key, &_btree, 1);
	*btree = _btree;
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
	else {
		retval = rl_read(db, &rl_data_type_header, 0, NULL, NULL, 1);
		if (retval == RL_NOT_FOUND && rl_has_flag(db, RLITE_OPEN_CREATE)) {
			db->page_size = DEFAULT_PAGE_SIZE;
			RL_CALL(rl_create_db, RL_OK, db);
			RL_CALL(rl_write, RL_OK, db, &rl_data_type_header, 0, NULL);
		}
		else if (retval != RL_FOUND) {
			goto cleanup;
		}
	}
	retval = RL_OK;
cleanup:
	return retval;
}

#ifdef DEBUG
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

#ifdef DEBUG
static int rl_search_cache(rlite *db, rl_data_type *type, long page_number, void **obj, long *position, rl_page **pages, long page_len)
#else
static int rl_search_cache(rlite *UNUSED(db), rl_data_type *UNUSED(type), long page_number, void **obj, long *position, rl_page **pages, long page_len)
#endif
{
	long pos, min = 0, max = page_len - 1;
	rl_page *page;
	if (max >= 0) {
		do {
			pos = min + (max - min) / 2;
			page = pages[pos];
			if (page->page_number == page_number) {
#ifdef DEBUG
				if (page->type != &rl_data_type_long && type != &rl_data_type_long && type != NULL && page->type != type) {
					fprintf(stderr, "Type of page in cache (%s) doesn't match the asked one (%s)\n", page->type->name, type->name);
					return RL_UNEXPECTED;
				}
#endif
				if (obj) {
					*obj = page->obj;
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
#ifdef DEBUG
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

int rl_read_from_cache(rlite *db, rl_data_type *type, long page_number, void **obj)
{
	int retval = rl_search_cache(db, type, page_number, obj, NULL, db->write_pages, db->write_pages_len);
	if (retval == RL_NOT_FOUND) {
		retval = rl_search_cache(db, type, page_number, obj, NULL, db->read_pages, db->read_pages_len);
	}
	return retval;
}

int rl_read(rlite *db, rl_data_type *type, long page, void *context, void **obj, int cache)
{
#ifdef DEBUG
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
	retval = rl_read_from_cache(db, type, page, obj);
	if (retval != RL_NOT_FOUND) {
		if (db->driver_type != RL_MEMORY_DRIVER && !cache) {
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
		RL_CALL(file_driver_fp, RL_OK, db, 1);
		rl_file_driver *driver = db->driver;
		fseek(driver->fp, page * db->page_size, SEEK_SET);
		size_t read = fread(data, sizeof(unsigned char), db->page_size, driver->fp);
		if (read != (size_t)db->page_size) {
			if (page > 0) {
#ifdef DEBUG
				print_cache(db);
#endif
				fprintf(stderr, "Unable to read page %ld: ", page);
				perror(NULL);
			}
			retval = RL_NOT_FOUND;
			goto cleanup;
		}
		long pos;
		retval = rl_search_cache(db, type, page, NULL, &pos, db->read_pages, db->read_pages_len);
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
#ifdef DEBUG
			keep = 1;
			if (initial_page_size != db->page_size) {
				page_obj->serialized_data = realloc(data, db->page_size * sizeof(unsigned char));
				if (page_obj->serialized_data == NULL) {
					fprintf(stderr, "realloc failed\n");
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
	}
	else {
		fprintf(stderr, "Unexpected driver %d when asking for page %ld\n", db->driver_type, page);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	if (retval == RL_OK) {
		retval = RL_FOUND;
	}
cleanup:
#ifdef DEBUG
	if (!keep) {
		rl_free(data);
	}
#endif
#ifndef DEBUG
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

	retval = rl_search_cache(db, type, page_number, NULL, &pos, db->write_pages, db->write_pages_len);
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
		rl_ensure_pages(db);
		RL_MALLOC(page, sizeof(*page));
#ifdef DEBUG
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

		retval = rl_search_cache(db, type, page_number, NULL, &pos, db->read_pages, db->read_pages_len);
		if (retval == RL_FOUND) {
#ifdef DEBUG
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
	retval = rl_search_cache(db, NULL, page_number, NULL, &pos, db->write_pages, db->write_pages_len);
	if (retval == RL_FOUND) {
		db->write_pages[pos]->obj = NULL;
	}
	else if (retval != RL_NOT_FOUND) {
		goto cleanup;
	}
	retval = rl_search_cache(db, NULL, page_number, NULL, &pos, db->read_pages, db->read_pages_len);
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
	int retval;
	RL_CALL(rl_long_set, RL_OK, db, db->next_empty_page, page_number);
	db->next_empty_page = page_number;
cleanup:
	return retval;
}

int rl_commit(struct rlite *db)
{
	int retval = RL_UNEXPECTED;
	long i, page_number;
	rl_page *page;
	size_t written;
	unsigned char *data;
	RL_MALLOC(data, db->page_size * sizeof(unsigned char));
#ifdef DEBUG
	for (i = 0; i < db->read_pages_len; i++) {
		page = db->read_pages[i];
		memset(data, 0, db->page_size);
		retval = page->type->serialize(db, page->obj, data);
		if (retval != RL_OK) {
			goto cleanup;
		}
		if (memcmp(data, page->serialized_data, db->page_size) != 0) {
			fprintf(stderr, "Read page %ld (%s) has changed\n", page->page_number, page->type->name);
			for (i = 0; i < db->page_size; i++) {
				if (page->serialized_data[i] != data[i]) {
					fprintf(stderr, "Different data in position %ld (expected %d, got %d)\n", i, page->serialized_data[i], data[i]);
				}
			}
			retval = rl_search_cache(db, page->type, page->page_number, NULL, NULL, db->write_pages, db->write_pages_len);
			if (retval == RL_FOUND) {
				fprintf(stderr, "Page found in write_pages\n");
			}
			else {
				fprintf(stderr, "Page not found in write_pages\n");
			}
			exit(1);
		}
	}
#endif
	if (db->driver_type == RL_FILE_DRIVER) {
		rl_file_driver *driver = db->driver;
		if (driver->fp) {
			fclose(driver->fp);
			driver->fp = NULL;
		}
		retval = file_driver_fp(db, 0);
		if (retval != RL_OK) {
			goto cleanup;
		}
		for (i = 0; i < db->write_pages_len; i++) {
			page = db->write_pages[i];
			page_number = page->page_number;
			memset(data, 0, db->page_size);
			if (page->type) {
				retval = page->type->serialize(db, page->obj, data);
			}
			fseek(driver->fp, page_number * db->page_size, SEEK_SET);
			written = fwrite(data, sizeof(unsigned char), db->page_size, driver->fp);
			if ((size_t)db->page_size != written) {
#ifdef DEBUG
				print_cache(db);
#endif
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
		}
		rl_discard(db);
	}
	else if (db->driver_type == RL_MEMORY_DRIVER) {
		retval = RL_OK;
	}
	rl_free(data);
cleanup:
	return retval;
}

int rl_discard(struct rlite *db)
{
	long i;
	void *tmp;
	int retval = RL_OK;

	rl_page *page;
	for (i = 0; i < db->read_pages_len; i++) {
		page = db->read_pages[i];
		if (page->type->destroy && page->obj) {
			page->type->destroy(db, page->obj);
		}
#ifdef DEBUG
		rl_free(page->serialized_data);
#endif
		rl_free(page);
	}
	for (i = 0; i < db->write_pages_len; i++) {
		page = db->write_pages[i];
		if (page->type && page->type->destroy && page->obj) {
			page->type->destroy(db, page->obj);
		}
#ifdef DEBUG
		rl_free(page->serialized_data);
#endif
		rl_free(page);
	}
	db->read_pages_len = 0;
	db->write_pages_len = 0;

	if (db->read_pages_alloc != DEFAULT_READ_PAGES_LEN) {
		tmp = realloc(db->read_pages, sizeof(rl_page *) * DEFAULT_READ_PAGES_LEN);
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
			tmp = realloc(db->write_pages, sizeof(rl_page *) * DEFAULT_WRITE_PAGES_LEN);
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

int rl_is_balanced(rlite *db)
{
	int retval;
	void *tmp = NULL;
	rl_key *key;
	rl_btree *btree;
	rl_btree_iterator *iterator = NULL;
	short *pages = NULL;
	long i;
	long missing_pages = 0;
	RL_CALL(rl_get_key_btree, RL_OK, db, &btree);
	RL_MALLOC(pages, sizeof(short) * db->number_of_pages);

	for (i = 1; i < db->number_of_pages; i++) {
		pages[i] = 0;
	}

	pages[GLOBAL_KEY_BTREE] = 1;

	RL_CALL(rl_btree_pages, RL_OK, db, btree, pages);
	RL_CALL(rl_btree_iterator_create, RL_OK, db, btree, &iterator);

	while ((retval = rl_btree_iterator_next(iterator, NULL, &tmp)) == RL_OK) {
		key = tmp;
		pages[key->string_page] = 1;
		pages[key->value_page] = 1;
		RL_CALL(rl_multi_string_pages, RL_OK, db, key->string_page, pages);
		if (key->type == RL_TYPE_ZSET) {
			rl_zset_pages(db, key->value_page, pages);
		}
		else {
			fprintf(stderr, "Unknown type %d\n", key->type);
			goto cleanup;
		}
		rl_free(tmp);
	}
	tmp = NULL;
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	long page_number = db->next_empty_page;
	while (page_number != db->number_of_pages) {
		pages[page_number] = 1;
		RL_CALL(rl_long_get, RL_OK, db, &page_number, page_number);
	}

	retval = RL_OK;

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
	rl_free(tmp);
	if (iterator) {
		rl_btree_iterator_destroy(iterator);
	}
	rl_free(pages);
	return retval;
}
