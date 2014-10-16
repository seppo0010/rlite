#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <openssl/md5.h>
#include "rlite.h"
#include "btree.h"
#include "util.h"

#define DEFAULT_READ_PAGES_LEN 16
#define DEFAULT_WRITE_PAGES_LEN 8

int rl_serialize_header(struct rlite *db, void *obj, unsigned char *data);
int rl_deserialize_header(struct rlite *db, void **obj, void *context, unsigned char *data);
int rl_has_flag(rlite *db, int flag);

rl_data_type rl_data_type_btree_hash_md5_long = {"btree_hash_md5_long", rl_serialize_btree_hash_md5_long, rl_deserialize_btree_hash_md5_long};
rl_data_type rl_data_type_btree_node_hash_md5_long = {"btree_node_hash_md5_long", rl_serialize_btree_node_hash_md5_long, rl_deserialize_btree_node_hash_md5_long};
rl_data_type rl_data_type_header = {"header", rl_serialize_header, rl_deserialize_header};

static const char *identifier = "rlite0.0";

int rl_serialize_header(struct rlite *db, void *obj, unsigned char *data)
{
	obj = obj;
	int retval = RL_OK;
	int identifier_len = strlen(identifier);
	memcpy(data, identifier, identifier_len);
	put_4bytes(&data[identifier_len], db->page_size);
	return retval;
}

int rl_deserialize_header(struct rlite *db, void **obj, void *context, unsigned char *data)
{
	obj = obj;
	context = context;
	int retval = RL_OK;
	int identifier_len = strlen(identifier);
	if (memcmp(data, identifier, identifier_len) != 0) {
		fprintf(stderr, "Unexpected header, expecting %s\n", identifier);
		return RL_INVALID_STATE;
	}
	db->page_size = get_4bytes(&data[identifier_len]);
	return retval;
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
	int retval = RL_OK;
	rlite *db = malloc(sizeof(rlite));
	if (db == NULL) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}

	db->page_size = 1024; // TODO: read db header
	db->read_pages = db->write_pages = NULL;

	db->read_pages = malloc(sizeof(rl_page *) * DEFAULT_READ_PAGES_LEN);
	if (db->read_pages == NULL) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	db->read_pages_len = 0;
	db->read_pages_alloc = DEFAULT_READ_PAGES_LEN;
	db->write_pages_len = 0;
	if ((flags & RLITE_OPEN_READWRITE) > 0) {
		db->write_pages_alloc = DEFAULT_WRITE_PAGES_LEN;
		db->write_pages = malloc(sizeof(rl_page *) * DEFAULT_WRITE_PAGES_LEN);
	}
	else {
		db->write_pages_alloc = 0;
	}

	if (memcmp(filename, ":memory:", 9) != 0) {
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

		rl_file_driver *driver = malloc(sizeof(rl_file_driver));
		if (driver == NULL) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		driver->fp = NULL;
		driver->filename = malloc(sizeof(char) * (strlen(filename) + 1));
		if (driver->filename == NULL) {
			free(driver);
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		strcpy(driver->filename, filename);
		driver->mode = flags;
		db->driver = driver;
		db->driver_type = RL_FILE_DRIVER;
	}

	rl_read_header(db);

	*_db = db;
cleanup:
	if (retval != RL_OK) {
		free(db->read_pages);
		free(db->write_pages);
		free(db);
	}
	return retval;
}

int rl_close(rlite *db)
{
	if (db->driver_type == RL_FILE_DRIVER) {
		rl_file_driver *driver = db->driver;
		if (driver->fp) {
			fclose(driver->fp);
		}
		free(driver->filename);
		free(db->driver);
	}
	free(db->read_pages);
	free(db->write_pages);
	free(db);
	return RL_OK;
}

int rl_has_flag(rlite *db, int flag)
{
	if (db->driver_type == RL_FILE_DRIVER) {
		return (((rl_file_driver *)db->driver)->mode & flag) > 0;
	}
	fprintf(stderr, "Unknown driver_type %d\n", db->driver_type);
	return RL_UNEXPECTED;
}

int rl_create_db(rlite *db)
{
	int retval = rl_ensure_pages(db);
	return retval;
}

int rl_read_header(rlite *db)
{
	db->page_size = 100;
	void *header;
	int retval = rl_read(db, &rl_data_type_header, 0, NULL, &header);
	if (retval == RL_NOT_FOUND && rl_has_flag(db, RLITE_OPEN_CREATE)) {
		retval = rl_create_db(db);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}
cleanup:
	return retval;
}

static int rl_search_cache(rlite *db, rl_data_type *type, long page_number, void **obj, rl_page **pages, long page_len)
{
	db = db;

	long pos, min = 0, max = page_len - 1;
	rl_page *page;
	if (max > 0) {
		do {
			pos = min + (max - min) / 2;
			page = pages[pos];
			if (page->page_number == page_number) {
				if (page->type != type) {
					fprintf(stderr, "Type of page in cache (%s) doesn't match the asked one (%s)\n", page->type->name, type->name);
					return RL_UNEXPECTED;
				}
				*obj = page->obj;
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
	}
	return RL_NOT_FOUND;
}

int rl_read_from_cache(rlite *db, rl_data_type *type, long page_number, void **obj)
{
	int retval = rl_search_cache(db, type, page_number, obj, db->write_pages, db->write_pages_len);
	if (retval == RL_NOT_FOUND) {
		retval = rl_search_cache(db, type, page_number, obj, db->read_pages, db->read_pages_len);
	}
	return retval;
}

int rl_read(rlite *db, rl_data_type *type, long page, void *context, void **obj)
{
	int retval = rl_read_from_cache(db, type, page, obj);
	if (retval != RL_NOT_FOUND) {
		return retval;
	}
	unsigned char *data = malloc(sizeof(unsigned char) * db->page_size);
	if (data == NULL) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	if (db->driver_type == RL_FILE_DRIVER) {
		rl_file_driver *driver = db->driver;
		if (driver->fp == NULL) {
			driver->fp = fopen(driver->filename, (driver->mode & RLITE_OPEN_READWRITE) == 0 ? "r" : "w");
			if (driver->fp == NULL) {
				fprintf(stderr, "Cannot open file %s, errno %d\n", driver->filename, errno);
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
		}
		fseek(driver->fp, page * db->page_size, SEEK_SET);
		size_t read = fread(data, sizeof(unsigned char), db->page_size, driver->fp);
		if (read != (size_t)db->page_size) {
			retval = RL_NOT_FOUND;
			goto cleanup;
		}
	}
	else {
		retval = RL_NOT_IMPLEMENTED;
		goto cleanup;
	}
	retval = type->deserialize(db, obj, context, data);
cleanup:
	if (retval != RL_OK) {
		free(data);
	}
	return retval;
}

static int md5(const char *data, long datalen, unsigned char digest[16])
{
	MD5_CTX md5;
	MD5_Init(&md5);
	MD5_Update(&md5, data, datalen);
	MD5_Final(digest, &md5);
	return RL_OK;
}

int rl_has_key(rlite *db, const char *key, long keylen)
{
	unsigned char digest[16];
	int retval = md5(key, keylen, digest);
	if (retval != RL_OK) {
		goto cleanup;
	}
	void *_btree;
	retval = rl_read(db, &rl_data_type_btree_hash_md5_long, 1, NULL, &_btree);
	if (retval != RL_OK) {
		goto cleanup;
	}
	rl_btree *btree = (rl_btree *)_btree;
	retval = rl_btree_find_score(btree, digest, NULL, NULL, NULL);
cleanup:
	return retval;
}
