#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "rlite/rlite.h"
#include "rlite/flock.h"
#include "rlite/sha1.h"

#ifdef RL_DEBUG
int rl_search_cache(rlite *db, rl_data_type *type, long page_number, void **obj, long *position, void *context, rl_page **pages, long page_len);
#endif

static const char *identifier = "rlwal0.0";

static char *get_wal_filename(const char *filename) {
	return rl_get_filename_with_suffix(filename, ".wal");
}

static int rl_delete_wal(const char *wal_path) {
	unlink(wal_path);
	return RL_OK;
}

static int rl_read_wal(const char *wal_path, unsigned char **_data, size_t *_datalen) {
	int retval = RL_OK;
	char *data = NULL;
	size_t datalen;
	FILE *fp = fopen(wal_path, "rb");
	if (fp == NULL) {
		// file does not exist! empty data, retval=ok are set
		goto cleanup;
	}
	RL_CALL(rl_flock, RL_OK, fp, RLITE_FLOCK_EX);
	fseek(fp, 0, SEEK_END);
	datalen = (size_t)ftell(fp);
	fseek(fp, 0, SEEK_SET);

	data = rl_malloc(datalen * sizeof(char));
	fread(data, datalen, 1, fp);
	fclose(fp);
cleanup:
	if (retval == RL_OK) {
		*_data = (unsigned char *)data;
		*_datalen = datalen;
	}
	return retval;
}

static int rl_apply_wal_data(rlite *db, unsigned char *data, size_t datalen, int skip_check) {
	if (skip_check == 0) {
		if (datalen < 36 + (size_t)db->page_size) {
			// too short to be a valid wal file
			return RL_UNEXPECTED;
		}
		if (memcmp(data, identifier, strlen(identifier))) {
			// expected identifier
			return RL_UNEXPECTED;
		}
		unsigned char digest[20];
		SHA1_CTX sha;
		SHA1Init(&sha);
		SHA1Update(&sha, &data[32], datalen - 32);
		SHA1Final(digest, &sha);
		if (memcmp(&data[8], digest, 20) != 0) {
			// digest mismatch
			return RL_UNEXPECTED;
		}
	}
	int retval;
	rl_file_driver *driver = db->driver;
	size_t written, position = 28;
	long page_number;
	long i, write_pages_len = get_4bytes(&data[position]);
	position += 4;
	int readwrite = (driver->mode & RLITE_OPEN_READWRITE) != 0;
	rl_page *page_obj;
	for (i = 0; i < write_pages_len; i++) {
		page_number = get_4bytes(&data[position]);
		position += 4;
		if (readwrite) {
			fseek(driver->fp, page_number * db->page_size, SEEK_SET);
			written = fwrite(&data[position], sizeof(unsigned char), db->page_size, driver->fp);
			if ((size_t)db->page_size != written) {
				// at this point we have corrupted the database
				// we have written something, but not all of it
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
		} else {
			/**
			 * Since we are in read-only mode, but the wal is fully written,
			 * we need to store its information as if it was written in the
			 * file, since it might be partially updated.
			 * We don't want to modify the file as a side effect when we are
			 * in a read only mode.
			 */

			// TODO: better cleanup on OOM
			rl_ensure_pages(db);
			RL_MALLOC(page_obj, sizeof(*page_obj));
#ifdef RL_DEBUG
			RL_MALLOC(page_obj->serialized_data, db->page_size * sizeof(unsigned char));
			memcpy(page_obj->serialized_data, &data[position], db->page_size);
#endif
			page_obj->page_number = page_number;
			page_obj->type = NULL;
			page_obj->obj = rl_malloc(sizeof(unsigned char) * db->page_size);
			if (page_obj->obj == NULL) {
				rl_free(page_obj);
				return RL_OUT_OF_MEMORY;
			}
			memcpy(page_obj->obj, &data[position], db->page_size);
			db->read_pages[db->read_pages_len] = page_obj;
			db->read_pages_len++;
		}
		if (page_number == 0) {
			// header has changed! need to parse it before using db->page_size
			RL_CALL(rl_header_deserialize, RL_OK, db, NULL, NULL, &data[position]);
		}
		position += db->page_size;
	}
	retval = RL_OK;
cleanup:
	return retval;
}

static int create_wal_data(rlite *db, unsigned char **_data, size_t *_datalen) {
	// 20 (sha1) + 8 (header) + 4 (number of pages)
	size_t datalen = db->write_pages_len * (db->page_size + 4) + 32;
	unsigned char *data;
	int i, retval = RL_OK;
	rl_page *page;
	SHA1_CTX sha;
	SHA1Init(&sha);
	RL_MALLOC(data, sizeof(char) * datalen);
	size_t position = strlen(identifier);
	memcpy(data, identifier, position);
	position += 20; // leaving space blank for sha1
	put_4bytes(&data[position], db->write_pages_len);
	position += 4;
	for (i = 0; i < db->write_pages_len; i++) {
		page = db->write_pages[i];
		put_4bytes(&data[position], page->page_number);
		position += 4;
		memset(&data[position], 0, db->page_size);
		if (page->type) {
			retval = page->type->serialize(db, page->obj, &data[position]);
		}
		position += db->page_size;
		SHA1Update(&sha, &data[position - db->page_size - 4], db->page_size + 4);
	}
	SHA1Final(&data[strlen(identifier)], &sha);
	*_data = data;
	*_datalen = datalen;
cleanup:
	if (retval != RL_OK) {
		rl_free(data);
	}
	return retval;
}

static int rl_write_wal_file(FILE *fp, rlite *db, unsigned char **_data, size_t *_datalen) {
	int retval;
	unsigned char *data = NULL;
	size_t datalen = 0;
	RL_CALL(create_wal_data, RL_OK, db, &data, &datalen);
	fwrite(data, sizeof(char), datalen, fp);
cleanup:
	if (retval == RL_OK && _data) {
		*_data = data;
		*_datalen = datalen;
	} else {
		rl_free(data);
	}
	return retval;
}
int rl_write_wal(const char *wal_path, rlite *db, unsigned char **_data, size_t *_datalen) {
	int retval;
	FILE *fp = NULL;
	fp = fopen(wal_path, "wb");
	RL_CALL(rl_flock, RL_OK, fp, RLITE_FLOCK_EX);
	RL_CALL(rl_write_wal_file, RL_OK, fp, db, _data, _datalen);
cleanup:
	if (fp) {
		fclose(fp);
	}
	return retval;
}

int rl_write_apply_wal(rlite *db) {
	FILE *fp = NULL;
	int retval = RL_OK;
	long i, page_number;
	rl_page *page;
	char *wal_path = NULL;
	unsigned char *data = NULL;
	size_t datalen;
#ifdef RL_DEBUG
	for (i = 0; i < db->read_pages_len; i++) {
		RL_MALLOC(data, db->page_size * sizeof(unsigned char));
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
			retval = rl_search_cache(db, page->type, page->page_number, NULL, NULL, NULL, db->write_pages, db->write_pages_len);
			if (retval == RL_FOUND) {
				fprintf(stderr, "Page found in write_pages\n");
			}
			else {
				fprintf(stderr, "Page not found in write_pages\n");
			}
			exit(1);
		}
		rl_free(data);
		data = NULL;
	}
#endif
	if (db->driver_type == RL_FILE_DRIVER) {
		rl_file_driver *driver = db->driver;
		wal_path = get_wal_filename(driver->filename);
		if (wal_path == NULL) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		fp = fopen(wal_path, "wb");
		if (fp == NULL) {
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		RL_CALL(rl_flock, RL_OK, fp, RLITE_FLOCK_EX);
		RL_CALL(rl_write_wal_file, RL_OK, fp, db, &data, &datalen);
		RL_CALL(rl_apply_wal_data, RL_OK, db, data, datalen, 1);
		ftruncate(fileno(fp), 0);
		fclose(fp);
		fp = NULL;
		RL_CALL(rl_delete_wal, RL_OK, wal_path);
		if (db->write_pages_len > 0) {
			fflush(driver->fp);
		}
		rl_free(data);
		data = NULL;
	}
	else if (db->driver_type == RL_MEMORY_DRIVER) {
		rl_memory_driver *driver = db->driver;
		if (db->write_pages_len > 0) {
			page = db->write_pages[db->write_pages_len - 1];
			if ((page->page_number + 1) * db->page_size > driver->datalen) {
				void *tmp = rl_realloc(driver->data, (page->page_number + 1) * db->page_size * sizeof(unsigned char));
				if (!tmp) {
					retval = RL_OUT_OF_MEMORY;
					goto cleanup;
				}
				driver->data = tmp;
				driver->datalen = (page->page_number + 1) * db->page_size;
			}
		}
		for (i = 0; i < db->write_pages_len; i++) {
			page = db->write_pages[i];
			page_number = page->page_number;
			memset(&driver->data[page_number * db->page_size], 0, db->page_size);
			if (page->type) {
				retval = page->type->serialize(db, page->obj, (unsigned char *)&driver->data[page_number * db->page_size]);
			}
		}
	}
cleanup:
	if (fp) {
		fclose(fp);
	}
	rl_free(data);
	rl_free(wal_path);
	return retval;
}

int rl_apply_wal(rlite *db) {
	rl_file_driver *driver = db->driver;
	int retval;
	unsigned char *data = NULL;
	size_t datalen;
	char *wal_path = get_wal_filename(driver->filename);
	if (wal_path == NULL) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	if (access(wal_path, F_OK) != 0) {
		retval = RL_OK;
		goto cleanup;
	}

	RL_CALL(rl_read_wal, RL_OK, wal_path, &data, &datalen);
	if (data != NULL) {
		// regardless the data applies or not, the wal file needs to go away
		// do not goto cleanup if it fails!
		rl_apply_wal_data(db, data, datalen, 0);
		if ((driver->mode & RLITE_OPEN_READWRITE) != 0) {
			RL_CALL(rl_delete_wal, RL_OK, wal_path);
		}
	}
	retval = RL_OK;
cleanup:
	rl_free(wal_path);
	rl_free(data);
	return retval;
}
