/* SORT command and helper functions.
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include "rlite/rlite.h"
#include "rlite/sort.h"
#include "rlite/pqsort.h" /* Partial qsort for SORT+LIMIT */
#include <math.h> /* isnan() */
#include <errno.h>

int strcoll(const char *, const char *);

static int sort_desc = 0;
static int sort_alpha = 0;
static int sort_bypattern = 0;
static int sort_store = 0;

static int compareString(unsigned char *obj1, long obj1len, unsigned char *obj2, long obj2len) {
	int cmp;
	long minlen = obj1len > obj2len ? obj2len : obj1len;
	cmp = memcmp(obj1, obj2, minlen);
	if (cmp == 0 && obj1len != obj2len) {
		cmp = obj1len > obj2len ? 1 : -1;
	}
	return cmp;
}
static int compareObjectsAsString(const rliteSortObject *so1, const rliteSortObject *so2) {
	return compareString(so1->obj, so1->objlen, so2->obj, so2->objlen);
}

static int compareObjectsAsLongLong(const rliteSortObject *so1, const rliteSortObject *so2) {
	return strcoll((char *)so1->obj, (char *)so2->obj);
}

/* Return the value associated to the key with a name obtained using
 * the following rules:
 *
 * 1) The first occurrence of '*' in 'pattern' is substituted with 'subst'.
 *
 * 2) If 'pattern' matches the "->" string, everything on the left of
 *	the arrow is treated as the name of a hash field, and the part on the
 *	left as the key name containing a hash. The value of the specified
 *	field is returned.
 *
 * 3) If 'pattern' equals "#", the function simply returns 'subst' itself so
 *	that the SORT command can be used like: SORT key GET # to retrieve
 *	the Set/List elements directly.
 *
 * The returned object will always have its refcount increased by 1
 * when it is non-NULL. */
static int lookupKeyByPattern(rlite *db, unsigned char *pattern, long patternlen, unsigned char *subst, long sublen, unsigned char **retobj, long *retobjlen) {
	int retval = RL_OK;
	unsigned char type;
	unsigned char *p, *f;
	unsigned char *key = NULL;
	long keylen;
	unsigned char *field = NULL;
	long fieldlen = 0;
	int prefixlen, postfixlen;

	/* If the pattern is "#" return the substitution object itself in order
	 * to implement the "SORT ... GET #" feature. */
	if (pattern[0] == '#' && pattern[1] == '\0') {
		*retobj = rl_malloc(sizeof(unsigned char) * sublen);
		if (!*retobj) {
			return RL_OUT_OF_MEMORY;
		}
		memcpy(*retobj, subst, sublen);
		*retobjlen = sublen;
		return RL_OK;
	}

	/* If we can't find '*' in the pattern we return NULL as to GET a
	 * fixed key does not make sense. */
	p = (unsigned char *)strchr((char *)pattern,'*');
	if (!p) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}

	/* Find out if we're dealing with a hash dereference. */
	if ((f = (unsigned char *)strstr((char *)p+1, "->")) != NULL && *(f+2) != '\0') {
		fieldlen = patternlen-(f-pattern)-2;
		field = f + 2;
	} else {
		fieldlen = 0;
	}

	/* Perform the '*' substitution. */
	prefixlen = p-pattern;
	postfixlen = patternlen-(prefixlen+1)-(fieldlen ? fieldlen+2 : 0);
	keylen = prefixlen+sublen+postfixlen;
	key = rl_malloc(sizeof(unsigned char) * keylen);
	if (!key) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	memcpy(key,pattern,prefixlen);
	memcpy(key+prefixlen,subst,sublen);
	memcpy(key+prefixlen+sublen,p+1,postfixlen);

	/* Lookup substituted key */
	RL_CALL(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, NULL, NULL, NULL);

	if (field) {
		if (type != RL_TYPE_HASH) {
			retval = RL_NOT_FOUND;
			goto cleanup;
		}

		/* Retrieve value from hash by the field name. This operation
		 * already increases the refcount of the returned object. */
		RL_CALL(rl_hget, RL_FOUND, db, key, keylen, field, fieldlen, retobj, retobjlen);
	} else {
		if (type != RL_TYPE_STRING) {
			retval = RL_NOT_FOUND;
			goto cleanup;
		}

		/* Every object that this function returns needs to have its refcount
		 * increased. sortCommand decreases it again. */
		RL_CALL(rl_get, RL_OK, db, key, keylen, retobj, retobjlen);
	}
	retval = RL_OK;
cleanup:
	rl_free(key);
	if (retval != RL_OK) {
		*retobj = NULL;
		*retobjlen = 0;
	}
	return retval;
}

/* sortCompare() is used by qsort in sortCommand(). Given that qsort_r with
 * the additional parameter is not standard but a BSD-specific we have to
 * pass sorting parameters via the global 'server' structure */
static int sortCompare(const void *s1, const void *s2) {
	const rliteSortObject *so1 = s1, *so2 = s2;
	int cmp;

	if (!sort_alpha) {
		/* Numeric sorting. Here it's trivial as we precomputed scores */
		if (so1->u.score > so2->u.score) {
			cmp = 1;
		} else if (so1->u.score < so2->u.score) {
			cmp = -1;
		} else {
			/* Objects have the same score, but we don't want the comparison
			 * to be undefined, so we compare objects lexicographically.
			 * This way the result of SORT is deterministic. */
			cmp = compareObjectsAsString(so1, so2);
		}
	} else {
		/* Alphanumeric sorting */
		if (sort_bypattern) {
			if (!so1->u.cmpobj.obj || !so2->u.cmpobj.obj) {
				/* At least one compare object is NULL */
				if (so1->u.cmpobj.obj == so2->u.cmpobj.obj)
					cmp = 0;
				else if (so1->u.cmpobj.obj == NULL)
					cmp = -1;
				else
					cmp = 1;
			} else {
				/* We have both the objects, compare them. */
				if (sort_store) {
					cmp = compareString(so1->u.cmpobj.obj, so1->u.cmpobj.objlen, so2->u.cmpobj.obj, so2->u.cmpobj.objlen);
				} else {
					/* Here we can use strcoll() directly as we are sure that
					 * the objects are decoded string objects. */
					cmp = strcoll((char *)so1->u.cmpobj.obj, (char *)so2->u.cmpobj.obj);
				}
			}
		} else {
			/* Compare elements directly. */
			if (sort_store) {
				cmp = compareObjectsAsString(so1,so2);
			} else {
				cmp = compareObjectsAsLongLong(so1,so2);
			}
		}
	}
	return sort_desc ? -cmp : cmp;
}

int rl_sort(struct rlite *db, unsigned char *key, long keylen, unsigned char *sortby, long sortbylen, int dontsort, int inLuaScript, int alpha, int desc, long limit_start, long limit_count, int getc, unsigned char **getv, long *getvlen, unsigned char *storekey, long storekeylen, long *retobjc, unsigned char ***retobjv, long **retobjvlen) {
	int i, j, k, retval;
	long vectorlen, start, end;
	long size;
	unsigned char **values = NULL, *value;
	long *valueslen = NULL, valuelen;
	rliteSortObject *vector = NULL;
	/* Lookup the key to sort. It must be of the right types */
	unsigned char type;
	long objc;
	unsigned char **objv = NULL;
	long *objvlen = NULL;

	if (storekey) {
		RL_CALL2(rl_key_delete_with_value, RL_OK, RL_NOT_FOUND, db, storekey, storekeylen);
	}

	RL_CALL(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, NULL, NULL, NULL);
	if (type != RL_TYPE_SET &&
		type != RL_TYPE_LIST &&
		type != RL_TYPE_ZSET)
	{
		retval = RL_WRONG_TYPE;
		goto cleanup;
	}

	/* When sorting a set with no sort specified, we must sort the output
	 * so the result is consistent across scripting and replication.
	 *
	 * The other types (list, sorted set) will retain their native order
	 * even if no sort order is requested, so they remain stable across
	 * scripting and replication. */
	if (dontsort &&
		type == RL_TYPE_SET &&
		(storekey || inLuaScript))
	{
		/* Force ALPHA sorting */
		dontsort = 0;
		alpha = 1;
		sortby = NULL;
	}

	/* Obtain the length of the object to sort. */
	switch(type) {
	case RL_TYPE_LIST:
		RL_CALL(rl_llen, RL_OK, db, key, keylen, &vectorlen);
		break;
	case RL_TYPE_SET:
		RL_CALL(rl_scard, RL_OK, db, key, keylen, &vectorlen);
		break;
	case RL_TYPE_ZSET:
		RL_CALL(rl_zcard, RL_OK, db, key, keylen, &vectorlen);
		break;
	default:
		vectorlen = 0;
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	/* Perform LIMIT start,count sanity checking. */
	start = (limit_start < 0) ? 0 : limit_start;
	end = (limit_count < 0) ? vectorlen-1 : start+limit_count-1;
	if (start >= vectorlen) {
		start = vectorlen-1;
		end = vectorlen-2;
	}
	if (end >= vectorlen) end = vectorlen-1;

	/* Whenever possible, we load elements into the output array in a more
	 * direct way. This is possible if:
	 *
	 * 1) The object to sort is a sorted set or a list (internally sorted).
	 * 2) There is nothing to sort as dontsort is true (BY <constant string>).
	 *
	 * In this special case, if we have a LIMIT option that actually reduces
	 * the number of elements to fetch, we also optimize to just load the
	 * range we are interested in and allocating a vector that is big enough
	 * for the selected range length. */
	if ((type == RL_TYPE_ZSET || type == RL_TYPE_LIST) &&
		dontsort &&
		(start != 0 || end != vectorlen-1))
	{
		vectorlen = end-start+1;
	}

	/* Load the sorting vector with all the objects to sort */
	RL_MALLOC(vector, sizeof(rliteSortObject) * vectorlen);
	j = 0;

	if (type == RL_TYPE_LIST && dontsort) {
		/* Special handling for a list, if 'dontsort' is true.
		 * This makes sure we return elements in the list original
		 * ordering, accordingly to DESC / ASC options.
		 *
		 * Note that in this case we also handle LIMIT here in a direct
		 * way, just getting the required range, as an optimization. */
		if (end >= start) {
			RL_CALL(rl_lrange, RL_OK, db, key, keylen, start, end, &size, &values, &valueslen);

			for (j = 0; j < size; j++) {
				vector[j].obj = values[j];
				vector[j].objlen = valueslen[j];
				vector[j].u.score = 0;
				vector[j].u.cmpobj.obj = NULL;
				vector[j].u.cmpobj.objlen = 0;
			}
			/* Fix start/end: output code is not aware of this optimization. */
			end -= start;
			start = 0;
		}
	} else if (type == RL_TYPE_LIST) {
		RL_CALL(rl_lrange, RL_OK, db, key, keylen, 0, -1, &size, &values, &valueslen);

		for (j = 0; j < size; j++) {
			vector[j].obj = values[j];
			vector[j].objlen = valueslen[j];
			vector[j].u.score = 0;
			vector[j].u.cmpobj.obj = NULL;
			vector[j].u.cmpobj.objlen = 0;
		}
	} else if (type == RL_TYPE_SET) {
		rl_set_iterator *siterator;
		RL_CALL(rl_smembers, RL_OK, db, &siterator, key, keylen);

		while ((retval = rl_set_iterator_next(siterator, NULL, &value, &valuelen)) == RL_OK) {
			vector[j].obj = value;
			vector[j].objlen = valuelen;
			vector[j].u.score = 0;
			vector[j].u.cmpobj.obj = NULL;
			vector[j].u.cmpobj.objlen = 0;
			j++;
		}
		if (retval != RL_END) {
			goto cleanup;
		}
	} else if (type == RL_TYPE_ZSET && dontsort) {
		/* Special handling for a sorted set, if 'dontsort' is true.
		 * This makes sure we return elements in the sorted set original
		 * ordering, accordingly to DESC / ASC options.
		 *
		 * Note that in this case we also handle LIMIT here in a direct
		 * way, just getting the required range, as an optimization. */

		rl_zset_iterator *ziterator;
		if (desc) {
			RL_CALL(rl_zrevrange, RL_OK, db, key, keylen, start, end, &ziterator);
		} else {
			RL_CALL(rl_zrange, RL_OK, db, key, keylen, start, end, &ziterator);
		}
		while ((retval = rl_zset_iterator_next(ziterator, NULL, NULL, &value, &valuelen)) == RL_OK) {
			vector[j].obj = value;
			vector[j].objlen = valuelen;
			vector[j].u.score = 0;
			vector[j].u.cmpobj.obj = NULL;
			vector[j].u.cmpobj.objlen = 0;
			j++;
		}
		if (retval != RL_END) {
			goto cleanup;
		}
		/* Fix start/end: output code is not aware of this optimization. */
		end -= start;
		start = 0;
	} else if (type == RL_TYPE_ZSET) {
		rl_zset_iterator *ziterator;
		RL_CALL(rl_zrange, RL_OK, db, key, keylen, 0, -1, &ziterator);
		while ((retval = rl_zset_iterator_next(ziterator, NULL, NULL, &value, &valuelen)) == RL_OK) {
			vector[j].obj = value;
			vector[j].objlen = valuelen;
			vector[j].u.score = 0;
			vector[j].u.cmpobj.obj = NULL;
			vector[j].u.cmpobj.objlen = 0;
			j++;
		}
		if (retval != RL_END) {
			goto cleanup;
		}
	} else {
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	if (j != vectorlen) {
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	/* Now it's time to load the right scores in the sorting vector */
	if (dontsort == 0) {
		for (j = 0; j < vectorlen; j++) {
			unsigned char *byval;
			long byvallen;
			if (sortby) {
				/* lookup value to sort by */
				RL_CALL2(lookupKeyByPattern, RL_OK, RL_NOT_FOUND, db, sortby, sortbylen, vector[j].obj, vector[j].objlen, &byval, &byvallen);
				if (!byval) continue;
			} else {
				/* use object itself to sort by */
				byval = vector[j].obj;
				byvallen = vector[j].objlen;
			}

			if (alpha) {
				if (sortby) {
					vector[j].u.cmpobj.obj = byval;
					vector[j].u.cmpobj.objlen = byvallen;
				}
			} else {
				unsigned char *eptr;
				vector[j].u.score = rl_strtod(byval, byvallen, &eptr);
				if (eptr[0] != '\0' || errno == ERANGE ||
					isnan(vector[j].u.score))
				{
					if (sortby) {
						rl_free(byval);
					}
					retval = RL_NAN;
					goto cleanup;
				}
				if (sortby) {
					rl_free(byval);
				}
			}
		}

		sort_desc = desc;
		sort_alpha = alpha;
		sort_bypattern = sortby ? 1 : 0;
		sort_store = storekey ? 1 : 0;
		if (sortby && (start != 0 || end != vectorlen-1)) {
			pqsort(vector,vectorlen,sizeof(rliteSortObject),sortCompare, start,end);
		}
		else {
			qsort(vector,vectorlen,sizeof(rliteSortObject),sortCompare);
		}

		if (sortby && alpha) {
			for (j = 0; j < vectorlen; j++) {
				rl_free(vector[j].u.cmpobj.obj);
				vector[j].u.cmpobj.obj = NULL;
			}
		}
	}

	/* Send command output to the output buffer, performing the specified
	 * GET/DEL/INCR/DECR operations if any. */
	objc = getc ? getc*(end-start+1) : end-start+1;
	RL_MALLOC(objv, sizeof(unsigned char *) * objc);
	RL_MALLOC(objvlen, sizeof(long) * objc);

	for (i = 0, j = start; j <= end; j++) {
		if (getc) {
			for (k = 0; k < getc; k++) {
				RL_CALL2(lookupKeyByPattern, RL_OK, RL_NOT_FOUND, db, getv[k], getvlen[k], vector[j].obj, vector[j].objlen, &objv[i], &objvlen[i]);
				i++;
			}
			rl_free(vector[j].obj);
		} else {
			objv[i] = vector[j].obj;
			objvlen[i] = vector[j].objlen;
			i++;
		}
	}

	*retobjc = objc;
	if (storekey) {
		if (objc) {
			RL_CALL(rl_push, RL_OK, db, storekey, storekeylen, 1, 0, objc, objv, objvlen, NULL);
		}
		for (j = 0; j < vectorlen; j++) {
			rl_free(vector[j].obj);
		}
		rl_free(objv);
		rl_free(objvlen);
		objv = NULL;
		objvlen = NULL;
	} else {
		*retobjv = objv;
		*retobjvlen = objvlen;
		for (j = 0; j < start; j++) {
			rl_free(vector[j].obj);
		}

		for (j = end + 1; j < vectorlen; j++) {
			rl_free(vector[j].obj);
		}
	}

	if (alpha) {
		for (j = 0; j < vectorlen; j++) {
			if (vector[j].u.cmpobj.obj) {
				rl_free(vector[j].u.cmpobj.obj);
			}
		}
	}

	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		if (vector) {
			for (j = 0; j < vectorlen; j++) {
				rl_free(vector[j].obj);
			}
		}
		*retobjc = 0;
		rl_free(objv);
		rl_free(objvlen);
	}
	rl_free(vector);
	rl_free(values);
	rl_free(valueslen);
	return retval;
}
