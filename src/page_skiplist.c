#include <stdlib.h>
#include "page_skiplist.h"
#include "obj_string.h"

static int rl_skiplist_random_level()
{
	int level = 1;
	while ((rand() & 0xFFFF) < (RL_SKIPLIST_PROBABILITY * 0xFFFF)) {
		level++;
	}
	return (level < RL_SKIPLIST_MAXLEVEL) ? level : RL_SKIPLIST_MAXLEVEL;
}

int rl_skiplist_create(rlite *db, rl_skiplist **_skiplist)
{
	rl_skiplist *skiplist = malloc(sizeof(rl_skiplist));
	if (skiplist == NULL) {
		return RL_OUT_OF_MEMORY;
	}
	skiplist->left = db->next_empty_page;
	skiplist->right = 0;
	skiplist->size = 0;
	skiplist->level = 1;

	rl_skiplist_node *node;
	int retval = rl_skiplist_node_create(db, &node, RL_SKIPLIST_MAXLEVEL, 0.0, 0);
	if (retval != RL_OK) {
		goto cleanup;
	}

	long j;
	for (j = 0; j < RL_SKIPLIST_MAXLEVEL; j++) {
		node->level[j].right = 0;
		node->level[j].span = 0;
	}

	retval = rl_write(db, &rl_data_type_skiplist_node, db->next_empty_page, node);
	if (retval != RL_OK) {
		goto cleanup;
	}

	*_skiplist = skiplist;
cleanup:
	return retval;
}

int rl_skiplist_destroy(rlite *db, void *skiplist)
{
	db = db;
	free(skiplist);
	return RL_OK;
}

int rl_skiplist_node_create(rlite *db, rl_skiplist_node **_node, long level, double score, long value)
{
	db = db;
#ifdef DEBUG
	level = RL_SKIPLIST_MAXLEVEL;
#endif
	rl_skiplist_node *node = malloc(sizeof(rl_skiplist_node) + level * sizeof(struct rl_skiplist_node_level));
	if (!node) {
		return RL_OUT_OF_MEMORY;
	}
	node->score = score;
	node->value = value;
#ifdef DEBUG
	node->left = 0;
	long i;
	for (i = 0; i < level; i++) {
		node->level[i].right = 0;
		node->level[i].span = 0;
	}
#endif
	*_node = node;
	return RL_OK;
}

int rl_skiplist_node_destroy(rlite *db, void *node)
{
	db = db;
	free(node);
	return RL_OK;
}

int rl_skiplist_add(rlite *db, rl_skiplist *skiplist, double score, long value)
{
	rl_skiplist_node *update_node[RL_SKIPLIST_MAXLEVEL];
	long update_node_page[RL_SKIPLIST_MAXLEVEL];
	long rank[RL_SKIPLIST_MAXLEVEL];
	void *_node;
	rl_skiplist_node *node, *next_node;
	long i, level, node_page, tmp;
	for (i = 0; i < RL_SKIPLIST_MAXLEVEL; i++) {
		update_node_page[i] = 0;
	}

	node_page = skiplist->left;
	int cmp, retval = rl_read(db, &rl_data_type_skiplist_node, skiplist->left, skiplist, &_node);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	update_node_page[0] = skiplist->left;
	node_page = skiplist->left;
	node = _node;

	for (i = skiplist->level - 1; i >= 0; i--) {
		rank[i] = i == (skiplist->level - 1) ? 0 : rank[i + 1];
		while (1) {
			tmp = node->level[i].right;
			if (tmp == 0) {
				break;
			}
			retval = rl_read(db, &rl_data_type_skiplist_node, tmp, skiplist, &_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			next_node = _node;
			// next_node->value = 0 when next_node is the root node
			if (next_node->value != 0) {
				if (next_node->score > score) {
					break;
				}
				if (next_node->score == score) {
					retval = rl_obj_string_cmp(db, next_node->value, value, &cmp);
					if (retval != RL_OK) {
						goto cleanup;
					}
					if (cmp < 0) {
						break;
					}
				}
			}
			rank[i] += node->level[i].span;
			node = next_node;
			node_page = tmp;
		}
		update_node[i] = node;
		update_node_page[i] = node_page;
	}

	level = rl_skiplist_random_level();
	if (level > skiplist->level) {
		for (i = skiplist->level; i < level; i++) {
			rank[i] = 0;
			retval = rl_read(db, &rl_data_type_skiplist_node, skiplist->left, skiplist, &_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			update_node[i] = _node;
			update_node[i]->level[i].span = skiplist->size;
			update_node_page[i] = skiplist->left;
		}
		skiplist->level = level;
	}
	retval = rl_skiplist_node_create(db, &node, level, score, value);
	if (retval != RL_OK) {
		goto cleanup;
	}
	node_page = db->next_empty_page;
	retval = rl_write(db, &rl_data_type_skiplist_node, db->next_empty_page, node);
	if (retval != RL_OK) {
		goto cleanup;
	}
	for (i = 0; i < level; i++) {
		node->level[i].right = update_node[i]->level[i].right;
		update_node[i]->level[i].right = node_page;

		node->level[i].span = update_node[i]->level[i].span - (rank[0] - rank[i]);
		update_node[i]->level[i].span = (rank[0] - rank[i]) + 1;
		retval = rl_write(db, &rl_data_type_skiplist_node, update_node_page[i], update_node[i]);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}

	for (i = level; i < skiplist->level; i++) {
		update_node[i]->level[i].span++;
	}

	node->left = (update_node_page[0] == skiplist->left) ? 0 : update_node_page[0];
	if (node->level[0].right) {
		tmp = node_page;
		node_page = node->level[0].right;
		retval = rl_read(db, &rl_data_type_skiplist_node, node_page, skiplist, &_node);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		node = _node;
		node->left = tmp;
		retval = rl_write(db, &rl_data_type_skiplist_node, node_page, node);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}
	else {
		skiplist->right = node_page;
	}
	skiplist->size++;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_skiplist_first_node(rlite *db, rl_skiplist *skiplist, double score, rl_skiplist_node **retnode, long *_rank)
{
	long i, page = skiplist->left;
	int retval;
	void *_node;
	long rank = 0;
	rl_skiplist_node *node = NULL, *next_node;
	for (i = skiplist->level - 1; i >= 0; i--) {
		do {
			if (node) {
				page = node->level[i].right;
			}
			if (page == 0) {
				break;
			}
			retval = rl_read(db, &rl_data_type_skiplist_node, page, skiplist, &_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			next_node = _node;
			if (next_node->value != 0 && next_node->score > score) {
				if (i == 0 && node->score < score) {
					node = next_node;
					rank++;
				}
				break;
			}
			if (node) {
				rank += node->level[i].span;
			}
			node = next_node;
		}
		while (page > 0);
	}
	if (node && node->score >= score) {
		if (retnode) {
			*retnode = node;
		}
		if (_rank) {
			*_rank = rank;
		}
		retval = RL_FOUND;
	}
	else {
		retval = RL_NOT_FOUND;
	}
cleanup:
	return retval;

}

#ifdef DEBUG
int rl_skiplist_print(rlite *db, rl_skiplist *skiplist)
{
	printf("left: %ld, right: %ld, size: %ld, level: %ld\n", skiplist->left, skiplist->right, skiplist->size, skiplist->level);
	long page = skiplist->left;
	int retval;
	long i, j;
	void *_node;
	rl_skiplist_node *node;

	j = 0;
	while (page > 0) {
		if (j++ > skiplist->size + 1) {
			fprintf(stderr, "Too many nodes, expected %ld\n", skiplist->size);
			return RL_UNEXPECTED;
		}
		retval = rl_read(db, &rl_data_type_skiplist_node, page, skiplist, &_node);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		node = _node;
		printf("page: %ld, value: %ld, score: %lf, left: %ld", page, node->value, node->score, node->left);
		for (i = 0; i < RL_SKIPLIST_MAXLEVEL; i++) {
			if (node->level[i].right) {
				printf(", level[%ld].right: %ld, level[%ld].span: %ld", i, node->level[i].right, i, node->level[i].span);
			}
		}
		printf("\n");
		page = node->level[0].right;
	}
	retval = RL_OK;
cleanup:
	return retval;
}
#endif

int rl_skiplist_is_balanced(rlite *db, rl_skiplist *skiplist)
{
	long page = skiplist->left;
	int retval;
	long i = 0;
	void *_node;
	rl_skiplist_node *node;
	rl_skiplist_node **nodes = malloc(sizeof(rl_skiplist_node *) * (skiplist->size + 1));
	long *nodes_page = malloc(sizeof(long) * (skiplist->size + 1));

	while (page > 0) {
		if (i > skiplist->size + 1) {
			fprintf(stderr, "Too many nodes, expected %ld\n", skiplist->size);
			return RL_UNEXPECTED;
		}
		nodes_page[i] = page;
		retval = rl_read(db, &rl_data_type_skiplist_node, page, skiplist, &_node);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		node = _node;
		nodes[i++] = node;
		page = node->level[0].right;
	}
	if (i != skiplist->size + 1) {
		fprintf(stderr, "Number of nodes (%ld) doesn't match skiplist size (%ld)\n", i, skiplist->size);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	int cmp;

	// node 0 is not really a value node, ignore it
	for (i = 2; i < skiplist->size; i++) {
		if (nodes[i - 1]->score > nodes[i]->score) {
			fprintf(stderr, "skiplist score is not sorted at position %ld (%lf > %lf)\n", i, nodes[i - 1]->score, nodes[i]->score);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		else if (nodes[i - 1]->score == nodes[i]->score) {
			retval = rl_obj_string_cmp(db, nodes[i - 1]->value, nodes[i]->value, &cmp);
			if (retval != RL_OK) {
				goto cleanup;
			}
			if (cmp < 0) {
				fprintf(stderr, "skiplist score is not sorted at position %ld (same score, cmp is %d)\n", i, cmp);
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
		}
	}

	for (i = 2; i < skiplist->size; i++) {
		if (nodes[i]->left != nodes_page[i - 1]) {
			fprintf(stderr, "expected skiplist node[%ld] left page to be %ld, got %ld\n", i, nodes_page[i - 1], nodes[i]->left);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
	}

	long pos;
	for (i = 1; i < skiplist->level; i++) {
		node = nodes[0];
		page = 1;
		pos = 0;
		while (1) {
			pos += node->level[i].span;
			page = node->level[i].right;
			if (page == 0) {
				break;
			}
			if (nodes_page[pos] != page) {
				fprintf(stderr, "expected skiplist at level %ld node page to be %ld, got %ld\n", i, nodes_page[pos], page);
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
			node = nodes[pos];
		}
	}

	retval = RL_OK;
cleanup:
	free(nodes_page);
	free(nodes);
	return retval;
}

int rl_skiplist_serialize(struct rlite *db, void *obj, unsigned char *data)
{
	db = db;
	obj = obj;
	data = data;
	return RL_NOT_IMPLEMENTED;
}
int rl_skiplist_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data)
{
	db = db;
	obj = obj;
	data = data;
	context = context;
	return RL_NOT_IMPLEMENTED;
}

int rl_skiplist_node_serialize(struct rlite *db, void *obj, unsigned char *data)
{
	db = db;
	obj = obj;
	data = data;
	return RL_NOT_IMPLEMENTED;
}

int rl_skiplist_node_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data)
{
	db = db;
	obj = obj;
	data = data;
	context = context;
	return RL_NOT_IMPLEMENTED;
}
