#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include "status.h"
#include "page_btree.h"
#include "rlite.h"
#include "util.h"

rl_btree_type rl_btree_type_hash_sha1_key = {
	0,
	0,
	sizeof(unsigned char) * 20,
	sizeof(rl_key),
	sha1_cmp,
#ifdef DEBUG
	sha1_formatter,
#endif
};

rl_btree_type rl_btree_type_set_long = {
	0,
	0,
	sizeof(long),
	0,
	long_cmp,
#ifdef DEBUG
	long_formatter,
#endif
};

rl_btree_type rl_btree_type_hash_long_long = {
	0,
	0,
	sizeof(long),
	sizeof(void *),
	long_cmp,
#ifdef DEBUG
	long_formatter,
#endif
};

rl_btree_type rl_btree_type_hash_sha1_double = {
	0,
	0,
	sizeof(long),
	sizeof(void *),
	sha1_cmp,
#ifdef DEBUG
	sha1_formatter,
#endif
};

void rl_btree_init()
{
	rl_btree_type_hash_sha1_key.btree_type = &rl_data_type_btree_hash_sha1_key;
	rl_btree_type_hash_sha1_key.btree_node_type = &rl_data_type_btree_node_hash_sha1_key;
	rl_btree_type_hash_long_long.btree_type = &rl_data_type_btree_hash_long_long;
	rl_btree_type_hash_long_long.btree_node_type = &rl_data_type_btree_node_hash_long_long;
	rl_btree_type_set_long.btree_type = &rl_data_type_btree_set_long;
	rl_btree_type_set_long.btree_node_type = &rl_data_type_btree_node_set_long;
	rl_btree_type_hash_sha1_double.btree_type = &rl_data_type_btree_hash_sha1_double;
	rl_btree_type_hash_sha1_double.btree_node_type = &rl_data_type_btree_node_hash_sha1_double;
}

int rl_btree_serialize(struct rlite *UNUSED(db), void *obj, unsigned char *data)
{
	rl_btree *tree = obj;
	put_4bytes(data, tree->root);
	put_4bytes(&data[4], tree->height);
	put_4bytes(&data[8], tree->max_node_size);
	return RL_OK;
}

int rl_btree_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data)
{
	rl_btree *btree;
	int retval = rl_btree_create(db, &btree, context, 0);
	if (retval != RL_OK) {
		return retval;
	}
	btree->db = db;
	btree->root = get_4bytes(data);
	btree->height = get_4bytes(&data[4]);
	btree->max_node_size = get_4bytes(&data[8]);
	*obj = btree;
	return retval;
}

#ifdef DEBUG
int rl_print_btree_node(rlite *UNUSED(db), rl_btree *btree, rl_btree_node *node, long level);
#endif


int rl_btree_node_create(rlite *UNUSED(db), rl_btree *btree, rl_btree_node **_node)
{
	int retval = RL_OK;
	rl_btree_node *node = malloc(sizeof(rl_btree_node));
	if (!node) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	node->scores = malloc(sizeof(void *) * btree->max_node_size);
	if (!node->scores) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	node->children = NULL;
	if (btree->type->value_size) {
		node->values = malloc(sizeof(void *) * btree->max_node_size);
		if (!node->values) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
	}
	else {
		node->values = NULL;
	}
	node->size = 0;
	*_node = node;
cleanup:
	if (retval != RL_OK && node) {
		if (node->scores) {
			free(node->scores);
		}
		free(node);
	}
	return retval;
}

int rl_btree_node_destroy(rlite *UNUSED(db), void *_node)
{
	rl_btree_node *node = _node;
	long i;
	if (node->scores) {
		for (i = 0; i < node->size; i++) {
			free(node->scores[i]);
		}
		free(node->scores);
	}
	if (node->values) {
		for (i = 0; i < node->size; i++) {
			free(node->values[i]);
		}
		free(node->values);
	}
	if (node->children) {
		free(node->children);
	}
	free(node);
	return RL_OK;
}

int rl_btree_create(rlite *db, rl_btree **_btree, rl_btree_type *type, long max_node_size)
{
	int retval = RL_OK;
	rl_btree *btree = malloc(sizeof(rl_btree));
	rl_btree_node *root = NULL;
	if (!btree) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	btree->number_of_elements = 0;
	btree->max_node_size = max_node_size;
	btree->type = type;
	btree->db = db;
	btree->root = db->next_empty_page;
	btree->height = 1;
	retval = rl_btree_node_create(db, btree, &root);
	if (retval != RL_OK) {
		goto cleanup;
	}
	root->size = 0;
	retval = rl_write(db, type->btree_node_type, db->next_empty_page, root);
	if (retval != RL_OK) {
		goto cleanup;
	}
	*_btree = btree;
cleanup:
	if (retval != RL_OK && btree) {
		if (root) {
			free(root);
		}
		free(btree);
	}
	return retval;
}

int rl_btree_destroy(rlite *UNUSED(db), void *btree)
{
	free(btree);
	return RL_OK;
}

int rl_btree_find_score(rlite *db, rl_btree *btree, void *score, void **value, rl_btree_node *** nodes, long **positions)
{
	if ((!nodes && positions) || (nodes && !positions)) {
		return RL_INVALID_PARAMETERS;
	}
	void *_node;
	int retval = rl_read(db, btree->type->btree_node_type, btree->root, btree, &_node, 1);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	rl_btree_node *node = _node;
	long i, pos, min, max;
	int cmp = 0;
	for (i = 0; i < btree->height; i++) {
		if (nodes) {
			(*nodes)[i] = node;
		}
		pos = 0;
		min = 0;
		max = node->size - 1;
		while (min <= max) {
			pos = (max - min) / 2 + min;
			cmp = btree->type->cmp(score, node->scores[pos]);
			if (cmp == 0) {
				if (value && node->values) {
					*value = node->values[pos];
				}
				if (nodes) {
					if (positions) {
						(*positions)[i] = pos;
					}
					for (i++; i < btree->height; i++) {
						(*nodes)[i] = NULL;
					}
				}
				retval = RL_FOUND;
				goto cleanup;
			}
			else if (cmp > 0) {
				if (max == pos) {
					pos++;
					break;
				}
				if (min == pos) {
					min++;
					continue;
				}
				min = pos;
			}
			else {
				if (max == pos) {
					break;
				}
				max = pos;
			}
		}
		if (positions) {
			if (pos == max && btree->type->cmp(score, node->scores[pos]) == 1) {
				pos++;
			}
			(*positions)[i] = pos;
		}
		if (node->children) {
			retval = rl_read(db, btree->type->btree_node_type, node->children[pos], btree, &_node, 1);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			node = _node;
		}
	}
	retval = RL_NOT_FOUND;
cleanup:
	return retval;
}

int rl_btree_add_element(rlite *db, rl_btree *btree, void *score, void *value)
{
	int retval;
	long *positions = NULL;
	rl_btree_node **nodes = malloc(sizeof(rl_btree_node *) * btree->height);
	if (!nodes) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	positions = malloc(sizeof(long) * btree->height);
	if (!positions) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	void *tmp;
	long i, pos;
	long node_page = 0;
	long child = -1;
	retval = rl_btree_find_score(db, btree, score, NULL, &nodes, &positions);

	if (retval != RL_NOT_FOUND) {
		goto cleanup;
	}
	retval = RL_OK;
	rl_btree_node *node = NULL;
	for (i = btree->height - 1; i >= 0; i--) {
		if (i == 0) {
			node_page = btree->root;
		}
		else {
			node_page = nodes[i - 1]->children[positions[i - 1]];
		}
		node = nodes[i];

		if (node->size < btree->max_node_size) {
			memmove_dbg(&node->scores[positions[i] + 1], &node->scores[positions[i]], sizeof(void *) * (node->size - positions[i]), __LINE__);
			if (node->values) {
				memmove_dbg(&node->values[positions[i] + 1], &node->values[positions[i]], sizeof(void *) * (node->size - positions[i]), __LINE__);
			}
			if (node->children) {
				memmove_dbg(&node->children[positions[i] + 2], &node->children[positions[i] + 1], sizeof(long) * (node->size - positions[i]), __LINE__);
			}
			node->scores[positions[i]] = score;
			if (node->values) {
				node->values[positions[i]] = value;
			}
			if (child != -1) {
				if (!node->children) {
					fprintf(stderr, "Adding child, but children is not initialized\n");
				}
				node->children[positions[i] + 1] = child;
			}
			node->size++;
			score = NULL;
			retval = rl_write(db, btree->type->btree_node_type, node_page, node);
			break;
		}
		else {
			pos = positions[i];

			rl_btree_node *right;
			retval = rl_btree_node_create(db, btree, &right);
			if (retval != RL_OK) {
				goto cleanup;
			}
			if (child != -1) {
				right->children = malloc(sizeof(long) * (btree->max_node_size + 1));
				if (!right->children) {
					retval = RL_OUT_OF_MEMORY;
					goto cleanup;
				}
			}

			if (pos < btree->max_node_size / 2) {
				if (child != -1) {
					memmove_dbg(right->children, &node->children[btree->max_node_size / 2], sizeof(void *) * (btree->max_node_size / 2 + 1), __LINE__);
					memmove_dbg(&node->children[pos + 2], &node->children[pos + 1], sizeof(void *) * (btree->max_node_size / 2 - 1 - pos), __LINE__);
					node->children[pos + 1] = child;

				}
				tmp = node->scores[btree->max_node_size / 2 - 1];
				memmove_dbg(&node->scores[pos + 1], &node->scores[pos], sizeof(void *) * (btree->max_node_size / 2 - 1 - pos), __LINE__);
				node->scores[pos] = score;
				score = tmp;
				memmove_dbg(right->scores, &node->scores[btree->max_node_size / 2], sizeof(void *) * btree->max_node_size / 2, __LINE__);
				if (node->values) {
					tmp = node->values[btree->max_node_size / 2 - 1];
					memmove_dbg(&node->values[pos + 1], &node->values[pos], sizeof(void *) * (btree->max_node_size / 2 - 1 - pos), __LINE__);
					node->values[pos] = value;
					memmove_dbg(right->values, &node->values[btree->max_node_size / 2], sizeof(void *) * btree->max_node_size / 2, __LINE__);
					value = tmp;
				}
			}

			if (pos == btree->max_node_size / 2) {
				if (child != -1) {
					memmove_dbg(&right->children[1], &node->children[btree->max_node_size / 2 + 1], sizeof(void *) * (btree->max_node_size / 2), __LINE__);
					right->children[0] = child;
				}
				memmove_dbg(right->scores, &node->scores[btree->max_node_size / 2], sizeof(void *) * btree->max_node_size / 2, __LINE__);
				if (node->values) {
					memmove_dbg(right->values, &node->values[btree->max_node_size / 2], sizeof(void *) * btree->max_node_size / 2, __LINE__);
				}
			}

			if (pos > btree->max_node_size / 2) {
				if (child != -1) {
					memmove_dbg(right->children, &node->children[btree->max_node_size / 2 + 1], sizeof(void *) * (pos - btree->max_node_size / 2), __LINE__);
					right->children[pos - btree->max_node_size / 2] = child;
					memmove_dbg(&right->children[pos - btree->max_node_size / 2 + 1], &node->children[pos + 1], sizeof(void *) * (btree->max_node_size - pos), __LINE__);
				}
				tmp = node->scores[btree->max_node_size / 2];
				memmove_dbg(right->scores, &node->scores[btree->max_node_size / 2 + 1], sizeof(void *) * (pos - btree->max_node_size / 2 - 1), __LINE__);
				right->scores[pos - btree->max_node_size / 2 - 1] = score;
				memmove_dbg(&right->scores[pos - btree->max_node_size / 2], &node->scores[pos], sizeof(void *) * (btree->max_node_size - pos), __LINE__);
				score = tmp;
				if (node->values) {
					tmp = node->values[btree->max_node_size / 2];
					memmove_dbg(right->values, &node->values[btree->max_node_size / 2 + 1], sizeof(void *) * (pos - btree->max_node_size / 2 - 1), __LINE__);
					right->values[pos - btree->max_node_size / 2 - 1] = value;
					memmove_dbg(&right->values[pos - btree->max_node_size / 2], &node->values[pos], sizeof(void *) * (btree->max_node_size - pos), __LINE__);
					value = tmp;
				}
			}

			node->size = right->size = btree->max_node_size / 2;
			child = db->next_empty_page;
			retval = rl_write(db, btree->type->btree_node_type, node_page, node);
			if (retval != RL_OK) {
				goto cleanup;
			}
			retval = rl_write(db, btree->type->btree_node_type, child, right);
			if (retval != RL_OK) {
				goto cleanup;
			}
		}
	}
	if (score) {
		rl_btree_node *old_root = node;
		retval = rl_btree_node_create(db, btree, &node);
		if (retval != RL_OK) {
			goto cleanup;
		}
		node->size = 1;
		node->scores[0] = score;
		if (node->values) {
			node->values[0] = value;
		}
		node->children = malloc(sizeof(long) * (btree->max_node_size + 1));
		if (!node->children) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		if (node_page == 0) {
			node_page = db->next_empty_page;
		}
		node->children[0] = node_page;
		retval = rl_write(db, btree->type->btree_node_type, node->children[0], old_root);
		if (retval != RL_OK) {
			goto cleanup;
		}
		node->children[1] = child;
		btree->root = db->next_empty_page;
		retval = rl_write(db, btree->type->btree_node_type, btree->root, node);
		if (retval != RL_OK) {
			goto cleanup;
		}
		btree->height++;
	}
	btree->number_of_elements++;
cleanup:
	free(nodes);
	free(positions);

	return retval;
}

int rl_btree_remove_element(rlite *db, rl_btree *btree, void *score)
{
	void *tmp;
	int retval;
	long *positions = NULL;
	rl_btree_node **nodes = malloc(sizeof(rl_btree_node *) * btree->height);
	if (!nodes) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	positions = malloc(sizeof(long) * btree->height);
	if (!positions) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	long i, j;
	long node_page = 0, child_node_page, sibling_node_page, parent_node_page;
	retval = rl_btree_find_score(db, btree, score, NULL, &nodes, &positions);

	if (retval != RL_FOUND) {
		goto cleanup;
	}
	retval = RL_OK;

	rl_btree_node *node, *parent_node, *sibling_node, *child_node;
	for (i = btree->height - 1; i >= 0; i--) {
		node = nodes[i];
		if (!node) {
			continue;
		}

		if (i == 0) {
			node_page = btree->root;
		}
		else {
			node_page = nodes[i - 1]->children[positions[i - 1]];
		}

		free(node->scores[positions[i]]);
		if (node->values) {
			free(node->values[positions[i]]);
		}
		if (node->children) {
			j = i;
			child_node = node;
			child_node_page = node_page;
			for (; i < btree->height - 1; i++) {
				child_node_page = child_node->children[positions[i]];
				retval = rl_read(db, btree->type->btree_node_type, child_node->children[positions[i]], btree, &tmp, 1);
				if (retval != RL_FOUND) {
					goto cleanup;
				}
				child_node = tmp;
				nodes[i + 1] = child_node;
				positions[i + 1] = child_node->size;
			}

			if (child_node->children) {
				fprintf(stderr, "last child_node mustn't have children\n");
				retval = RL_UNEXPECTED;
				goto cleanup;
			}

			// only the leaf node loses an element, to replace the deleted one
			child_node->size--;
			node->scores[positions[j]] = child_node->scores[child_node->size];
			if (node->values) {
				node->values[positions[j]] = child_node->values[child_node->size];
			}
			retval = rl_write(db, btree->type->btree_node_type, node_page, node);
			if (retval != RL_OK) {
				goto cleanup;
			}
			retval = rl_write(db, btree->type->btree_node_type, child_node_page, child_node);
			if (retval != RL_OK) {
				goto cleanup;
			}
			break;
		}
		else {
			memmove_dbg(&node->scores[positions[i]], &node->scores[positions[i] + 1], sizeof(void *) * (node->size - positions[i] - 1), __LINE__);
			if (node->values) {
				memmove_dbg(&node->values[positions[i]], &node->values[positions[i] + 1], sizeof(void *) * (node->size - positions[i]), __LINE__);
			}

			if (--node->size > 0) {
				retval = rl_write(db, btree->type->btree_node_type, node_page, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
			}
			else {
				if (i == 0 && node->children) {
					// we have an empty root, promote the only child if any
					btree->height--;
					retval = rl_read(db, btree->type->btree_node_type, node->children[0], btree, &tmp, 1);
					if (retval != RL_FOUND) {
						goto cleanup;
					}
					parent_node = tmp;
					retval = rl_write(db, btree->type->btree_node_type, node->children[0], parent_node);
					if (retval != RL_OK) {
						goto cleanup;
					}
					retval = rl_delete(db, node_page);
					if (retval != RL_OK) {
						goto cleanup;
					}
					retval = rl_btree_node_destroy(db, node);
					if (retval != RL_OK) {
						goto cleanup;
					}
				}
			}
			break;
		}
	}

	for (; i >= 0; i--) {
		if (i == 0) {
			node_page = btree->root;
		}
		else {
			node_page = nodes[i - 1]->children[positions[i - 1]];
		}
		node = nodes[i];
		if (i == 0) {
			if (node->size == 0) {
				btree->height--;
				if (node->children) {
					btree->root = node->children[0];
				}
			}
			break;
		}
		if (node->size >= btree->max_node_size / 2 || i == 0) {
			break;
		}
		else {
			if (i == 1) {
				parent_node_page = btree->root;
			}
			else {
				parent_node_page = nodes[i - 2]->children[positions[i - 2]];
			}
			parent_node = nodes[i - 1];
			if (parent_node->size == 0) {
				fprintf(stderr, "Empty parent\n");
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
			if (positions[i - 1] > 0) {
				sibling_node_page = parent_node->children[positions[i - 1] - 1];
				retval = rl_read(db, btree->type->btree_node_type, parent_node->children[positions[i - 1] - 1], btree, &tmp, 1);
				if (retval != RL_FOUND) {
					goto cleanup;
				}
				sibling_node = tmp;
				if (sibling_node->size > btree->max_node_size / 2) {
					memmove_dbg(&node->scores[1], &node->scores[0], sizeof(void *) * (node->size), __LINE__);
					if (node->values) {
						memmove_dbg(&node->values[1], &node->values[0], sizeof(void *) * (node->size), __LINE__);
					}
					if (node->children) {
						memmove_dbg(&node->children[1], &node->children[0], sizeof(long) * (node->size + 1), __LINE__);
						node->children[0] = sibling_node->children[sibling_node->size];
					}
					node->scores[0] = parent_node->scores[positions[i - 1] - 1];
					if (node->values) {
						node->values[0] = parent_node->values[positions[i - 1] - 1];
					}

					parent_node->scores[positions[i - 1] - 1] = sibling_node->scores[sibling_node->size - 1];
					if (parent_node->values) {
						parent_node->values[0] = sibling_node->values[sibling_node->size - 1];
					}

					sibling_node->size--;
					node->size++;
					retval = rl_write(db, btree->type->btree_node_type, node_page, node);
					if (retval != RL_OK) {
						goto cleanup;
					}
					retval = rl_write(db, btree->type->btree_node_type, sibling_node_page, sibling_node);
					if (retval != RL_OK) {
						goto cleanup;
					}
					retval = rl_write(db, btree->type->btree_node_type, parent_node_page, parent_node);
					if (retval != RL_OK) {
						goto cleanup;
					}
					break;
				}
			}
			if (positions[i - 1] < parent_node->size) {
				sibling_node_page = parent_node->children[positions[i - 1] + 1];
				retval = rl_read(db, btree->type->btree_node_type, parent_node->children[positions[i - 1] + 1], btree, &tmp, 1);
				if (retval != RL_FOUND) {
					return retval;
				}
				sibling_node = tmp;
				if (sibling_node->size > btree->max_node_size / 2) {
					node->scores[node->size] = parent_node->scores[positions[i - 1]];
					if (node->values) {
						node->values[node->size] = parent_node->values[positions[i - 1]];
					}

					parent_node->scores[positions[i - 1]] = sibling_node->scores[0];
					if (parent_node->values) {
						parent_node->values[positions[i - 1]] = sibling_node->values[0];
					}

					memmove_dbg(&sibling_node->scores[0], &sibling_node->scores[1], sizeof(void *) * (sibling_node->size - 1), __LINE__);
					if (node->values) {
						memmove_dbg(&sibling_node->values[0], &sibling_node->values[1], sizeof(void *) * (sibling_node->size - 1), __LINE__);
					}
					if (node->children) {
						node->children[node->size + 1] = sibling_node->children[0];
						memmove_dbg(&sibling_node->children[0], &sibling_node->children[1], sizeof(long) * (sibling_node->size), __LINE__);
					}

					sibling_node->size--;
					node->size++;
					retval = rl_write(db, btree->type->btree_node_type, sibling_node_page, sibling_node);
					if (retval != RL_OK) {
						goto cleanup;
					}
					retval = rl_write(db, btree->type->btree_node_type, node_page, node);
					if (retval != RL_OK) {
						goto cleanup;
					}
					retval = rl_write(db, btree->type->btree_node_type, parent_node_page, parent_node);
					if (retval != RL_OK) {
						goto cleanup;
					}
					break;
				}
			}

			// not taking from either slibing, need to merge with either
			// if either of them exists, they have the minimum number of elements
			if (positions[i - 1] > 0) {
				sibling_node_page = parent_node->children[positions[i - 1] - 1];
				retval = rl_read(db, btree->type->btree_node_type, parent_node->children[positions[i - 1] - 1], btree, &tmp, 1);
				if (retval != RL_FOUND) {
					goto cleanup;
				}
				sibling_node = tmp;
				sibling_node->scores[sibling_node->size] = parent_node->scores[positions[i - 1] - 1];
				if (parent_node->values) {
					sibling_node->values[sibling_node->size] = parent_node->values[positions[i - 1] - 1];
				}
				memmove_dbg(&sibling_node->scores[sibling_node->size + 1], &node->scores[0], sizeof(void *) * (node->size), __LINE__);
				if (sibling_node->values) {
					memmove_dbg(&sibling_node->values[sibling_node->size + 1], &node->values[0], sizeof(void *) * (node->size), __LINE__);
				}
				if (sibling_node->children) {
					memmove_dbg(&sibling_node->children[sibling_node->size + 1], &node->children[0], sizeof(void *) * (node->size + 1), __LINE__);
				}

				if (positions[i - 1] < parent_node->size) {
					memmove_dbg(&parent_node->scores[positions[i - 1] - 1], &parent_node->scores[positions[i - 1]], sizeof(void *) * (parent_node->size - positions[i - 1]), __LINE__);
					if (parent_node->values) {
						memmove_dbg(&parent_node->values[positions[i - 1] - 1], &parent_node->values[positions[i - 1]], sizeof(void *) * (parent_node->size - positions[i - 1]), __LINE__);
					}
					memmove_dbg(&parent_node->children[positions[i - 1]], &parent_node->children[positions[i - 1] + 1], sizeof(void *) * (parent_node->size - positions[i - 1]), __LINE__);
				}
				parent_node->size--;
				sibling_node->size += 1 + node->size;
				retval = rl_write(db, btree->type->btree_node_type, sibling_node_page, sibling_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				retval = rl_write(db, btree->type->btree_node_type, parent_node_page, parent_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				retval = rl_write(db, btree->type->btree_node_type, node_page, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				free(node->scores);
				node->scores = NULL;
				retval = rl_delete(db, node_page);
				if (retval != RL_OK) {
					goto cleanup;
				}
				retval = rl_btree_node_destroy(db, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				continue;
			}
			if (positions[i - 1] < parent_node->size) {
				sibling_node_page = parent_node->children[positions[i - 1] + 1];
				retval = rl_read(db, btree->type->btree_node_type, parent_node->children[positions[i - 1] + 1], btree, &tmp, 1);
				if (retval != RL_FOUND) {
					goto cleanup;
				}
				sibling_node = tmp;
				node->scores[node->size] = parent_node->scores[positions[i - 1]];
				if (parent_node->values) {
					node->values[node->size] = parent_node->values[positions[i - 1]];
				}
				memmove_dbg(&node->scores[node->size + 1], &sibling_node->scores[0], sizeof(void *) * (sibling_node->size), __LINE__);
				if (node->values) {
					memmove_dbg(&node->values[node->size + 1], &sibling_node->values[0], sizeof(void *) * (sibling_node->size), __LINE__);
				}
				if (node->children) {
					memmove_dbg(&node->children[node->size + 1], &sibling_node->children[0], sizeof(void *) * (sibling_node->size + 1), __LINE__);
				}


				memmove_dbg(&parent_node->scores[positions[i - 1]], &parent_node->scores[positions[i - 1] + 1], sizeof(void *) * (parent_node->size - positions[i - 1] - 1), __LINE__);
				if (parent_node->values) {
					memmove_dbg(&parent_node->values[positions[i - 1]], &parent_node->values[positions[i - 1] + 1], sizeof(void *) * (parent_node->size - positions[i - 1] - 1), __LINE__);
				}
				memmove_dbg(&parent_node->children[positions[i - 1] + 1], &parent_node->children[positions[i - 1] + 2], sizeof(void *) * (parent_node->size - positions[i - 1] - 1), __LINE__);

				parent_node->size--;
				node->size += 1 + sibling_node->size;
				retval = rl_write(db, btree->type->btree_node_type, node_page, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				retval = rl_write(db, btree->type->btree_node_type, parent_node_page, parent_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				// freeing manually scores before calling destroy to avoid deleting scores that were handed over to `node`
				free(sibling_node->scores);
				sibling_node->scores = NULL;
				retval = rl_delete(db, sibling_node_page);
				if (retval != RL_OK) {
					goto cleanup;
				}
				retval = rl_btree_node_destroy(db, sibling_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				continue;
			}

			// this shouldn't happen
			fprintf(stderr, "No sibling to borrow or merge\n");
			retval = RL_INVALID_STATE;
			goto cleanup;
		}
	}

	btree->number_of_elements--;
cleanup:
	free(nodes);
	free(positions);

	return retval;
}

int rl_btree_node_is_balanced(rlite *db, rl_btree *btree, rl_btree_node *node, int is_root)
{
	if (!is_root && node->size < btree->max_node_size / 2) {
		fprintf(stderr, "Non root node is below maximum\n");
		return RL_INVALID_STATE;
	}

	void *tmp;
	int i, retval = RL_OK;
	rl_btree_node *child;
	for (i = 0; i < node->size + 1; i++) {
		if (node->children) {
			retval = rl_read(db, btree->type->btree_node_type, node->children[i], btree, &tmp, 1);
			if (retval != RL_FOUND) {
				break;
			}
			child = tmp;
			retval = rl_btree_node_is_balanced(db, btree, child, 0);
			if (retval != RL_OK) {
				fprintf(stderr, "Child is not balanced %p\n", (void *)child);
				break;
			}
		}
	}
	return retval;
}

int rl_btree_is_balanced(rlite *db, rl_btree *btree)
{
	void **scores = NULL, *tmp;
	rl_btree_node *node;
	int retval = rl_read(db, btree->type->btree_node_type, btree->root, btree, &tmp, 1);
	if (retval != RL_FOUND) {
		fprintf(stderr, "Unable to read btree in page %ld (%d)\n", btree->root, retval);
		goto cleanup;
	}
	node = tmp;
	retval = rl_btree_node_is_balanced(db, btree, node, 1);
	if (retval != RL_OK) {
		goto cleanup;
	}

	long size = (long)pow(btree->max_node_size + 1, btree->height + 1);
	scores = malloc(sizeof(void *) * size);
	if (!scores) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	size = 0;
	rl_flatten_btree(db, btree, &scores, &size);
	long i, j;
	for (i = 0; i < size; i++) {
		for (j = i + 1; j < size; j++) {
			if (btree->type->cmp(scores[i], scores[j]) >= 0) {
#ifdef DEBUG
				rl_print_btree(db, btree);
#endif
				fprintf(stderr, "btree is not sorted");
				char *str = malloc(sizeof(char) * 100);
				if (str == NULL) {
					retval = RL_OUT_OF_MEMORY;
					goto cleanup;
				}
#ifdef DEBUG
				fprintf(stderr, " (");
				int strlen;
				btree->type->formatter(scores[i], &str, &strlen);
				str[strlen] = 0;
				fprintf(stderr, "%s >= ", str);
				btree->type->formatter(scores[j], &str, &strlen);
				str[strlen] = 0;
				fprintf(stderr, "%s)\n", str);
#else
				fprintf(stderr, "\n");
#endif
				retval = RL_INVALID_STATE;
				goto cleanup;
			}
		}
	}
cleanup:
	free(scores);
	return retval;
}

#ifdef DEBUG
int rl_print_btree_node(rlite *db, rl_btree *btree, rl_btree_node *node, long level)
{
	int retval = RL_OK;
	long i, j;
	void *tmp;
	rl_btree_node *child;
	if (node->children) {
		retval = rl_read(db, btree->type->btree_node_type, node->children[0], btree, &tmp, 1);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		child = tmp;
		retval = rl_print_btree_node(db, btree, child, level + 1);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}
	char *score;
	int size;
	for (i = 0; i < node->size; i++) {
		for (j = 0; j < level; j++) {
			putchar('=');
		}
		btree->type->formatter(node->scores[i], &score, &size);
		fwrite(score, sizeof(char), size, stdout);
		free(score);
		printf("\n");
		if (node->values) {
			for (j = 0; j < level; j++) {
				putchar('*');
			}
			printf("%p\n", node->values[i]);
		}
		if (node->children) {
			retval = rl_read(db, btree->type->btree_node_type, node->children[i + 1], btree, &tmp, 1);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			child = tmp;
			retval = rl_print_btree_node(db, btree, child, level + 1);
			if (retval != RL_OK) {
				goto cleanup;
			}
		}
	}
cleanup:
	return retval;
}

int rl_print_btree(rlite *db, rl_btree *btree)
{
	rl_btree_node *node;
	void *tmp;
	int retval = rl_read(db, btree->type->btree_node_type, btree->root, btree, &tmp, 1);
	if (retval != RL_FOUND) {
		return retval;
	}
	node = tmp;
	printf("-------\n");
	retval = rl_print_btree_node(db, btree, node, 1);
	printf("-------\n");
	return retval;
}
#endif

int rl_flatten_btree_node(rlite *db, rl_btree *btree, rl_btree_node *node, void *** scores, long *size)
{
	int retval = RL_OK;
	long i;
	void *tmp;
	rl_btree_node *child;
	if (node->children) {
		retval = rl_read(db, btree->type->btree_node_type, node->children[0], btree, &tmp, 1);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		child = tmp;
		retval = rl_flatten_btree_node(db, btree, child, scores, size);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}
	for (i = 0; i < node->size; i++) {
		(*scores)[*size] = node->scores[i];
		(*size)++;
		if (node->children) {
			retval = rl_read(db, btree->type->btree_node_type, node->children[i + 1], btree, &tmp, 1);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			child = tmp;
			retval = rl_flatten_btree_node(db, btree, child, scores, size);
			if (retval != RL_OK) {
				goto cleanup;
			}
		}
	}
cleanup:
	return retval;
}

int rl_flatten_btree(rlite *db, rl_btree *btree, void *** scores, long *size)
{
	void *node;
	int retval = rl_read(db, btree->type->btree_node_type, btree->root, btree, &node, 1);
	if (retval != RL_FOUND) {
		return retval;
	}
	return rl_flatten_btree_node(db, btree, node, scores, size);
}

int rl_btree_node_serialize_hash_sha1_key(rlite *UNUSED(db), void *obj, unsigned char *data)
{
	rl_btree_node *node = (rl_btree_node *)obj;
	put_4bytes(data, node->size);
	long i, pos = 4;
	rl_key *key;
	for (i = 0; i < node->size; i++) {
		memcpy(&data[pos], node->scores[i], sizeof(unsigned char) * 20);
		key = node->values[i];
		data[pos + 20] = key->type;
		put_4bytes(&data[pos + 21], key->string_page);
		put_4bytes(&data[pos + 25], key->value_page);
		put_4bytes(&data[pos + 29], node->children ? node->children[i] : 0);
		pos += 33;
	}
	put_4bytes(&data[pos], node->children ? node->children[node->size] : 0);
	return RL_OK;
}

int rl_btree_node_deserialize_hash_sha1_key(rlite *db, void **obj, void *context, unsigned char *data)
{
	rl_btree_node *node;
	rl_btree *btree = (rl_btree *) context;
	int retval = rl_btree_node_create(db, btree, &node);
	if (retval != RL_OK) {
		return retval;
	}
	node->size = (long)get_4bytes(data);
	long i, pos = 4, child;
	rl_key *key;
	for (i = 0; i < node->size; i++) {
		node->scores[i] = malloc(sizeof(unsigned char) * 20);
		if (!node->scores[i]) {
			node->size = i;
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		memcpy(node->scores[i], &data[pos], sizeof(unsigned char) * 20);
		key = node->values[i] = malloc(sizeof(rl_key));
		if (!node->values[i]) {
			free(node->scores[i]);
			node->size = i;
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		key->type = data[pos + 20];
		key->string_page = get_4bytes(&data[pos + 21]);
		key->value_page = get_4bytes(&data[pos + 25]);
		child = get_4bytes(&data[pos + 29]);
		if (child != 0) {
			if (!node->children) {
				node->children = malloc(sizeof(long) * (btree->max_node_size + 1));
				if (!node->children) {
					free(node->scores[i]);
					free(node->values[i]);
					node->size = i;
					retval = RL_OUT_OF_MEMORY;
					goto cleanup;
				}
			}
			node->children[i] = child;
		}
		pos += 33;
	}
	child = get_4bytes(&data[pos]);
	if (child != 0) {
		node->children[node->size] = child;
	}
	*obj = node;
cleanup:
	if (retval != RL_OK && node) {
		rl_btree_node_destroy(db, node);
	}
	return retval;
}

int rl_btree_node_serialize_set_long(struct rlite *UNUSED(db), void *obj, unsigned char *data)
{
	rl_btree_node *node = obj;
	put_4bytes(data, node->size);
	long i, pos = 4;
	for (i = 0; i < node->size; i++) {
		put_4bytes(&data[pos], *(long *)(node->scores[i]));
		put_4bytes(&data[pos + 4], node->children ? node->children[i] : 0);
		pos += 8;
	}
	put_4bytes(&data[pos], node->children ? node->children[node->size] : 0);
	return RL_OK;
}

int rl_btree_node_deserialize_set_long(struct rlite *db, void **obj, void *context, unsigned char *data)
{
	rl_btree *btree = context;
	rl_btree_node *node;
	long i, pos = 4, child;
	int retval;
	retval = rl_btree_node_create(db, btree, &node);
	if (retval != RL_OK) {
		goto cleanup;
	}
	node->size = (long)get_4bytes(data);
	for (i = 0; i < node->size; i++) {
		node->scores[i] = malloc(sizeof(long));
		if (!node->scores[i]) {
			node->size = i;
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		*(long *)node->scores[i] = get_4bytes(&data[pos]);
		child = get_4bytes(&data[pos + 4]);
		if (child != 0) {
			if (!node->children) {
				node->children = malloc(sizeof(long) * (btree->max_node_size + 1));
				if (!node->children) {
					free(node->scores[i]);
					node->size = i;
					retval = RL_OUT_OF_MEMORY;
					goto cleanup;
				}
			}
			node->children[i] = child;
		}
		pos += 8;
	}
	child = get_4bytes(&data[pos]);
	if (child != 0) {
		node->children[node->size] = child;
	}
	*obj = node;
cleanup:
	if (retval != RL_OK && node) {
		rl_btree_node_destroy(db, node);
	}
	return retval;
}

int rl_btree_node_serialize_hash_long_long(struct rlite *UNUSED(db), void *obj, unsigned char *data)
{
	rl_btree_node *node = obj;
	int retval = RL_OK;
	if (!data) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	long i, pos = 4;
	put_4bytes(data, node->size);
	for (i = 0; i < node->size; i++) {
		put_4bytes(&data[pos], *(long *)(node->scores[i]));
		put_4bytes(&data[pos + 4], node->children ? node->children[i] : 0);
		put_4bytes(&data[pos + 8], *(long *)node->values[i]);
		pos += 12;
	}
	put_4bytes(&data[pos], node->children ? node->children[node->size] : 0);
cleanup:
	return retval;
}

int rl_btree_node_deserialize_hash_long_long(struct rlite *db, void **obj, void *context, unsigned char *data)
{
	rl_btree *btree = context;
	rl_btree_node *node;
	long i, pos = 4, child;
	int retval = rl_btree_node_create(db, btree, &node);
	if (retval != RL_OK) {
		goto cleanup;
	}
	node->size = (long)get_4bytes(data);
	for (i = 0; i < node->size; i++) {
		node->scores[i] = malloc(sizeof(long));
		if (!node->scores[i]) {
			node->size = i;
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		*(long *)node->scores[i] = get_4bytes(&data[pos]);
		child = get_4bytes(&data[pos + 4]);
		if (child != 0) {
			if (!node->children) {
				node->children = malloc(sizeof(long) * (btree->max_node_size + 1));
				if (!node->children) {
					free(node->scores[i]);
					node->size = i;
					retval = RL_OUT_OF_MEMORY;
					goto cleanup;
				}
			}
			node->children[i] = child;
		}
		node->values[i] = malloc(sizeof(long));
		if (!node->values[i]) {
			free(node->scores[i]);
			node->size = i;
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		*(long *)node->values[i] = get_4bytes(&data[pos + 8]);
		pos += 12;
	}
	child = get_4bytes(&data[pos]);
	if (child != 0) {
		node->children[node->size] = child;
	}
	*obj = node;
cleanup:
	if (retval != RL_OK && node) {
		rl_btree_node_destroy(db, node);
	}
	return retval;
}

int rl_btree_node_serialize_hash_sha1_double(struct rlite *UNUSED(db), void *obj, unsigned char *data)
{
	rl_btree_node *node = obj;
	int retval = RL_OK;
	if (!data) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	long i, pos = 4;
	put_4bytes(data, node->size);
	for (i = 0; i < node->size; i++) {
		memcpy(&data[pos], node->scores[i], sizeof(unsigned char) * 20);
		put_double(&data[pos + 20], *(double *)(node->values[i]));
		put_4bytes(&data[pos + 28], node->children ? node->children[node->size] : 0);
		pos += 32;
	}
	put_4bytes(&data[pos], node->children ? node->children[node->size] : 0);
cleanup:
	return retval;
}

int rl_btree_node_deserialize_hash_sha1_double(struct rlite *db, void **obj, void *context, unsigned char *data)
{
	rl_btree *btree = context;
	rl_btree_node *node;
	long i, pos = 4, child;
	int retval = rl_btree_node_create(db, btree, &node);
	if (retval != RL_OK) {
		goto cleanup;
	}
	node->size = (long)get_4bytes(data);
	for (i = 0; i < node->size; i++) {
		node->scores[i] = malloc(sizeof(unsigned char) * 20);
		if (!node->scores[i]) {
			node->size = i;
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		memcpy(node->scores[i], &data[pos], sizeof(unsigned char) * 20);
		node->values[i] = malloc(sizeof(double));
		if (!node->values[i]) {
			free(node->scores[i]);
			node->size = i;
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		*(double *)node->values[i] = get_double(&data[pos + 20]);
		child = get_4bytes(&data[pos + 28]);
		if (child != 0) {
			if (!node->children) {
				node->children = malloc(sizeof(long) * (btree->max_node_size + 1));
				if (!node->children) {
					free(node->scores[i]);
					node->size = i;
					retval = RL_OUT_OF_MEMORY;
					goto cleanup;
				}
			}
			node->children[i] = child;
		}
		pos += 32;
	}
	child = get_4bytes(&data[pos]);
	if (child != 0) {
		node->children[node->size] = child;
	}
	*obj = node;
cleanup:
	if (retval != RL_OK && node) {
		rl_btree_node_destroy(db, node);
	}
	return retval;
}

int rl_btree_iterator_create(rlite *db, rl_btree *btree, rl_btree_iterator **_iterator)
{
	long i;
	void *tmp;
	rl_btree_iterator *iterator = malloc(sizeof(rl_btree_iterator) + sizeof(struct rl_btree_iterator_nodes) * btree->height);
	if (!iterator) {
		return RL_OUT_OF_MEMORY;
	}
	iterator->db = db;
	iterator->btree = btree;
	iterator->size = btree->height;

	int retval = rl_read(db, btree->type->btree_node_type, btree->root, btree, &tmp, 0);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	iterator->nodes[0].node = tmp;
	iterator->nodes[0].position = 0;

	for (i = 1; i < btree->height; i++) {
		retval = rl_read(db, btree->type->btree_node_type, iterator->nodes[i - 1].node->children[0], btree, &tmp, 0);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		iterator->nodes[i].node = tmp;
		iterator->nodes[i].position = 0;
	}

	*_iterator = iterator;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_btree_iterator_next(rl_btree_iterator *iterator, void **score, void **value)
{
	if (iterator->size == 0) {
		return RL_END;
	}
	int retval;
	long position;
	void *tmp;
	rl_btree *btree = iterator->btree;
	rl_btree_node *node = iterator->nodes[iterator->size - 1].node;
	position = iterator->nodes[iterator->size - 1].position;
	if (score) {
		*score = malloc(iterator->btree->type->score_size);
		memcpy(*score, node->scores[position], iterator->btree->type->score_size);
	}
	if (value) {
		*value = malloc(iterator->btree->type->value_size);
		memcpy(*value, node->values[position], iterator->btree->type->value_size);
	}

	if (node->children) {
		iterator->nodes[iterator->size - 1].position++;
		while (iterator->size < iterator->btree->height) {
			position = node->children[iterator->nodes[iterator->size - 1].position];
			retval = rl_read(iterator->db, btree->type->btree_node_type, position, btree, &tmp, 0);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			iterator->nodes[iterator->size].position = 0;
			node = iterator->nodes[iterator->size++].node = tmp;
		}
	}
	else {
		if (node->size == iterator->nodes[iterator->size - 1].position + 1) {
			while (--iterator->size > 0) {
				retval = rl_btree_node_nocache_destroy(iterator->db, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				node = iterator->nodes[iterator->size - 1].node;
				if (iterator->nodes[iterator->size - 1].position < node->size) {
					break;
				}
			}
			if (iterator->size == 0) {
				retval = rl_btree_node_nocache_destroy(iterator->db, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
			}
		}
		else {
			iterator->nodes[iterator->size - 1].position++;
		}
	}

	retval = RL_OK;
cleanup:
	return retval;
}

int rl_btree_iterator_destroy(rl_btree_iterator *iterator)
{
	free(iterator);
	return RL_OK;
}
