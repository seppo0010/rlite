#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include "btree.h"

void rl_print_btree_node(rl_btree *btree, rl_btree_node *node, long level);
rl_btree_node *rl_btree_node_create(rl_btree *btree);

void *memmove_dbg(void *dest, void *src, size_t n, int flag)
{
	void *data = malloc(n);
	memmove(data, src, n);
	memset(src, flag, n);
	void *retval = memmove(dest, data, n);
	free(data);
	return retval;
}


int get_4bytes(const unsigned char *p)
{
	return (p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
}


void put_4bytes(unsigned char *p, long v)
{
	p[0] = (unsigned char)(v >> 24);
	p[1] = (unsigned char)(v >> 16);
	p[2] = (unsigned char)(v >> 8);
	p[3] = (unsigned char)v;
}

int long_cmp(void *v1, void *v2)
{
	long a = *((long *)v1), b = *((long *)v2);
	if (a == b) {
		return 0;
	}
	return a > b ? 1 : -1;
}

void *long_score_create(void *_btree)
{
	_btree = _btree;
	return malloc(sizeof(long));
}

void long_score_destroy(void *_btree, void *score)
{
	_btree = _btree;
	free(score);
}

long long_set_btree_node_serialize_length(void *_btree)
{
	rl_btree *btree = (rl_btree *)_btree;
	return sizeof(unsigned char) * ((8 * btree->max_size) + 8);
}

long long_set_btree_node_serialize(void *_btree, void *_node, unsigned char **_data, long *data_size)
{
	rl_btree *btree = (rl_btree *)_btree;
	rl_btree_node *node = (rl_btree_node *)_node;
	unsigned char *data = malloc(btree->type->serialize_length(btree));
	put_4bytes(data, node->size);
	long i, pos = 4;
	for (i = 0; i < node->size; i++) {
		put_4bytes(&data[pos], *(long *)(node->scores[i]));
		put_4bytes(&data[pos + 4], node->children ? node->children[i] : 0);
		pos += 8;
	}
	put_4bytes(&data[pos], node->children ? node->children[node->size] : 0);
	*_data = data;
	if (data_size) {
		*data_size = pos + 4;
	}
	return 0;
}

long long_set_btree_node_deserialize(void *_btree, unsigned char *data, void **_node)
{
	rl_btree *btree = (rl_btree *)_btree;
	rl_btree_node *node = rl_btree_node_create(btree);
	if (!node) {
		return 1;
	}
	node->size = (long)get_4bytes(data);
	long i, pos = 4, child;
	for (i = 0; i < node->size; i++) {
		node->scores[i] = btree->type->score_create(btree);
		*(long *)node->scores[i] = get_4bytes(&data[pos]);
		child = get_4bytes(&data[pos + 4]);
		if (child != 0) {
			if (!node->children) {
				node->children = malloc(sizeof(long) * (btree->max_size + 1));
				if (!node->children) {
					free(node);
					return 1;
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
	*_node = node;
	return 0;
}

void long_formatter(void *v2, char **formatted, int *size)
{
	*formatted = malloc(sizeof(char) * 22);
	*size = snprintf(*formatted, 22, "%ld", *(long *)v2);
}

void init_long_set()
{
	long_set.score_size = sizeof(long);
	long_set.value_size = 0;
	long_set.cmp = long_cmp;
	long_set.formatter = long_formatter;
	long_set.serialize_length = long_set_btree_node_serialize_length;
	long_set.serialize = long_set_btree_node_serialize;
	long_set.deserialize = long_set_btree_node_deserialize;
	long_set.score_create = long_score_create;
	long_set.score_destroy = long_score_destroy;
}

void init_long_hash()
{
	long_hash.score_size = sizeof(long);
	long_hash.value_size = sizeof(void *);
	long_hash.cmp = long_cmp;
	long_hash.formatter = long_formatter;
	long_hash.serialize_length = long_set_btree_node_serialize_length;
	long_hash.serialize = long_set_btree_node_serialize;
	long_hash.deserialize = long_set_btree_node_deserialize;
	long_hash.score_create = long_score_create;
	long_hash.score_destroy = long_score_destroy;
}

rl_btree_node *rl_btree_node_create(rl_btree *btree)
{
	rl_btree_node *node = malloc(sizeof(rl_btree_node));
	node->scores = malloc(sizeof(void *) * btree->max_size);
	node->children = NULL;
	if (btree->type->value_size) {
		node->values = malloc(sizeof(void *) * btree->max_size);
	}
	else {
		node->values = NULL;
	}
	node->size = 0;
	return node;
}

long rl_btree_node_destroy(rl_btree *btree, rl_btree_node *node)
{
	long i;
	if (node->scores) {
		for (i = 0; i < node->size; i++) {
			btree->type->score_destroy(btree, node->scores[i]);
		}
		free(node->scores);
	}
	if (node->values) {
		free(node->values);
	}
	if (node->children) {
		free(node->children);
	}
	free(node);
	return 0;
}

rl_btree *rl_btree_create(rl_btree_type *type, long max_size, rl_accessor *accessor)
{
	rl_btree *btree = malloc(sizeof(rl_btree));
	if (!btree) {
		return NULL;
	}
	btree->max_size = max_size;
	btree->type = type;
	rl_btree_node *root = rl_btree_node_create(btree);
	root->size = 0;
	if (!root) {
		free(btree);
		return NULL;
	}
	btree->accessor = accessor;
	btree->height = 1;
	if (0 != btree->accessor->insert(btree, &btree->root, root)) {
		free(root);
		free(btree);
		return NULL;
	}
	return btree;
}

int rl_btree_destroy(rl_btree *btree)
{
	long i;
	rl_btree_node **nodes;
	long size;
	if (0 != btree->accessor->list(btree, &nodes, &size)) {
		return 1;
	}
	for (i = 0; i < size; i++) {
		if (0 != rl_btree_node_destroy(btree, nodes[i])) {
			return 1;
		}
	}
	free(btree);
	return 0;
}

long rl_btree_find_score(rl_btree *btree, void *score, void **value, rl_btree_node *** nodes, long **positions)
{
	rl_btree_node *node = btree->accessor->select(btree, btree->root);
	if ((!nodes && positions) || (nodes && !positions)) {
		return -1;
	}
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
				return 1;
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
			node = btree->accessor->select(btree, node->children[pos]);
		}
	}
	return 0;
}

int rl_btree_add_element(rl_btree *btree, void *score, void *value)
{
	rl_btree_node **nodes = malloc(sizeof(rl_btree_node *) * btree->height);
	long *positions = malloc(sizeof(long) * btree->height);
	void *tmp;
	long found = rl_btree_find_score(btree, score, NULL, &nodes, &positions);
	long i, pos;
	long child = -1;
	int retval = 0;

	if (found) {
		retval = 1;
		goto cleanup;
	}
	rl_btree_node *node = NULL;
	for (i = btree->height - 1; i >= 0; i--) {
		node = nodes[i];
		if (node->size < btree->max_size) {
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
			break;
		}
		else {
			pos = positions[i];

			rl_btree_node *right = rl_btree_node_create(btree);
			if (child != -1) {
				right->children = malloc(sizeof(long) * (btree->max_size + 1));
			}

			if (pos < btree->max_size / 2) {
				if (child != -1) {
					memmove_dbg(&node->children[pos + 2], &node->children[pos + 1], sizeof(void *) * (btree->max_size / 2 - 1 - pos), __LINE__);
					memmove_dbg(right->children, &node->children[btree->max_size / 2], sizeof(void *) * (btree->max_size / 2 + 1), __LINE__);
					node->children[pos + 1] = child;

				}
				tmp = node->scores[btree->max_size / 2 - 1];
				memmove_dbg(&node->scores[pos + 1], &node->scores[pos], sizeof(void *) * (btree->max_size / 2 - 1 - pos), __LINE__);
				node->scores[pos] = score;
				score = tmp;
				memmove_dbg(right->scores, &node->scores[btree->max_size / 2], sizeof(void *) * btree->max_size / 2, __LINE__);
				if (node->values) {
					tmp = node->values[btree->max_size / 2 - 1];
					memmove_dbg(&node->values[pos + 1], &node->values[pos], sizeof(void *) * (btree->max_size / 2 - 1 - pos), __LINE__);
					node->values[pos] = value;
					memmove_dbg(right->values, &node->values[btree->max_size / 2], sizeof(void *) * btree->max_size / 2, __LINE__);
					value = tmp;
				}
			}

			if (pos == btree->max_size / 2) {
				if (child != -1) {
					memmove_dbg(&right->children[1], &node->children[btree->max_size / 2 + 1], sizeof(void *) * (btree->max_size / 2), __LINE__);
					right->children[0] = child;
				}
				memmove_dbg(right->scores, &node->scores[btree->max_size / 2], sizeof(void *) * btree->max_size / 2, __LINE__);
				if (node->values) {
					memmove_dbg(right->values, &node->values[btree->max_size / 2], sizeof(void *) * btree->max_size / 2, __LINE__);
				}
			}

			if (pos > btree->max_size / 2) {
				if (child != -1) {
					memmove_dbg(right->children, &node->children[btree->max_size / 2 + 1], sizeof(void *) * (pos - btree->max_size / 2), __LINE__);
					right->children[pos - btree->max_size / 2] = child;
					memmove_dbg(&right->children[pos - btree->max_size / 2 + 1], &node->children[pos + 1], sizeof(void *) * (btree->max_size - pos), __LINE__);
				}
				tmp = node->scores[btree->max_size / 2];
				memmove_dbg(right->scores, &node->scores[btree->max_size / 2 + 1], sizeof(void *) * (pos - btree->max_size / 2 - 1), __LINE__);
				right->scores[pos - btree->max_size / 2 - 1] = score;
				memmove_dbg(&right->scores[pos - btree->max_size / 2], &node->scores[pos], sizeof(void *) * (btree->max_size - pos), __LINE__);
				score = tmp;
				if (node->values) {
					tmp = node->values[btree->max_size / 2];
					memmove_dbg(right->values, &node->values[btree->max_size / 2 + 1], sizeof(void *) * (pos - btree->max_size / 2 - 1), __LINE__);
					right->values[pos - btree->max_size / 2 - 1] = value;
					memmove_dbg(&right->values[pos - btree->max_size / 2], &node->values[pos], sizeof(void *) * (btree->max_size - pos), __LINE__);
					value = tmp;
				}
			}

			node->size = right->size = btree->max_size / 2;
			btree->accessor->insert(btree, &child, right);
		}
	}
	if (score) {
		rl_btree_node *old_root = node;
		node = rl_btree_node_create(btree);
		node->size = 1;
		node->scores[0] = score;
		if (node->values) {
			node->values[0] = value;
		}
		node->children = malloc(sizeof(long) * (btree->max_size + 1));
		btree->accessor->update(btree, &node->children[0], old_root);
		node->children[1] = child;
		btree->accessor->insert(btree, &btree->root, node);
		btree->height++;
	}

cleanup:
	free(nodes);
	free(positions);

	return retval;
}

int rl_btree_remove_element(rl_btree *btree, void *score)
{
	rl_btree_node **nodes = malloc(sizeof(rl_btree_node *) * btree->height);
	long *positions = malloc(sizeof(long) * btree->height);
	long found = rl_btree_find_score(btree, score, NULL, &nodes, &positions);
	long i, j;
	int retval = 0;

	if (!found) {
		goto cleanup;
	}

	rl_btree_node *node, *parent_node, *sibling_node, *child_node;
	for (i = btree->height - 1; i >= 0; i--) {
		node = nodes[i];
		if (!node) {
			continue;
		}

		btree->type->score_destroy(btree, node->scores[positions[i]]);
		if (node->children) {
			j = i;
			child_node = node;
			for (; i < btree->height - 1; i++) {
				child_node = (rl_btree_node *)btree->accessor->select(btree, child_node->children[positions[i]]);
				nodes[i + 1] = child_node;
				positions[i + 1] = child_node->size;
			}

			if (child_node->children) {
				fprintf(stderr, "last child_node mustn't have children\n");
				retval = 1;
				goto cleanup;
			}

			// only the leaf node loses an element, to replace the deleted one
			child_node->size--;
			node->scores[positions[j]] = child_node->scores[child_node->size];
			if (node->values) {
				node->values[positions[j]] = child_node->values[child_node->size];
			}
			btree->accessor->update(btree, NULL, node);
			btree->accessor->update(btree, NULL, child_node);
			break;
		}
		else {
			memmove_dbg(&node->scores[positions[i]], &node->scores[positions[i] + 1], sizeof(void *) * (node->size - positions[i] - 1), __LINE__);
			if (node->values) {
				memmove_dbg(&node->values[positions[i]], &node->values[positions[i] + 1], sizeof(void *) * (node->size - positions[i]), __LINE__);
			}

			if (--node->size > 0) {
				btree->accessor->update(btree, NULL, node);
			}
			else {
				if (i == 0 && node->children) {
					// we have an empty root, promote the only child if any
					btree->height--;
					btree->accessor->update(btree, &btree->root, btree->accessor->select(btree, node->children[0]));
					btree->accessor->remove(btree, node);
					rl_btree_node_destroy(btree, node);
				}
			}
			break;
		}
	}

	for (; i >= 0; i--) {
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
		if (node->size >= btree->max_size / 2 || i == 0) {
			break;
		}
		else {
			parent_node = nodes[i - 1];
			if (parent_node->size == 0) {
				fprintf(stderr, "Empty parent\n");
				retval = 1;
				goto cleanup;
			}
			if (positions[i - 1] > 0) {
				sibling_node = btree->accessor->select(btree, parent_node->children[positions[i - 1] - 1]);
				if (sibling_node->size > btree->max_size / 2) {
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
					btree->accessor->update(btree, NULL, sibling_node);
					btree->accessor->update(btree, NULL, node);
					break;
				}
			}
			if (positions[i - 1] < parent_node->size) {
				sibling_node = btree->accessor->select(btree, parent_node->children[positions[i - 1] + 1]);
				if (sibling_node->size > btree->max_size / 2) {
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
					btree->accessor->update(btree, NULL, sibling_node);
					btree->accessor->update(btree, NULL, node);
					break;
				}
			}

			// not taking from either slibing, need to merge with either
			// if either of them exists, they have the minimum number of elements
			if (positions[i - 1] > 0) {
				sibling_node = btree->accessor->select(btree, parent_node->children[positions[i - 1] - 1]);
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
				btree->accessor->update(btree, NULL, sibling_node);
				btree->accessor->update(btree, NULL, parent_node);
				btree->accessor->remove(btree, node);
				free(node->scores);
				node->scores = NULL;
				rl_btree_node_destroy(btree, node);
				continue;
			}
			if (positions[i - 1] < parent_node->size) {
				sibling_node = btree->accessor->select(btree, parent_node->children[positions[i - 1] + 1]);
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
				btree->accessor->update(btree, NULL, node);
				btree->accessor->update(btree, NULL, parent_node);
				// freeing manually scores before calling destroy to avoid deleting scores that were handed over to `node`
				free(sibling_node->scores);
				sibling_node->scores = NULL;
				btree->accessor->remove(btree, sibling_node);
				rl_btree_node_destroy(btree, sibling_node);
				continue;
			}

			// this shouldn't happen
			fprintf(stderr, "No sibling to borrow or merge\n");
			retval = 1;
			goto cleanup;
		}
	}

cleanup:
	free(nodes);
	free(positions);

	return retval;
}

int rl_btree_node_is_balanced(rl_btree *btree, rl_btree_node *node, int is_root)
{
	if (!is_root && node->size < btree->max_size / 2) {
		return 0;
	}

	int i;
	for (i = 0; i < node->size + 1; i++) {
		if (node->children) {
			if (rl_btree_node_is_balanced(btree, (rl_btree_node *)btree->accessor->select(btree, node->children[i]), 0) == 0) {
				return 0;
			}
		}
	}
	return 1;
}

int rl_btree_is_balanced(rl_btree *btree)
{
	if (!rl_btree_node_is_balanced(btree, btree->accessor->select(btree, btree->root), 1)) {
		return 0;
	}

	long size = (long)pow(btree->max_size + 1, btree->height + 1);
	void **scores = malloc(sizeof(void *) * size);
	size = 0;
	rl_flatten_btree(btree, &scores, &size);
	long i, j;
	for (i = 0; i < size; i++) {
		for (j = i + 1; j < size; j++) {
			if (btree->type->cmp(scores[i], scores[j]) == 0) {
				free(scores);
				return 0;
			}
		}
	}
	free(scores);
	return 1;
}

void rl_print_btree_node(rl_btree *btree, rl_btree_node *node, long level)
{
	long i, j;
	if (node->children) {
		rl_print_btree_node(btree, (rl_btree_node *)btree->accessor->select(btree, node->children[0]), level + 1);
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
		printf("\n");
		if (node->children) {
			rl_print_btree_node(btree, (rl_btree_node *)btree->accessor->select(btree, node->children[i + 1]), level + 1);
		}
	}
}

void rl_print_btree(rl_btree *btree)
{
	printf("-------\n");
	rl_print_btree_node(btree, btree->accessor->select(btree, btree->root), 1);
	printf("-------\n");
}

void rl_flatten_btree_node(rl_btree *btree, rl_btree_node *node, void *** scores, long *size)
{
	long i;
	if (node->children) {
		rl_flatten_btree_node(btree, btree->accessor->select(btree, node->children[0]), scores, size);
	}
	for (i = 0; i < node->size; i++) {
		(*scores)[*size] = node->scores[i];
		(*size)++;
		if (node->children) {
			rl_flatten_btree_node(btree, btree->accessor->select(btree, node->children[i + 1]), scores, size);
		}
	}
}

void rl_flatten_btree(rl_btree *btree, void *** scores, long *size)
{
	rl_flatten_btree_node(btree, btree->accessor->select(btree, btree->root), scores, size);
}
