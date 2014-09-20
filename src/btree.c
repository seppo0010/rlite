#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "btree.h"

void rl_print_node(rl_tree *tree, rl_tree_node *node, long level);
rl_tree_node *rl_tree_node_create(rl_tree *tree);

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

long long_set_node_serialize_length(void *_tree)
{
	rl_tree *tree = (rl_tree *)_tree;
	return sizeof(unsigned char) * ((8 * tree->max_size) + 8);
}

long long_set_node_serialize(void *_tree, void *_node, unsigned char **_data, long *data_size)
{
	rl_tree *tree = (rl_tree *)_tree;
	rl_tree_node *node = (rl_tree_node *)_node;
	unsigned char *data = malloc(tree->type->serialize_length(tree));
	put_4bytes(data, node->size);
	long i, pos = 4;
	for (i = 0; i < node->size; i++) {
		put_4bytes(&data[pos], *(long *)(node->scores[i]));
		put_4bytes(&data[pos + 4], node->children ? node->children[i] : 0);
		pos += 8;
	}
	put_4bytes(&data[pos], node->children ? node->children[node->size] : 0);
	*_data = data;
	*data_size = pos + 4;
	return 0;
}

long long_set_node_deserialize(void *_tree, unsigned char *data, void **_node)
{
	rl_tree *tree = (rl_tree *)_tree;
	rl_tree_node *node = rl_tree_node_create(tree);
	if (!node) {
		return 1;
	}
	node->size = (long)get_4bytes(data);
	long i, pos = 4, child;
	for (i = 0; i < node->size; i++) {
		node->scores[i] = (void *)(long)get_4bytes(&data[pos]);
		child = get_4bytes(&data[pos + 4]);
		if (child != 0) {
			if (!node->children) {
				node->children = malloc(sizeof(long) * (tree->max_size + 1));
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
		node->children[node->size + 1] = child;
	}
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
	long_set.serialize_length = long_set_node_serialize_length;
	long_set.serialize = long_set_node_serialize;
	long_set.deserialize = long_set_node_deserialize;
}

void init_long_hash()
{
	long_set.score_size = sizeof(long);
	long_set.value_size = sizeof(void *);
	long_set.cmp = long_cmp;
	long_set.formatter = long_formatter;
}

rl_tree_node *rl_tree_node_create(rl_tree *tree)
{
	rl_tree_node *node = malloc(sizeof(rl_tree_node));
	node->scores = malloc(sizeof(void *) * tree->max_size);
	node->children = NULL;
	if (tree->type->value_size) {
		node->values = malloc(sizeof(void *) * tree->max_size);
	}
	else {
		node->values = NULL;
	}
	node->size = 0;
	return node;
}

long rl_tree_node_destroy(rl_tree *tree, rl_tree_node *node)
{
	if (node->scores) {
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

rl_tree *rl_tree_create(rl_tree_type *type, long max_size, rl_accessor *accessor)
{
	rl_tree *tree = malloc(sizeof(rl_tree));
	if (!tree) {
		return NULL;
	}
	tree->max_size = max_size;
	tree->type = type;
	rl_tree_node *root = rl_tree_node_create(tree);
	root->size = 0;
	if (!root) {
		free(tree);
		return NULL;
	}
	tree->accessor = accessor;
	tree->height = 1;
	if (0 != tree->accessor->insert(tree, &tree->root, root)) {
		free(root);
		free(tree);
		return NULL;
	}
	return tree;
}

int rl_tree_destroy(rl_tree *tree)
{
	long i;
	rl_tree_node **nodes;
	long size;
	if (0 != tree->accessor->list(tree, &nodes, &size)) {
		return 1;
	}
	for (i = 0; i < size; i++) {
		if (0 != rl_tree_node_destroy(tree, nodes[i])) {
			return 1;
		}
	}
	free(tree);
	return 0;
}

long rl_tree_find_score(rl_tree *tree, void *score, rl_tree_node *** nodes, long **positions)
{
	rl_tree_node *node = tree->accessor->select(tree, tree->root);
	if ((!nodes && positions) || (nodes && !positions)) {
		return -1;
	}
	long i, pos, min, max;
	int cmp = 0;
	for (i = 0; i < tree->height; i++) {
		if (nodes) {
			(*nodes)[i] = node;
		}
		pos = 0;
		min = 0;
		max = node->size - 1;
		while (min <= max) {
			pos = (max - min) / 2 + min;
			cmp = tree->type->cmp(score, node->scores[pos]);
			if (cmp == 0) {
				if (nodes) {
					for (; i < tree->height; i++) {
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
			if (pos == max && tree->type->cmp(score, node->scores[pos]) == 1) {
				pos++;
			}
			(*positions)[i] = pos;
		}
		if (node->children) {
			node = tree->accessor->select(tree, node->children[pos]);
		}
	}
	return 0;
}

int rl_tree_add_child(rl_tree *tree, void *score, void *value)
{
	rl_tree_node **nodes = malloc(sizeof(rl_tree_node *) * tree->height);
	long *positions = malloc(sizeof(long) * tree->height);
	void *tmp;
	long found = rl_tree_find_score(tree, score, &nodes, &positions);
	long i, pos;
	long child = -1;
	int retval = 0;

	if (found) {
		retval = 1;
		goto cleanup;
	}
	rl_tree_node *node;
	for (i = tree->height - 1; i >= 0; i--) {
		node = nodes[i];
		if (node->size < tree->max_size) {
			memmove_dbg(&node->scores[positions[i] + 1], &node->scores[positions[i]], sizeof(void *) * (node->size - positions[i]), 1);
			if (node->values) {
				memmove_dbg(&node->values[positions[i] + 1], &node->values[positions[i]], sizeof(void *) * (node->size - positions[i]), 2);
			}
			if (node->children) {
				memmove_dbg(&node->children[positions[i] + 2], &node->children[positions[i] + 1], sizeof(long) * (node->size - positions[i]), 3);
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

			rl_tree_node *right = rl_tree_node_create(tree);
			if (child != -1) {
				right->children = malloc(sizeof(long) * (tree->max_size + 1));
			}

			if (pos < tree->max_size / 2) {
				if (child != -1) {
					memmove_dbg(&node->children[pos + 2], &node->children[pos + 1], sizeof(void *) * (tree->max_size / 2 - 1 - pos), 4);
					memmove_dbg(right->children, &node->children[tree->max_size / 2], sizeof(void *) * (tree->max_size / 2 + 1), 5);
					node->children[pos + 1] = child;

				}
				tmp = node->scores[tree->max_size / 2 - 1];
				memmove_dbg(&node->scores[pos + 1], &node->scores[pos], sizeof(void *) * (tree->max_size / 2 - 1 - pos), 6);
				node->scores[pos] = score;
				score = tmp;
				memmove_dbg(right->scores, &node->scores[tree->max_size / 2], sizeof(void *) * tree->max_size / 2, 7);
			}

			if (pos == tree->max_size / 2) {
				if (child != -1) {
					memmove_dbg(&right->children[1], &node->children[tree->max_size / 2 + 1], sizeof(void *) * (tree->max_size / 2), 8);
					right->children[0] = child;
				}
				memmove_dbg(right->scores, &node->scores[tree->max_size / 2], sizeof(void *) * tree->max_size / 2, 9);
			}

			if (pos > tree->max_size / 2) {
				if (child != -1) {
					memmove_dbg(right->children, &node->children[tree->max_size / 2 + 1], sizeof(void *) * (pos - tree->max_size / 2), 10);
					right->children[pos - tree->max_size / 2] = child;
					memmove_dbg(&right->children[pos - tree->max_size / 2 + 1], &node->children[pos + 1], sizeof(void *) * (tree->max_size - pos), 11);
				}
				tmp = node->scores[tree->max_size / 2];
				memmove_dbg(right->scores, &node->scores[tree->max_size / 2 + 1], sizeof(void *) * (pos - tree->max_size / 2 - 1), 12);
				right->scores[pos - tree->max_size / 2 - 1] = score;
				memmove_dbg(&right->scores[pos - tree->max_size / 2], &node->scores[pos], sizeof(void *) * (tree->max_size - pos), 13);
				score = tmp;
			}

			node->size = right->size = tree->max_size / 2;
			tree->accessor->insert(tree, &child, right);
		}
	}
	if (score) {
		rl_tree_node *old_root = node;
		node = rl_tree_node_create(tree);
		node->size = 1;
		node->scores[0] = score;
		node->children = malloc(sizeof(long) * (tree->max_size + 1));
		tree->accessor->update(tree, &node->children[0], old_root);
		node->children[1] = child;
		tree->accessor->insert(tree, &tree->root, node);
		tree->height++;
	}

cleanup:
	free(nodes);
	free(positions);

	return retval;
}

void rl_print_node(rl_tree *tree, rl_tree_node *node, long level)
{
	long i, j;
	if (node->children) {
		rl_print_node(tree, (rl_tree_node *)tree->accessor->select(tree, node->children[0]), level + 1);
	}
	char *score;
	int size;
	for (i = 0; i < node->size; i++) {
		for (j = 0; j < level; j++) {
			putchar('=');
		}
		tree->type->formatter(node->scores[i], &score, &size);
		fwrite(score, sizeof(char), size, stdout);
		free(score);
		printf("\n");
		if (node->children) {
			rl_print_node(tree, (rl_tree_node *)tree->accessor->select(tree, node->children[i + 1]), level + 1);
		}
	}
}

void rl_print_tree(rl_tree *tree)
{
	printf("-------\n");
	rl_print_node(tree, tree->accessor->select(tree, tree->root), 1);
	printf("-------\n");
}

void rl_flatten_node(rl_tree *tree, rl_tree_node *node, void *** scores, long *size)
{
	long i;
	if (node->children) {
		rl_flatten_node(tree, tree->accessor->select(tree, node->children[0]), scores, size);
	}
	for (i = 0; i < node->size; i++) {
		(*scores)[*size] = node->scores[i];
		(*size)++;
		if (node->children) {
			rl_flatten_node(tree, tree->accessor->select(tree, node->children[i + 1]), scores, size);
		}
	}
}

void rl_flatten_tree(rl_tree *tree, void *** scores, long *size)
{
	rl_flatten_node(tree, tree->accessor->select(tree, tree->root), scores, size);
}
