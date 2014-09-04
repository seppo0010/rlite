#include "btree.h"
#include <stdlib.h>
#include <stdio.h>

static void* getter(void* tree, long number) {
   return (void*)number;
}

static long setter(void* tree, void* node) {
   return (long)node;
}

int main() {
   rl_accessor accessor;
   accessor.getter = getter;
   accessor.setter = setter;
   init_long_set();
   rl_tree* tree = rl_tree_create(&long_set, 2, &accessor);
   long vals[7] = {1, 2, 3, 4, 5, 6, 7};
   int i;
   for (i = 0; i < 7; i++) {
      if (0 != rl_tree_add_child(tree, &vals[i], NULL)) {
         fprintf(stderr, "Failed to add child %d\n", i);
         return 1;
      }
      rl_print_tree(tree);
   }
   for (i = 0; i < 7; i++) {
      if (1 != rl_tree_find_score(tree, &vals[i], NULL, NULL)) {
         fprintf(stderr, "Failed to find child %d\n", i);
         return 1;
      }
   }
   long nonexistent_vals[2] = {0, 8};
   for (i = 0; i < 2; i++) {
      if (0 != rl_tree_find_score(tree, &nonexistent_vals[i], NULL, NULL)) {
         fprintf(stderr, "Failed to not find child %d\n", i);
         return 1;
      }
   }
   return 0;
}
