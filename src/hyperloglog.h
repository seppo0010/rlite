#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

extern size_t rl_hll_sparse_max_bytes;
int rl_str_pfadd(unsigned char *str, long strlen, int argc, unsigned char **argv, long *argvlen, unsigned char **_str, long *_strlen);
int rl_str_pfcount(int argc, unsigned char **argv, long *argvlen, long *_card, unsigned char **updatevalue, long *updatevaluelen);
int rl_str_pfmerge(int argc, unsigned char **argv, long *argvlen, unsigned char **_str, long *_strlen);
int rl_str_pfselftest();
int rl_str_pfdebug_getreg(unsigned char *str, long strlen, int *size, long **elements, unsigned char **_str, long *_strlen);
int rl_str_pfdebug_decode(unsigned char *str, long strlen, unsigned char **response, long *responselen);
int rl_str_pfdebug_encoding(unsigned char *str, long strlen, unsigned char **response, long *responselen);
int rl_str_pfdebug_todense(unsigned char *str, long strlen, unsigned char **_str, long *_strlen);
