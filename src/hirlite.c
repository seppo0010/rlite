#include <assert.h>
#include <errno.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "hirlite.h"
#include "util.h"

static rliteReply *createReplyObject(int type) {
	rliteReply *r = calloc(1,sizeof(*r));

	if (r == NULL)
		return NULL;

	r->type = type;
	return r;
}

void __rliteSetError(rliteContext *c, int type, const char *str) {
	size_t len;

	c->err = type;
	if (str != NULL) {
		len = strlen(str);
		len = len < (sizeof(c->errstr)-1) ? len : (sizeof(c->errstr)-1);
		memcpy(c->errstr,str,len);
		c->errstr[len] = '\0';
	} else {
		/* Only RLITE_ERR_IO may lack a description! */
		assert(type == RLITE_ERR_IO);
		strerror_r(errno,c->errstr,sizeof(c->errstr));
	}
}
/* Free a reply object */
void freeReplyObject(void *reply) {
	rliteReply *r = reply;
	size_t j;

	if (r == NULL)
		return;

	switch(r->type) {
	case RLITE_REPLY_INTEGER:
		break; /* Nothing to free */
	case RLITE_REPLY_ARRAY:
		if (r->element != NULL) {
			for (j = 0; j < r->elements; j++)
				if (r->element[j] != NULL)
					freeReplyObject(r->element[j]);
			free(r->element);
		}
		break;
	case RLITE_REPLY_ERROR:
	case RLITE_REPLY_STATUS:
	case RLITE_REPLY_STRING:
		if (r->str != NULL)
			free(r->str);
		break;
	}
	free(r);
}
int rlitevFormatCommand(char **UNUSED(target), const char *UNUSED(format), va_list UNUSED(ap)) { return 1; }
int rliteFormatCommand(char **UNUSED(target), const char *UNUSED(format), ...) { return 1; }
int rliteFormatCommandArgv(char **UNUSED(target), int UNUSED(argc), const char **UNUSED(argv), const size_t *UNUSED(argvlen)) { return 1; }

#define DEFAULT_REPLIES_SIZE 16
static rliteContext *_rliteConnect(const char *path) {
	rliteContext *context = malloc(sizeof(*context));
	if (!context) {
		return NULL;
	}
	context->replies = malloc(sizeof(rliteReply*) * DEFAULT_REPLIES_SIZE);
	if (!context->replies) {
		free(context);
		context = NULL;
		goto cleanup;
	}
	context->replyPosition = 0;
	context->replyLength = 0;
	context->replyAlloc = DEFAULT_REPLIES_SIZE;
	int retval = rl_open(path, &context->db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	if (retval != RL_OK) {
		free(context);
		context = NULL;
		goto cleanup;
	}
cleanup:
	return context;
}

rliteContext *rliteConnect(const char *ip, int UNUSED(port)) {
	return _rliteConnect(ip);
}

rliteContext *rliteConnectWithTimeout(const char *ip, int UNUSED(port), const struct timeval UNUSED(tv)) {
	return _rliteConnect(ip);
}

rliteContext *rliteConnectNonBlock(const char *ip, int UNUSED(port)) {
	return _rliteConnect(ip);
}

rliteContext *rliteConnectBindNonBlock(const char *ip, int UNUSED(port), const char *UNUSED(source_addr)) {
	return _rliteConnect(ip);
}

rliteContext *rliteConnectUnix(const char *path) {
	return _rliteConnect(path);
}

rliteContext *rliteConnectUnixWithTimeout(const char *path, const struct timeval UNUSED(tv)) {
	return _rliteConnect(path);
}

rliteContext *rliteConnectUnixNonBlock(const char *path) {
	return _rliteConnect(path);
}

rliteContext *rliteConnectFd(int UNUSED(fd)) {
	return NULL;
}
int rliteSetTimeout(rliteContext *UNUSED(c), const struct timeval UNUSED(tv)) {
	return 0;
}

int rliteEnableKeepAlive(rliteContext *UNUSED(c)) {
	return 0;
}
void rliteFree(rliteContext *c) {
	rl_close(c->db);
	int i;
	for (i = c->replyPosition; i < c->replyLength; i++) {
		freeReplyObject(c->replies[i]);
	}
	free(c->replies);
	free(c);
}

int rliteFreeKeepFd(rliteContext *UNUSED(c)) {
	return 0;
}

int rliteBufferRead(rliteContext *UNUSED(c)) {
	return 0;
}

int rliteBufferWrite(rliteContext *UNUSED(c), int *UNUSED(done)) {
	return 0;
}

static void *_popReply(rliteContext *c) {
	if (c->replyPosition < c->replyLength) {
		void *ret;
		ret = c->replies[c->replyPosition];
		c->replyPosition++;
		if (c->replyPosition == c->replyLength) {
			c->replyPosition = c->replyLength = 0;
		}
		return ret;
	} else {
		return NULL;
	}
}

int rliteGetReply(rliteContext *c, void **reply) {
	*reply = _popReply(c);
	return RLITE_OK;
}

int __rliteAppendCommand(rliteContext *c, const char *UNUSED(cmd), size_t UNUSED(len)) {
	if (c->replyPosition == c->replyAlloc) {
		void *tmp;
		c->replyAlloc *= 2;
		tmp = realloc(c->replies, sizeof(rliteReply*) * c->replyAlloc);
		if (!tmp) {
			__rliteSetError(c,RLITE_ERR_OOM,"Out of memory");
			return RLITE_ERR;
		}
		c->replies = tmp;
	}
	c->replies[c->replyPosition] = createReplyObject(RLITE_REPLY_INTEGER);
	c->replies[c->replyPosition]->integer = 1234;
	c->replyLength++;
	return RLITE_OK;
}

int rliteAppendFormattedCommand(rliteContext *c, const char *cmd, size_t len) {
	if (__rliteAppendCommand(c, cmd, len) != RLITE_OK) {
		return RLITE_ERR;
	}

	return RLITE_OK;
}

int rlitevAppendCommand(rliteContext *c, const char *format, va_list ap) {
	char *cmd;
	int len;

	len = rlitevFormatCommand(&cmd,format,ap);
	if (len == -1) {
		__rliteSetError(c,RLITE_ERR_OOM,"Out of memory");
		return RLITE_ERR;
	}

	if (__rliteAppendCommand(c,cmd,len) != RLITE_OK) {
		free(cmd);
		return RLITE_ERR;
	}

	free(cmd);
	return RLITE_OK;
}

int rliteAppendCommand(rliteContext *c, const char *format, ...) {
	va_list ap;
	int ret;

	va_start(ap,format);
	ret = rlitevAppendCommand(c,format,ap);
	va_end(ap);
	return ret;
}

int rliteAppendCommandArgv(rliteContext *c, int argc, const char **argv, const size_t *argvlen) {
	char *cmd;
	int len;

	len = rliteFormatCommandArgv(&cmd,argc,argv,argvlen);
	if (len == -1) {
		__rliteSetError(c,RLITE_ERR_OOM,"Out of memory");
		return RLITE_ERR;
	}

	if (__rliteAppendCommand(c,cmd,len) != RLITE_OK) {
		free(cmd);
		return RLITE_ERR;
	}

	free(cmd);
	return RLITE_OK;
}

void *rlitevCommand(rliteContext *c, const char *format, va_list ap) {
	if (rlitevAppendCommand(c,format,ap) != RLITE_OK)
		return NULL;
	return _popReply(c);
}

void *rliteCommand(rliteContext *c, const char *format, ...) {
	va_list ap;
	void *reply = NULL;
	va_start(ap,format);
	reply = rlitevCommand(c,format,ap);
	va_end(ap);
	return reply;
}

void *rliteCommandArgv(rliteContext *c, int argc, const char **argv, const size_t *argvlen) {
	if (rliteAppendCommandArgv(c,argc,argv,argvlen) != RLITE_OK)
		return NULL;
	return _popReply(c);
}
