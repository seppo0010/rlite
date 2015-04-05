/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2015, Sebastian Waisbrot <seppo0010 at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *	 this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *	 notice, this list of conditions and the following disclaimer in the
 *	 documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *	 to endorse or promote products derived from this software without
 *	 specific prior written permission.
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

#include "rlite.h"
#include "rlite/hirlite.h"
#include "rlite/sha1.h"
#include "rlite/rand.h"
#include "rlite/constants.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <ctype.h>
#include <math.h>

#define RLITE_DEBUG 0
#define RLITE_VERBOSE 1
#define RLITE_NOTICE 2
#define RLITE_WARNING 3
#define RLITE_LOG_RAW (1<<10) /* Modifier to log without timestamp */
#define RLITE_DEFAULT_VERBOSITY RLITE_NOTICE

void sha1hex(char *digest, char *script, size_t len);
static int setScript(rliteClient *c, char *script, long scriptlen) {
	int retval;
	char hash[40];
	sha1hex(hash, script, scriptlen);
	RL_CALL(rl_select_internal, RL_OK, c->context->db, RLITE_INTERNAL_DB_LUA);

	RL_CALL(rl_set, RL_OK, c->context->db, (unsigned char *)hash, 40, (unsigned char *)script, scriptlen, 0, 0);

cleanup:
	rl_select_internal(c->context->db, RLITE_INTERNAL_DB_NO);
	return retval;
}

static int getScript(rliteClient *c, char hash[40], char **script, long *scriptlen) {
	int retval;
	RL_CALL(rl_select_internal, RL_OK, c->context->db, RLITE_INTERNAL_DB_LUA);

	RL_CALL(rl_get, RL_OK, c->context->db, (unsigned char *)hash, 40, (unsigned char **)script, scriptlen);

cleanup:
	rl_select_internal(c->context->db, RLITE_INTERNAL_DB_NO);
	return retval;
}

void rliteLogRaw(int UNUSED(level), const char *UNUSED(fmt), ...) {
	// TODO
}
void rliteLog(int UNUSED(level), const char *UNUSED(fmt), ...) {
	// TODO
}
void rliteToLuaType_Int(lua_State *lua, rliteReply *reply);
void rliteToLuaType_Bulk(lua_State *lua, rliteReply *reply);
void rliteToLuaType_Status(lua_State *lua, rliteReply *reply);
void rliteToLuaType_Error(lua_State *lua, rliteReply *reply);
void rliteToLuaType_MultiBulk(lua_State *lua, rliteReply *reply);
int rlite_math_random (lua_State *L);
int rlite_math_randomseed (lua_State *L);

/* Take a Redis reply in the Redis protocol format and convert it into a
 * Lua type. Thanks to this function, and the introduction of not connected
 * clients, it is trivial to implement the rlite() lua function.
 *
 * Basically we take the arguments, execute the Redis command in the context
 * of a non connected client, then take the generated reply and convert it
 * into a suitable Lua type. With this trick the scripting feature does not
 * need the introduction of a full Redis internals API. Basically the script
 * is like a normal client that bypasses all the slow I/O paths.
 *
 * Note: in this function we do not do any sanity check as the reply is
 * generated by Redis directly. This allows us to go faster.
 * The reply string can be altered during the parsing as it is discarded
 * after the conversion is completed.
 *
 * Errors are returned as a table with a single 'err' field set to the
 * error string.
 */

void rliteToLuaType(lua_State *lua, rliteReply *reply) {
	switch(reply->type) {
	case RLITE_REPLY_INTEGER:
		rliteToLuaType_Int(lua,reply);
		break;
	case RLITE_REPLY_NIL:
		lua_pushboolean(lua,0);
		break;
	case RLITE_REPLY_STRING:
		rliteToLuaType_Bulk(lua,reply);
		break;
	case RLITE_REPLY_STATUS:
		rliteToLuaType_Status(lua,reply);
		break;
	case RLITE_REPLY_ERROR:
		rliteToLuaType_Error(lua,reply);
		break;
	case RLITE_REPLY_ARRAY:
		rliteToLuaType_MultiBulk(lua,reply);
		break;
	}
}

void rliteToLuaType_Int(lua_State *lua, rliteReply *reply) {
	lua_pushnumber(lua,(lua_Number)reply->integer);
}

void rliteToLuaType_Bulk(lua_State *lua, rliteReply *reply) {
	lua_pushlstring(lua, reply->str, reply->len);
}

void rliteToLuaType_Status(lua_State *lua, rliteReply *reply) {
	lua_newtable(lua);
	lua_pushstring(lua,"ok");
	lua_pushlstring(lua, reply->str, reply->len);
	lua_settable(lua,-3);
}

void rliteToLuaType_Error(lua_State *lua, rliteReply *reply) {
	lua_newtable(lua);
	lua_pushstring(lua,"err");
	lua_pushlstring(lua, reply->str, reply->len);
	lua_settable(lua,-3);
}

void rliteToLuaType_MultiBulk(lua_State *lua, rliteReply *reply) {
	size_t j;
	lua_newtable(lua);
	for (j = 0; j < reply->elements; j++) {
		lua_pushnumber(lua,j+1);
		rliteToLuaType(lua,reply->element[j]);
		lua_settable(lua,-3);
	}
}

void luaPushError(lua_State *lua, char *error) {
	lua_Debug dbg;

	lua_newtable(lua);
	lua_pushstring(lua,"err");

	/* Attempt to figure out where this function was called, if possible */
	if(lua_getstack(lua, 1, &dbg) && lua_getinfo(lua, "nSl", &dbg)) {
		// 4 for formatting, 1 for null termination, 35 ought to be enough for an integer
		size_t len = strlen(dbg.source) + strlen(error) + 40;
		char *msg = malloc(sizeof(char) * len);
		snprintf(msg, len, "%s: %d: %s",
			dbg.source, dbg.currentline, error);
		lua_pushstring(lua, msg);
		free(msg);
	} else {
		lua_pushstring(lua, error);
	}
	lua_settable(lua,-3);
}

/* Sort the array currently in the stack. We do this to make the output
 * of commands like KEYS or SMEMBERS something deterministic when called
 * from Lua (to play well with AOf/replication).
 *
 * The array is sorted using table.sort itself, and assuming all the
 * list elements are strings. */
void luaSortArray(lua_State *lua) {
	/* Initial Stack: array */
	lua_getglobal(lua,"table");
	lua_pushstring(lua,"sort");
	lua_gettable(lua,-2);	   /* Stack: array, table, table.sort */
	lua_pushvalue(lua,-3);	  /* Stack: array, table, table.sort, array */
	if (lua_pcall(lua,1,0,0)) {
		/* Stack: array, table, error */

		/* We are not interested in the error, we assume that the problem is
		 * that there are 'false' elements inside the array, so we try
		 * again with a slower function but able to handle this case, that
		 * is: table.sort(table, __rlite__compare_helper) */
		lua_pop(lua,1);			 /* Stack: array, table */
		lua_pushstring(lua,"sort"); /* Stack: array, table, sort */
		lua_gettable(lua,-2);	   /* Stack: array, table, table.sort */
		lua_pushvalue(lua,-3);	  /* Stack: array, table, table.sort, array */
		lua_getglobal(lua,"__rlite__compare_helper");
		/* Stack: array, table, table.sort, array, __rlite__compare_helper */
		lua_call(lua,2,0);
	}
	/* Stack: array (sorted), table */
	lua_pop(lua,1);			 /* Stack: array (sorted) */
}

static lua_State *lua = NULL;
static rliteClient *lua_caller = NULL;
static rliteClient *lua_client = NULL;
static int lua_random_dirty;
static int lua_write_dirty;
static unsigned long long lua_time_start;
static unsigned long long lua_time_limit;
static int lua_timedout;
static int lua_kill;

#define LUA_CMD_OBJCACHE_SIZE 32
#define LUA_CMD_OBJCACHE_MAX_LEN 64
int luaRedisGenericCommand(lua_State *lua, int raise_error) {
	int j, argc = lua_gettop(lua);
	struct rliteCommand *cmd;
	rliteClient *c = lua_client;
	rliteReply *reply;

	/* Cached across calls. */
	static char **argv = NULL;
	static size_t *argvlen = NULL;
	static int argv_size = 0;
	static int inuse = 0;   /* Recursive calls detection. */

	/* By using Lua debug hooks it is possible to trigger a recursive call
	 * to luaRedisGenericCommand(), which normally should never happen.
	 * To make this function reentrant is futile and makes it slower, but
	 * we should at least detect such a misuse, and abort. */
	if (inuse) {
		char *recursion_warning =
			"luaRedisGenericCommand() recursive call detected. "
			"Are you doing funny stuff with Lua debug hooks?";
		rliteLog(RLITE_WARNING,"%s",recursion_warning);
		luaPushError(lua,recursion_warning);
		return 1;
	}
	inuse++;

	/* Require at least one argument */
	if (argc == 0) {
		luaPushError(lua,
			"Please specify at least one argument for rlite.call()");
		inuse--;
		return 1;
	}

	/* Build the arguments vector */
	if (argv_size < argc) {
		argv = realloc(argv, sizeof(char *) * argc);
		argvlen = realloc(argvlen, sizeof(size_t) * argc);
		argv_size = argc;
	}

	for (j = 0; j < argc; j++) {
		char *obj_s;
		size_t obj_len;
		char dbuf[64];

		if (lua_type(lua,j+1) == LUA_TNUMBER) {
			/* We can't use lua_tolstring() for number -> string conversion
			 * since Lua uses a format specifier that loses precision. */
			lua_Number num = lua_tonumber(lua,j+1);

			obj_len = snprintf(dbuf,sizeof(dbuf),"%.17g",(double)num);
			obj_s = dbuf;
		} else {
			obj_s = (char*)lua_tolstring(lua,j+1,&obj_len);
			if (obj_s == NULL) break; /* Not a string. */
		}

		argv[j] = obj_s;
		argvlen[j] = obj_len;
	}

	/* Check if one of the arguments passed by the Lua script
	 * is not a string or an integer (lua_isstring() return true for
	 * integers as well). */
	if (j != argc) {
		j--;
		while (j >= 0) {
			free(argv[j]);
			j--;
		}
		luaPushError(lua,
			"Lua rlite() command arguments must be strings or integers");
		inuse--;
		return 1;
	}

	/* Setup our fake client for command execution */
	c->argvlen = argvlen;
	c->argv = argv;
	c->argc = argc;

	/* Command lookup */
	cmd = rliteLookupCommand(argv[0], argvlen[0]);
	if (!cmd || ((cmd->arity > 0 && cmd->arity != argc) ||
				   (argc < -cmd->arity)))
	{
		if (cmd)
			luaPushError(lua,
				"Wrong number of args calling Redis command From Lua script");
		else
			luaPushError(lua,"Unknown Redis command called from Lua script");
		goto cleanup;
	}

	/* There are commands that are not allowed inside scripts. */
	if (cmd->flags & RLITE_CMD_NOSCRIPT) {
		luaPushError(lua, "This Redis command is not allowed from scripts");
		goto cleanup;
	}

	/* Write commands are forbidden against read-only slaves, or if a
	 * command marked as non-deterministic was already called in the context
	 * of this script. */
	if (cmd->flags & RLITE_CMD_WRITE) {
		if (lua_random_dirty) {
			luaPushError(lua,
				"Write commands not allowed after non deterministic commands");
			goto cleanup;
		}
	}

	if (cmd->flags & RLITE_CMD_RANDOM) lua_random_dirty = 1;
	if (cmd->flags & RLITE_CMD_WRITE) lua_write_dirty = 1;

	/* Run the command */
	rliteAppendCommandClient(c);
	rliteGetReply(c->context, (void **)&reply);

	if (raise_error && reply->type != RLITE_REPLY_ERROR) raise_error = 0;
	rliteToLuaType(lua,reply);
	/* Sort the output array if needed, assuming it is a non-null multi bulk
	 * reply as expected. */
	if ((cmd->flags & RLITE_CMD_SORT_FOR_SCRIPT) &&
		(reply->type == RLITE_REPLY_ARRAY && reply->elements > 0)) {
			luaSortArray(lua);
	}
	rliteFreeReplyObject(reply);
cleanup:
	if (c->argv != argv) {
		free(c->argv);
		argv = NULL;
		argv_size = 0;
	}

	if (raise_error) {
		/* If we are here we should have an error in the stack, in the
		 * form of a table with an "err" field. Extract the string to
		 * return the plain error. */
		lua_pushstring(lua,"err");
		lua_gettable(lua,-2);
		inuse--;
		return lua_error(lua);
	}
	inuse--;
	return 1;
}

int luaRedisCallCommand(lua_State *lua) {
	return luaRedisGenericCommand(lua,1);
}

int luaRedisPCallCommand(lua_State *lua) {
	return luaRedisGenericCommand(lua,0);
}

/* This adds rlite.sha1hex(string) to Lua scripts using the same hashing
 * function used for sha1ing lua scripts. */
int luaRedisSha1hexCommand(lua_State *lua) {
	int argc = lua_gettop(lua);
	char digest[41];
	size_t len;
	char *s;

	if (argc != 1) {
		luaPushError(lua, "wrong number of arguments");
		return 1;
	}

	s = (char*)lua_tolstring(lua,1,&len);
	sha1hex(digest,s,len);
	lua_pushstring(lua,digest);
	return 1;
}

/* Returns a table with a single field 'field' set to the string value
 * passed as argument. This helper function is handy when returning
 * a Redis Protocol error or status reply from Lua:
 *
 * return rlite.error_reply("ERR Some Error")
 * return rlite.status_reply("ERR Some Error")
 */
int luaRedisReturnSingleFieldTable(lua_State *lua, char *field) {
	if (lua_gettop(lua) != 1 || lua_type(lua,-1) != LUA_TSTRING) {
		luaPushError(lua, "wrong number or type of arguments");
		return 1;
	}

	lua_newtable(lua);
	lua_pushstring(lua, field);
	lua_pushvalue(lua, -3);
	lua_settable(lua, -3);
	return 1;
}

int luaRedisErrorReplyCommand(lua_State *lua) {
	return luaRedisReturnSingleFieldTable(lua,"err");
}

int luaRedisStatusReplyCommand(lua_State *lua) {
	return luaRedisReturnSingleFieldTable(lua,"ok");
}

int luaLogCommand(lua_State *lua) {
	int j, argc = lua_gettop(lua);
	int level;
	char *log;
	char **strs;
	size_t *strlens;

	if (argc < 2) {
		luaPushError(lua, "rlite.log() requires two arguments or more.");
		return 1;
	} else if (!lua_isnumber(lua,-argc)) {
		luaPushError(lua, "First argument must be a number (log level).");
		return 1;
	}
	level = lua_tonumber(lua,-argc);
	if (level < RLITE_DEBUG || level > RLITE_WARNING) {
		luaPushError(lua, "Invalid debug level.");
		return 1;
	}

	size_t totalsize = 0;
	strs = malloc(sizeof(char *) * (argc - 1));
	if (!strs) {
		return 1;
	}
	strlens = malloc(sizeof(size_t) * (argc - 1));
	if (!strlens) {
		free(strs);
		return 1;
	}
	strlens = malloc(sizeof(size_t) * (argc - 1));
	for (j = 1; j < argc; j++) {
		strs[j - 1] = (char*)lua_tolstring(lua,(-argc)+j,&strlens[j - 1]);
		totalsize += strlens[j - 1];
	}
	log = malloc(sizeof(char) * (totalsize + 1));
	if (!log) {
		free(strs);
		free(strlens);
		return 1;
	}

	totalsize = 0;
	for (j = 0; j < argc - 1; j++) {
		memcpy(&log[totalsize], strs[j], strlens[j]);
		totalsize += strlens[j];
	}
	log[totalsize] = 0;
	rliteLogRaw(level,log);
	return 0;
}

void luaMaskCountHook(lua_State *lua, lua_Debug *UNUSED(ar)) {
	unsigned long long elapsed;

	elapsed = rl_mstime() - lua_time_start;
	if (elapsed >= lua_time_limit && lua_timedout == 0) {
		rliteLog(RLITE_WARNING,"Lua slow script detected: still in execution after %lld milliseconds. You can try killing the script using the SCRIPT KILL command.",elapsed);
		lua_timedout = 1;
	}
	if (lua_kill) {
		rliteLog(RLITE_WARNING, "Lua script killed by user with SCRIPT KILL.");
		lua_pushstring(lua, "Script killed by user with SCRIPT KILL...");
		lua_error(lua);
	}
}

void luaLoadLib(lua_State *lua, const char *libname, lua_CFunction luafunc) {
  lua_pushcfunction(lua, luafunc);
  lua_pushstring(lua, libname);
  lua_call(lua, 1, 0);
}

LUALIB_API int (luaopen_cjson) (lua_State *L);
LUALIB_API int (luaopen_struct) (lua_State *L);
LUALIB_API int (luaopen_cmsgpack) (lua_State *L);
LUALIB_API int (luaopen_bit) (lua_State *L);

void luaLoadLibraries(lua_State *lua) {
	luaLoadLib(lua, "", luaopen_base);
	luaLoadLib(lua, LUA_TABLIBNAME, luaopen_table);
	luaLoadLib(lua, LUA_STRLIBNAME, luaopen_string);
	luaLoadLib(lua, LUA_MATHLIBNAME, luaopen_math);
	luaLoadLib(lua, LUA_DBLIBNAME, luaopen_debug);
	luaLoadLib(lua, "cjson", luaopen_cjson);
	luaLoadLib(lua, "struct", luaopen_struct);
	luaLoadLib(lua, "cmsgpack", luaopen_cmsgpack);
	luaLoadLib(lua, "bit", luaopen_bit);

#if 0 /* Stuff that we don't load currently, for sandboxing concerns. */
	luaLoadLib(lua, LUA_LOADLIBNAME, luaopen_package);
	luaLoadLib(lua, LUA_OSLIBNAME, luaopen_os);
#endif
}

/* Remove a functions that we don't want to expose to the Redis scripting
 * environment. */
void luaRemoveUnsupportedFunctions(lua_State *lua) {
	lua_pushnil(lua);
	lua_setglobal(lua,"loadfile");
}

/* This function installs metamethods in the global table _G that prevent
 * the creation of globals accidentally.
 *
 * It should be the last to be called in the scripting engine initialization
 * sequence, because it may interact with creation of globals. */
void scriptingEnableGlobalsProtection(lua_State *lua) {
	/* strict.lua from: http://metalua.luaforge.net/src/lib/strict.lua.html.
	 * Modified to be adapted to Redis. */
	char *code =""
	"local mt = {}\n"
	"setmetatable(_G, mt)\n"
	"mt.__newindex = function (t, n, v)\n"
	"  if debug.getinfo(2) then\n"
	"	local w = debug.getinfo(2, \"S\").what\n"
	"	if w ~= \"main\" and w ~= \"C\" then\n"
	"	  error(\"Script attempted to create global variable '\"..tostring(n)..\"'\", 2)\n"
	"	end\n"
	"  end\n"
	"  rawset(t, n, v)\n"
	"end\n"
	"mt.__index = function (t, n)\n"
	"  if debug.getinfo(2) and debug.getinfo(2, \"S\").what ~= \"C\" then\n"
	"	error(\"Script attempted to access unexisting global variable '\"..tostring(n)..\"'\", 2)\n"
	"  end\n"
	"  return rawget(t, n)\n"
	"end\n";

	luaL_loadbuffer(lua,code,strlen(code),"@enable_strict_lua");
	lua_pcall(lua,0,0,0);
}

/* Initialize the scripting environment.
 * It is possible to call this function to reset the scripting environment
 * assuming that we call scriptingRelease() before.
 * See scriptingReset() for more information. */
void scriptingInit(void) {
	if (lua) {
		return;
	}
	lua = lua_open();

	luaLoadLibraries(lua);
	luaRemoveUnsupportedFunctions(lua);

	/* Initialize a dictionary we use to map SHAs to scripts.
	 * This is useful for replication, as we need to replicate EVALSHA
	 * as EVAL, so we need to remember the associated script. */

	/* Register the rlite commands table and fields */
	lua_newtable(lua);

	/* rlite.call */
	lua_pushstring(lua,"call");
	lua_pushcfunction(lua,luaRedisCallCommand);
	lua_settable(lua,-3);

	/* rlite.pcall */
	lua_pushstring(lua,"pcall");
	lua_pushcfunction(lua,luaRedisPCallCommand);
	lua_settable(lua,-3);

	/* rlite.log and log levels. */
	lua_pushstring(lua,"log");
	lua_pushcfunction(lua,luaLogCommand);
	lua_settable(lua,-3);

	lua_pushstring(lua,"LOG_DEBUG");
	lua_pushnumber(lua,RLITE_DEBUG);
	lua_settable(lua,-3);

	lua_pushstring(lua,"LOG_VERBOSE");
	lua_pushnumber(lua,RLITE_VERBOSE);
	lua_settable(lua,-3);

	lua_pushstring(lua,"LOG_NOTICE");
	lua_pushnumber(lua,RLITE_NOTICE);
	lua_settable(lua,-3);

	lua_pushstring(lua,"LOG_WARNING");
	lua_pushnumber(lua,RLITE_WARNING);
	lua_settable(lua,-3);

	/* rlite.sha1hex */
	lua_pushstring(lua, "sha1hex");
	lua_pushcfunction(lua, luaRedisSha1hexCommand);
	lua_settable(lua, -3);

	/* rlite.error_reply and rlite.status_reply */
	lua_pushstring(lua, "error_reply");
	lua_pushcfunction(lua, luaRedisErrorReplyCommand);
	lua_settable(lua, -3);
	lua_pushstring(lua, "status_reply");
	lua_pushcfunction(lua, luaRedisStatusReplyCommand);
	lua_settable(lua, -3);

	/* Finally set the table as 'redis' global var. */
	lua_setglobal(lua,"redis");

	/* Replace math.random and math.randomseed with our implementations. */
	lua_getglobal(lua,"math");

	lua_pushstring(lua,"random");
	lua_pushcfunction(lua,rlite_math_random);
	lua_settable(lua,-3);

	lua_pushstring(lua,"randomseed");
	lua_pushcfunction(lua,rlite_math_randomseed);
	lua_settable(lua,-3);

	lua_setglobal(lua,"math");

	/* Add a helper function that we use to sort the multi bulk output of non
	 * deterministic commands, when containing 'false' elements. */
	{
		char *compare_func =	"function __rlite__compare_helper(a,b)\n"
								"  if a == false then a = '' end\n"
								"  if b == false then b = '' end\n"
								"  return a<b\n"
								"end\n";
		luaL_loadbuffer(lua,compare_func,strlen(compare_func),"@cmp_func_def");
		lua_pcall(lua,0,0,0);
	}

	/* Add a helper function we use for pcall error reporting.
	 * Note that when the error is in the C function we want to report the
	 * information about the caller, that's what makes sense from the point
	 * of view of the user debugging a script. */
	{
		char *errh_func =	   "function __rlite__err__handler(err)\n"
								"  local i = debug.getinfo(2,'nSl')\n"
								"  if i and i.what == 'C' then\n"
								"	i = debug.getinfo(3,'nSl')\n"
								"  end\n"
								"  if i then\n"
								"	return i.source .. ':' .. i.currentline .. ': ' .. err\n"
								"  else\n"
								"	return err\n"
								"  end\n"
								"end\n";
		luaL_loadbuffer(lua,errh_func,strlen(errh_func),"@err_handler_def");
		lua_pcall(lua,0,0,0);
	}

	/* Create the (non connected) client that we use to execute Redis commands
	 * inside the Lua interpreter.
	 * Note: there is no need to create it again when this function is called
	 * by scriptingReset(). */
	if (lua_client == NULL) {
		lua_client = malloc(sizeof(*lua_client));

		lua_client->flags = RLITE_LUA_CLIENT;
	}

	/* Lua beginners often don't use "local", this is likely to introduce
	 * subtle bugs in their code. To prevent problems we protect accesses
	 * to global variables. */
	scriptingEnableGlobalsProtection(lua);
}

/* Release resources related to Lua scripting.
 * This function is used in order to reset the scripting environment. */
void scriptingRelease(void) {
	lua_close(lua);
}

void scriptingReset(void) {
	scriptingRelease();
	scriptingInit();
}

/* Perform the SHA1 of the input string. We use this both for hashing script
 * bodies in order to obtain the Lua function name, and in the implementation
 * of rlite.sha1().
 *
 * 'digest' should point to a 41 bytes buffer: 40 for SHA1 converted into an
 * hexadecimal number, plus 1 byte for null term. */
void sha1hex(char *digest, char *script, size_t len) {
	SHA1_CTX ctx;
	unsigned char hash[20];
	char *cset = "0123456789abcdef";
	int j;

	SHA1Init(&ctx);
	SHA1Update(&ctx,(unsigned char*)script,len);
	SHA1Final(hash,&ctx);

	for (j = 0; j < 20; j++) {
		digest[j*2] = cset[((hash[j]&0xF0)>>4)];
		digest[j*2+1] = cset[(hash[j]&0xF)];
	}
	digest[40] = '\0';
}

rliteReply *luaReplyToStringReply(int type) {
	rliteReply* reply;
	const char *_err = lua_tostring(lua,-1);
	size_t i, len = strlen(_err);
	char *err = malloc(sizeof(char) * (len + 1));
	if (!err) {
		return NULL;
	}
	for (i = 0; i < len; i++) {
		if (_err[i] == '\r' || _err[i] == '\n') err[i] = ' ';
		else err[i] = _err[i];
	}
	err[len] = '\0';
	reply = createStringTypeObject(type, err, len);
	free(err);
	return reply;
}
void luaReplyToRedisReply(rliteClient *c, lua_State *lua) {
	int t = lua_type(lua,-1);

	switch(t) {
	case LUA_TSTRING:
		c->reply = createStringObject((char*)lua_tostring(lua,-1),lua_strlen(lua,-1));
		break;
	case LUA_TBOOLEAN:
		if (lua_toboolean(lua,-1)) {
			c->reply = createLongLongObject(1);
		} else {
			c->reply = createNullReplyObject();
		}
		break;
	case LUA_TNUMBER:
		c->reply = createLongLongObject((long long)lua_tonumber(lua,-1));
		break;
	case LUA_TTABLE:
		/* We need to check if it is an array, an error, or a status reply.
		 * Error are returned as a single element table with 'err' field.
		 * Status replies are returned as single element table with 'ok' field */
		lua_pushstring(lua,"err");
		lua_gettable(lua,-2);
		t = lua_type(lua,-1);
		if (t == LUA_TSTRING) {
			c->reply = luaReplyToStringReply(RLITE_REPLY_ERROR);
			lua_pop(lua,2);
			return;
		}

		lua_pop(lua,1);
		lua_pushstring(lua,"ok");
		lua_gettable(lua,-2);
		t = lua_type(lua,-1);
		if (t == LUA_TSTRING) {
			c->reply = luaReplyToStringReply(RLITE_REPLY_STATUS);
			lua_pop(lua,1);
		} else {
			rliteReply *reply = createArrayObject(2);
			void *tmp;
			size_t j = 1;

			lua_pop(lua,1); /* Discard the 'ok' field value we popped */
			while(1) {
				lua_pushnumber(lua,j++);
				lua_gettable(lua,-2);
				t = lua_type(lua,-1);
				if (t == LUA_TNIL) {
					lua_pop(lua,1);
					break;
				}
				luaReplyToRedisReply(c, lua);
				if (j - 2 == reply->elements) {
					tmp = realloc(reply->element, sizeof(rliteReply *) * reply->elements * 2);
					if (!tmp) {
						// TODO: free stuff, panic
						c->reply = NULL;
						return;
					}
					reply->element = tmp;
				}
				reply->element[j - 2] = c->reply;
			}
			// TODO: shrink reply->element
			reply->elements = j - 2;
			c->reply = reply;
		}
		break;
	default:
		c->reply = createNullReplyObject();
	}
	lua_pop(lua,1);
}

/* Set an array of Redis String Objects as a Lua array (table) stored into a
 * global variable. */
void luaSetGlobalArray(lua_State *lua, char *var, char **elev, size_t *elevlen, int elec) {
	int j;

	lua_newtable(lua);
	for (j = 0; j < elec; j++) {
		lua_pushlstring(lua,elev[j],elevlen[j]);
		lua_rawseti(lua,-2,j+1);
	}
	lua_setglobal(lua,var);
}

/* Define a lua function with the specified function name and body.
 * The function name musts be a 2 characters long string, since all the
 * functions we defined in the Lua context are in the form:
 *
 *   f_<hex sha1 sum>
 *
 * On success RLITE_OK is returned, and nothing is left on the Lua stack.
 * On error RLITE_ERR is returned and an appropriate error is set in the
 * client context. */
int luaCreateFunction(rliteClient *c, lua_State *lua, char *funcname, char *body, size_t bodylen) {
	const char *f = "function ";
	const char *params = "() ";
	const char *end = " end";
	size_t funcnamelen = 42;
	size_t funcdeflen = bodylen + funcnamelen + strlen(f) + strlen(params) + strlen(end) + 1;
	char *funcdef = malloc(sizeof(char) * funcdeflen);


	strcpy(funcdef, f);
	funcdeflen = strlen(f);

	memcpy(&funcdef[funcdeflen], funcname, funcnamelen);
	funcdeflen += funcnamelen;

	strcpy(&funcdef[funcdeflen], params);
	funcdeflen += strlen(params);

	memcpy(&funcdef[funcdeflen], body, bodylen);
	funcdeflen += bodylen;

	strcpy(&funcdef[funcdeflen], end);
	funcdeflen += strlen(end);

	funcdef[funcdeflen] = 0;

	if (luaL_loadbuffer(lua,funcdef,funcdeflen,"@user_script")) {
		char err[1024];
		snprintf(err, 1024, "ERR Error compiling script (new function): %s",
			lua_tostring(lua,-1));
		c->reply = createErrorObject(err);
		lua_pop(lua,1);
		free(funcdef);
		return RLITE_ERR;
	}
	free(funcdef);
	if (lua_pcall(lua,0,0,0)) {
		char err[1024];
		snprintf(err, 1024, "ERR Error running script (new function): %s",
			lua_tostring(lua,-1));
		c->reply = createErrorObject(err);
		lua_tostring(lua,-1);
		lua_pop(lua,1);
		return RLITE_ERR;
	}

	/* We also save a SHA1 -> Original script map in a dictionary
	 * so that we can replicate / write in the AOF all the
	 * EVALSHA commands as EVAL using the original script. */
	{
		int retval = setScript(c, body, bodylen);
		if (retval != RL_OK) {
			return retval;
		}
	}
	return RLITE_OK;
}

void evalGenericCommand(rliteClient *c, int evalsha) {
	scriptingInit();
	lua_client->context = c->context;
	char funcname[43];
	long long numkeys;
	int delhook = 0, err;

	/* We want the same PRNG sequence at every call so that our PRNG is
	 * not affected by external state. */
	rliteSrand48(0);

	/* We set this flag to zero to remember that so far no random command
	 * was called. This way we can allow the user to call commands like
	 * SRANDMEMBER or RANDOMKEY from Lua scripts as far as no write command
	 * is called (otherwise the replication and AOF would end with non
	 * deterministic sequences).
	 *
	 * Thanks to this flag we'll raise an error every time a write command
	 * is called after a random command was used. */
	lua_random_dirty = 0;
	lua_write_dirty = 0;

	/* Get the number of arguments that are keys */
	if (getLongLongFromObjectOrReply(c,c->argv[2],c->argvlen[2],&numkeys,NULL) != RLITE_OK)
		return;
	if (numkeys > (c->argc - 3)) {
		c->reply = createErrorObject("Number of keys can't be greater than number of args");
		return;
	} else if (numkeys < 0) {
		c->reply = createErrorObject("Number of keys can't be negative");
		return;
	}

	/* We obtain the script SHA1, then check if this function is already
	 * defined into the Lua state */
	funcname[0] = 'f';
	funcname[1] = '_';
	if (!evalsha) {
		/* Hash the code if this is an EVAL call */
		sha1hex(funcname+2,c->argv[1],c->argvlen[1]);
	} else {
		/* We already have the SHA if it is a EVALSHA */
		int j;
		char *sha = c->argv[1];

		/* Convert to lowercase. We don't use tolower since the function
		 * managed to always show up in the profiler output consuming
		 * a non trivial amount of time. */
		for (j = 0; j < 40; j++)
			funcname[j+2] = (sha[j] >= 'A' && sha[j] <= 'Z') ?
				sha[j]+('a'-'A') : sha[j];
		funcname[42] = '\0';

		char *body;
		long bodylen;
		int retval = getScript(c, funcname + 2, &body, &bodylen);
		if (retval != RL_OK) {
			c->reply = createErrorObject(RLITE_NOSCRIPTERR);
			return;
		}
		luaCreateFunction(c, lua, funcname, body, bodylen);
		rl_free(body);
	}

	/* Push the pcall error handler function on the stack. */
	lua_getglobal(lua, "__rlite__err__handler");

	/* Try to lookup the Lua function */
	lua_getglobal(lua, funcname);
	if (lua_isnil(lua,-1)) {
		lua_pop(lua,1); /* remove the nil from the stack */
		/* Function not defined... let's define it if we have the
		 * body of the function. If this is an EVALSHA call we can just
		 * return an error. */
		if (evalsha) {
			lua_pop(lua,1); /* remove the error handler from the stack. */
			c->reply = createErrorObject(RLITE_NOSCRIPTERR);
			return;
		}
		if (luaCreateFunction(c,lua,funcname,c->argv[1], c->argvlen[1]) == RLITE_ERR) {
			lua_pop(lua,1); /* remove the error handler from the stack. */
			/* The error is sent to the client by luaCreateFunction()
			 * itself when it returns RLITE_ERR. */
			return;
		}
		/* Now the following is guaranteed to return non nil */
		lua_getglobal(lua, funcname);
		if (lua_isnil(lua,-1)) {
			// TODO: panic
			return;
		}
	}

	/* Populate the argv and keys table accordingly to the arguments that
	 * EVAL received. */
	luaSetGlobalArray(lua,"KEYS",c->argv+3,c->argvlen+3,numkeys);
	luaSetGlobalArray(lua,"ARGV",c->argv+3+numkeys,c->argvlen+3+numkeys,c->argc-3-numkeys);

	/* Set a hook in order to be able to stop the script execution if it
	 * is running for too much time.
	 * We set the hook only if the time limit is enabled as the hook will
	 * make the Lua script execution slower. */
	lua_caller = c;
	lua_time_start = rl_mstime();
	lua_kill = 0;
	if (lua_time_limit > 0) {
		lua_sethook(lua,luaMaskCountHook,LUA_MASKCOUNT,100000);
		delhook = 1;
	}

	/* At this point whether this script was never seen before or if it was
	 * already defined, we can call it. We have zero arguments and expect
	 * a single return value. */
	err = lua_pcall(lua,0,1,-2);

	/* Perform some cleanup that we need to do both on error and success. */
	if (delhook) lua_sethook(lua,luaMaskCountHook,0,0); /* Disable hook */
	if (lua_timedout) {
		lua_timedout = 0;
		/* Restore the readable handler that was unregistered when the
		 * script timeout was detected. */
	}
	lua_caller = NULL;

	/* Call the Lua garbage collector from time to time to avoid a
	 * full cycle performed by Lua, which adds too latency.
	 *
	 * The call is performed every LUA_GC_CYCLE_PERIOD executed commands
	 * (and for LUA_GC_CYCLE_PERIOD collection steps) because calling it
	 * for every command uses too much CPU. */
	#define LUA_GC_CYCLE_PERIOD 50
	{
		static long gc_count = 0;

		gc_count++;
		if (gc_count == LUA_GC_CYCLE_PERIOD) {
			lua_gc(lua,LUA_GCSTEP,LUA_GC_CYCLE_PERIOD);
			gc_count = 0;
		}
	}

	if (err) {
		char err[1024];
		snprintf(err, 1024, "Error running script (call to %s): %s\n",
			funcname, lua_tostring(lua,-1));
		c->reply = createErrorObject(err);
		lua_pop(lua,2); /* Consume the Lua reply and remove error handler. */
	} else {
		/* On success convert the Lua return value into Redis protocol, and
		 * send it to * the client. */
		luaReplyToRedisReply(c,lua); /* Convert and consume the reply. */
		lua_pop(lua,1); /* Remove the error handler. */
	}
}

void evalCommand(rliteClient *c) {
	evalGenericCommand(c,0);
}

void evalShaCommand(rliteClient *c) {
	if (c->argvlen[1] != 40) {
		/* We know that a match is not possible if the provided SHA is
		 * not the right length. So we return an error ASAP, this way
		 * evalGenericCommand() can be implemented without string length
		 * sanity check */
		c->reply = createErrorObject(RLITE_NOSCRIPTERR);
		return;
	}
	evalGenericCommand(c,1);
}

/* We replace math.random() with our implementation that is not affected
 * by specific libc random() implementations and will output the same sequence
 * (for the same seed) in every arch. */

/* The following implementation is the one shipped with Lua itself but with
 * rand() replaced by rliteLrand48(). */
int rlite_math_random (lua_State *L) {
  /* the `%' avoids the (rare) case of r==1, and is needed also because on
	 some systems (SunOS!) `rand()' may return a value larger than RAND_MAX */
  lua_Number r = (lua_Number)(rliteLrand48()%RLITE_LRAND48_MAX) /
								(lua_Number)RLITE_LRAND48_MAX;
  switch (lua_gettop(L)) {  /* check number of arguments */
	case 0: {  /* no arguments */
	  lua_pushnumber(L, r);  /* Number between 0 and 1 */
	  break;
	}
	case 1: {  /* only upper limit */
	  int u = luaL_checkint(L, 1);
	  luaL_argcheck(L, 1<=u, 1, "interval is empty");
	  lua_pushnumber(L, floor(r*u)+1);  /* int between 1 and `u' */
	  break;
	}
	case 2: {  /* lower and upper limits */
	  int l = luaL_checkint(L, 1);
	  int u = luaL_checkint(L, 2);
	  luaL_argcheck(L, l<=u, 2, "interval is empty");
	  lua_pushnumber(L, floor(r*(u-l+1))+l);  /* int between `l' and `u' */
	  break;
	}
	default: return luaL_error(L, "wrong number of arguments");
  }
  return 1;
}

int rlite_math_randomseed (lua_State *L) {
  rliteSrand48(luaL_checkint(L, 1));
  return 0;
}

/* ---------------------------------------------------------------------------
 * SCRIPT command for script environment introspection and control
 * ------------------------------------------------------------------------- */

void scriptCommand(rliteClient *c) {
	if (c->argc == 2 && !strcasecmp(c->argv[1],"flush")) {
		scriptingReset();
		c->reply = createStatusObject(RLITE_STR_OK);
	} else if (c->argc >= 2 && !strcasecmp(c->argv[1],"exists")) {
		int j;

		c->reply = createArrayObject(c->argc - 2);
		for (j = 2; j < c->argc; j++) {
			c->reply->element[j - 2] = createLongLongObject(getScript(c, c->argv[j], NULL, NULL) == RL_OK ? 1 : 0);
		}
	} else if (c->argc == 3 && !strcasecmp(c->argv[1],"load")) {
		char sha[41];

		sha1hex(sha,c->argv[2],c->argvlen[2]);
		setScript(c, c->argv[2], c->argvlen[2]);
		c->reply = createStringObject(sha,40);
	} else if (c->argc == 2 && !strcasecmp(c->argv[1],"kill")) {
		if (lua_caller == NULL) {
			c->reply = createCStringObject("NOTBUSY No scripts in execution right now.\r\n");
		} else if (lua_write_dirty) {
			c->reply = createCStringObject("UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.\r\n");
		} else {
			lua_kill = 1;
			c->reply = createStatusObject(RLITE_STR_OK);
		}
	} else {
		c->reply = createErrorObject("ERR Unknown SCRIPT subcommand or wrong # of args.");
	}
}
