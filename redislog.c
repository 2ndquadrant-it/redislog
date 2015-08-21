/*-------------------------------------------------------------------------
 *
 * redislog.c
 *
 *    Extension that allows PostgreSQL to send log entries to a
 *    Redis server directly in JSON format.
 *    Requires the Hiredis library (https://github.com/redis/hiredis)
 *
 *    One of the goals of redislog is to allow administrators
 *    to tap PostgreSQL directly into the Logstash pipeline
 *    for real-time monitoring, by acting as a "Shipper" component
 *    that sends events to a "Broker", such as Redis.
 *
 * Copyright (c) 1996-2015, PostgreSQL Global Development Group
 *
 * Authors:
 *   Marco Nenciarini <marco.nenciarini@2ndquadrant.it>
 *   Gabriele Bartolini <gabriele.bartolini@2ndquadrant.it>
 *
 * Partially based on jsonlog by Michael Paquier
 * https://github.com/michaelpq/pg_plugins/blob/master/jsonlog/jsonlog.c
 *
 * IDENTIFICATION
 *		redislog/redislog.c
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>
#include <sys/time.h>

#include "postgres.h"
#include "libpq/libpq.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "access/transam.h"
#include "lib/stringinfo.h"
#include "postmaster/syslogger.h"
#include "storage/proc.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/json.h"

#include "hiredis/hiredis.h"

/* Allow load of this module in shared libs */
PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

/* Hold previous logging hook */
static emit_log_hook_type prev_log_hook = NULL;

/* GUC Variables */
char  *Redislog_host = NULL;
int   Redislog_port = 6379;
int   Redislog_timeout = 1000;
char  *Redislog_key = NULL;

/* Log timestamp */
#define LOG_TIMESTAMP_LEN 128
static char log_time[LOG_TIMESTAMP_LEN];

/* Redis context */
static redisContext *redis_context = NULL;

/* Used to detect if values inherited over fork need resetting. */
static int lastPid = 0;

/* String mapper for error severity */
static const char *error_severity(int elevel);

/* Configuration options management */
static void guc_on_assign_reopen_string(const char *newval, void *extra);
static void guc_on_assign_reopen_int(int newval, void *extra);

/* Redis specific prototypes */
static void redis_log_hook(ErrorData *edata);
static void redis_close_connection(void);
static bool redis_open_connection(void);
static bool redis_log_shipper(char *data, int len);

/*
 * Useful for HUP triggered host reassignment: close the connection, a new one
 * will be opened on next event.
 */
static void
guc_on_assign_reopen_string(const char *newval, void *extra)
{
	redis_close_connection();
}

/*
 * Useful for HUP triggered port reassignment: close the connection, a new one
 * will be opened on next event.
 */
static void
guc_on_assign_reopen_int(int newval, void *extra)
{
	redis_close_connection();
}


/*
 * error_severity
 * Print string showing error severity based on integer level.
 * Taken from elog.c.
 */
static const char *
error_severity(int elevel)
{
	const char *prefix;

	switch (elevel)
	{
		case DEBUG1:
		case DEBUG2:
		case DEBUG3:
		case DEBUG4:
		case DEBUG5:
			prefix = _("DEBUG");
			break;
		case LOG:
		case COMMERROR:
			prefix = _("LOG");
			break;
		case INFO:
			prefix = _("INFO");
			break;
		case NOTICE:
			prefix = _("NOTICE");
			break;
		case WARNING:
			prefix = _("WARNING");
			break;
		case ERROR:
			prefix = _("ERROR");
			break;
		case FATAL:
			prefix = _("FATAL");
			break;
		case PANIC:
			prefix = _("PANIC");
			break;
		default:
			prefix = "???";
			break;
	}

	return prefix;
}

/*
 * redis_close_connection
 * Close the remote Redis connection.
 */
static void
redis_close_connection()
{
	if (redis_context)
		redisFree(redis_context);
	redis_context = NULL;
}

/*
 * redis_open_connection
 * Connect to remote Redis server, returns false on failure.
 */
static bool
redis_open_connection()
{
	struct timeval	timeout;

	if (!redis_context)
	{
		timeout.tv_sec = Redislog_timeout / 1000;
		timeout.tv_usec = Redislog_timeout % 1000 * 1000;
		redis_context = redisConnectWithTimeout(Redislog_host, Redislog_port, timeout);

		if (redis_context == NULL || redis_context->err)
		{
			/*
			 * Something went wrong.
			 */
			redis_close_connection();
			return false;
		}
	}
	return true;
}


/*
 * redis_log_shipper
 * Ship log events to Redis. In case of network issues, retry once.
 */
static bool
redis_log_shipper(char *data, int len)
{
	redisReply *reply;
	unsigned attempts = 0;

	Assert(len > 0);

	while(attempts <= 1)
	{
		if (!redis_open_connection())
		{
			/*
			 * Connection failed. This message will not be sent.
			 */
			return false;
		}

		/* Push the event using binary safe API */
		reply = redisCommand(redis_context, "RPUSH %s %b", Redislog_key, data, (size_t) len);
		if (reply != NULL || !redis_context->err)
		{
			/* The event have been sent correctly, so we are done */
			freeReplyObject(reply);
			return true;
		}
		/* something occurred, close the connection and try again once */
		attempts++;

		/* Frees the reply object in Redis */
		if (reply)
			freeReplyObject(reply);

		/* Close the Redis connection */
		redis_close_connection();
	}
	return false;
}

/*
 * setup_formatted_log_time
 * (taken from jsonlog.c)
 */
static void
setup_formatted_log_time(void)
{
	struct timeval tv;
	pg_time_t stamp_time;
	char msbuf[8];

	gettimeofday(&tv, NULL);
	stamp_time = (pg_time_t) tv.tv_sec;

	/*
	 * Note: we expect that guc.c will ensure that log_timezone is set up (at
	 * least with a minimal GMT value) before Log_line_prefix can become
	 * nonempty or CSV mode can be selected.
	 */
	pg_strftime(log_time, LOG_TIMESTAMP_LEN,
				/* leave room for milliseconds... */
				"%Y-%m-%dT%H:%M:%S    %z",
				pg_localtime(&stamp_time, log_timezone));

	/* 'paste' milliseconds into place... */
	sprintf(msbuf, ".%03d", (int) (tv.tv_usec / 1000));
	strncpy(log_time + 19, msbuf, 4);
}

/*
 * append_json_literal
 * Append to given StringInfo a JSON with a given key and a value
 * not yet made literal.
 * (taken from jsonlog.c)
 */
static void
append_json_literal(StringInfo buf, char *key, char *value, bool is_comma)
{
	StringInfoData literal_json;

	initStringInfo(&literal_json);
	Assert(key && value);

	/*
	 * Call in-core function able to generate wanted strings, there is
	 * no need to reinvent the wheel.
	 */
	escape_json(&literal_json, value);

	/* Now append the field */
	appendStringInfo(buf, "\"%s\":%s", key, literal_json.data);

	/* Add comma if necessary */
	if (is_comma)
		appendStringInfoChar(buf, ',');

	/* Clean up */
	pfree(literal_json.data);
}

/*
 * redis_log_hook
 * Hook for shipping log events to Redis
 * (based on jsonlog.c)
 */
static void
redis_log_hook(ErrorData *edata)
{
	StringInfoData	buf;
	TransactionId	txid = GetTopTransactionIdIfAny();

	/*
	 * This is one of the few places where we'd rather not inherit a static
	 * variable's value from the postmaster.  But since we will, reset it when
	 * MyProcPid changes.
	 */
	if (lastPid != MyProcPid)
	{
		lastPid = MyProcPid;
		redis_close_connection();
	}

	initStringInfo(&buf);

	/* Initialize string */
	appendStringInfoChar(&buf, '{');

	/* Timestamp */
	if (log_time[0] == '\0')
		setup_formatted_log_time();
	append_json_literal(&buf, "@timestamp", log_time, true);

	/* Username */
	if (MyProcPort)
		append_json_literal(&buf, "user", MyProcPort->user_name, true);

	/* Database name */
	if (MyProcPort)
		append_json_literal(&buf, "dbname", MyProcPort->database_name, true);

	/* Process ID */
	if (MyProcPid != 0)
		appendStringInfo(&buf, "\"pid\":%d,", MyProcPid);

	/* Remote host and port */
	if (MyProcPort && MyProcPort->remote_host)
	{
		append_json_literal(&buf, "remote_host",
						  MyProcPort->remote_host, true);
		if (MyProcPort->remote_port && MyProcPort->remote_port[0] != '\0')
			append_json_literal(&buf, "remote_port",
							  MyProcPort->remote_port, true);
	}

	/* Session id */
	if (MyProcPid != 0)
		appendStringInfo(&buf, "\"session_id\":\"%lx.%x\",",
						 (long) MyStartTime, MyProcPid);

	/* Virtual transaction id */
	/* keep VXID format in sync with lockfuncs.c */
	if (MyProc != NULL && MyProc->backendId != InvalidBackendId)
		appendStringInfo(&buf, "\"vxid\":\"%d/%u\",",
						 MyProc->backendId, MyProc->lxid);

	/* Transaction id */
	if (txid != InvalidTransactionId)
		appendStringInfo(&buf, "\"txid\":%u,", GetTopTransactionIdIfAny());

	/* Error severity */
	append_json_literal(&buf, "error_severity",
					  (char *) error_severity(edata->elevel), true);

	/* SQL state code */
	if (edata->sqlerrcode != ERRCODE_SUCCESSFUL_COMPLETION)
		append_json_literal(&buf, "state_code",
						  unpack_sql_state(edata->sqlerrcode), true);

	/* Error detail or Error detail log */
	if (edata->detail_log)
		append_json_literal(&buf, "detail_log", edata->detail_log, true);
	else if (edata->detail)
		append_json_literal(&buf, "detail", edata->detail, true);

	/* Error hint */
	if (edata->hint)
		append_json_literal(&buf, "hint", edata->hint, true);

	/* Internal query */
	if (edata->internalquery)
		append_json_literal(&buf, "internal_query",
						  edata->internalquery, true);

	/* Error context */
	if (edata->context)
		append_json_literal(&buf, "context", edata->context, true);

	/* File error location */
	if (Log_error_verbosity >= PGERROR_VERBOSE)
	{
		StringInfoData msgbuf;

		initStringInfo(&msgbuf);

		if (edata->funcname && edata->filename)
			appendStringInfo(&msgbuf, "%s, %s:%d",
							 edata->funcname, edata->filename,
							 edata->lineno);
		else if (edata->filename)
			appendStringInfo(&msgbuf, "%s:%d",
							 edata->filename, edata->lineno);
		append_json_literal(&buf, "file_location", msgbuf.data, true);
		pfree(msgbuf.data);
	}

	/* Application name */
	if (application_name && application_name[0] != '\0')
		append_json_literal(&buf, "application_name",
						  application_name, true);

	/* Error message */
	append_json_literal(&buf, "message", edata->message, false);

	/* Finish string */
	appendStringInfoChar(&buf, '}');
	appendStringInfoChar(&buf, '\n');

	/* Send the data to Redis */
	redis_log_shipper(buf.data, buf.len);

	/* Cleanup */
	pfree(buf.data);

	/* Continue chain to previous hook */
	if (prev_log_hook)
		(*prev_log_hook) (edata);
}

/*
 * _PG_init
 * Entry point loading hooks
 */
void
_PG_init(void)
{
	/* Set up GUCs */
	DefineCustomStringVariable("redislog.host",
	  "Redis server host name or IP address.",
	  NULL,
	  &Redislog_host,
	  "127.0.0.1",
	  PGC_SIGHUP,
	  GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY,
	  NULL,
	  &guc_on_assign_reopen_string,
	  NULL);

	DefineCustomIntVariable("redislog.port",
	  "Redis server port number.",
	  NULL,
	  &Redislog_port,
	  6379,
	  0,
	  65535,
	  PGC_SIGHUP,
	  GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY,
	  NULL,
	  &guc_on_assign_reopen_int,
	  NULL);

	DefineCustomIntVariable("redislog.connection_timeout",
	  "Redis server connection timeout.",
	  NULL,
	  &Redislog_timeout,
	  1000,
	  1,
	  INT_MAX,
	  PGC_SIGHUP,
	  GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY | GUC_UNIT_MS,
	  NULL,
	  NULL,
	  NULL);

	DefineCustomStringVariable("redislog.key",
	  "Redis server key name.",
	  NULL,
	  &Redislog_key,
	  "postgres",
	  PGC_SIGHUP,
	  GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY,
	  NULL,
	  NULL,
	  NULL);

	prev_log_hook = emit_log_hook;
	emit_log_hook = redis_log_hook;

	EmitWarningsOnPlaceholders("redislog");
}

/*
 * _PG_fini
 * Exit point unloading hooks
 */
void
_PG_fini(void)
{
	emit_log_hook = prev_log_hook;
	if (redis_context)
		redisFree(redis_context);
	redis_context = NULL;
}
