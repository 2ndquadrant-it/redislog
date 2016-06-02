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
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/json.h"
#include "utils/ps_status.h"

#include "hiredis/hiredis.h"

#define REDIS_DEFAULT_PORT	6379

/* Allow load of this module in shared libs */
PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

/* Hold previous logging hook */
static emit_log_hook_type prev_log_hook = NULL;

/* GUC Variables */
char  *Redislog_hosts = NULL;
int   Redislog_timeout = 1000;
char  *Redislog_key = NULL;
int   Redislog_min_error_statement = ERROR;
int   Redislog_min_messages = WARNING;
bool  Redislog_ship_to_redis_only = TRUE;
bool  Redislog_shuffle_hosts = TRUE;
char  *Redislog_fields = NULL;

static MemoryContext	redislog_cfg_memory_context;

typedef struct redis_server_info {
	char *host_name;	/* Hold the Redis server hostname */
	int port;				/* Hold the server port */
} redis_server_info;

typedef struct json_field_mapping {
	int field_index;			/* The field index */
	char *custom_field_name;	/* The name choosen by the user */
} json_field_mapping;

/*
 * The redis server list. This list is terminated by an element
 * with host_name==NULL
 */
static redis_server_info	*Redislog_server_info = NULL;

/*
 * The json field list. This list is terminated by an element
 * which has field_index == FIELD_INVALID and custom_field_name==NULL
 */
static json_field_mapping	*Redislog_json_field_mapping = NULL;

#define FIELD_INVALID					-1

#define FIELD_USER_NAME				1
#define FIELD_USER_NAME_DESC			"user_name"

#define FIELD_DATABASE_NAME				2
#define FIELD_DATABASE_NAME_DESC		"database_name"

#define FIELD_PROCESS_ID				3
#define FIELD_PROCESS_ID_DESC			"process_id"

#define FIELD_REMOTE_HOST				4
#define FIELD_REMOTE_HOST_DESC			"remote_host"

#define FIELD_REMOTE_PORT				5
#define FIELD_REMOTE_PORT_DESC			"remote_port"

#define FIELD_SESSION_ID				6
#define FIELD_SESSION_ID_DESC			"session_id"

#define FIELD_SESSION_LINE_NUM			7
#define FIELD_SESSION_LINE_NUM_DESC		"session_line_num"

#define FIELD_COMMAND_TAG				8
#define FIELD_COMMAND_TAG_DESC			"command_tag"

#define FIELD_SESSION_START_TIME		9
#define FIELD_SESSION_START_TIME_DESC	"session_start_time"

#define FIELD_VIRTUAL_TRANSACTION_ID		10
#define FIELD_VIRTUAL_TRANSACTION_ID_DESC	"virtual_transaction_id"

#define FIELD_TRANSACTION_ID			11
#define FIELD_TRANSACTION_ID_DESC		"transaction_id"

#define FIELD_ERROR_SEVERITY			12
#define FIELD_ERROR_SEVERITY_DESC		"error_severity"

#define FIELD_SQL_STATE_CODE			13
#define FIELD_SQL_STATE_CODE_DESC		"sql_state_code"

#define FIELD_DETAIL_LOG				14
#define FIELD_DETAIL_LOG_DESC			"detail_log"

#define FIELD_DETAIL					15
#define FIELD_DETAIL_DESC				"detail"

#define FIELD_HINT						16
#define FIELD_HINT_DESC					"hint"

#define FIELD_INTERNAL_QUERY			17
#define FIELD_INTERNAL_QUERY_DESC		"internal_query"

#define FIELD_INTERNAL_QUERY_POS		18
#define FIELD_INTERNAL_QUERY_POS_DESC	"internal_query_pos"

#define FIELD_CONTEXT					19
#define FIELD_CONTEXT_DESC				"context"

#define FIELD_QUERY						20
#define FIELD_QUERY_DESC				"query"

#define FIELD_QUERY_POS					21
#define FIELD_QUERY_POS_DESC			"query_pos"

#define FIELD_FILE_LOCATION				22
#define FIELD_FILE_LOCATION_DESC		"file_location"

#define FIELD_APPLICATION_NAME			23
#define FIELD_APPLICATION_NAME_DESC		"application_name"

#define FIELD_MESSAGE					24
#define FIELD_MESSAGE_DESC				"message"


/* Log timestamp */
#define LOG_TIMESTAMP_LEN 128
static char formatted_log_time[LOG_TIMESTAMP_LEN];

/* Session start timestamp */
static char formatted_start_time[LOG_TIMESTAMP_LEN];

/* Redis context */
static redisContext *redis_context = NULL;

/* Used to detect if values inherited over fork need resetting. */
static int lastPid = 0;

/* String mapper for error severity */
static const char *error_severity(int elevel);

/* Configuration options management */
static bool guc_check_hosts_list(char **newvalue, void **extra, GucSource source);
static void guc_assign_hosts_list(const char *newval, void *extra);
static bool guc_check_fields(char **newvalue, void **extra, GucSource source);
static void guc_assign_fields(const char *newval, void *extra);
static bool guc_check_field_entry(const char *str);
static bool guc_field_name_is_valid(const char *str);
static int guc_field_name_get_idx(const char *str);
static void split_host_port(const char *token, char **hostName, int *port);
static void split_field_name(const char *str, char **field, char **name);
static char **create_host_list(char *hosts_string, int *hosts_count);
static bool host_port_pair_is_correct(const char *str);
static void free_redis_server_info(void);
static void free_json_field_mapping(void);
static void shuffle_redis_server_list(void);

/* Redis specific prototypes */
static void redis_log_hook(ErrorData *edata);
static void redis_close_connection(void);
static bool redis_open_connection(void);
static bool redis_log_shipper(char *data, int len);

/*
 * Enum definition for redislog.min_error_statement and redislog.min_messages
 */
static const struct config_enum_entry server_message_level_options[] = {
	{"debug", DEBUG2, true},
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"error", ERROR, false},
	{"log", LOG, false},
	{"fatal", FATAL, false},
	{"panic", PANIC, false},
	{NULL, 0, false}
};

/*
 * Useful for HUP triggered host reassignment: close the connection, a new one
 * will be opened on next event.
 */
static void
guc_assign_hosts_list(const char *newval, void *extra)
{
	MemoryContext	oldcontext;
	char			**server_lookup;
	char			*hosts_string;
	int				hosts_count;
	int				i;

	redis_close_connection();

	oldcontext = MemoryContextSwitchTo(redislog_cfg_memory_context);
	free_redis_server_info();

	hosts_string = pstrdup(newval);
	server_lookup = create_host_list(hosts_string, &hosts_count);
	Redislog_server_info = palloc(sizeof(redis_server_info)*(hosts_count+1));

	for(i=0; i<hosts_count; i++)
	{
		char	*tok = server_lookup[i];
		char	*hostname = NULL;
		int	port;

		split_host_port(tok, &hostname, &port);
		Redislog_server_info[i].host_name = hostname;
		Redislog_server_info[i].port = port;
	}

	Redislog_server_info[i].host_name = NULL;
	Redislog_server_info[i].port = 0;

	pfree(hosts_string);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Deallocate the redis server struct
 */
static void free_redis_server_info() {
	int i = 0;

	if (Redislog_server_info==NULL)
	{
		return;
	}

	while(true)
	{
		if (Redislog_server_info[i].host_name==NULL)
			break;
		else
			pfree(Redislog_server_info[i].host_name);
		i++;
	}

	pfree(Redislog_server_info);
}

/*
 * Check if the hosts list is syntactically correct
 */
static bool guc_check_hosts_list(char **newvalue, void **extra, GucSource source)
{
	char	*hosts_string;
	int		hosts_count;
	int		i;
	char	**server_lookup;

	hosts_string = pstrdup(*newvalue);
	server_lookup = create_host_list(hosts_string, &hosts_count);

	if (server_lookup==NULL)
	{
		GUC_check_errdetail("redislog.hosts list syntax is invalid");
		pfree(hosts_string);
		return false;
	}

	if (hosts_count==0)
	{
		GUC_check_errdetail("redislog.hosts must not be empty");
		pfree(server_lookup);
		pfree(hosts_string);
		return false;
	}

	for (i=0; i<hosts_count; i++)
	{
		if (!host_port_pair_is_correct(server_lookup[i]))
		{
			GUC_check_errdetail("redislog.hosts \"%s\" entry must be of form HOST[:PORT]", server_lookup[i]);
			pfree(server_lookup);
			pfree(hosts_string);
			return false;
		}
	}

	pfree(server_lookup);
	pfree(hosts_string);

	return true;
}

static void
guc_assign_fields(const char *newval, void *extra)
{
	List			*field_list;
	ListCell		*l;
	char			*field_string;
	MemoryContext	oldcontext;
	int			i;

	oldcontext = MemoryContextSwitchTo(redislog_cfg_memory_context);
	free_json_field_mapping();
	field_string = pstrdup(newval);

	if (!SplitIdentifierString(field_string, ',', &field_list))
	{
		list_free(field_list);
		pfree(field_string);
	}
	else
	{
		Redislog_json_field_mapping = palloc(
			sizeof(json_field_mapping) * (list_length(field_list)+1));

		i = 0;
		foreach(l, field_list)
		{
			char	*field;
			char	*name;

			split_field_name(lfirst(l), &field, &name);
			Redislog_json_field_mapping[i].field_index = guc_field_name_get_idx(field);

			if (name==NULL)
			{
				Redislog_json_field_mapping[i].custom_field_name = pstrdup(field);
			}
			else
			{
				Redislog_json_field_mapping[i].custom_field_name = name;
			}

			pfree(field);

			i++;
		}

		Redislog_json_field_mapping[i].field_index = FIELD_INVALID;
		Redislog_json_field_mapping[i].custom_field_name = NULL;
	}

	pfree(field_string);
	list_free(field_list);
	MemoryContextSwitchTo(oldcontext);
}

static void
free_json_field_mapping(void)
{
	int i;

	if (Redislog_json_field_mapping==NULL) return;

	i = 0;
	while (true)
	{
		if (Redislog_json_field_mapping[i].custom_field_name==NULL &&
			Redislog_json_field_mapping[i].field_index==FIELD_INVALID) break;

		pfree(Redislog_json_field_mapping[i].custom_field_name);
		i = i+1;
	}

	pfree(Redislog_json_field_mapping);
}

static bool
guc_check_fields(char **newvalue, void **extra, GucSource source)
{
	List		*field_list;
	ListCell	*l;
	char		*field_string;

	field_string = pstrdup(*newvalue);

	if (!SplitIdentifierString(field_string, ',', &field_list))
	{
		GUC_check_errdetail("redislog.fields list syntax is invalid");
		list_free(field_list);
		pfree(field_string);
		return false;
	}

	foreach(l, field_list)
	{
		char	*field;

		if (!guc_check_field_entry(lfirst(l)))
		{
			list_free(field_list);
			pfree(field_string);
			return false;
		}

		split_field_name(lfirst(l), &field, NULL);

		if (!guc_field_name_is_valid(field))
		{
			GUC_check_errdetail("redislog.field: Field \"%s\" is unknown", field);
			pfree(field);
			list_free(field_list);
			pfree(field_string);
			return false;
		}

		pfree(field);
	}

	pfree(field_string);
	list_free(field_list);

	return true;
}

/*
 * Check if the field name is valid
 */
static bool
guc_check_field_entry(const char *str)
{
	char	*p_colon;

	Assert(str!=NULL);

	if(strlen(str)==0)
	{
		return false;
	}

	p_colon = strchr(str, ':');
	if (p_colon==str)
	{
		/* missing field name */
		GUC_check_errdetail("redislog \"%s\".fields entry must be of the form FIELD[:NAME]", str);
		return false;
	}

	if (p_colon==NULL)
	{
		/* will use the default field name */
		return true;
	}

	if (p_colon[1]=='\x0')
	{
		/* missing custom field name */
		return false;
	}

	return true;
}

/*
 * Split field from its custom name
 */
static void
split_field_name(const char *str, char **field, char **name)
{
	const char *p_colon;

	Assert(str!=NULL);
	p_colon = strchr(str, ':');
	if (name!=NULL)
	{
		if (p_colon==NULL)
			*name = NULL;
		else
			*name = pstrdup(p_colon+1);
	}

	if (field!=NULL)
	{
		*field = pstrdup(str);
		if (p_colon!=NULL)
			(*field)[p_colon-str]='\0';
	}
}

/*
 * Check if a field name is known
 */
static bool
guc_field_name_is_valid(const char *str)
{
	Assert(str!=NULL);
	return guc_field_name_get_idx(str) != FIELD_INVALID;
}

/*
 * Check if a field name is known
 */
static int
guc_field_name_get_idx(const char *str)
{
	Assert(str!=NULL);

	if (0==strcmp(str, FIELD_USER_NAME_DESC)) return FIELD_USER_NAME;
	if (0==strcmp(str, FIELD_DATABASE_NAME_DESC)) return FIELD_DATABASE_NAME;
	if (0==strcmp(str, FIELD_PROCESS_ID_DESC)) return FIELD_PROCESS_ID;
	if (0==strcmp(str, FIELD_REMOTE_HOST_DESC)) return FIELD_REMOTE_HOST;
	if (0==strcmp(str, FIELD_REMOTE_PORT_DESC)) return FIELD_REMOTE_PORT;
	if (0==strcmp(str, FIELD_SESSION_ID_DESC)) return FIELD_SESSION_ID;
	if (0==strcmp(str, FIELD_SESSION_LINE_NUM_DESC)) return FIELD_SESSION_LINE_NUM;
	if (0==strcmp(str, FIELD_COMMAND_TAG_DESC)) return FIELD_COMMAND_TAG;
	if (0==strcmp(str, FIELD_SESSION_START_TIME_DESC)) return FIELD_SESSION_START_TIME;
	if (0==strcmp(str, FIELD_VIRTUAL_TRANSACTION_ID_DESC)) return FIELD_VIRTUAL_TRANSACTION_ID;
	if (0==strcmp(str, FIELD_TRANSACTION_ID_DESC)) return FIELD_TRANSACTION_ID;
	if (0==strcmp(str, FIELD_ERROR_SEVERITY_DESC)) return FIELD_ERROR_SEVERITY;
	if (0==strcmp(str, FIELD_SQL_STATE_CODE_DESC)) return FIELD_SQL_STATE_CODE;
	if (0==strcmp(str, FIELD_DETAIL_LOG_DESC)) return FIELD_DETAIL_LOG;
	if (0==strcmp(str, FIELD_DETAIL_DESC)) return FIELD_DETAIL;
	if (0==strcmp(str, FIELD_HINT_DESC)) return FIELD_HINT;
	if (0==strcmp(str, FIELD_INTERNAL_QUERY_DESC)) return FIELD_INTERNAL_QUERY;
	if (0==strcmp(str, FIELD_INTERNAL_QUERY_POS_DESC)) return FIELD_INTERNAL_QUERY_POS;
	if (0==strcmp(str, FIELD_CONTEXT_DESC)) return FIELD_CONTEXT;
	if (0==strcmp(str, FIELD_QUERY_DESC)) return FIELD_QUERY;
	if (0==strcmp(str, FIELD_QUERY_POS_DESC)) return FIELD_QUERY_POS;
	if (0==strcmp(str, FIELD_FILE_LOCATION_DESC)) return FIELD_FILE_LOCATION;
	if (0==strcmp(str, FIELD_APPLICATION_NAME_DESC)) return FIELD_APPLICATION_NAME;
	if (0==strcmp(str, FIELD_MESSAGE_DESC)) return FIELD_MESSAGE;

	return FIELD_INVALID;
}

/*
 * Check if the string is of the form HOST[:PORT]
 */
static bool
host_port_pair_is_correct(const char *str)
{
	char	*p_colon;
	int		port;

	Assert(str!=NULL);

	if (strlen(str)==0)
	{
		/* the string is empty */
		return false;
	}

	p_colon = strchr(str, ':');
	if (p_colon==str)
	{
		/* missing hostname */
		return false;
	}

	if (p_colon==NULL)
	{
		/* will use the default port */
		return true;
	}

	port = pg_atoi(p_colon+1, sizeof(int32), '\0');
	if (port==0)
	{
		/* port is not valid */
		return false;
	}

	return true;
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
 * shuffle_redis_server_list
 * Shuffle the redis server list
 */
static void
shuffle_redis_server_list()
{
	int i, j;
	int server_count;

	if (!Redislog_shuffle_hosts)
		return;

	for (i=0; Redislog_server_info[i].host_name!=NULL; i++);
	server_count = i;

	for (i=server_count-1; i>=1; i--) {
		char *tmp_host_name;
		int tmp_port;

		j = random() % (i+1);

		tmp_host_name = Redislog_server_info[i].host_name;
		tmp_port = Redislog_server_info[i].port;

		Redislog_server_info[i].host_name = Redislog_server_info[j].host_name;
		Redislog_server_info[i].port = Redislog_server_info[j].port;
		Redislog_server_info[j].host_name = tmp_host_name;
		Redislog_server_info[j].port = tmp_port;
	}
}

/*
 * redis_open_connection
 * Connect to remote Redis server, returns false on failure.
 */
static bool
redis_open_connection()
{
	struct timeval	timeout;
	int		i;

	if (redis_context)
	{
		/*
		 * The connection is already opened
		 */
		return true;
	}

	i = 0;
	while (true)
	{
		if (Redislog_server_info[i].host_name==NULL)
			break;

		timeout.tv_sec = Redislog_timeout / 1000;
		timeout.tv_usec = Redislog_timeout % 1000 * 1000;

		if (Redislog_server_info[i].host_name[0] == '/')
			redis_context = redisConnectUnixWithTimeout(
				Redislog_server_info[i].host_name,
				timeout);
		else
			redis_context = redisConnectWithTimeout(
				Redislog_server_info[i].host_name,
				Redislog_server_info[i].port,
				timeout);

		if (redis_context == NULL || redis_context->err)
		{
			/*
			 * Something went wrong.
			 */
			redis_close_connection();
		}
		else
		{
			/*
			 * target server found
			 */
			break;
		}

		i++;
	}

	return Redislog_server_info[i].host_name!=NULL;
}


/*
 * Create the host list array, that must be freed by the
 * caller. Returns NULL if the redislog.hosts list is
 * syntactitally incorrect.
 */
static char **
create_host_list(char *hosts_string, int *hosts_count)
{
	List		*hosts_list;
	ListCell	*l;
	char		**server_lookup;
	int		i;

	Assert(hosts_count!=NULL);
	Assert(hosts_string!=NULL);

	*hosts_count = 0;

	if (!SplitIdentifierString(hosts_string, ',', &hosts_list))
	{
		/*
		 * The hosts list have been checked in the GUC's check hook
		 */
		list_free(hosts_list);
		return NULL;
	}

	*hosts_count = list_length(hosts_list);
	server_lookup = palloc(*hosts_count * sizeof(char *));
	i = 0;
	foreach(l, hosts_list)
	{
		server_lookup[i] = lfirst(l);
		i++;
	}
	list_free(hosts_list);

	return server_lookup;
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
 * Parse a string in the form HOST[:PORT]
 * The default port is REDIS_DEFAULT_PORT
 */
static void
split_host_port(const char *token, char **hostName, int *port)
{
	char *colon;

	*hostName = pstrdup(token);
	colon = strchr(*hostName, ':');

	if (colon==NULL)
	{
		*port = REDIS_DEFAULT_PORT;
	}
	else
	{
		*port = pg_atoi(colon+1, sizeof(int32), '\0');
		*colon = '\x0';
	}
}

/*
 * setup formatted_start_time
 * (taken from backend/utils/error/elog.c)
 */
static void
setup_formatted_start_time(void)
{
	pg_time_t	stamp_time = (pg_time_t) MyStartTime;

	/*
	 * Note: we expect that guc.c will ensure that log_timezone is set up (at
	 * least with a minimal GMT value) before Log_line_prefix can become
	 * nonempty or CSV mode can be selected.
	 *
	 * Note: we don't have the exact millisecond here.
	 */
	pg_strftime(formatted_start_time, LOG_TIMESTAMP_LEN,
				"%Y-%m-%dT%H:%M:%S%z",
				pg_localtime(&stamp_time, log_timezone));
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
	pg_strftime(formatted_log_time, LOG_TIMESTAMP_LEN,
				/* leave room for milliseconds... */
				"%Y-%m-%dT%H:%M:%S    %z",
				pg_localtime(&stamp_time, log_timezone));

	/* 'paste' milliseconds into place... */
	sprintf(msbuf, ".%03d", (int) (tv.tv_usec / 1000));
	strncpy(formatted_log_time + 19, msbuf, 4);
}

/*
 * is_log_level_output -- is elevel logically >= log_min_level?
 *
 * We use this for tests that should consider LOG to sort out-of-order,
 * between ERROR and FATAL.  Generally this is the right thing for testing
 * whether a message should go to the postmaster log, whereas a simple >=
 * test is correct for testing whether the message should go to the client.
 * (taken from backend/utils/elog.c)
 */
static bool
is_log_level_output(int elevel, int log_min_level)
{
	if (elevel == LOG || elevel == COMMERROR)
	{
		if (log_min_level == LOG || log_min_level <= ERROR)
			return true;
	}
	else if (log_min_level == LOG)
	{
		/* elevel != LOG */
		if (elevel >= FATAL)
			return true;
	}
	/* Neither is LOG */
	else if (elevel >= log_min_level)
		return true;

	return false;
}

/*
 * append_json_literal
 * Append to given StringInfo a JSON with a given key and a value
 * not yet made literal.
 * (taken from jsonlog.c)
 */
static void
append_json_literal(StringInfo buf, const char *key, const char *value, bool is_comma)
{
	StringInfoData literal_json;

	initStringInfo(&literal_json);
	Assert(key && value);

	/*
	 * Call in-core function able to generate wanted strings, there is
	 * no need to reinvent the wheel.
	 */
	if (value==NULL) {
		appendStringInfo(&literal_json, "null");
	} else {
		escape_json(&literal_json, value);
	}

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
	bool		print_stmt = false;
	bool		send_status = false;
	int		i;

	/* static counter for line numbers */
	static long log_line_number = 0;

	/*
	 * This is one of the few places where we'd rather not inherit a static
	 * variable's value from the postmaster.  But since we will, reset it when
	 * MyProcPid changes.
	 */
	if (lastPid != MyProcPid)
	{
		log_line_number = 0;
		lastPid = MyProcPid;
		formatted_start_time[0] = '\0';
		redis_close_connection();

		shuffle_redis_server_list();
	}

	/*
	 * Check if the log has to be written, if not just exit.
	 */
	if (!is_log_level_output(edata->elevel, Redislog_min_messages))
	{
		goto quickExit;
	}

	log_line_number++;

	initStringInfo(&buf);

	if (is_log_level_output(edata->elevel, Redislog_min_error_statement) &&
		debug_query_string != NULL &&
		!edata->hide_stmt)
		print_stmt = true;

	/* Initialize string */
	appendStringInfoChar(&buf, '{');

	i = 0;
	while (true)
	{
		if (Redislog_json_field_mapping[i].field_index == FIELD_INVALID &&
			Redislog_json_field_mapping[i].custom_field_name == NULL)
			break;

		switch (Redislog_json_field_mapping[i].field_index)
		{
		case FIELD_USER_NAME:
			/* Username */
			if (MyProcPort)
				append_json_literal(&buf,
									Redislog_json_field_mapping[i].custom_field_name,
									MyProcPort->user_name, true);
			break;

		case FIELD_DATABASE_NAME:
			/* Database name */
			if (MyProcPort)
				append_json_literal(&buf,
									Redislog_json_field_mapping[i].custom_field_name,
									MyProcPort->database_name, true);
			break;

		case FIELD_PROCESS_ID:
			/* Process ID */
			if (MyProcPid != 0)
				appendStringInfo(&buf, "\"%s\":%d,",
								 Redislog_json_field_mapping[i].custom_field_name,
								 MyProcPid);
			break;

		case FIELD_REMOTE_HOST:
			/* Remote host */
			if (MyProcPort && MyProcPort->remote_host)
			{
				append_json_literal(&buf,
									Redislog_json_field_mapping[i].custom_field_name,
									MyProcPort->remote_host, true);
			}
			break;

		case FIELD_REMOTE_PORT:
			/* Remote port */
			if(MyProcPort->remote_port && MyProcPort->remote_port[0] != '\0')
			{
				append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name,
									MyProcPort->remote_port, true);
			}
			break;

		case FIELD_SESSION_ID:
			/* Session id */
			if (MyProcPid != 0)
				appendStringInfo(&buf, "\"%s\":\"%lx.%x\",",
								 Redislog_json_field_mapping[i].custom_field_name,
								 (long) MyStartTime, MyProcPid);
			break;

		case FIELD_SESSION_LINE_NUM:
			/* Session line num */
			if (MyProcPid != 0)
				appendStringInfo(&buf, "\"%s\":%ld,",
								 Redislog_json_field_mapping[i].custom_field_name,
								 log_line_number);
			break;

		case FIELD_COMMAND_TAG:
			/* PS display */
			if (MyProcPort)
			{
				StringInfoData msgbuf;
				const char *psdisp;
				int displen;

				initStringInfo(&msgbuf);

				psdisp = get_ps_display(&displen);
				appendBinaryStringInfo(&msgbuf, psdisp, displen);
				append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name, msgbuf.data, true);

				pfree(msgbuf.data);
			}
			break;

		case FIELD_SESSION_START_TIME:
			/* session start timestamp */
			if (formatted_start_time[0] == '\0')
				setup_formatted_start_time();
			append_json_literal(&buf,
								Redislog_json_field_mapping[i].custom_field_name,
								formatted_start_time, true);
			break;

		case FIELD_VIRTUAL_TRANSACTION_ID:
			/* Virtual transaction id */
			/* keep VXID format in sync with lockfuncs.c */
			if (MyProc != NULL && MyProc->backendId != InvalidBackendId)
				appendStringInfo(&buf, "\"%s\":\"%d/%u\",",
								 Redislog_json_field_mapping[i].custom_field_name,
								 MyProc->backendId, MyProc->lxid);
			break;

		case FIELD_TRANSACTION_ID:
			/* Transaction id */
			if (txid != InvalidTransactionId)
				appendStringInfo(&buf, "\"%s\":%u,",
								 Redislog_json_field_mapping[i].custom_field_name,
								 GetTopTransactionIdIfAny());
			break;

		case FIELD_ERROR_SEVERITY:
			/* Error severity */
			append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name,
								(char *) error_severity(edata->elevel), true);
			break;

		case FIELD_SQL_STATE_CODE:
			/* SQL state code */
			if (edata->sqlerrcode != ERRCODE_SUCCESSFUL_COMPLETION)
				append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name,
									unpack_sql_state(edata->sqlerrcode), true);
			break;

		case FIELD_DETAIL_LOG:
			if (edata->detail_log)
				append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name, edata->detail_log, true);
			break;

		case FIELD_DETAIL:
			if (edata->detail)
				append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name, edata->detail, true);
			break;

		case FIELD_HINT:
			/* Error hint */
			if (edata->hint)
				append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name, edata->hint, true);
			break;

		case FIELD_INTERNAL_QUERY:
			/* Internal query */
			if (edata->internalquery)
				append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name,
									edata->internalquery, true);
			break;

		case FIELD_INTERNAL_QUERY_POS:
			/* if printed internal query, print internal pos too */
			if (edata->internalpos > 0 && edata->internalquery != NULL)
				appendStringInfo(&buf, "\"%s\":%d,", Redislog_json_field_mapping[i].custom_field_name, edata->internalpos);
			break;

		case FIELD_CONTEXT:
			/* Error context */
			if (edata->context)
				append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name, edata->context, true);
			break;

		case FIELD_QUERY:
			/* user query --- only reported if not disabled by the caller */
			if (print_stmt)
				append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name, debug_query_string, true);
			break;

		case FIELD_QUERY_POS:
			/* user query position -- only reposted if not disabled by the caller */
			if (print_stmt && edata->cursorpos > 0)
				appendStringInfo(&buf, "\"%s\":%d,", Redislog_json_field_mapping[i].custom_field_name, edata->cursorpos);
			break;

		case FIELD_FILE_LOCATION:
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
				append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name, msgbuf.data, true);
				pfree(msgbuf.data);
			}
			break;

		case FIELD_APPLICATION_NAME:
			/* Application name */
			if (application_name && application_name[0] != '\0')
				append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name,
									application_name, true);
			break;

		case FIELD_MESSAGE:
			/* Error message */
			append_json_literal(&buf, Redislog_json_field_mapping[i].custom_field_name, edata->message, true);
			break;
		}

		i++;
	}

	/* Timestamp */
	setup_formatted_log_time();
	append_json_literal(&buf, "@timestamp", formatted_log_time, false);

	/* Finish string */
	appendStringInfoChar(&buf, '}');
	appendStringInfoChar(&buf, '\n');

	/* Send the data to Redis */
	send_status = redis_log_shipper(buf.data, buf.len);

	/* Skip sending the event to the server, if it was correctly
	 * shipped to Redis and if 'ship_to_redis_only' is set to true
	 */
	if (Redislog_ship_to_redis_only && send_status) {
		edata->output_to_server = false;
	}

	/* Cleanup */
	pfree(buf.data);

quickExit:

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
	/* Set up the memory context */
	redislog_cfg_memory_context = AllocSetContextCreate(TopMemoryContext,
														"redislog memory context",
														ALLOCSET_DEFAULT_MINSIZE,
														ALLOCSET_DEFAULT_INITSIZE,
														ALLOCSET_DEFAULT_MAXSIZE);

	/* Set up GUCs */
	DefineCustomStringVariable("redislog.hosts",
	  "List of Redis servers",
	  "List of HOST[:PORT] pairs separated by commas, ad example:"
	  "HOST[:PORT][, ...]. If port is not specified the default "
	  "6379 is assumed",
	  &Redislog_hosts,
	  "127.0.0.1",
	  PGC_SIGHUP,
	  GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY | GUC_LIST_INPUT,
	  &guc_check_hosts_list,
	  &guc_assign_hosts_list,
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

	DefineCustomEnumVariable("redislog.min_error_statement",
	  "Controls which SQL statements that cause an error condition are "
	  "recorded in the server log.",
	  "Each level includes all the levels that follow it. The later "
	  "the level, the fewer messages are sent.",
	  &Redislog_min_error_statement,
	  log_min_error_statement,
	  server_message_level_options,
	  PGC_SUSET,
	  GUC_NOT_IN_SAMPLE,
	  NULL,
	  NULL,
	  NULL);

	DefineCustomEnumVariable("redislog.min_messages",
	  "Set the message levels that are logged.",
	  "Each level includes all the levels that follow it. The higher "
	  "the level, the fewer messages are sent.",
	  &Redislog_min_messages,
	  WARNING,
	  server_message_level_options,
	  PGC_SUSET,
	  GUC_NOT_IN_SAMPLE,
	  NULL,
	  NULL,
	  NULL);

	DefineCustomBoolVariable("redislog.ship_to_redis_only",
	  "Send log messages to Redis only.",
	  "If set to true, send log messages to Redis only and skip "
	  "journaling them into the main PostgreSQL log. Use the "
	  "PostgreSQL main logger facility for fallback purposes only, "
	  "in case no Redis service is available. "
	  "By default it is set to false.",
	  &Redislog_ship_to_redis_only,
	  FALSE,
	  PGC_SUSET,
	  GUC_NOT_IN_SAMPLE,
	  NULL,
	  NULL,
	  NULL);

	DefineCustomBoolVariable("redislog.shuffle_hosts",
	  "If true the list of available Redis servers is shuffled",
	  "Shuffle the list of available Redis server in order to"
	  "balance events servers",
	  &Redislog_shuffle_hosts,
	  TRUE,
	  PGC_SUSET,
	  GUC_NOT_IN_SAMPLE,
	  NULL,
	  NULL,
	  NULL);

	DefineCustomStringVariable("redislog.fields",
	  "Alternative field names for the produced JSON output",
	  "Allow users to define an alternative name for the produced"
	  "JSON output using this syntax: "
	  "redislog.fields = \'FIELD[:NAME][, ...]\'",
	  &Redislog_fields,
	  "user_name,database_name,process_id,remote_host,session_id,"
	  "session_line_num,command_tag,session_start_time,"
	  "virtual_transaction_id,transaction_id,error_severity,"
	  "sql_state_code,detail_log,detail,hint,internal_query,"
	  "internal_query_pos,context,query,query_pos,"
	  "file_location,application_name,message",
	  PGC_SIGHUP,
	  GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY,
	  guc_check_fields,
	  guc_assign_fields,
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
	MemoryContextDelete(redislog_cfg_memory_context);

	emit_log_hook = prev_log_hook;
	if (redis_context)
		redisFree(redis_context);
	redis_context = NULL;
}
