# redislog: shipping PostgreSQL logs to Redis

`redislog` is an extension that allows PostgreSQL to _ship_ log entries to a Redis server, directly in JSON format.

One of the goals of `redislog` is to allow administrators to tap PostgreSQL directly into the [Logstash](https://www.elastic.co/products/logstash) pipeline for real-time monitoring. In such an architecture, `redislog` acts as a "Shipper" component able to send events to a "Broker", such as Redis.

Thanks to hooks made available by PostgreSQL through `elog.c`, `redislog` is a logging facility that generates log events in JSON format and sends them to a Redis server over the network. 

## Requirements

* [Hiredis library](https://github.com/redis/hiredis)
* A Redis server for data collection

## Installation

`redislog` can be installed as any other PostgreSQL extension by executing:

    make
    make install

In order to activate the module, you only need to add the following
configuration option in `postgresql.conf`:

    shared_preload_libraries = 'redislog'

`redislog` will use the default values for any configuration option.

For a detailed list of options, see the following section.

## Configuration options

redislog.hosts

: List of `HOST[:PORT]` pairs separated by commas. If no port is specified,
  the default 6379 is assumed. By default, `redislog.hosts` is set
  to `127.0.0.1`. In case of more than a host, the actual behaviour is
  influenced by the `redislog.shuffle_hosts` option:

    - if set to `on`, multiple hosts are used for load balancing purposes

    - if set to `off`, multiple hosts are used for high availability purposes
      and defined as a priority ordered list.


redislog.shuffle\_hosts

: Only if `redislog.hosts` defines more than a host. If set to `on`,
  the order of hosts is shuffled and hosts are used in a load balancing
  scenario.


redislog.connection\_timeout

: Redis server connection timeout in milliseconds (by default 1000).


redislog.key

: Redis server key name (by default `postgres`).


redislog.ship\_to\_redis\_only

: If set to `on`, log messages that are correctly sent to Redis are
  not journaled into the main PostgreSQL log (by default `off`).


redislog.fields

: List of fields to be placed in the output JSON object. Users have
  the opportunity to customise the list of fields as well as the
  name of each key in the objects through the following syntax:

        redislog.fields = 'FIELD[:NAME][, ...]'

  By default, the list is set to:

  - `user_name`
  - `database_name`
  - `process_id`
  - `remote_host`
  - `session_id`
  - `session_line_num`
  - `command_tag`
  - `session_start_time`
  - `virtual_transaction_id`
  - `transaction_id`
  - `error_severity`
  - `sql_state_code`
  - `detail_log`
  - `detail`
  - `hint`
  - `internal_query`
  - `internal_query_pos`
  - `context`
  - `query`
  - `query_pos`
  - `file_location`
  - `application_name`
  - `message`


redislog.min_error_statement

: Controls which SQL statements that cause an error condition are
	recorded in the server log. Each level includes all the levels that
  follow it. The later the level, the fewer messages are sent.
  
redislog.min_messages

: Set the message levels that are logged. Each level includes all
  the levels that follow it. The higher the level, the fewer messages
  are sent.

## TODO list

- Support for CSV output format
- Support for `channel` data type
- Support for custom fields in JSON objects

## Authors

* Marco Nenciarini <marco.nenciarini@2ndquadrant.it>
* Leonardo Cecchi <leonardo.cecchi@2ndquadrant.it>
* Gabriele Bartolini <gabriele.bartolini@2ndquadrant.it>

Thanks to Michael Paquier for the initial exchange of ideas and for his
previous work on [jsonlog](https://github.com/michaelpq/pg_plugins/blob/master/jsonlog/jsonlog.c).

## Licence

    Copyright (c) 1996-2015,  PostgreSQL Global Development Group
    
    Permission to use, copy, modify, and distribute this software and its
    documentation for any purpose, without fee, and without a written agreement
    is hereby granted, provided that the above copyright notice and this
    paragraph and the following two paragraphs appear in all copies.
    
    IN NO EVENT SHALL POSTGRESQL GLOBAL DEVELOPMENT GROUP BE LIABLE TO ANY
    PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
    INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
    DOCUMENTATION, EVEN IF POSTGRESQL GLOBAL DEVELOPMENT GROUP HAS BEEN
    ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
    
    POSTGRESQL GLOBAL DEVELOPMENT GROUP SPECIFICALLY DISCLAIMS ANY WARRANTIES,
    INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
    AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
    ON AN "AS IS" BASIS, AND POSTGRESQL GLOBAL DEVELOPMENT GROUP HAS NO
    OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
    MODIFICATIONS.

