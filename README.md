# redislog: shipping PostgreSQL logs to Redis

`redislog` is an extension that allows PostgreSQL to _ship_ log entries to a Redis server, directly in JSON format.

One of the goals of `redislog` is to allow administrators to tap PostgreSQL directly into the [Logstash](https://www.elastic.co/products/logstash) pipeline for real-time monitoring. In such an architecture, `redislog` acts as a "Shipper" component able to send events to a "Broker", such as Redis.

Thanks to hooks made available by PostgreSQL through `elog.c`, `redislog` is a logging facility that generates log events in JSON format and sends them to a Redis server over the network. 

## Requirements

* [Hiredis library](https://github.com/redis/hiredis)
* A Redis server for data collection

## Installation

The module can be activated by adding the following parameters in
`postgresql.conf`:

    shared_preload_libraries = 'redislog'
    redislog.hosts = '127.0.0.1'
    redislog.port = 6379
    redislog.key = 'postgres'
    redislog.min_error_statement = error
    redislog.min_messages = warning
    redislog.ship_to_redis_only = true
    redislog.shuffle_hosts = false

## TODO

* Ship to multiple Redis servers
* Allow DBAs to specify which fields to include in the JSON object
* ...

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

