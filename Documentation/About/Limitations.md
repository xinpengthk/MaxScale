# Limitations and Known Issues within MariaDB MaxScale

This document lists known issues and limitations in MariaDB MaxScale and its
plugins. Since limitations are related to specific plugins, this document is
divided into several sections.

[TOC]

## Configuration limitations

In versions 2.1.2 and earlier, the configuration files are limited to 1024
characters per line. This limitation was increased to 16384 characters in
MaxScale 2.1.3. MaxScale 2.3.0 increased this limit to 16777216 characters.

In versions 2.2.12 and earlier, the section names in the configuration files
were limited to 49 characters. This limitation was increased to 1023 characters
in MaxScale 2.2.13.

### Multiple MaxScales on same server

Starting with MaxScale 2.4.0, on systems with Linux kernels 3.9 or newer due to
the addition of SO_REUSEPORT support, it is possible for multiple MaxScale
instances to listen on the same network port if the directories used by both
instances are completely separate and there are no conflicts which can cause
unexpected splitting of connections. This will only happen if users explicitly
tell MaxScale to ignore the default directories and will not happen in normal
use.

## Security limitiations

### MariaDB 10.2

The parser of MaxScale correctly parses `WITH` statements, but fails to
collect columns, functions and tables used in the `SELECT` defining the
`WITH` clause.

Consequently, the database firewall will **not** block `WITH` statements
where the `SELECT` of the `WITH` clause refers to forbidden columns.

## Query Classification

Follow the [MXS-1350](https://jira.mariadb.org/browse/MXS-1350) Jira issue
to track the progress on this limitation.

XA transactions are not detected as transactions by MaxScale. This means
that all XA commands will be treated as unknown commands and will be
treated as operations that potentially modify the database (in the case of
readwritesplit, the statements are routed to the master).

MaxScale **will not** track the XA transaction state which means that any
SELECT queries done inside an XA transaction can be routed to servers that
are not part of the XA transaction.

This limitation can be avoided on the client side by disabling autocommit
before any XA transactions are done. The following example shows how a
simple XA transaction is done via MaxScale by disabling autocommit for the
duration of the XA transaction.

```
SET autocommit=0;
XA START 'MyXA';
INSERT INTO test.t1 VALUES(1);
XA END 'MyXA';
XA PREPARE 'MyXA';
XA COMMIT 'MyXA';
SET autocommit=1;
```

## Prepared Statements

For its proper functioning, MaxScale needs in general to be aware of the
transaction state and _autocommit_ mode. In order to be that, MaxScale
parses statements going through it.

However, if a transaction is commited or rolled back, or the autocommit
mode is changed using a prepared statement, MaxScale will miss that and its
internal state will be incorrect, until the transaction state or autocommit
mode is changed using an explicit statement.

For instance, after the following sequence of commands, MaxScale will still
think _autocommit_ is on:
```
set autocommit=1
PREPARE hide_autocommit FROM "set autocommit=0"
EXECUTE hide_autocommit
```

To ensure that MaxScale functions properly, do not commit or rollback a
transaction or change the autocommit mode using a prepared statement.

## Protocol limitations

### Limitations with MySQL/MariaDB Protocol support (MariaDBClient)

* Compression is not included in the server handshake.

* MariaDB MaxScale does not support `KILL QUERY ID <query_id>` type
  statements. If a query by a query ID is to be killed, it needs to be done
  directly on the backend databases.

* The `KILL` commands are executed asynchronously and the results are
  ignored. Due to this, they will always appear to succeed even if the user is
  lacking the permissions.

## Authenticator limitations

### Limitations in the GSSAPI authenticator

Currently, MariaDB MaxScale only supports GSSAPI authentication when the backend
connections use GSSAPI authentication. Client side GSSAPI authentication with a
different backend authentication module is not supported.

### Limitations in the MySQL authenticator (MySQLAuth)

* MySQL old style passwords are not supported. MySQL versions 4.1 and newer use
a new authentication protocol which does not support pre-4.1 style passwords.

* When users have different passwords based on the host from which they connect
MariaDB MaxScale is unable to determine which password it should use to connect
to the backend database. This results in failed connections and unusable
usernames in MariaDB MaxScale.

* Only a subset of netmasks are supported for the *Host*-column in the
*mysql.user*-table (and related tables). Specifically, if the *Host* is of the
form `base_ip/netmask`, then the netmask must only contain the numbers 0 or 255.
For example, a netmask of 255.255.255.0 is fine while 255.255.255.192 is not.

## Filter limitations

### Database Firewall limitations (dbfwfilter)

The Database Firewall filter does not support multi-statements. Using them will
result in an error being sent to the client.

### Tee filter limitations (tee)

The Tee filter does not support binary protocol prepared statements. The
execution of a prepared statements through a service that uses the tee filter is
not guaranteed to succeed on the service where the filter branches to as it does
on the original service.

This possibility exists due to the fact that the binary protocol prepared
statements are identified by a server-generated ID. The ID sent to the client
from the main service is not guaranteed to be the same that is sent by the
branch service.

## Monitor limitations

A server can only be monitored by one monitor. Two or more monitors monitoring
the same server is considered an error.

### Limitations with Galera Cluster Monitoring (galeramon)

The default master selection is based only on MIN(wsrep_local_index). This
can be influenced with the server priority mechanic described in the
[Galera Monitor](../Monitors/Galera-Monitor.md) manual.

## Router limitations

### Avrorouter limitations (avrorouter)

The avrorouter does not support the following data types, conversions or SQL statements:

* BIT
* Fields CAST from integer types to string types
* [CREATE TABLE ... AS SELECT statements](https://mariadb.com/kb/en/library/create-table/#create-select)

The avrorouter does not do any crash recovery. This means that the avro files
need to be truncated to valid block lengths before starting the avrorouter.

#### Binlog Checksums

The avrorouter does not support binlog checksums. They must must not be used in
any of the binlogs that the avrorouter will process.

Follow [MXS-1341](https://jira.mariadb.org/browse/MXS-1341) for progress
on this issue.

### Limitations in the connection router (readconnroute)

Sending of binary data with `LOAD DATA LOCAL INFILE` is not supported.

### Limitations in the Read/Write Splitter (readwritesplit)

Read queries are routed to the master server in the following situations:

* query is executed inside an open transaction
* statement includes a stored procedure or an UDF call
* if there are multiple statements inside one query e.g. `INSERT INTO ... ; SELECT
LAST_INSERT_ID();`

#### JDBC Batched Statements

Readwritesplit does not support pipelining of JDBC batched statements. This is
caused by the fact that readwritesplit executes the statements one at a time to
track the state of the response.

#### Limitations in multi-statement handling

When a multi-statement query is executed through the readwritesplit router, it
will always be routed to the master. See
[`strict_multi_stmt`](../Routers/ReadWriteSplit.md#strict_multi_stmt) for more
details.

Execution of LOAD DATA LOCAL INFILE statements inside a multi-statement query is
not supported. If one is executed MaxScale will most likely hang (see
[MXS-1828](https://jira.mariadb.org/browse/MXS-1828)).

#### Limitations in client session handling

Some of the queries that a client sends are routed to all backends instead of
just to one. These queries include `USE <db name>` and `SET autocommit=0`, among
many others. Readwritesplit sends a copy of these queries to each backend server
and forwards the master's reply to the client. Below is a list of MySQL commands
which are classified as session commands.

```
COM_INIT_DB (USE <db name> creates this)
COM_CHANGE_USER
COM_STMT_CLOSE
COM_STMT_SEND_LONG_DATA
COM_STMT_RESET
COM_STMT_PREPARE
COM_QUIT (no response, session is closed)
COM_REFRESH
COM_DEBUG
COM_PING
SQLCOM_CHANGE_DB (USE ... statements)
SQLCOM_DEALLOCATE_PREPARE
SQLCOM_PREPARE
SQLCOM_SET_OPTION
SELECT ..INTO variable|OUTFILE|DUMPFILE
SET autocommit=1|0
```

Prior to MaxScale 2.3.0, session commands that were 2²⁴ - 1 bytes or longer were
not supported and caused the session to be closed.

There is a possibility for misbehavior. If `USE mytable` is executed in one of
the slaves and fails, it may be due to replication lag rather than the database
not existing. Thus, the same command may produce different result in different
backend servers. The slaves which fail to execute a session command will be
dropped from the active list of slaves for this session to guarantee a
consistent session state across all the servers used by the session. In
addition, the server will not used again for routing for the duration of the
session.

The above-mentioned behavior for user variables can be partially controlled with
the configuration parameter `use_sql_variables_in`:

```
use_sql_variables_in=[master|all] (default: all)
```

**WARNING**

If a SELECT query modifies a user variable when the `use_sql_variables_in`
parameter is set to `all`, it will not be routed and the client will receive an
error. A log message is written into the log further explaining the reason for
the error. Here is an example use of a SELECT query which modifies a user
variable and how MariaDB MaxScale responds to it.

```
MySQL [(none)]> set @id=1;
Query OK, 0 rows affected (0.00 sec)

MySQL [(none)]> SELECT @id := @id + 1 FROM test.t1;
ERROR 1064 (42000): Routing query to backend failed. See the error log for further details.
```

Allow user variable modification in SELECT queries by setting
`use_sql_variables_in=master`. This will route all queries that use user
variables to the master.

### Schemarouter limitations (schemarouter)

The schemarouter currently has some limitations due to the nature of the
sharding implementation and the way the session variables are detected and
routed. Here is a list of the current limitations:

* Cross-database queries (e.g. `SELECT column FROM database1.table UNION select
column FROM database2.table`) are not supported and are routed either to the
first explicit database in the query, the current database in use or to the
first available database, depending on which succeeds.

* Without a default database, queries without explicit databases that do not
modify the session state will be routed to the first available server. This
means that, for example when creating a new database, queries should be done
directly on the node or the router should be equipped with the hint filter and a
routing hint should be used. Queries that modify the session state (e.g. `SET
autocommit=1`) will be routed to all servers regardless of the default database.

* SELECT queries that modify session variables are not currently supported because
uniform results can not be guaranteed. If such a query is executed, the behavior
of the router is undefined. To work around this limitation, the query must be
executed in separate parts.

* If a query targets a database the schemarouter hasn't mapped to a server, the
query will be routed to the first available server. This possibly returns an
error about database rights instead of a missing database.

* The preparation of a prepared statement is routed to all servers. The
execution of a prepared statement is routed to the first available server or to
the server pointed by a routing hint attached to the query. In practice this
means that prepared statements aren't supported by the schemarouter.
