/*
 * Copyright (c) 2019 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2022-01-01
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

#include <mysql.h>
#include <mariadb_rpl.h>

#include <memory>

#include "config.hh"

// Convenience class that wraps a MYSQL connection and provides a minimal C++ interface
class SQL
{
public:
    using Row = std::vector<std::string>;
    using Result = std::vector<Row>;

    /**
     * Create a new connection from a list of servers
     *
     * The first available server is chosen from the provided list
     *
     * @param servers List of server candidates
     *
     * @return The error message and a unique_ptr. If an error occurred, the error string contains the
     *         error description and the unique_ptr is empty.
     */
    static std::pair<std::string, std::unique_ptr<SQL>> connect(const std::vector<cdc::Server>& servers);

    ~SQL();

    /**
     * Execute a query
     *
     * @param sql SQL to execute
     *
     * @return True on success, false on error
     */
    bool query(const std::string& sql);
    bool query(const std::vector<std::string>& sql);
    bool query(const char* fmt, ...) __attribute((format(printf, 2, 3)));

    /**
     * Fetch all rows of a result set
     *
     * @return The rows of the result set
     */
    Result fetch();

    /**
     * Fetch one row of the result set
     *
     * @return A row of the result set
     */
    Row fetch_row();

    /**
     * Return latest error string
     *
     * @return The latest error
     */
    std::string error() const;

    /**
     * Return latest error number
     *
     * @return The latest number
     */
    int errnum() const;

    /**
     * Return the server where the connection was created
     *
     * @return The server where the connection was created
     */
    const cdc::Server& server() const;

    /**
     * Start replicating data from the server
     *
     * @param server_id Server ID to connect with
     *
     * @return True if replication was started successfully
     */
    bool replicate(int server_id);

    /**
     * Fetch one replication event
     *
     * @return The next replicated event or null on error
     */
    MARIADB_RPL_EVENT* fetch_event();

private:
    SQL(MYSQL* mysql, const cdc::Server& server);

    MYSQL*       m_mysql {nullptr};     // Database handle
    MYSQL_RES*   m_res {nullptr};       // Open result set
    MARIADB_RPL* m_rpl {nullptr};       // Replication handle
    cdc::Server  m_server;              // The server where the connection was made
};

// String conversion helper
static inline std::string to_string(const MARIADB_STRING& str)
{
    return std::string(str.str, str.length);
}
