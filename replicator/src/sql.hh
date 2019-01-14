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

#include <memory>

#include "config.hh"

// Convenience class that wraps a MYSQL connection and provides a minimal C++ interface
class SQL
{
public:

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

    operator MYSQL*();
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

private:
    SQL(MYSQL* mysql);
    MYSQL* m_mysql;
};
