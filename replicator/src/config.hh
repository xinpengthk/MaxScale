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

#include <chrono>
#include <string>
#include <vector>

namespace cdc
{

struct Server
{
    std::string host;       // Address to connect to
    int         port;       // Port where the server is listening
    std::string user;       // Username used for the connection
    std::string password;   // Password for the user
};

struct Config
{
    // Replication configuration
    struct
    {
        std::vector<Server>      servers;   // List of master servers to replicate from
        int                      server_id; // Server ID used in registration
        std::string              gtid;      // Starting GTID
        std::vector<std::string> tables;    // Table identifiers that are processed
    } mariadb;

    // ColumnStore configuration
    struct
    {
        std::vector<Server>       server;           // List of UMs
        std::string               xml;              // Path to Columnstore.xml
        std::chrono::milliseconds flush_interval;   // How often to flush per-table data to ColumnStore
    } cs;
};
}
