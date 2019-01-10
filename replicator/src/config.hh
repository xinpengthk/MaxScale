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

#include <string>
#include <vector>

namespace cdc
{

struct Config
{
    // Replication configuration
    struct
    {
        std::string              host;      // Address to connect to
        int                      port;      // Port where the master is listening
        std::string              user;      // Username used for the connection
        std::string              password;  // Password for the user
        int                      server_id; // Server ID used in registration
        std::string              gtid;      // Starting GTID
        std::vector<std::string> tables;    // Table identifiers that are processed
    } mariadb;

    // ColumnStore configuration
    struct
    {
        std::string user;       // Username used for the SQL connection
        std::string password;   // Password for the user
        std::string xml;        // Path to Columnstore.xml
    } cs;
};
}
