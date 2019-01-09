#pragma once

#include <string>
#include <vector>

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
        std::string user;     // Username used for the SQL connection
        std::string password; // Password for the user
        std::string xml;      // Path to Columnstore.xml
    } cs;
};

/**
 * Parse INI format configuration file
 *
 * @param path Path to the file containing key-value pairs
 *
 * @return The error message and the parsed configuration. If the error message is empty,
 *         the parsing was successful. Otherwise the error describes the reason for
 *         the failure.
 */
std::pair<std::string, Config> process_options(const std::string& path);

/**
 * Get help text for options
 *
 * @return A human-readable help output for all of the options
 */
std::string describe_options();
