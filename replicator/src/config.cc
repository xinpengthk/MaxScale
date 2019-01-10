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

#include "config.hh"

// Provides a convenient INI parser and configuration processing
#include <boost/program_options.hpp>
namespace po = boost::program_options;

#include <fstream>
#include <mutex>
#include <regex>
#include <sstream>

// Class for processing configuration options from INI files
class Configurator
{
public:
    Configurator();
    /**
     * Process INI file into a Configuration
     */
    std::pair<std::string, Config> process(const std::string& path);
    std::string describe() const;

private:
    po::options_description  m_desc;
    Config                   m_config;
    std::string              m_error;
    std::vector<std::string> m_tables;
    std::mutex               m_lock;
};

static std::string default_cnf_path = "/etc/figure-this-out-later.cnf";
static Configurator configurator;

Configurator::Configurator()
    : m_desc("All options are stored in an INI format file located by default in: " + default_cnf_path + "\n\n"
             "Program options")
{
    po::options_description  mariadb_opts("Options for master MariaDB server, defined in the [mariadb] section", 500);

    mariadb_opts.add_options()
        (
         "mariadb.user",
         po::value<std::string>(&m_config.mariadb.user)->default_value("root"),
         "Username used to connect to the MariaDB server"
         )
        (
         "mariadb.password",
         po::value<std::string>(&m_config.mariadb.password),
         "Password for the MariaDB user"
         )
        (
         "mariadb.host",
         po::value<std::string>(&m_config.mariadb.host)->default_value("127.0.0.1"),
         "Hostname of the MariaDB server"
         )
        (
         "mariadb.port",
         po::value<int>(&m_config.mariadb.port)->default_value(3306),
         "Port of the MariaDB server"
         )
        (
         "mariadb.server_id",
         po::value<int>(&m_config.mariadb.server_id)->default_value(9999),
         "Server ID given to the master (shown in SHOW SLAVE HOSTS output)"
         )
        (
         "mariadb.gtid",
         po::value<std::string>(&m_config.mariadb.gtid),
         "Start replicating from this GTID"
         )
        (
         "mariadb.tables",
         po::value<std::vector<std::string>>(&m_tables)->multitoken()->composing(),
         "List of tables to replicate in DATABASE.TABLE format separated by spaces"
         )
        ;

        po::options_description  cs_opts("Options for ColumnStore, defined in the [columnstore] section", 500);
        cs_opts.add_options()
        (
         "columnstore.user",
         po::value<std::string>(&m_config.cs.user)->default_value("root"),
         "Username for the MariaDB ColumnStore user"
         )
        (
         "columnstore.password",
         po::value<std::string>(&m_config.cs.password),
         "Password for the MariaDB ColumnStore user"
         )
        (
         "columnstore.xml",
         po::value<std::string>(&m_config.cs.xml)->default_value("/usr/local/mariadb/columnstore/etc/Columnstore.xml"),
         "Location of Columnstore.xml"
         )
        ;

    m_desc.add(mariadb_opts);
    m_desc.add(cs_opts);
}

std::pair<std::string, Config> Configurator::process(const std::string& path)
{
    try
    {
        std::lock_guard<std::mutex> guard(m_lock);
        m_config = {};
        m_error.clear();
        m_tables.clear();

        po::variables_map vm;
        std::ifstream ini(path);

        po::store(po::parse_config_file(ini, m_desc), vm);
        po::notify(vm);

        // Parsing of lists needs some hand-holding to work nicely
        for (auto t : m_tables)
        {
            auto split = po::split_unix(t);
            m_config.mariadb.tables.insert(m_config.mariadb.tables.end(), split.begin(), split.end());
        }
    }
    catch (const std::exception& ex)
    {
        m_error = ex.what();
    }

    return {m_error, m_config};
}

std::string Configurator::describe() const
{
    std::stringstream ss;
    ss << m_desc;

    // Make the output slightly prettier by removing the command line formatting
    // `--mariadb.option arg (=default_value)` and replacing it with `option (=default_value)`.
    return std::regex_replace(ss.str(), std::regex("--(?:[a-z]*)[.]([-_a-z]*) arg"), "$1");
}

std::pair<std::string, Config> process_options(const std::string& path)
{
    return configurator.process(path);
}

std::string describe_options()
{
    return configurator.describe();
}
