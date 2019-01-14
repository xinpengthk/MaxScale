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

// The public header
#include "replicator.hh"

#include <atomic>
#include <cstdint>
#include <future>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <mysql.h>
#include <mariadb_rpl.h>

// Private headers
#include "table.hh"
#include "sql.hh"

namespace cdc
{


// A very small daemon. The main class that drives the whole conversion process
class Replicator::Imp
{
public:
    Imp& operator=(Imp&) = delete;
    Imp(Imp&) = delete;

    // Creates a new replication stream and starts it
    static std::pair<std::string, std::unique_ptr<Replicator::Imp>> start(const Config& cnf);

    // Stops a running replication stream
    void stop();

    // Get error message
    std::string error() const;

    ~Imp();

private:
    Imp(const Config& cnf);
    bool connect();
    bool run();
    void process_events(std::promise<bool> promise);
    void set_error(const std::string& err);

    Config               m_cnf;                 // The configuration the stream was started with
    std::unique_ptr<SQL> m_sql;                 // Database connection
    std::thread          m_thr;                 // Thread that receives the replication events
    std::atomic<bool>    m_running {true};      // Whether the stream is running
    std::string          m_error;               // The latest error message
    std::string          m_gtid;                // GTID position to start from
    mutable std::mutex   m_lock;

    // Map of active tables
    std::unordered_map<uint64_t, std::unique_ptr<Table>> m_tables;
};

Replicator::Imp::Imp(const Config& cnf)
    : m_cnf(cnf)
{
}

// static
std::pair<std::string, std::unique_ptr<Replicator::Imp>> Replicator::Imp::start(const Config& config)
{
    std::unique_ptr<Imp> rval(new(std::nothrow) Imp(config));
    std::string err;

    if (!rval)
    {
        err = "Memory allocation failed";
    }
    else if (!rval->run())
    {
        err = rval->error();
        rval.reset();
    }

    return {err, std::move(rval)};
}

void Replicator::Imp::stop()
{
    if (m_running)
    {
        m_running = false;
        m_thr.join();
    }
}

std::string Replicator::Imp::error() const
{
    std::lock_guard<std::mutex> guard(m_lock);
    return m_error;
}

void Replicator::Imp::set_error(const std::string& err)
{
    std::lock_guard<std::mutex> guard(m_lock);
    m_error = err;
}

bool Replicator::Imp::run()
{
    std::promise<bool> promise;
    auto future = promise.get_future();

    // Start the thread and wait for the result
    m_thr = std::thread(&Imp::process_events, this, std::move(promise));
    future.wait();

    return future.get();
}

bool Replicator::Imp::connect()
{
    std::string gtid_start_pos = "SET @slave_connect_state='" + m_gtid + "'";
    std::string err;

    std::tie(err, m_sql) = SQL::connect(m_cnf.mariadb.servers);

    if (!err.empty())
    {
        set_error(err);
        return false;
    }

    // Queries required to start GTID replication
    std::vector<std::string> queries =
    {
        "SET @master_binlog_checksum = @@global.binlog_checksum",
        "SET @mariadb_slave_capability=4",
        gtid_start_pos,
        "SET @slave_gtid_strict_mode=1",
        "SET @slave_gtid_ignore_duplicates=1",
        "SET NAMES latin1"
    };

    if (!m_sql->query(queries))
    {
        set_error("Failed to prepare connection: " + m_sql->error());
        return false;
    }

    if (!m_sql->replicate(m_cnf.mariadb.server_id))
    {
        set_error("Failed to open replication channel: " + m_sql->error());
        return false;
    }

    return true;
}

void Replicator::Imp::process_events(std::promise<bool> promise)
{
    bool ok = connect();
    promise.set_value(ok);

    if (ok)
    {
        MARIADB_RPL_EVENT* event;

        while (m_running && (event = mariadb_rpl_fetch(m_rpl, nullptr)))
        {
            /**
             * TODO: Implement event processing
             *
             * Pseudo-code implementation:
             *
             *  if (event == TABLE_MAP_EVENT)
             *    m_tables[event.table_map.table_id] = Table::open(event);
             *  else if (event == ROW_EVENT)
             *    m_tables[event.rows.table_id].enqueue(event)
             *  else if (event == QUERY_EVENT)
             *  {
             *    for (auto& a : m_tables) // Sync tables
             *      a.process()
             *    execute_query(event.sql)
             *  }
             */

            mariadb_free_rpl_event(event);
        }
    }
}

Replicator::Imp::~Imp()
{
    if (m_running)
    {
        stop();
    }
}

//
// The public API
//

// static
std::pair<std::string, std::unique_ptr<Replicator>> Replicator::start(const Config& cnf)
{
    std::unique_ptr<Replicator> rval;
    std::unique_ptr<Imp> real;
    std::string error;
    std::tie(error, real) = Replicator::Imp::start(cnf);

    if (real)
    {
        rval.reset((new(std::nothrow) Replicator(std::move(real))));
    }

    return {error, std::move(rval)};
}

void Replicator::stop()
{
    m_imp->stop();
}

std::string Replicator::error() const
{
    return m_imp->error();
}

Replicator::~Replicator()
{
}

Replicator::Replicator(std::unique_ptr<Imp> real)
    : m_imp(std::move(real))
{
}
}
