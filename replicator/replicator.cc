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
#include <sstream>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <mysql.h>
#include <mariadb_rpl.h>

#include <maxscale/query_classifier.hh>
#include <maxscale/buffer.hh>

// Private headers
#include "table.hh"
#include "sql.hh"


namespace std
{
template<>
struct default_delete<MARIADB_RPL_EVENT>
{
    void operator()(MARIADB_RPL_EVENT* event)
    {
        mariadb_free_rpl_event(event);
    }
};
}

using Event = std::unique_ptr<MARIADB_RPL_EVENT>;

namespace cdc
{


// A very small daemon. The main class that drives the whole conversion process
class Replicator::Imp
{
public:
    Imp& operator=(Imp&) = delete;
    Imp(Imp&) = delete;

    // Creates a new replication stream and starts it
    Imp(const Config& cnf);

    // Stops a running replication stream
    void stop();

    // Get error message
    std::string error() const;

    ~Imp();

private:
    bool connect();
    void process_events();
    void process_one_event(Event& event);
    void set_error(const std::string& err);
    void flush_tables();
    bool should_process(Event& event);

    Config               m_cnf;                 // The configuration the stream was started with
    std::unique_ptr<SQL> m_sql;                 // Database connection
    std::thread          m_thr;                 // Thread that receives the replication events
    std::atomic<bool>    m_running {true};      // Whether the stream is running
    std::string          m_error;               // The latest error message
    std::string          m_gtid;                // GTID position to start from
    std::string          m_current_gtid;        // GTID of the transaction being processed
    mutable std::mutex   m_lock;

    // Map of active tables
    std::unordered_map<uint64_t, std::unique_ptr<Table>> m_tables;
};

Replicator::Imp::Imp(const Config& cnf)
    : m_cnf(cnf)
    , m_thr(std::thread(&Imp::process_events, this))
{
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

bool Replicator::Imp::connect()
{
    if (m_sql)
    {
        // We already have a connection
        return true;
    }

    bool rval = false;
    std::string gtid_start_pos = "SET @slave_connect_state='" + m_gtid + "'";
    std::string err;

    std::tie(err, m_sql) = SQL::connect(m_cnf.mariadb.servers);

    if (!err.empty())
    {
        set_error(err);
    }
    else
    {
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
        }
        else if (!m_sql->replicate(m_cnf.mariadb.server_id))
        {
            set_error("Failed to open replication channel: " + m_sql->error());
        }
        else
        {
            rval = true;
        }
    }

    if (!rval)
    {
        m_sql.reset();
    }

    return rval;
}

void Replicator::Imp::process_events()
{
    while (m_running)
    {
        if (!connect())
        {
            // We failed to connect to any of the servers, try again in a few seconds
            std::this_thread::sleep_for(std::chrono::seconds(5));
            continue;
        }

        Event event(m_sql->fetch_event());

        if (event)
        {
            if (should_process(event))
            {
                // The CS API likes to throw exceptions
                try
                {
                    process_one_event(event);
                }
                catch (mcsapi::ColumnStoreConfigError& err)
                {
                    set_error(std::string("Could not open table: ") + err.what());
                }
            }
        }
        else
        {
            // Something went wrong, close the connection and connect again at the start of the next loop
            m_sql.reset();
        }
    }
}

std::string to_gtid_string(const MARIADB_RPL_EVENT& event)
{
    std::stringstream ss;
    ss << event.event.gtid.domain_id << '-' << event.server_id << '-' << event.event.gtid.sequence_nr;
    return ss.str();
}

void Replicator::Imp::flush_tables()
{
    for (auto& t : m_tables)
    {
        t.second->flush();
    }
}

bool Replicator::Imp::should_process(Event& event)
{
    bool rval = true;

    if (!m_cnf.mariadb.tables.empty())
    {
        if (event->event_type == TABLE_MAP_EVENT)
        {
            auto tbl = to_string(event->event.table_map.table);
            auto db = to_string(event->event.table_map.database);
            rval = m_cnf.mariadb.tables.count(db + '.' + tbl);
        }
        else if (event->event_type == QUERY_EVENT)
        {
            // For query events, all participating tables must be in the list of accepted tables
            auto db = to_string(event->event.query.database);
            mxs::Buffer buffer(event->event.query.statement.str, event->event.query.statement.length);
            int sz = 0;
            char** tables = qc_get_table_names(buffer.get(), &sz, true);

            for (int i = 0; i < sz; i++)
            {
                std::string tbl = tables[i];

                /**
                 * This is not very reliable (the table name can have a dot in it) and the query classifier
                 * would need to tell us the database and table names separately.
                 */
                if (tbl.find('.') == std::string::npos)
                {
                    tbl = db + '.' + tbl;
                }

                if (m_cnf.mariadb.tables.count(tbl) == 0)
                {
                    rval = false;
                }
            }

            qc_free_table_names(tables, sz);
        }
    }

    return rval;
}

void Replicator::Imp::process_one_event(Event& event)
{
    switch (event->event_type)
    {
    case GTID_EVENT:
        m_current_gtid = to_gtid_string(*event);
        break;

    case XID_EVENT:
        m_gtid = m_current_gtid;
        break;

    case TABLE_MAP_EVENT:
        m_tables[event->event.table_map.table_id] = Table::open(m_cnf, event.get());
        break;

    case QUERY_EVENT:
        flush_tables();
        // TODO: Execute the query
        break;

    case WRITE_ROWS_EVENT_V1:
        {
            const auto& t = m_tables[event->event.rows.table_id];

            if (t)
            {
                t->enqueue(event.release());
            }
        }
        break;

    case UPDATE_ROWS_EVENT_V1:
    case DELETE_ROWS_EVENT_V1:
        if (m_tables[event->event.rows.table_id])
        {
            flush_tables();
            // TODO: Convert to SQL and execute it
        }
        break;

    default:
        // Ignore the event
        break;
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
std::unique_ptr<Replicator> Replicator::start(const Config& cnf)
{
    return std::unique_ptr<Replicator>(new Replicator(cnf));
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

Replicator::Replicator(const Config& cnf)
    : m_imp(new Imp(cnf))
{
}
}
