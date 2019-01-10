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
#include "config.hh"

namespace cdc
{

namespace real
{

//
// Private implementation details
//

// The main class that drives the whole conversion process
class Replicator
{
public:
    Replicator& operator=(Replicator&) = delete;
    Replicator(Replicator&) = delete;

    // Creates a new replication stream and starts it
    static std::pair<std::string, std::unique_ptr<Replicator>> start(const std::string& cnf);

    // Stops a running replication stream
    void stop();

    // Get error message
    std::string error() const;

    ~Replicator();

private:
    Replicator(const Config& cnf);
    bool connect();
    bool run();
    void process_events(std::promise<bool> promise);
    void set_error(const std::string& err);

    Config             m_cnf;            // The configuration the stream was started with
    MYSQL*             m_mysql {nullptr};// Database connection
    MARIADB_RPL*       m_rpl {nullptr};  // Replication handle
    std::thread        m_thr;            // Thread that receives the replication events
    std::atomic<bool>  m_running {true}; // Whether the stream is running
    std::string        m_error;          // The latest error message
    std::string        m_gtid;           // GTID position to start from
    mutable std::mutex m_lock;

    // Map of active tables
    std::unordered_map<uint64_t, std::unique_ptr<Table>> m_tables;
};

Replicator::Replicator(const Config& cnf)
    : m_cnf(cnf)
{
}

// static
std::pair<std::string, std::unique_ptr<Replicator>> Replicator::start(const std::string& path)
{
    std::unique_ptr<Replicator> rval;
    Config config;
    std::string err;

    std::tie(err, config) = process_options(path);

    if (err.empty())
    {
        rval.reset(new(std::nothrow) Replicator(config));

        if (rval && !rval->run())
        {
            rval.reset();
        }
    }

    return {err, std::move(rval)};
}

void Replicator::stop()
{
    if (m_running)
    {
        m_running = false;
        m_thr.join();
    }
}

std::string Replicator::error() const
{
    std::lock_guard<std::mutex> guard(m_lock);
    return m_error;
}

void Replicator::set_error(const std::string& err)
{
    std::lock_guard<std::mutex> guard(m_lock);
    m_error = err;
}

bool Replicator::run()
{
    std::promise<bool> promise;
    auto future = promise.get_future();

    //Start the thread and wait for the result
    m_thr = std::thread(&Replicator::process_events, this, std::move(promise));
    future.wait();

    return future.get();
}

bool Replicator::connect()
{
    std::string gtid_start_pos = "SET @slave_connect_state='" + m_gtid + "'";

    if (!(m_mysql = mysql_init(nullptr)))
    {
        set_error("Connection initialization failed");
        return false;
    }

    if (!mysql_real_connect(m_mysql, m_cnf.mariadb.host.c_str(), m_cnf.mariadb.user.c_str(),
                            m_cnf.mariadb.password.c_str(), nullptr, m_cnf.mariadb.port, nullptr, 0))
    {
        set_error("Connection creation failed: " + std::string(mysql_error(m_mysql)));
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


    for (auto a : queries)
    {
        if (mysql_query(m_mysql, a.c_str()))
        {
            set_error("Failed to prepare connection: " + std::string(mysql_error(m_mysql)));
            return false;
        }
    }

    if (!(m_rpl = mariadb_rpl_init(m_mysql)))
    {
        set_error("Failed to initialize replication context");
        return false;
    }

    mariadb_rpl_optionsv(m_rpl, MARIADB_RPL_SERVER_ID, &m_cnf.mariadb.server_id);

    if (mariadb_rpl_open(m_rpl))
    {
        set_error("Failed to open replication channel");
        return false;
    }

    return true;
}

void Replicator::process_events(std::promise<bool> promise)
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

Replicator::~Replicator()
{
    if (m_running)
    {
        stop();
    }

    mariadb_rpl_close(m_rpl);
    mysql_close(m_mysql);
}

}

//
// The public API
//

// static
std::pair<std::string, std::unique_ptr<Replicator>> Replicator::start(const std::string& path)
{
    std::unique_ptr<Replicator> rval;
    std::unique_ptr<real::Replicator> real;
    std::string error;
    std::tie(error, real) = real::Replicator::start(path);

    if (real)
    {
        rval.reset((new(std::nothrow) Replicator(std::move(real))));
    }

    return {error, std::move(rval)};
}

void Replicator::stop()
{
    m_real->stop();
}

std::string Replicator::error() const
{
    return m_real->error();
}

Replicator::~Replicator()
{
}

Replicator::Replicator(std::unique_ptr<real::Replicator> real)
    : m_real(std::move(real))
{
}

}
