#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include <mysql.h>
#include <mariadb_rpl.h>
#include <libmcsapi/mcsapi.h>

#include "config.hh"

namespace cdc
{

namespace real
{
// Private namespace for implementation details

/**
 * A class that converts replicated row events into ColumnStore bulk API writes.
 */
class Table
{
public:
    Table(Table&) = delete;
    Table& operator=(Table&) = delete;

    /**
     * Open a new table
     *
     * @param table_map The table map event to create the table from
     *
     * @return The opened table
     */
    static std::unique_ptr<Table> open(const Config& cnf, MARIADB_RPL_EVENT* table_map);

    /**
     * Queue an event for processing
     *
     * The event will be processed the next time Table::process is called.
     *
     * @param rows The rows event to be queued
     */
    void enqueue(MARIADB_RPL_EVENT* rows);

    /**
     * Process all currently queued row events
     *
     * Only one thread can actively process events for a particular table. This keeps
     * the order of the events correct.
     *
     * @return True if all rows were successfully processed
     */
    bool process();

    ~Table();

private:
    using Driver = std::unique_ptr<mcsapi::ColumnStoreDriver>;
    using Bulk = std::unique_ptr<mcsapi::ColumnStoreBulkInsert>;

    Table(const Config& cnf, MARIADB_RPL_EVENT* table_map);
    bool process_row(MARIADB_RPL_EVENT* rows, const Bulk& bulk);

    MARIADB_RPL_EVENT*              m_tm;           // The table map event, used in the conversion process
    std::string                     m_table;        // Table name
    std::string                     m_database;     // Database name where the table is located
    Driver                          m_driver;       // The ColumnStore API handle
    std::vector<MARIADB_RPL_EVENT*> m_queue;        // List of events queued for this table
    std::mutex                      m_queue_lock;   // Protects use of m_queue
    std::mutex                      m_process_lock; // Prevents concurrent calls to Table::process
};
}
}
