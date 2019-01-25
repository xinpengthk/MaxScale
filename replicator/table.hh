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

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>
#include <thread>

#include <libmcsapi/mcsapi.h>

#include "config.hh"
#include "processor.hh"
#include "sql.hh"

using Bulk = std::unique_ptr<mcsapi::ColumnStoreBulkInsert>;

// Minimal interface for binlog to native type conversion
class Converter
{
public:
    virtual void setNull(int i) = 0;
    virtual void setColumn(int i, int64_t t) = 0;
    virtual void setColumn(int i, uint64_t t) = 0;
    virtual void setColumn(int i, const std::string& t) = 0;
    virtual void setColumn(int i, double t) = 0;
};

/**
 * A class that converts replicated row events into ColumnStore bulk API writes.
 */
class Table : public REProc
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
    static std::unique_ptr<Table> open(const cdc::Config& cnf, MARIADB_RPL_EVENT* table_map);

    const char* db() const
    {
        return m_database.c_str();
    }

    const char* table() const
    {
        return m_table.c_str();
    }

protected:
    /**
     * Process all currently queued row events
     *
     * Only one thread can actively process events for a particular table. This keeps
     * the order of the events correct.
     *
     * @return True if all rows were successfully processed
     */
    bool process(const std::vector<MARIADB_RPL_EVENT*>& queue) override;

    bool start_transaction() override;
    bool commit_transaction() override;
    void rollback_transaction() override;

private:
    using Driver = std::unique_ptr<mcsapi::ColumnStoreDriver>;
    using Values = std::vector<std::string>;

    Table(const cdc::Config& cnf, MARIADB_RPL_EVENT* table_map);

    // Open a new bulk insert
    bool open_bulk();

    // Open a new SQL connection
    bool open_sql();

    // Convert DELETE_ROWS into string values
    std::vector<Values> get_delete_values(MARIADB_RPL_EVENT* row);

    // Convert UPDATE_ROWS before and after image into string values
    std::vector<std::pair<Values, Values>> get_update_values(MARIADB_RPL_EVENT* row);

    // Converts DESCRIBE result and string values to SQL DELETE statement
    std::string to_sql_delete(const SQL::Result& desc, const Values& values);

    // Converts DESCRIBE result and string values to SQL UPDATE statement
    std::string to_sql_update(const SQL::Result& desc, const Values& before, const Values& after);

    // Executes given ROWS event as an SQL statement
    bool execute_as_sql(MARIADB_RPL_EVENT* row);

    // Processes all available rows and adds them to the bulk load
    bool     process_row(MARIADB_RPL_EVENT* rows, const Bulk& bulk);
    uint8_t* process_data(MARIADB_RPL_EVENT* rows, Converter& t, uint8_t* column_present, uint8_t* row);
    uint8_t* process_numeric_field(int i, uint8_t type, uint8_t* ptr, Converter& t);

    std::vector<uint8_t> m_metadata;                // Table metadata
    std::vector<uint8_t> m_column_types;            // Column types in the table
    std::string          m_table;                   // Table name
    std::string          m_database;                // Database name where the table is located
    Driver               m_driver;                  // The ColumnStore API handle
    Bulk                 m_bulk;
    cdc::Config          m_cnf;
    std::unique_ptr<SQL> m_sql;     // Database connection, used only in replication mode
};
