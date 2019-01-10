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

#include "table.hh"

namespace cdc
{

namespace real
{

Table::Table(const Config& cnf, MARIADB_RPL_EVENT* table_map)
    : m_tm(table_map)
    , m_table(m_tm->event.table_map.table.str, m_tm->event.table_map.table.length)
    , m_database(m_tm->event.table_map.database.str, m_tm->event.table_map.database.length)
    , m_driver(new mcsapi::ColumnStoreDriver(cnf.cs.xml))
{
}

// static
std::unique_ptr<Table> Table::open(const Config& cnf, MARIADB_RPL_EVENT* table_map)
{
    return std::unique_ptr<Table>(new Table(cnf, table_map));
}

void Table::enqueue(MARIADB_RPL_EVENT* rows)
{
    std::lock_guard<std::mutex> guard(m_queue_lock);
    m_queue.push_back(rows);
}

bool Table::process()
{
    // This allows "flushing" of all Tables by the main replication thread by
    // iterating over all open tables and calling the process method. As new
    // events are only added by the replication thread, no new events are
    // added. This is required to synchronize the state of the tables before
    // query events are processed (DDLs etc. would cause problems if done
    // mid-insert).
    std::lock_guard<std::mutex> process_guard(m_process_lock);

    // Grab all available events
    std::unique_lock<std::mutex> guard(m_queue_lock);
    std::vector<MARIADB_RPL_EVENT*> queue;
    m_queue.swap(queue);
    guard.unlock();

    bool rval = true;

    if (!queue.empty())
    {
        // Open a new bulk insert for this batch of rows
        Bulk bulk(m_driver->createBulkInsert(m_database, m_table, 0, 0));

        for (auto row : queue)
        {
            if (!process_row(row, bulk))
            {
                rval = false;
                break;
            }
        }

        if (rval)
        {
            // Successfully processed all rows, commit the batch
            bulk->commit();
        }
        else
        {
            // Failed to process rows, roll back the transaction
            bulk->rollback();
        }
    }

    return rval;
}

// Calculates how many bytes the metadata is for a particular type
int metadata_length(uint8_t type)
{
    switch (type)
    {
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_NEWDECIMAL:
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
        return 2;

    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIME2:
        return 1;

    default:
        return 0;
    }
}

bool Table::process_row(MARIADB_RPL_EVENT* rows, const Bulk& bulk)
{
    uint8_t* metadata = (uint8_t*)m_tm->event.table_map.metadata.str;
    uint8_t* column_present = (uint8_t*)rows->event.rows.column_bitmap;
    uint8_t* row = (uint8_t*)rows->event.rows.row_data;
    uint8_t* null_ptr = row;
    uint8_t offset = 1;

    // Jump over the null bitmap
    row += (rows->event.rows.column_count + 7) / 8;

    for (int i = 0; i < rows->event.rows.column_count; i++)
    {
        if (*column_present & offset)
        {
            if (*null_ptr & offset)
            {
                bulk->setNull(i);
            }
            else
            {
                // TODO: Decode the row data into native types
                std::string value = "dummy value";
                bulk->setColumn(i, value);
            }
        }

        offset <<= 1;

        if (offset == 0)
        {
            offset = 1;
            ++null_ptr;
            ++column_present;
        }

        // Use this version when https://github.com/MariaDB/mariadb-connector-c/pull/93 is merged
        // metadata += metadata_length(m_tm->event.table_map.column_types.str[i]);
        metadata += metadata_length(m_tm->event.table_map.column_types[i]);
    }

    bulk->writeRow();

    return true;
}

Table::~Table()
{
    mariadb_free_rpl_event(m_tm);
}
}
}
