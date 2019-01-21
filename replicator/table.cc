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

#define MXB_MODULE_NAME "BulkLoad"

#include "table.hh"

#include <maxscale/mysql_binlog.h>
#include <maxbase/log.h>
#include <maxbase/assert.h>

Table::Table(const cdc::Config& cnf, MARIADB_RPL_EVENT* table_map)
    : m_metadata(table_map->event.table_map.metadata.str,
                 table_map->event.table_map.metadata.str
                 + table_map->event.table_map.metadata.length)
    , m_column_types(table_map->event.table_map.column_types.str,
                     table_map->event.table_map.column_types.str
                     + table_map->event.table_map.column_types.length)
    , m_table(table_map->event.table_map.table.str,
              table_map->event.table_map.table.length)
    , m_database(table_map->event.table_map.database.str,
                 table_map->event.table_map.database.length)
    , m_driver(new mcsapi::ColumnStoreDriver(cnf.cs.xml))
{
}

// static
std::unique_ptr<Table> Table::open(const cdc::Config& cnf, MARIADB_RPL_EVENT* table_map)
{
    return std::unique_ptr<Table>(new Table(cnf, table_map));
}

bool Table::start_transaction()
{
    bool rval = false;

    try
    {
        m_bulk.reset(m_driver->createBulkInsert(m_database, m_table, 0, 0));
        rval = true;
    }
    catch (const std::exception& ex)
    {
        MXB_ERROR("%s", ex.what());
    }

    return rval;
}

bool Table::commit_transaction()
{
    bool rval = false;

    try
    {
        m_bulk->commit();
        m_bulk.reset();
        rval = true;
    }
    catch (const std::exception& ex)
    {
        MXB_ERROR("%s", ex.what());
    }

    return rval;
}

void Table::rollback_transaction()
{
    try
    {
        m_bulk->rollback();
        m_bulk.reset();
    }
    catch (const std::exception& ex)
    {
        MXB_ERROR("%s", ex.what());
    }
}

bool Table::process(const std::vector<MARIADB_RPL_EVENT*>& queue)
{
    bool rval = true;

    for (auto row : queue)
    {
        if (!process_row(row, m_bulk))
        {
            rval = false;
            break;
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

constexpr int64_t get_byte8(uint8_t* ptr)
{
    return (int64_t)ptr[0] + ((int64_t)ptr[1] << 8) + ((int64_t)ptr[2] << 16)
           + ((int64_t)ptr[3] << 24) + ((int64_t)ptr[4] << 32) + ((int64_t)ptr[5] << 40)
           + ((int64_t)ptr[6] << 48) + ((int64_t)ptr[7] << 56);
}

constexpr int32_t get_byte4(uint8_t* ptr)
{
    return ptr[0] + ((int32_t)ptr[1] << 8) + ((int32_t)ptr[2] << 16) + ((int32_t)ptr[3] << 24);
}

constexpr int32_t get_byte3(uint8_t* ptr)
{
    return ptr[0] + ((int32_t)ptr[1] << 8) + ((int32_t)ptr[2] << 16);
}

constexpr int16_t get_byte2(uint8_t* ptr)
{
    return ptr[0] + ((int16_t)ptr[1] << 8);
}

int64_t get_byte(uint8_t* ptr, int bytes)
{
    switch (bytes)
    {
    case 1:
        return *ptr;

    case 2:
        return get_byte2(ptr);

    case 3:
        return get_byte3(ptr);

    case 4:
        return get_byte4(ptr);

    case 8:
        return get_byte8(ptr);

    default:
        return 0;
    }
}

uint8_t* Table::process_numeric_field(int i, uint8_t type, uint8_t* ptr, const Bulk& bulk)
{
    switch (type)
    {
    case TABLE_COL_TYPE_LONG:
        bulk->setColumn(i, get_byte4(ptr));
        return ptr + 4;

    case TABLE_COL_TYPE_FLOAT:
        bulk->setColumn(i, *(float*)ptr);
        return ptr + 4;

    case TABLE_COL_TYPE_INT24:
        bulk->setColumn(i, get_byte3(ptr));
        return ptr + 3;

    case TABLE_COL_TYPE_LONGLONG:
        bulk->setColumn(i, get_byte8(ptr));
        return ptr + 8;

    case TABLE_COL_TYPE_DOUBLE:
        bulk->setColumn(i, *(double*)ptr);
        return ptr + 8;

    case TABLE_COL_TYPE_SHORT:
        bulk->setColumn(i, get_byte2(ptr));
        return ptr + 2;

    case TABLE_COL_TYPE_TINY:
        bulk->setColumn(i, (int8_t)*ptr);
        return ptr + 1;

    default:
        break;
    }

    return ptr;
}


bool Table::process_row(MARIADB_RPL_EVENT* rows, const Bulk& bulk)
{
    uint8_t* row = (uint8_t*)rows->event.rows.row_data;
    row = process_data(rows, bulk, (uint8_t*)rows->event.rows.column_bitmap, row);

    if (rows->event.rows.type == UPDATE_ROWS)
    {
        m_bulk->writeRow();
        process_data(rows, bulk, (uint8_t*)rows->event.rows.column_update_bitmap, row);
    }

    m_bulk->writeRow();
    return true;
}

uint8_t* Table::process_data(MARIADB_RPL_EVENT* rows, const Bulk& bulk, uint8_t* column_present, uint8_t* row)
{
    uint8_t* metadata = m_metadata.data();
    uint8_t* null_ptr = row;
    uint8_t offset = 1;

    // Jump over the null bitmap
    row += (rows->event.rows.column_count + 7) / 8;

    for (uint32_t i = 0; i < rows->event.rows.column_count; i++)
    {
        mxb_assert(m_column_types.size() == rows->event.rows.column_count);

        if (*column_present & offset)
        {
            if (*null_ptr & offset)
            {
                bulk->setNull(i);
            }
            else if (column_is_fixed_string(m_column_types[i]))
            {
                /** ENUM and SET are stored as STRING types with the type stored
                 * in the metadata. */
                if (fixed_string_is_enum(*metadata))
                {
                    uint8_t val[*(metadata + 1)];
                    uint64_t bytes = unpack_enum(row, metadata, val);
                    bulk->setColumn(i, get_byte(row, std::min(bytes, 8UL)));
                    row += bytes;
                }
                else
                {
                    /**
                     * The first byte in the metadata stores the real type of
                     * the string (ENUM and SET types are also stored as fixed
                     * length strings).
                     *
                     * The first two bits of the second byte contain the XOR'ed
                     * field length but as that information is not relevant for
                     * us, we just use this information to know whether to read
                     * one or two bytes for string length.
                     */

                    uint16_t meta = *(metadata + 1) + (*metadata << 8);
                    int bytes = 0;
                    uint16_t extra_length = (((meta >> 4) & 0x300) ^ 0x300);
                    uint16_t field_length = (meta & 0xff) + extra_length;

                    if (field_length > 255)
                    {
                        bytes = row[0] + (row[1] << 8);
                        row += 2;
                    }
                    else
                    {
                        bytes = *row++;
                    }

                    bulk->setColumn(i, std::string((char*)row, bytes));
                    row += bytes;
                }
            }
            else if (column_is_bit(m_column_types[i]))
            {
                uint8_t len = *(metadata + 1);
                uint8_t bit_len = *metadata > 0 ? 1 : 0;
                size_t bytes = len + bit_len;

                // TODO: Figure out how this works
                bulk->setColumn(i, 0xdead);
                row += bytes;
            }
            else if (column_is_decimal(m_column_types[i]))
            {
                double f_value = 0.0;
                row += unpack_decimal_field(row, metadata, &f_value);
                bulk->setColumn(i, f_value);
            }
            else if (column_is_variable_string(m_column_types[i]))
            {
                size_t sz;
                int bytes = *metadata | *(metadata + 1) << 8;
                if (bytes > 255)
                {
                    sz = get_byte2(row);
                    row += 2;
                }
                else
                {
                    sz = *row;
                    row++;
                }

                bulk->setColumn(i, std::string((char*)row, sz));
                row += sz;
            }
            else if (column_is_blob(m_column_types[i]))
            {
                uint8_t bytes = *metadata;
                uint64_t len = get_byte(row, bytes);
                row += bytes;
                bulk->setColumn(i, std::string((char*)row, len));
                row += len;
            }
            else if (column_is_temporal(m_column_types[i]))
            {
                char buf[80];
                struct tm tm;
                row += unpack_temporal_value(m_column_types[i], row, metadata, 0, &tm);
                format_temporal_value(buf, sizeof(buf), m_column_types[i], &tm);
                bulk->setColumn(i, buf);
            }
            /** All numeric types (INT, LONG, FLOAT etc.) */
            else
            {
                row = process_numeric_field(i, m_column_types[i], row, bulk);
            }
        }

        offset <<= 1;

        if (offset == 0)
        {
            offset = 1;
            ++null_ptr;
            ++column_present;
        }

        metadata += metadata_length(m_column_types[i]);
    }

    return row;
}
