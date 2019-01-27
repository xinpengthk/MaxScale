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

#include <sstream>

#include <maxscale/mysql_binlog.h>
#include <maxbase/log.h>
#include <maxbase/assert.h>

class BulkConverter : public Converter
{
public:
    BulkConverter(const Bulk& bulk)
        : m_bulk(bulk)
    {
    }

    void setNull(int i) override
    {
        m_bulk->setNull(i);
    }

    void setColumn(int i, int64_t t) override
    {
        m_bulk->setColumn(i, t);
    }

    void setColumn(int i, uint64_t t) override
    {
        m_bulk->setColumn(i, t);
    }

    void setColumn(int i, const std::string& t) override
    {
        m_bulk->setColumn(i, t);
    }

    void setColumn(int i, double t) override
    {
        m_bulk->setColumn(i, t);
    }

private:
    const Bulk& m_bulk;
};

class StringConverter : public Converter
{
public:
    void setNull(int i) override
    {
        m_values.push_back("NULL");
    }

    void setColumn(int i, int64_t t) override
    {
        m_values.push_back(std::to_string(t));
    }

    void setColumn(int i, uint64_t t) override
    {
        m_values.push_back(std::to_string(t));
    }

    void setColumn(int i, const std::string& t) override
    {
        m_values.push_back('\'' + t + '\'');
    }

    void setColumn(int i, double t) override
    {
        m_values.push_back(std::to_string(t));
    }

    const std::vector<std::string>& values() const
    {
        return m_values;
    }

private:
    std::vector<std::string> m_values;
};

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
    , m_cnf(cnf)
{
    update_table_description();
}

// static
std::unique_ptr<Table> Table::open(const cdc::Config& cnf, MARIADB_RPL_EVENT* table_map)
{
    return std::unique_ptr<Table>(new Table(cnf, table_map));
}

bool Table::open_bulk()
{
    bool rval = false;

    try
    {
        if (!m_bulk)
        {
            m_bulk.reset(m_driver->createBulkInsert(m_database, m_table, 0, 0));
        }
        rval = true;
    }
    catch (const std::exception& ex)
    {
        MXB_ERROR("%s", ex.what());
    }

    return rval;
}

bool Table::open_sql()
{
    std::string err;

    if (!m_sql)
    {
        std::tie(err, m_sql) = SQL::connect({m_cnf.cs.server});

        if (!err.empty())
        {
            MXB_ERROR("%s", err.c_str());
        }
    }

    return err.empty();
}

bool Table::start_transaction()
{
    // The transaction is started when the first event is processed
    return true;
}

bool Table::commit_transaction()
{
    bool rval = false;

    try
    {
        if (m_bulk)
        {
            m_bulk->commit();
            m_bulk.reset();
        }
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
        if (m_bulk)
        {
            m_bulk->rollback();
            m_bulk.reset();
        }
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

uint8_t* Table::process_numeric_field(int i, uint8_t type, uint8_t* ptr, Converter& c)
{
    switch (type)
    {
    case MYSQL_TYPE_LONG:
        c.setColumn(i, (int64_t)get_byte4(ptr));
        return ptr + 4;

    case MYSQL_TYPE_FLOAT:
        c.setColumn(i, *(float*)ptr);
        return ptr + 4;

    case MYSQL_TYPE_INT24:
        c.setColumn(i, (int64_t)get_byte3(ptr));
        return ptr + 3;

    case MYSQL_TYPE_LONGLONG:
        c.setColumn(i, get_byte8(ptr));
        return ptr + 8;

    case MYSQL_TYPE_DOUBLE:
        c.setColumn(i, *(double*)ptr);
        return ptr + 8;

    case MYSQL_TYPE_SHORT:
        c.setColumn(i, (int64_t)get_byte2(ptr));
        return ptr + 2;

    case MYSQL_TYPE_TINY:
        c.setColumn(i, (int64_t)(int8_t)*ptr);
        return ptr + 1;

    default:
        break;
    }

    return ptr;
}

bool Table::process_row(MARIADB_RPL_EVENT* rows, const Bulk& bulk)
{
    bool rval = true;
    uint8_t* row = (uint8_t*)rows->event.rows.row_data;
    uint8_t* end = (uint8_t*)rows->event.rows.row_data + rows->event.rows.row_data_size;
    BulkConverter conv(bulk);

    // TODO: Add event metadata fields to the created table if required (GTID, event type etc.)

    switch (rows->event.rows.type)
    {
    case DELETE_ROWS:
    case UPDATE_ROWS:
        // If we have an open bulk insert, we need to commit and close it to release the locks on the table
        if (open_sql() && commit_transaction())
        {
            rval = execute_as_sql(rows);
        }
        else
        {
            rval = false;
        }
        break;

    case WRITE_ROWS:
        if (open_bulk())
        {
            while (row < end)
            {
                row = process_data(rows, conv, (uint8_t*)rows->event.rows.column_bitmap, row);
                mxb_assert(row <= end);
                m_bulk->writeRow();
            }
        }
        else
        {
            rval = false;
        }
        break;
    }

    return rval;
}

uint8_t* Table::process_data(MARIADB_RPL_EVENT* rows, Converter& conv, uint8_t* column_present, uint8_t* row)
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
                conv.setNull(i);
            }
            else if (column_is_fixed_string(m_column_types[i]))
            {
                /** ENUM and SET are stored as STRING types with the type stored
                 * in the metadata. */
                if (fixed_string_is_enum(*metadata))
                {
                    uint8_t val[*(metadata + 1)];
                    uint64_t bytes = unpack_enum(row, metadata, val);
                    conv.setColumn(i, get_byte(row, std::min(bytes, 8UL)));
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

                    conv.setColumn(i, std::string((char*)row, bytes));
                    row += bytes;
                }
            }
            else if (column_is_bit(m_column_types[i]))
            {
                uint8_t len = *(metadata + 1);
                uint8_t bit_len = *metadata > 0 ? 1 : 0;
                size_t bytes = len + bit_len;

                // TODO: Figure out how this works
                conv.setColumn(i, (int64_t)0xdead);
                row += bytes;
            }
            else if (column_is_decimal(m_column_types[i]))
            {
                double f_value = 0.0;
                row += unpack_decimal_field(row, metadata, &f_value);
                conv.setColumn(i, f_value);
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

                conv.setColumn(i, std::string((char*)row, sz));
                row += sz;
            }
            else if (column_is_blob(m_column_types[i]))
            {
                uint8_t bytes = *metadata;
                uint64_t len = get_byte(row, bytes);
                row += bytes;
                conv.setColumn(i, std::string((char*)row, len));
                row += len;
            }
            else if (column_is_temporal(m_column_types[i]))
            {
                char buf[80];
                struct tm tm;
                row += unpack_temporal_value(m_column_types[i], row, metadata, 0, &tm);
                format_temporal_value(buf, sizeof(buf), m_column_types[i], &tm);
                conv.setColumn(i, std::string(buf));
            }
            /** All numeric types (INT, LONG, FLOAT etc.) */
            else
            {
                row = process_numeric_field(i, m_column_types[i], row, conv);
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

std::vector<Table::Values> Table::get_delete_values(MARIADB_RPL_EVENT* event)
{
    uint8_t* row = (uint8_t*)event->event.rows.row_data;
    uint8_t* end = (uint8_t*)event->event.rows.row_data + event->event.rows.row_data_size;
    std::vector<Table::Values> rval;

    while (row < end)
    {
        StringConverter conv;
        row = process_data(event, conv, (uint8_t*)event->event.rows.column_bitmap, row);
        rval.push_back(conv.values());
    }
    return rval;
}

std::vector<std::pair<Table::Values, Table::Values>> Table::get_update_values(MARIADB_RPL_EVENT* event)
{
    uint8_t* row = (uint8_t*)event->event.rows.row_data;
    uint8_t* end = (uint8_t*)event->event.rows.row_data + event->event.rows.row_data_size;
    std::vector<std::pair<Table::Values, Table::Values>> rval;

    while (row < end)
    {
        StringConverter before;
        StringConverter after;
        row = process_data(event, before, (uint8_t*)event->event.rows.column_bitmap, row);
        row = process_data(event, after, (uint8_t*)event->event.rows.column_update_bitmap, row);
        rval.push_back({before.values(), after.values()});
    }

    return rval;
}

std::string Table::to_sql_delete(const Table::Values& values)
{
    std::stringstream ss;
    ss << "DELETE FROM `" << m_database << "`.`" << m_table << "` WHERE ";

    for (size_t i = 0; i < m_fields.size(); i++)
    {
        if (i != 0)
        {
            ss << " AND ";
        }

        // Use IS instead of = when comparing fields to SQL NULLs
        const char* operand = values[i] == "NULL" ? "IS" : "=";
        ss << "`" << m_fields[i].id << "` " << operand << " " << values[i];
    }

    // Using the LIMIT 1 clause makes sure each row event targets only one record in the database
    ss << " LIMIT 1";

    return ss.str();
}

std::string Table::to_sql_update(const Table::Values& before,
                                 const Table::Values& after)
{
    std::stringstream ss;
    ss << "UPDATE `" << m_database << "`.`" << m_table << "` SET ";

    for (size_t i = 0; i < m_fields.size(); i++)
    {
        if (i != 0)
        {
            ss << ",";
        }

        // Use IS instead of = when comparing fields to SQL NULLs
        const char* operand = after[i] == "NULL" ? "IS" : "=";
        ss << "`" << m_fields[i].id << "` " << operand << " " << after[i];
    }

    ss << " WHERE ";

    for (size_t i = 0; i < m_fields.size(); i++)
    {
        if (i != 0)
        {
            ss << " AND ";
        }

        // Use IS instead of = when comparing fields to SQL NULLs
        const char* operand = before[i] == "NULL" ? "IS" : "=";
        ss << "`" << m_fields[i].id << "` " << operand << " " << before[i];
    }

    // Using the LIMIT 1 clause makes sure each row event targets only one record in the database
    ss << " LIMIT 1";

    return ss.str();
}

bool Table::execute_as_sql(MARIADB_RPL_EVENT* row)
{
    bool rval = false;

    std::vector<std::string> statements = {"BEGIN"};

    if (row->event.rows.type == UPDATE_ROWS)
    {
        for (const auto& p : get_update_values(row))
        {
            statements.push_back(to_sql_update(p.first, p.second));
            MXB_INFO("%s", statements.back().c_str());
        }
    }
    else
    {
        mxb_assert(row->event.rows.type == DELETE_ROWS);
        for (const auto& values : get_delete_values(row))
        {
            statements.push_back(to_sql_delete(values));
            MXB_INFO("%s", statements.back().c_str());
        }
    }

    statements.push_back("COMMIT");

    // Execute the converted SQL statements
    if (m_sql->query(statements))
    {
        rval = true;
    }

    if (!rval)
    {
        MXB_ERROR("%s", m_sql->error().c_str());
    }

    return rval;
}

bool Table::update_table_description()
{
    bool rval = false;

    if (open_sql() && m_sql->query("DESCRIBE `%s`.`%s`", m_database.c_str(), m_table.c_str()))
    {
        rval = true;
        m_fields.clear();

        for (const auto& row : m_sql->fetch())
        {
            m_fields.push_back({row[0], row[1], row[4], row[2] == "NO"});
        }
    }

    return rval;
}
