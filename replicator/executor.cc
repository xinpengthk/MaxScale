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

#include "executor.hh"

SQLExecutor::SQLExecutor(const std::vector<cdc::Server>& servers)
    : m_servers(servers)
{
}

bool SQLExecutor::connect()
{
    bool rval = true;

    if (!m_sql)
    {
        auto res = SQL::connect(m_servers);
        m_sql = std::move(res.second);

        if (!m_sql)
        {
            set_error(res.first);
            rval = false;
        }
        else if (!m_sql->query("SET default_storage_engine=COLUMNSTORE")
                 || !m_sql->query("SET autocommit=0"))
        {
            set_error(m_sql->error());
            m_sql.reset();
            rval = false;
        }
    }

    return rval;
}

bool SQLExecutor::process(const std::vector<MARIADB_RPL_EVENT*>& queue)
{
    // Database connection was created in SQLExecutor::start_transaction
    for (MARIADB_RPL_EVENT* event : queue)
    {
        // TODO: Filter out ENGINE=... parts and index definitions from CREATE and ALTER statements

        // This is probably quite close to what the server actually does to execute query events
        if (!m_sql->query("USE " + to_string(event->event.query.database))
            || !m_sql->query(to_string(event->event.query.statement)))
        {
            m_sql.reset();
            return false;
        }
    }

    return true;
}

bool SQLExecutor::start_transaction()
{
    return connect();
}

bool SQLExecutor::commit_transaction()
{
    bool rval = m_sql->query("COMMIT");

    if (!rval)
    {
        set_error(m_sql->error());
    }

    return rval;
}


void SQLExecutor::rollback_transaction()
{
    if (m_sql)
    {
        m_sql->query("ROLLBACK");
    }
}
