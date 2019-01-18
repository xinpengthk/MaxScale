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

#define MXB_MODULE_NAME "SQLExecutor"

#include "executor.hh"

#include <maxbase/log.h>

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
            MXB_ERROR("%s", res.first.c_str());
            rval = false;
        }
        else if (!m_sql->query("SET default_storage_engine=COLUMNSTORE")
                 || !m_sql->query("SET autocommit=0"))
        {
            MXB_ERROR("%s", m_sql->error().c_str());
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

        auto db = to_string(event->event.query.database);
        auto stmt = to_string(event->event.query.statement);

        // This is probably quite close to what the server actually does to execute query events
        if ((!db.empty() && !m_sql->query("USE " + db)) || !m_sql->query(stmt))
        {
            MXB_ERROR("%s", m_sql->error().c_str());
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
        MXB_ERROR("%s", m_sql->error().c_str());
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
