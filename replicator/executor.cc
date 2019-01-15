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
        m_sql = std::move(SQL::connect(m_servers).second);

        if (!m_sql || !m_sql->query("SET default_storage_engine=COLUMNSTORE"))
        {
            rval = false;
        }
    }

    return rval;
}

bool SQLExecutor::process(const std::vector<MARIADB_RPL_EVENT*>& queue)
{
    bool rval = connect();

    if (rval)
    {
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
    }

    return true;
}
