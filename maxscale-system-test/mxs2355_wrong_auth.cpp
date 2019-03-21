/*
 * Copyright (c) 2019 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2020-01-01
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include "testconnections.h"

// Try to connect with mysql client using the plugin "mysql_clear_password". MaxScale should switch back
// to "mysql_native_password".

int main(int argc, char** argv)
{
    TestConnections test(argc, argv);
    const char auth[] = "mysql_clear_password";
    char cmd[256];
    sprintf(cmd, "echo \"quit\" | mysql --default-auth=%s --host=%s --port=%i "
                 "--user=%s --password=%s",
            auth, test.maxscales->hostname[0], test.maxscales->rwsplit_port[0],
            test.maxscales->user_name, test.maxscales->password);
    int rval = system(cmd);
    test.expect(rval == 0, "Logging with %s failed.", auth);
    if (test.ok())
    {
        printf("Logging with %s succeeded.\n", auth);
    }
    return test.global_result;
}