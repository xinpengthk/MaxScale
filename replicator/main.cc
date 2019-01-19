#include <iostream>
#include <thread>

#include "config.hh"
#include "replicator.hh"

#include <maxbase/log.hh>

using std::cout;
using std::endl;

int main(int argc, char** argv)
{
    mxb::Log log(MXB_LOG_TARGET_STDOUT);
    cdc::Config cnf;
    cnf.cs.servers.push_back({"127.0.0.1", 3306, "maxuser", "maxpwd"});
    cnf.cs.xml = "./Columnstore.xml";
    cnf.mariadb.servers.push_back({"127.0.0.1", 3000, "maxuser", "maxpwd"});
    cnf.mariadb.server_id = 1234;
    cnf.mariadb.gtid = argc > 1 ? argv[1] : "";

    auto rpl = cdc::Replicator::start(cnf);

    while (rpl->ok())
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    rpl->stop();

    return 0;
}
