#include <iostream>
#include <thread>

#include "config.hh"
#include "replicator.hh"

using std::cout;
using std::endl;

int main(int argc, char** argv)
{
    cdc::Config cnf;
    cnf.cs.servers.push_back({"127.0.0.1", 3306, "maxuser", "maxpwd"});
    cnf.mariadb.servers.push_back({"127.0.0.1", 3000, "maxuser", "maxpwd"});
    cnf.mariadb.server_id = 1234;

    auto rpl = cdc::Replicator::start(cnf);

    while (true)
    {
        auto err = rpl->error();
        if (!err.empty())
        {
            cout << err << endl;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    rpl->stop();

    return 0;
}
