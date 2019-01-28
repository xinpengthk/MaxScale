#include <csignal>
#include <iostream>
#include <thread>

#include "config.hh"
#include "replicator.hh"

#include <maxbase/log.hh>
#include <maxbase/stacktrace.hh>

using std::cout;
using std::endl;

volatile sig_atomic_t running = 1;

void set_signal(int sig, void (* handler)(int))
{
    struct sigaction sa = {};
    sa.sa_handler = handler;
    sigaction(sig, &sa, nullptr);
}

void terminate_handler(int sig)
{
    running = 0;
}

void fatal_handler(int sig)
{
    set_signal(sig, SIG_DFL);

    MXB_ALERT("Received fatal signal %d", sig);

    mxb::dump_stacktrace([](const char* symbol, const char* cmd) {
                             MXB_ALERT("%s: %s", symbol, cmd);
                         });

    raise(sig);
}

int main(int argc, char** argv)
{
    for (auto a : {SIGTERM, SIGINT, SIGHUP})
    {
        set_signal(a, terminate_handler);
    }

    for (auto a : {SIGSEGV, SIGABRT, SIGFPE, SIGHUP})
    {
        set_signal(a, fatal_handler);
    }

    mxb::Log log(MXB_LOG_TARGET_STDOUT);
    mxb_log_set_priority_enabled(LOG_INFO, true);
    cdc::Config cnf;
    cnf.cs.server = {"127.0.0.1", 3306, "maxuser", "maxpwd"};
    cnf.cs.xml = "./Columnstore.xml";
    cnf.mariadb.servers.push_back({"127.0.0.1", 3000, "maxuser", "maxpwd"});
    cnf.mariadb.server_id = 1234;
    cnf.mariadb.gtid = argc > 1 ? argv[1] : "";

    auto rpl = cdc::Replicator::start(cnf);

    while (rpl->ok() && running)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    MXB_NOTICE("Shutting down");

    return rpl->ok() ? 0 : 1;
}
