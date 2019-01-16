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

#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>
#include <thread>

#include <mysql.h>
#include <mariadb_rpl.h>

/**
 * A class that handles processing of replicated events. The actual work is done by
 * subclasses that convert the events into other forms of data.
 */
class REProc
{
public:
    REProc(REProc&) = delete;
    REProc& operator=(REProc&) = delete;

    /**
     * Queue an event for processing
     *
     * The event will be processed the next time either Processor::process is called by the processing thread
     * or Processor::flush is called by the main thread.
     *
     * @param rows The rows event to be queued
     */
    void enqueue(MARIADB_RPL_EVENT* rows)
    {
        std::lock_guard<std::mutex> guard(m_queue_lock);
        m_queue.push_back(rows);
    }

    /**
     * Synchronize with the Process thread and process any pending changes
     */
    void flush()
    {
        std::lock_guard<std::mutex> guard(m_process_lock);
        process_queue();
    }

    virtual ~REProc()
    {
        std::unique_lock<std::mutex> guard(m_process_lock);
        m_running = false;
        m_cv.notify_one();
        guard.unlock();

        m_thr.join();
    }

protected:
    REProc()
        : m_thr(&REProc::run, this)
    {
    }

    /**
     * Process all currently queued events
     *
     * Only one thread can actively process events for a particular table. This keeps
     * the order of the events correct.
     *
     * @return True if events rows were successfully processed
     */
    virtual bool process(const std::vector<MARIADB_RPL_EVENT*>& queue) = 0;

private:

    void process_queue()
    {
        // Grab all available events
        std::unique_lock<std::mutex> guard(m_queue_lock);
        std::vector<MARIADB_RPL_EVENT*> queue;
        m_queue.swap(queue);
        guard.unlock();

        if (!queue.empty())
        {
            // Process all pending events
            process(queue);
        }
    }

    // The "main" function where the processing thread runs
    void run()
    {
        while (m_running)
        {
            std::unique_lock<std::mutex> process_guard(m_process_lock);
            process_queue();

            // Wait until a notification arrives or a timeout is reached.
            // Empirical studies have shown five seconds to be adequate.
            m_cv.wait_for(process_guard, std::chrono::seconds(5));
        }
    }

    std::vector<MARIADB_RPL_EVENT*> m_queue;        // List of events queued for this table
    std::mutex                      m_queue_lock;   // Protects use of m_queue
    std::mutex                      m_process_lock; // Prevents concurrent calls to Processor::process
    std::atomic<bool>               m_running {true};
    std::condition_variable         m_cv;
    std::thread                     m_thr;
};
