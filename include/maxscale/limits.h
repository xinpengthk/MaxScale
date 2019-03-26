/*
 * Copyright (c) 2018 MariaDB Corporation Ab
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

/**
 * @file limits.h
 *
 * This file contains defines for hard limits of MaxScale.
 */

#include <maxscale/cdefs.h>

MXS_BEGIN_DECLS

/**
 * MXS_MAX_THREADS
 *
 * The maximum number of threads/workers.
 */
#define MXS_MAX_THREADS 128

/**
 * MXS_MAX_ROUTING_THREADS
 *
 * The maximum number of routing threads/workers.
 */
#define MXS_MAX_ROUTING_THREADS 100

MXS_END_DECLS
