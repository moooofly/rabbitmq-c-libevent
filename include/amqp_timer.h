/* vim:set ft=c ts=2 sw=2 sts=2 et cindent: */
/*
 * Copyright 2013 Alan Antonuk
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
#ifndef AMQP_TIMER_H
#define AMQP_TIMER_H

#include <stdint.h>

#include "amqp.h"

#ifdef _WIN32
# ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
# endif
# include <Winsock2.h>
#else
# include <sys/time.h>
#endif

#define AMQP_NS_PER_S 1000000000
#define AMQP_NS_PER_US 1000

#define AMQP_INIT_TIMER(structure) { \
  structure.current_timestamp = 0;   \
  structure.timeout_timestamp = 0;   \
}

typedef struct amqp_timer_t_ {
  uint64_t current_timestamp;
  uint64_t timeout_timestamp;
  uint64_t ns_until_next_timeout;
  struct timeval tv;
} amqp_timer_t;

/* Gets a monotonic timestamp in ns */
AMQP_PUBLIC_FUNCTION
uint64_t
AMQP_CALL
amqp_get_monotonic_timestamp(void);

/* Prepare timeout value and modify timer state based on timer state. */
AMQP_PUBLIC_FUNCTION
int
AMQP_CALL
amqp_timer_update(amqp_timer_t *timer, struct timeval *timeout);

#endif /* AMQP_TIMER_H */

