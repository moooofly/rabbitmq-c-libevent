/* vim:set ft=c ts=2 sw=2 sts=2 et cindent: */
/*
* ***** BEGIN LICENSE BLOCK *****
* Version: MIT
*
* Portions created by moooofly are Copyright (c) 2013-2014
* moooofly. All Rights Reserved.
*
* Portions created by Alan Antonuk are Copyright (c) 2012-2013
* Alan Antonuk. All Rights Reserved.
*
* Portions created by VMware are Copyright (c) 2007-2012 VMware, Inc.
* All Rights Reserved.
*
* Portions created by Tony Garnock-Jones are Copyright (c) 2009-2010
* VMware, Inc. and Tony Garnock-Jones. All Rights Reserved.
*
* Permission is hereby granted, free of charge, to any person
* obtaining a copy of this software and associated documentation
* files (the "Software"), to deal in the Software without
* restriction, including without limitation the rights to use, copy,
* modify, merge, publish, distribute, sublicense, and/or sell copies
* of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
* MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
* BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
* ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
* CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
* ***** END LICENSE BLOCK *****
*/

#ifndef __AMQP_MQ_H__
#define __AMQP_MQ_H__

#include <stddef.h>
#include <stdint.h>

#include "amqp.h"

AMQP_BEGIN_DECLS

#if defined(WIN32) || defined(_WIN32)

	#include <winsock2.h>
	#include <process.h>

	typedef unsigned sp_thread_t;
	typedef HANDLE  sp_thread_mutex_t;
	typedef HANDLE  sp_thread_cond_t;
	typedef DWORD   sp_thread_attr_t;

	typedef unsigned sp_thread_result_t;
	#define SP_THREAD_CALL __stdcall
	typedef sp_thread_result_t ( __stdcall * sp_thread_func_t )( void * args );

	int sp_thread_mutex_init( sp_thread_mutex_t * mutex, void * attr );
	int sp_thread_mutex_destroy( sp_thread_mutex_t * mutex );
	int sp_thread_mutex_lock( sp_thread_mutex_t * mutex );
	int sp_thread_mutex_unlock( sp_thread_mutex_t * mutex );

	int sp_thread_cond_init( sp_thread_cond_t * cond, void * attr );
	int sp_thread_cond_destroy( sp_thread_cond_t * cond );
	int sp_thread_cond_wait( sp_thread_cond_t * cond, sp_thread_mutex_t * mutex );
	int sp_thread_cond_signal( sp_thread_cond_t * cond );

	AMQP_PUBLIC_FUNCTION int sp_thread_attr_init( sp_thread_attr_t * attr );
	AMQP_PUBLIC_FUNCTION int sp_thread_attr_destroy( sp_thread_attr_t * attr );
	AMQP_PUBLIC_FUNCTION int sp_thread_attr_setdetachstate( sp_thread_attr_t * attr, int detachstate );
	#define SP_THREAD_CREATE_DETACHED 1

	AMQP_PUBLIC_FUNCTION sp_thread_t sp_thread_self();
	AMQP_PUBLIC_FUNCTION int sp_thread_create( sp_thread_t * thread, sp_thread_attr_t * attr, sp_thread_func_t myfunc, void * args );

#else // !( defined(WIN32) || defined(_WIN32) )

	#include <pthread.h>
	#include <unistd.h>

	typedef void * sp_thread_result_t;
	typedef pthread_mutex_t sp_thread_mutex_t;
	typedef pthread_cond_t  sp_thread_cond_t;
	typedef pthread_t       sp_thread_t;
	typedef pthread_attr_t  sp_thread_attr_t;

	#define sp_thread_mutex_init(m,a)   pthread_mutex_init(m,a)
	#define sp_thread_mutex_destroy(m)  pthread_mutex_destroy(m)
	#define sp_thread_mutex_lock(m)     pthread_mutex_lock(m)
	#define sp_thread_mutex_unlock(m)   pthread_mutex_unlock(m)

	#define sp_thread_cond_init(c,a)    pthread_cond_init(c,a)
	#define sp_thread_cond_destroy(c)   pthread_cond_destroy(c)
	#define sp_thread_cond_wait(c,m)    pthread_cond_wait(c,m)
	#define sp_thread_cond_signal(c)    pthread_cond_signal(c)

	#define sp_thread_attr_init(a)        pthread_attr_init(a)
	#define sp_thread_attr_destroy(a)     pthread_attr_destroy(a)
	#define sp_thread_attr_setdetachstate pthread_attr_setdetachstate
	#define SP_THREAD_CREATE_DETACHED     PTHREAD_CREATE_DETACHED

	#define sp_thread_self    pthread_self
	#define sp_thread_create  pthread_create
	#define sp_thread_detach  pthread_detach

	#define SP_THREAD_CALL
	typedef sp_thread_result_t ( * sp_thread_func_t )( void * args );

#endif

// AMQP_BEGIN_DECLS

// ==================  MQ  =======================

#define ITEMS_PER_ALLOC 64

#define MSG_PERSISTENT     1
#define MSG_NOT_PERSISTENT 2
#define RPC_MODE     1
#define NOT_RPC_MODE 2

/* An item in the message queue. */
typedef struct msg_queue_item MQ_ITEM;
struct msg_queue_item {
	amqp_bytes_t exchange;
	amqp_bytes_t routingkey;
	amqp_bytes_t content;

	amqp_boolean_t msg_persistent;  // 针对单条 msg 设置持久属性

	amqp_boolean_t rpc_mode;        // 标志是否处于 RPC 模式下
	amqp_bytes_t correlation_id;    // 用于 RPC 模式下关联 response 和 request
	amqp_bytes_t reply_to;          // 用于 RPC 模式下告之 callback queue

	amqp_boolean_t ttl_per_msg;		// 是否设置了 per-Message TTL
	amqp_bytes_t expiration;		// 用于设置 per-Message TTL 单位是 ms

	MQ_ITEM  *next;
};

/* A message queue. */
// typedef struct msg_queue MQ;
struct msg_queue {
	MQ_ITEM *head;
	MQ_ITEM *tail;
	sp_thread_mutex_t lock;
// 	sp_thread_cond_t  cond;
};

AMQP_PUBLIC_FUNCTION void mq_init( MQ *mq );
AMQP_PUBLIC_FUNCTION void mq_deinit( MQ *mq );
AMQP_PUBLIC_FUNCTION MQ_ITEM *mq_pop( MQ *mq );
AMQP_PUBLIC_FUNCTION void mq_push( MQ *mq, MQ_ITEM *item );
AMQP_PUBLIC_FUNCTION MQ_ITEM *mqi_new( void );

// 约束：
// 1. exchange   - 以 '\0' 结束的标准字符串；若值为 "" ，则表示采用 default exchange ；该值不可以为 NULL ；
// 2. routingkey - 以 '\0' 结束的标准字符串；若值为 "" ，则表示采用 fanout 类型的 exchange ；该值不可以为 NULL ；
// 2. content    - 可能是标准字符串，也可能是二进制数据；需要通过 len 指定其长度；
// 3. correlation_id - 任何标准字符串，或者非 RPC 模式下设为 NULL。简单起见最好使用字符串形式的阿拉伯数字，如 "1"、"50" 等
// 4. reply_to   - 以 '\0' 结束的标准字符串，或者非 PRC 模式和 RPC server 下设为 NULL
// 5. expiration  - 以 '\0' 结束的标准字符串，单位 ms

// 6. 所有 const char* 类型的字符串必须通过拷贝复制的方式使用，不能假设上层使用的是静态字符数组
AMQP_PUBLIC_FUNCTION MQ_ITEM *mqi_prepare( const char *exchange, const char *routingkey, 
	const char *content, size_t len, 
	amqp_boolean_t persistent, amqp_boolean_t rpc_mode, 
	const char *correlation_id, const char *reply_to,
	amqp_boolean_t ttl_per_msg, const char *expiration );

AMQP_PUBLIC_FUNCTION void mqi_free( MQ_ITEM *item );
AMQP_PUBLIC_FUNCTION void mqi_free_all( MQ_ITEM *item );


// ==================  MQ  =======================

AMQP_END_DECLS

#endif // __AMQP_MQ_H__

