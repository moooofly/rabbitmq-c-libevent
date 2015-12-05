/* vim:set ft=c ts=2 sw=2 sts=2 et cindent: */
/*
* ***** BEGIN LICENSE BLOCK *****
* Version: MIT
*
* Portions created by moooofly are Copyright (c) 2013-2015
* moooofly. All Rights Reserved.
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <time.h>
#if !defined(WIN32) && !defined(_WIN32)
	#include <sys/time.h>
#endif

#include "amqp_mq.h"


#if defined(WIN32) || defined(_WIN32)

	int sp_thread_mutex_init( sp_thread_mutex_t * mutex, void * attr )
	{
		*mutex = CreateMutex( NULL, FALSE, NULL );
		return NULL == * mutex ? GetLastError() : 0;
	}

	int sp_thread_mutex_destroy( sp_thread_mutex_t * mutex )
	{
		int ret = CloseHandle( *mutex );
		return 0 == ret ? GetLastError() : 0;
	}

	int sp_thread_mutex_lock( sp_thread_mutex_t * mutex )
	{
		int ret = WaitForSingleObject( *mutex, INFINITE );
		return WAIT_OBJECT_0 == ret ? 0 : GetLastError();
	}

	int sp_thread_mutex_unlock( sp_thread_mutex_t * mutex )
	{
		int ret = ReleaseMutex( *mutex );
		return 0 != ret ? 0 : GetLastError();
	}

	int sp_thread_cond_init( sp_thread_cond_t * cond, void * attr )
	{
		*cond = CreateEvent( NULL, FALSE, FALSE, NULL );
		return NULL == *cond ? GetLastError() : 0;
	}

	int sp_thread_cond_destroy( sp_thread_cond_t * cond )
	{
		int ret = CloseHandle( *cond );
		return 0 == ret ? GetLastError() : 0;
	}

	int sp_thread_cond_wait( sp_thread_cond_t * cond, sp_thread_mutex_t * mutex )
	{
		int ret = 0;

		sp_thread_mutex_unlock( mutex );

		ret = WaitForSingleObject( *cond, INFINITE );

		sp_thread_mutex_lock( mutex );

		return WAIT_OBJECT_0 == ret ? 0 : GetLastError();
	}

	int sp_thread_cond_signal( sp_thread_cond_t * cond )
	{
		int ret = SetEvent( *cond );
		return 0 == ret ? GetLastError() : 0;
	}

	int sp_thread_attr_init( sp_thread_attr_t * attr )
	{
		*attr = 0;
		return 0;
	}
	int sp_thread_attr_destroy( sp_thread_attr_t * attr )
	{
		return 0;
	}

	int sp_thread_attr_setdetachstate( sp_thread_attr_t * attr, int detachstate )
	{
		*attr |= detachstate;
		return 0;
	}

	sp_thread_t sp_thread_self()
	{
		return GetCurrentThreadId();
	}

	int sp_thread_create( sp_thread_t * thread, sp_thread_attr_t * attr, sp_thread_func_t myfunc, void * args )
	{
		// _beginthreadex returns 0 on an error
		HANDLE h = (HANDLE)_beginthreadex( NULL, 0, myfunc, args, 0, thread );
		CloseHandle(h);
		return h > 0 ? 0 : GetLastError();
	}
	
#endif

#if defined(WIN32) || defined(_WIN32)
#define strdup _strdup
#endif
	
// ==================  MQ  =======================

// 用于管理 MQ_ITEM 的 list
static MQ_ITEM *mqi_freelist;
static sp_thread_mutex_t mqi_freelist_lock;


void mq_init( MQ *mq ) {
	if ( NULL == mq )
	{
		fprintf( stderr, "mq_init: why set mq=NULL\n" );
		return;
	}
	sp_thread_mutex_init( &mq->lock, NULL );
// 	sp_thread_cond_init( &mq->cond, NULL );
	mq->head = NULL;
	mq->tail = NULL;
}

void mq_deinit( MQ *mq ) {
	if ( NULL == mq )
	{
		fprintf( stderr, "mq_deinit: why set mq=NULL\n" );
		return;
	}
	sp_thread_mutex_destroy( &mq->lock );
// 	sp_thread_cond_destroy( &mq->cond );
	mq->head = NULL;
	mq->tail = NULL;
}

MQ_ITEM *mq_pop( MQ *mq ) {
	MQ_ITEM *item;

	if ( NULL == mq )
	{
		return NULL;
	}

	sp_thread_mutex_lock( &mq->lock );
	item = mq->head;
	if ( NULL != item ) {
		mq->head = item->next;
		if ( NULL == mq->head )
			mq->tail = NULL;
	}
	sp_thread_mutex_unlock( &mq->lock );

	return item;
}

void mq_push( MQ *mq, MQ_ITEM *item ) {

	if ( NULL == mq || NULL == item )
	{
		return;
	}

	item->next = NULL;

	sp_thread_mutex_lock( &mq->lock );
	if ( NULL == mq->tail )
		mq->head = item;
	else
		mq->tail->next = item;
	mq->tail = item;
// 	sp_thread_cond_signal( &mq->cond );
	sp_thread_mutex_unlock( &mq->lock );

#if !defined(WIN32) && !defined(_WIN32)
	{
		int off = 0;
		struct timeval tv;
		char buf[64] = {0};

		gettimeofday( &tv, NULL );
		off = strftime( buf, sizeof(buf), "%d %b %H:%M:%S.", localtime( &tv.tv_sec ) );
		snprintf( buf + off, sizeof(buf) - off, "%03d", (int)tv.tv_usec/1000 );
		fprintf( stderr, "[MQ] mq_push => timestamp = %s\n", buf );
	}
#endif

}

MQ_ITEM *mqi_new( void ) {
	MQ_ITEM *item = NULL;
	sp_thread_mutex_lock( &mqi_freelist_lock );
	if ( NULL != mqi_freelist ) {
		item = mqi_freelist;
		mqi_freelist = item->next;
	}
	sp_thread_mutex_unlock( &mqi_freelist_lock );

	if ( NULL == item ) {
		int i;

		/* Allocate a bunch of items at once to reduce fragmentation */
		item = (MQ_ITEM *)malloc( sizeof(MQ_ITEM) * ITEMS_PER_ALLOC );
		if ( NULL == item )
		{
			fprintf( stderr, "Out of Memory! Error!\n" );
			return NULL;
		}

		/*
		* Link together all the new items except the first one
		* (which we'll return to the caller) for placement on
		* the freelist.
		*/
		for ( i = 2; i < ITEMS_PER_ALLOC; i++ )
			item[i - 1].next = &item[i];

		sp_thread_mutex_lock( &mqi_freelist_lock );
		item[ITEMS_PER_ALLOC - 1].next = mqi_freelist;
		mqi_freelist = &item[1];
		sp_thread_mutex_unlock( &mqi_freelist_lock );
	}

	return item;
}

// 约束：
// 1. exchange   - 以 '\0' 结束的标准字符串；若值为 "" ，则表示采用 default exchange ；该值不可以为 NULL ；
// 2. routingkey - 以 '\0' 结束的标准字符串；若值为 "" ，则表示采用 fanout 类型的 exchange ；该值不可以为 NULL ；
// 2. content    - 可能是标准字符串，也可能是二进制数据；需要通过 len 指定其长度；
// 3. correlation_id - 任何标准字符串，或者非 RPC 模式下设为 NULL。简单起见最好使用字符串形式的阿拉伯数字，如 "1"、"50" 等
// 4. reply_to   - 以 '\0' 结束的标准字符串，或者非 PRC 模式和 RPC server 下设为 NULL
// 5. 所有 const char* 类型的字符串必须通过拷贝复制的方式使用，不能假设上层使用的是静态字符数组
MQ_ITEM *mqi_prepare( const char *exchange, const char *routingkey, const char *content, size_t len, 
	amqp_boolean_t persistent, amqp_boolean_t rpc_mode, const char *correlation_id, const char *reply_to )
{
	MQ_ITEM *mq_item = NULL;

	if ( NULL == exchange || NULL == routingkey )
	{
		fprintf( stderr, "mqi_prepare: exchange or routingkey is NULl! Error!\n" );
		return NULL;
	}

	// RPC client 必须提供 reply_to 和 correlation_id
	// RPC server 无需提供 reply_to 但必须提供 correlation_id
	if ( RPC_MODE == rpc_mode )
	{
		if ( NULL == correlation_id || '\0' == correlation_id[0] )
		{
			fprintf( stderr, "mqi_prepare: RPC_MODE with correlation_id is NULL! Error!\n" );
			return NULL;
		}
	}

	if ( NULL == content || '\0' == content[0] || 0 == len )
	{
		fprintf( stderr, "mqi_prepare: content is NULL or len is 0! Weird!\n" );
	}

	mq_item = mqi_new();
	if ( NULL == mq_item )
	{
		fprintf( stderr, "mqi_prepare: malloc failed! - 1\n" );
		return NULL;
	}
	memset( mq_item, 0 , sizeof(*mq_item) );
	
	mq_item->exchange = amqp_bytes_malloc_dup_cstring( exchange );
	mq_item->routingkey = amqp_bytes_malloc_dup_cstring( routingkey );
	mq_item->content = amqp_bytes_malloc_dup_binary( content, len );

	switch ( persistent )
	{
	case MSG_PERSISTENT:
	case MSG_NOT_PERSISTENT:
		mq_item->msg_persistent = persistent;
		break;
	default:
		mq_item->msg_persistent = MSG_NOT_PERSISTENT;
		break;
	}

	switch ( rpc_mode )
	{
	case RPC_MODE:
		mq_item->rpc_mode = RPC_MODE;
		mq_item->correlation_id = amqp_bytes_malloc_dup_cstring( correlation_id );
		mq_item->reply_to = amqp_bytes_malloc_dup_cstring( reply_to );
		break;
	case NOT_RPC_MODE:
		mq_item->rpc_mode = NOT_RPC_MODE;
		mq_item->correlation_id = amqp_empty_bytes;
		mq_item->reply_to = amqp_empty_bytes;
		break;
	default:
		mq_item->rpc_mode = NOT_RPC_MODE;
		mq_item->correlation_id = amqp_empty_bytes;
		mq_item->reply_to = amqp_empty_bytes;
		break;
	}

	if ( NULL == mq_item->exchange.bytes || NULL == mq_item->routingkey.bytes )
	{
		fprintf( stderr, "mqi_prepare: malloc failed! - 2\n" );
		mqi_free_all( mq_item );

		return NULL;
	}
	if ( RPC_MODE == rpc_mode )
	{
		if ( NULL == mq_item->correlation_id.bytes )
		{
			fprintf( stderr, "mqi_prepare: malloc failed! - 3\n" );
			mqi_free_all( mq_item );
			return NULL;
		}
	}

	return mq_item;
}

void mqi_free( MQ_ITEM *item )
{
	sp_thread_mutex_lock( &mqi_freelist_lock );
	item->next = mqi_freelist;
	mqi_freelist = item;
	sp_thread_mutex_unlock( &mqi_freelist_lock );
}

void mqi_free_all( MQ_ITEM *item )
{
	if ( NULL == item )
		return;

	if ( RPC_MODE == item->rpc_mode )
	{
		amqp_bytes_free( item->correlation_id );
		amqp_bytes_free( item->reply_to );
	}
	amqp_bytes_free( item->exchange );
	amqp_bytes_free( item->routingkey );
	amqp_bytes_free( item->content );

	mqi_free( item );
	item = NULL;

	return;
}
