/* vim:set ft=c ts=2 sw=2 sts=2 et cindent: */
/*
* ***** BEGIN LICENSE BLOCK *****
* Version: MIT
*
* Portions created by moooofly are Copyright (c) 2013-2016
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "amqp_private.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>


extern void fsm( amqp_connection_state_t conn );

struct event_base* amqp_create_evbase_with_lock()
{
	struct event_base *evbase = NULL;

#if !defined(WIN32) && !defined(_WIN32)
	if ( 0 != evthread_use_pthreads() )
#else
	if ( 0 != evthread_use_windows_threads() )
#endif
	{
		fprintf( stderr, "amqp_create_evbase_with_lock => evthread_use_pthreads failed! Error!" );
		return NULL;
	}

	evbase = event_base_new();
	if ( NULL == evbase )
	{
		fprintf( stderr, "amqp_create_evbase_with_lock => event_base_new failed! Error!" );
		return NULL;
	}
	 return evbase;
}

void amqp_destroy_evbase( struct event_base *evbase )
{
	if ( NULL != evbase )
	{
		event_base_free( evbase );
		evbase = NULL;
	}
	return ;
}

struct event_base* amqp_get_evbase( amqp_connection_state_t conn )
{
	return conn->base;
}

int amqp_evbase_loop( struct event_base *evbase )
{
	int ret = -1;
	if ( NULL != evbase )
	{
		ret = event_base_dispatch( evbase );
	}
	return ret;
}

int amqp_evbase_loopbreak( struct event_base *evbase )
{
	int ret = -1;
	if ( NULL != evbase )
	{
		ret = event_base_loopbreak( evbase );
	}
	return ret;
}


// 添加 Producer
int add_producer( struct event_base *ev_base, char *dstip, uint16_t dstport, char *vhost, char *luname, char *lupass, 
	char *exchangen, char *routingkey, unsigned attr, MQ *msgQ,
	content_type contentType, amqp_boolean_t mandatory, int heartbeat, char *producertag,
	ConnectionSucc_CB conn_success_cb, ConnectionFail_CB conn_disconnect_cb, PublisherConfirm_CB publisher_confirm_cb )
{
	conn_config_t *producer_conf = NULL;
	amqp_connection_state_t conn_producer = NULL;

	producer_conf = (conn_config_t *)malloc( sizeof(conn_config_t) );
	if ( NULL == producer_conf )
	{
		fprintf( stderr, "[MQ] add_producer: out of memory for producer_conf! Error!\n" );
		return 1;  // 需要定义错误码
	}
	memset( producer_conf, 0, sizeof(*producer_conf) );

	producer_conf->channel_max = 0;
	producer_conf->frame_max = AMQP_DEFAULT_FRAME_SIZE;
	producer_conf->heartbeat = heartbeat;
	producer_conf->sasl_method = AMQP_SASL_METHOD_PLAIN;
	producer_conf->identity = PRODUCER;

	// dstip 必须为 '\0' 结束的字符串
	if ( NULL != dstip )
	{
		memcpy( producer_conf->hostname, dstip, strlen(dstip) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_producer: dstip is NULL, Error!\n" );
		free( producer_conf );
		return 1;
	}	
	producer_conf->port = dstport;
	if ( NULL != vhost )
	{
		memcpy( producer_conf->vhost, vhost, strlen(vhost) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_producer: vhost is NULL! Error!\n");
		free( producer_conf );
		return 1;
	}
	if ( NULL != luname )
	{
		memcpy( producer_conf->login_user, luname, strlen(luname) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_producer: luname is NULL! Error!\n" );
		free( producer_conf );
		return 1;
	}
	if ( NULL != lupass )
	{
		memcpy( producer_conf->login_pwd, lupass, strlen(lupass) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_producer: lupass is NULL, Error!\n" );
		free( producer_conf );
		return 1;
	}

	// 由调用者控制
	if ( NULL != exchangen )
	{
		memcpy( producer_conf->exchange, exchangen, strlen(exchangen) );
	}
	if ( NULL != routingkey )
	{
		memcpy( producer_conf->routingkey, routingkey, strlen(routingkey) );
	}

	producer_conf->contentType = contentType; // CT_JSON
// 	producer_conf->msg_persistent = (msg_persistent==0)?0:1; // msg 是否持久化
	producer_conf->mandatory = (mandatory==0)?0:1; // 是否允许 msg 被 blackholed

	// producertag 可以按需使用
	if ( NULL == producertag || '\0' == producertag[0] )
	{
		memcpy( producer_conf->tag, "PRODUCER", strlen("PRODUCER") );
	}
	else
	{
		memcpy( producer_conf->tag, producertag, strlen(producertag) );
	}

	// 一般情况下 producer 不会设置除 P_CONFIRM_SELECT 之外的其它 flag
	// 设置 Publisher Confirm 是否使能
	producer_conf->broker_flag = attr;

	producer_conf->conn_success_cb = conn_success_cb;
	producer_conf->conn_disconnect_cb = conn_disconnect_cb;
	producer_conf->publisher_confirm_cb = publisher_confirm_cb;

	conn_producer = amqp_new_connection2( *producer_conf, ev_base );
	if ( NULL == conn_producer )
	{
		fprintf( stderr, "[MQ] add_producer: failed to create a new producer, Error!\n" );
		free( producer_conf );
		return 1;
	}

	amqp_set_msg_queue( conn_producer, msgQ );
	fsm( conn_producer );

	free( producer_conf );
	return 0;
}


// 添加 Consumer
int add_consumer( struct event_base *ev_base, char *dstip, uint16_t dstport, char *vhost, char *luname, char *lupass,
	char *queuen, char *exchangen, char *bindingkey, unsigned attr, MQ *msgQ,
	amqp_boolean_t auto_ack, uint16_t prefetch_count, int heartbeat, char *consumertag,
	ConnectionSucc_CB conn_success_cb, ConnectionFail_CB conn_disconnect_cb, 
	ContenHeaderProps_CB header_props_cb, ContentBody_CB body_cb, AnonymousQueueDeclare_CB anonymous_queue_declare_cb )
{
	conn_config_t *consumer_conf = NULL;
	amqp_connection_state_t conn_consumer = NULL;

	consumer_conf = (conn_config_t *)malloc( sizeof(conn_config_t) );
	if ( NULL == consumer_conf )
	{
		fprintf( stderr, "[MQ] add_consumer: out of memory! Error!\n" );
		return 1;  // 需要定义错误码
	}
	memset( consumer_conf, 0, sizeof(*consumer_conf) );

	// by default
	consumer_conf->channel_max = 0;
	consumer_conf->frame_max = AMQP_DEFAULT_FRAME_SIZE;
	consumer_conf->heartbeat = heartbeat;
	consumer_conf->sasl_method = AMQP_SASL_METHOD_PLAIN;
	consumer_conf->identity = CONSUMER;

	consumer_conf->no_ack = (auto_ack==0)?0:1;
	consumer_conf->prefetch_count = prefetch_count;

	// dstip 必须为 '\0' 结束的字符串
	// 仅对目标ip地址进行判空，不做ip合法性判定
	if ( NULL != dstip )
	{
		memcpy( consumer_conf->hostname, dstip, strlen(dstip) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_consumer: dstip is NULL, Error!\n" );
		free( consumer_conf );
		return 1;
	}	
	consumer_conf->port = dstport;

	if ( NULL != vhost )
	{
		memcpy( consumer_conf->vhost, vhost, strlen(vhost) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_consumer: vhost is NULL, Error!\n" );
		free( consumer_conf );
		return 1;
	}
	if ( NULL != luname )
	{
		memcpy( consumer_conf->login_user, luname, strlen(luname) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_consumer: luname is NULL, Error!\n" );
		free( consumer_conf );
		return 1;
	}
	if ( NULL != lupass )
	{
		memcpy( consumer_conf->login_pwd, lupass, strlen(lupass) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_consumer: lupass is NULL, Error!\n" );
		free( consumer_conf );
		return 1;
	}
	
	// Consumer 可以不设置 exchangen | bindingkey | queuen
	if ( NULL != exchangen )
	{
		memcpy( consumer_conf->exchange, exchangen, strlen(exchangen) );
	}
	if ( NULL != bindingkey )
	{
		memcpy( consumer_conf->bindingkey, bindingkey, strlen(bindingkey) );
	}
	if ( NULL != queuen )
	{
		memcpy( consumer_conf->queue, queuen, strlen(queuen) );
	}

	// consumertag 可以按需使用
	if ( NULL == consumertag || '\0' == consumertag[0] )
	{
		memcpy( consumer_conf->tag, "CONSUMER", strlen("CONSUMER") );
	}
	else
	{
		memcpy( consumer_conf->tag, consumertag, strlen(consumertag) );
	}

	// 设置 queue 动作和属性
	consumer_conf->broker_flag = attr;

	consumer_conf->conn_success_cb = conn_success_cb;
	consumer_conf->conn_disconnect_cb = conn_disconnect_cb;

	consumer_conf->header_props_cb = header_props_cb;
	consumer_conf->body_cb = body_cb;

	consumer_conf->anonymous_queue_declare_cb = anonymous_queue_declare_cb;


	conn_consumer = amqp_new_connection2( *consumer_conf, ev_base );
	if ( NULL == conn_consumer )
	{
		fprintf( stderr, "[MQ] add_consumer: failed to create a new consumer, Error!\n" );
		free( consumer_conf );
		return 1;
	}

	amqp_set_msg_queue( conn_consumer, msgQ );
	fsm( conn_consumer );

	free( consumer_conf );
	return 0;
}


// 添加 Manager
int add_manager( struct event_base *ev_base, char *dstip, uint16_t dstport, char *vhost, char *luname, char *lupass,
	char *queuen, char *exchangen, char *exchange_type, char *bindingkey, unsigned attr, int heartbeat, char *managertag )
{
	conn_config_t *manager_conf = NULL;
	amqp_connection_state_t conn_manager = NULL;

	manager_conf = (conn_config_t *)malloc( sizeof(conn_config_t) );
	if ( NULL == manager_conf )
	{
		fprintf( stderr, "[MQ] add_manager: out of memory! Error!\n" );
		return 1;  // 需要定义错误码
	}
	memset( manager_conf, 0, sizeof(*manager_conf) );

	// by default
	manager_conf->channel_max = 0;
	manager_conf->frame_max = AMQP_DEFAULT_FRAME_SIZE;
	manager_conf->heartbeat = heartbeat;
	manager_conf->sasl_method = AMQP_SASL_METHOD_PLAIN;
	manager_conf->identity = MANAGER;

	if ( NULL != dstip )
	{
		memcpy( manager_conf->hostname, dstip, strlen(dstip) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_manager: dstip is NULL, Error!\n" );
		free( manager_conf );
		return 1;
	}	
	manager_conf->port = dstport;

	if ( NULL != vhost )
	{
		memcpy( manager_conf->vhost, vhost, strlen(vhost) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_manager: vhost is NULL, Error!\n" );
		free( manager_conf );
		return 1;
	}
	if ( NULL != luname )
	{
		memcpy( manager_conf->login_user, luname, strlen(luname) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_manager: luname is NULL, Error!\n" );
		free( manager_conf );
		return 1;
	}
	if ( NULL != lupass )
	{
		memcpy( manager_conf->login_pwd, lupass, strlen(lupass) );
	}
	else
	{
		fprintf( stderr, "[MQ] add_manager: lupass is NULL, Error!\n" );
		free( manager_conf );
		return 1;
	}

	if ( NULL != queuen )
	{
		memcpy( manager_conf->queue, queuen, strlen(queuen) );
	}
	if ( NULL != exchangen )
	{
		memcpy( manager_conf->exchange, exchangen, strlen(exchangen) );
	}
	if ( NULL != exchange_type )
	{
		memcpy( manager_conf->exchange_type, exchange_type, strlen(exchange_type) );
	}
	if ( NULL != bindingkey )
	{
		memcpy( manager_conf->bindingkey, bindingkey, strlen(bindingkey) );
	}

	// managertag 可以按需使用
	if ( NULL == managertag || '\0' == managertag[0] )
	{
		memcpy( manager_conf->tag, "MANAGER", strlen("MANAGER") );
	}
	else
	{
		memcpy( manager_conf->tag, managertag, strlen(managertag) );
	}

	// 设置 manager 动作和属性
	manager_conf->broker_flag = attr;

	conn_manager = amqp_new_connection2( *manager_conf, ev_base );
	if ( NULL == conn_manager )
	{
		fprintf( stderr, "[MQ] add_manager: failed to create a new manager, Error!\n" );
		free( manager_conf );
		return 1;
	}
	fsm( conn_manager );

	free( manager_conf );
	return 0;
}


