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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "amqp_private.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>

#include "amqp_mq.h"

amqp_connection_state_t amqp_new_connection2( conn_config_t config, struct event_base *cur_event_base )
{
	int res;

	amqp_connection_state_t state =	(amqp_connection_state_t)calloc( 1, sizeof(struct amqp_connection_state_t_) );
	if ( state == NULL )
	{
		fprintf( stderr, "[MQ] amqp_new_connection2: out of memory! Error!\n" );
		return NULL;
	}

	if ( '\0' == config.hostname[0] )
	{
		fprintf( stderr, "[MQ] amqp_new_connection2: hostname needed! Error!\n" );
		free( state );
		return NULL;
	}
	else
	{
		memcpy( state->hostname, config.hostname, sizeof(config.hostname) );
	}

	state->port = config.port;

	if ( '\0' == config.login_user[0] || '\0' == config.login_pwd[0] )
	{
		fprintf( stderr, "[MQ] amqp_new_connection2: login_user or login_pwd is illegal! Use Default!\n" );
		memcpy( state->login_user, DEFAULT_USER, strlen(DEFAULT_USER) );
		memcpy( state->login_pwd, DEFAULT_PASS, strlen(DEFAULT_PASS) );
	}
	else
	{
		memcpy( state->login_user, config.login_user, strlen(config.login_user) );
		memcpy( state->login_pwd, config.login_pwd, strlen(config.login_pwd) );
	}

	if ( '\0' == config.vhost[0] )
	{
		fprintf( stderr, "[MQ] amqp_new_connection2: vhost is absent! Use Default!\n" );
		memcpy( state->vhost, VHOST, strlen(VHOST) );
	}
	else
	{
		memcpy( state->vhost, config.vhost, strlen(config.vhost) );
	}

	if ( UN_KNOWN == config.identity )
	{
		fprintf( stderr, "[MQ] amqp_new_connection2: Identity is UN_KNOWN! Error!\n" );
		free( state );
		return NULL;
	}
	else
	{
		state->identity = config.identity;
	}
	state->sasl_method = config.sasl_method;

	switch ( config.identity )
	{
	case PRODUCER:
		{
			// 此处为连接创建时候的初始化，当做默认值使用
			state->msg_persistent = config.msg_persistent;
			state->mandatory = config.mandatory;
			state->contentType = config.contentType;

			// 改为允许最初创建 Producer 时不指定 exchange 和 routingkey
			if ( '\0' == config.exchange[0] )
			{
				fprintf( stderr, "[MQ] amqp_new_connection2: producer without a exchange! OK!\n" );
			}
			state->exchange = amqp_bytes_malloc_dup_cstring( config.exchange );

			if ( '\0' == config.routingkey[0] )
			{
				fprintf( stderr, "[MQ] amqp_new_connection2: producer without a rounting_key! OK!\n" );
			}
			state->routingkey = amqp_bytes_malloc_dup_cstring( config.routingkey );

			// 可以为 Producer 指定自定义 tag
			if ( '\0' == config.tag[0] )
			{
				fprintf( stderr, "[MQ] amqp_new_connection2: producer without a producer_tag! OK!\n" );
			}
			memcpy( state->tag, config.tag, strlen(config.tag) );
		}
		break;

	case CONSUMER:
		{
			state->no_ack = config.no_ack;
			state->prefetch_count = config.prefetch_count;

			if ( config.broker_flag & Q_BIND )
			{
				if ( '\0' == config.exchange[0] )
				{
					fprintf( stderr, "[MQ] amqp_new_connection2: Consumer(Q_BIND) without exchange! Error!(can not bind)\n" );
					free( state );
					return NULL;
				}
				else if ( '\0' == config.bindingkey[0] )
				{
					fprintf( stderr, "[MQ] amqp_new_connection2: Consumer(Q_BIND) without bindingkey! OK!(for fanout)\n" );
				}
				else
				{
					fprintf( stderr, "[MQ] amqp_new_connection2: Consumer(Q_BIND)! OK!(for direct or topic)\n" );
				}
			}
			else
			{
				if ( '\0' == config.exchange[0] && '\0' == config.bindingkey[0] )
				{
					fprintf( stderr, "[MQ] amqp_new_connection2: Consumer(without Q_BIND) without exchange and bindingkey! "
						"OK!(for default exchange)\n" );
				}
				else
				{
					fprintf( stderr, "[MQ] amqp_new_connection2: Consumer(without Q_BIND)! "
						"Should not set either exchange or bindingkey\n" );
				}
			}

			state->exchange = amqp_bytes_malloc_dup_cstring( config.exchange );
			state->bindingkey = amqp_bytes_malloc_dup_cstring( config.bindingkey );

			if ( '\0' == config.queue[0] )
			{
				// 这里强制要求 consumer 一定进行 Queue.Declare
				fprintf( stderr, "[MQ] amqp_new_connection2: Consumer without queue! OK!(for anonymous queue)\n" );
			}
			else
			{
				memcpy( state->queue, config.queue, strlen(config.queue) );
			}

			// 可以为 Consumer 指定自定义 tag
			if ( '\0' == config.tag[0] )
			{
				fprintf( stderr, "[MQ] amqp_new_connection2: Consumer without a consumer_tag! OK!\n" );
			}
			memcpy( state->tag, config.tag, strlen(config.tag) );
		}
		break;

	case MANAGER:
		{
			if ( config.broker_flag & (Q_DELETE|Q_PURGE) )
			{
				if ( '\0' == config.queue[0] )
				{
					fprintf( stderr, "[MQ] amqp_new_connection2: manager delete|purge without a queue! Error!\n" );
					free( state );
					return NULL;
				}
				memcpy( state->queue, config.queue, strlen(config.queue) );
			}

			if ( config.broker_flag & Q_DECLARE )
			{
				if ( '\0' == config.queue[0] )
				{
					fprintf( stderr, "[MQ] amqp_new_connection2: manager declare an anonymous queue!\n" );
				}
				memcpy( state->queue, config.queue, strlen(config.queue) );
			}

			if ( config.broker_flag & E_DECLARE )
			{
				if ( '\0' == config.exchange[0] || '\0' == config.exchange_type[0] )
				{
					fprintf( stderr, "[MQ] amqp_new_connection2: manager declare without a exchange or exchange_type! Error!\n" );
					free( state );
					return NULL;
				}
				state->exchange = amqp_bytes_malloc_dup_cstring( config.exchange );
				memcpy( state->exchange_type, config.exchange_type, strlen(config.exchange_type) );
			}

			if ( config.broker_flag & E_DELETE )
			{
				if ( '\0' == config.exchange[0] )
				{
					fprintf( stderr, "[MQ] amqp_new_connection2: manager delete without a exchange! Error!\n" );
					free( state );
					return NULL;
				}
				state->exchange = amqp_bytes_malloc_dup_cstring( config.exchange );
			}

			if ( config.broker_flag & (Q_BIND|Q_UNBIND|E_BIND|E_UNBIND) )
			{
				if ( '\0' == config.exchange[0] || '\0' == config.queue[0] )
				{
					fprintf( stderr, "[MQ] amqp_new_connection2: manager unbind without a exchange or queue! Error!\n" );
					free( state );
					return NULL;
				}
				if (  '\0' == config.routingkey[0] )
				{
					fprintf( stderr, "[MQ] amqp_new_connection2: manager unbind without a routing_key! Weird!\n" );
				}
				state->exchange = amqp_bytes_malloc_dup_cstring( config.exchange );
				state->routingkey = amqp_bytes_malloc_dup_cstring( config.routingkey );
				memcpy( state->queue, config.queue, strlen(config.queue) );
			}
			// 可以为 Manager 指定自定义 tag
			if ( '\0' == config.tag[0] )
			{
				fprintf( stderr, "[MQ] amqp_new_connection2: manager without a manager_tag! OK!\n" );
			}
			memcpy( state->tag, config.tag, strlen(config.tag) );
		}
		break;
	default:
		break;
	}

	state->broker_flag = config.broker_flag;

	// CallBack 设置
	if ( NULL != config.conn_success_cb )
	{
		state->conn_success_cb = config.conn_success_cb;
	}
	if ( NULL != config.conn_disconnect_cb )
	{
		state->conn_disconnect_cb = config.conn_disconnect_cb;
	}

	if ( NULL != config.header_props_cb )
	{
		state->header_props_cb = config.header_props_cb;
	}
	if ( NULL != config.body_cb )
	{
		state->body_cb = config.body_cb;
	}

	if ( NULL != config.publisher_confirm_cb )
	{
		state->publisher_confirm_cb = config.publisher_confirm_cb;
	}
	if ( NULL != config.anonymous_queue_declare_cb )
	{
		state->anonymous_queue_declare_cb = config.anonymous_queue_declare_cb;
	}

	if ( config.heartbeat == 0 )
	{
		fprintf( stderr, "[MQ] amqp_new_connection2: heartbeat(=0) is not enabled!\n" );
	}
	else if ( config.heartbeat < 0 || config.heartbeat >= 600 )
	{
		fprintf( stderr, "[MQ] amqp_new_connection2: heartbeat(=%d) is out of range! Set to default(30)!\n", config.heartbeat );
		config.heartbeat = 30;
	}
	else
	{
		fprintf( stderr, "[MQ] amqp_new_connection2: heartbeat(=%d) is enabled!\n", config.heartbeat );
	}

	res = amqp_tune_connection(state, config.channel_max, config.frame_max, config.heartbeat);
	if (0 != res) {
		goto out_nomem;
	}

	state->inbound_buffer.bytes = state->header_buffer;
	state->inbound_buffer.len = sizeof(state->header_buffer);

	state->state = CONNECTION_STATE_INITIAL;
	/* the server protocol version response is 8 bytes, which conveniently
	is also the minimum frame size */
	state->target_size = 8;

	state->sock_inbound_buffer.len = INITIAL_INBOUND_SOCK_BUFFER_SIZE;
	state->sock_inbound_buffer.bytes = malloc(INITIAL_INBOUND_SOCK_BUFFER_SIZE);
	if (state->sock_inbound_buffer.bytes == NULL) {
		goto out_nomem;
	}

	state->base = cur_event_base;

	state->retry_cnt = 0;
	state->conn_timeout = 0;
	state->ev_tri = 0;

	state->socket = NULL;

	amqp_set_state( state, conn_init );

	state->sock_outbound_offset = 0;
	state->sock_outbound_limit = 0;

	state->hb_counter = 0;

	return state;

out_nomem:
	free(state->sock_inbound_buffer.bytes);
	free(state);
	return NULL;
}


amqp_conn_state_enum amqp_get_state( amqp_connection_state_t conn )
{
	return conn->cur_conn_state;
}

void amqp_set_state( amqp_connection_state_t conn, amqp_conn_state_enum state )
{
	conn->cur_conn_state = state;
}

MQ* amqp_get_msg_queue( amqp_connection_state_t conn )
{
	return conn->msgQ;
}

void amqp_set_msg_queue( amqp_connection_state_t conn, MQ *msgQ )
{
	conn->msgQ = msgQ;
}

char* amqp_get_tag( amqp_connection_state_t conn )
{
	return conn->tag;
}



#ifdef MSG_NOSIGNAL
	#define DEFAULT_SEND_FLAGS MSG_NOSIGNAL
#else
	#define DEFAULT_SEND_FLAGS 0
#endif

try_write_result_enum amqp_send_frame_nonblock( amqp_connection_state_t state, const amqp_frame_t *frame )
{
	void *out_frame = NULL;
	size_t out_frame_len;
	int res;
	int last_error = 0;
	int total_len = 0;

	if ( 0 == state->sock_outbound_offset )
	{
		amqp_bytes_t encoded;
		out_frame = state->outbound_buffer.bytes;

		amqp_e8(out_frame, 0, frame->frame_type);
		amqp_e16(out_frame, 1, frame->channel);

		switch ( frame->frame_type )
		{
//  The content body payload is an opaque binary block followed by a frame end octet:
//			+-----------------------+ +-----------+
//			| Opaque binary payload | | frame-end |
//			+-----------------------+ +-----------+
		case AMQP_FRAME_BODY:
			{
				size_t offset = HEADER_SIZE;
				out_frame_len = frame->payload.body_fragment.len;

				amqp_e32(out_frame, 3, out_frame_len);
				// HACK:
				res = amqp_encode_bytes( state->outbound_buffer, &offset, frame->payload.body_fragment );
				if ( 0 == res )
				{
					fprintf( stderr, "[MQ] amqp_send_frame_nonblock: amqp_encode_bytes failed! Error!\n" );
					return WRITE_ERROR;
				}
				amqp_e8(out_frame, out_frame_len + HEADER_SIZE, AMQP_FRAME_END);
			}
			break;
// All method bodies start with identifier numbers for the class and method:
//
//			0          2           4
//			+----------+-----------+-------------- - -
//			| class-id | method-id | arguments...
//			+----------+-----------+-------------- - -
//			    short      short        ...
		case AMQP_FRAME_METHOD:
			{
				amqp_e32(out_frame, HEADER_SIZE, frame->payload.method.id);

				encoded.bytes = amqp_offset(out_frame, HEADER_SIZE + 4);
				encoded.len = state->outbound_buffer.len - HEADER_SIZE - 4 - FOOTER_SIZE;

				res = amqp_encode_method( frame->payload.method.id,frame->payload.method.decoded, encoded );
				if ( res < 0 )
				{
					fprintf( stderr, "[MQ] amqp_send_frame_nonblock: amqp_encode_method failed! Error!\n" );
					return WRITE_ERROR;
				}
				out_frame_len = res + 4;

				amqp_e32(out_frame, 3, out_frame_len);
				amqp_e8(out_frame, out_frame_len + HEADER_SIZE, AMQP_FRAME_END);
			}
			break;
//   A content header payload has this format:
//			0          2        4           12               14
//			+----------+--------+-----------+----------------+------------- - -
//			| class-id | weight | body size | property flags | property list...
//			+----------+--------+-----------+----------------+------------- - -
//			    short     short   long long        short        remainder...
//
		case AMQP_FRAME_HEADER:
			{
				amqp_e16(out_frame, HEADER_SIZE, frame->payload.properties.class_id);
				amqp_e16(out_frame, HEADER_SIZE+2, 0); /* "weight" */
				amqp_e64(out_frame, HEADER_SIZE+4, frame->payload.properties.body_size);

				encoded.bytes = amqp_offset(out_frame, HEADER_SIZE + 12);
				encoded.len = state->outbound_buffer.len - HEADER_SIZE - 12 - FOOTER_SIZE;

				res = amqp_encode_properties( frame->payload.properties.class_id, frame->payload.properties.decoded, encoded );
				if (res < 0)
				{
					fprintf( stderr, "[MQ] amqp_send_frame_nonblock: amqp_encode_properties failed! Error!\n" );
					return WRITE_ERROR;
				}
				out_frame_len = res + 12;

				amqp_e32(out_frame, 3, out_frame_len);
				amqp_e8(out_frame, out_frame_len + HEADER_SIZE, AMQP_FRAME_END);
			}
			break;
		case AMQP_FRAME_HEARTBEAT:
			{
				//fprintf( stderr, "[MQ] amqp_send_frame_nonblock: get a HEARTBEAT frame!\n" );
				out_frame_len = 0;
			}			
			break;
		default:
			fprintf( stderr, "[MQ] Should not be here! Error!\n" );
			return WRITE_ERROR;
		}
		state->sock_outbound_limit = out_frame_len + HEADER_SIZE + FOOTER_SIZE;
	}

	res = send( amqp_get_sockfd(state), (char *)out_frame + state->sock_outbound_offset, 
		state->sock_outbound_limit - state->sock_outbound_offset, DEFAULT_SEND_FLAGS );
	if ( -1 == res )
	{
		last_error = amqp_os_socket_error();
		switch ( last_error )
		{
		case E_NET_WOULDBLOCK:
		case EAGAIN:
			{
				fprintf( stderr, "[MQ][%s s:%d] amqp_send_frame_nonblock ==> send (-1), socket sendbuffer is full, try later!\n",
					state->tag, amqp_get_sockfd(state) );
				return WRITE_NO_DATA_SENDED;
			}
			break;
		case EPIPE:
		case E_NET_CONNRESET:
		case E_NET_CONNABORTED:
			{
				fprintf( stderr, "[MQ][%s s:%d] amqp_send_frame_nonblock ==> send (-1), Remote side closed the connection!\n",
					state->tag, amqp_get_sockfd(state) );
				return WRITE_ERROR;
			}
			break;
		default:
			{
				fprintf( stderr, "[MQ][%s s:%d] amqp_send_frame_nonblock ==> send (-1), Something bad happened!\n",
					state->tag, amqp_get_sockfd(state) );
				return WRITE_ERROR;
			}
			break;
		}
	}
	else if ( 0 == res )
	{
		fprintf( stderr, "[MQ][%s s:%d] amqp_send_frame_nonblock ==> send (0), Current connection is broken!\n",
			state->tag, amqp_get_sockfd(state) );
		return WRITE_ERROR;
	}

	state->sock_outbound_offset += res;

	if ( state->sock_outbound_offset == state->sock_outbound_limit )
	{
		state->sock_outbound_offset = 0;
		state->sock_outbound_limit = 0;
		return WRITE_COMPLETE;
	}
	else
	{
		fprintf( stderr, "[MQ][%s s:%d] amqp_send_frame_nonblock ==> Still part of data need to send!\n",
			state->tag, amqp_get_sockfd(state) );
		return WRITE_INCOMPLETE;
	}
}

static const char *state_text(enum conn_states state)
{
	const char* const statenames[] = { 

		// 1. 通用状态
		"init",
		"connecting",
		"connected",
		"idle",
		"closing",
		"close",

		// 2.通用协议状态
		"snd_protocol_header",
		"rcv_connection_start_method",
		"snd_connection_start_rsp_method",
		"rcv_connection_tune_method",
		"snd_connection_tune_rsp_method",
		"snd_connection_open_method",
		"rcv_connection_open_rsp_method",
		"snd_connection_close_method",
		"rcv_connection_close_rsp_method",
		"snd_connection_close_rsp_method",

		"snd_channel_open_method",
		"rcv_channel_open_rsp_method",
		"snd_channel_close_method",
		"rcv_channel_close_rsp_method",
		"snd_channel_flow_method",
		"snd_channel_close_rsp_method",

		"snd_channel_confirm_select_method",
		"rcv_channel_confirm_select_rsp_method",

		// 2.客户操作相关协议状态
		"snd_queue_declare_method",
		"rcv_queue_declare_rsp_method",
		"snd_queue_delete_method",
		"rcv_queue_delete_rsp_method",

		"snd_queue_bind_method",
		"rcv_queue_bind_rsp_method",
		"snd_queue_unbind_method",
		"rcv_queue_unbind_rsp_method",

		"snd_queue_purge_method",
		"rcv_queue_purge_rsp_method",

		"snd_exchange_declare_method",
		"rcv_exchange_declare_rsp_method",
		"snd_exchange_delete_method",
		"rcv_exchange_delete_rsp_method",

		"snd_basic_consume_method",
		"rcv_basic_consume_rsp_method",

		"snd_basic_qos_method",
		"rcv_basic_qos_rsp_method",
		"snd_basic_get_method",
		"rcv_basic_get_rsp_method",
		"snd_basic_cancel_method",
		"rcv_basic_cancel_rsp_method",

		"snd_basic_publish_method",
		"snd_basic_content_header",
		"snd_basic_content_body",

		"rcv_basic_deliver_method",
		"rcv_basic_content_header",
		"rcv_basic_content_body",

		"rcv_basic_return_method",
		"snd_basic_ack_method",
		"rcv_basic_ack_method",
		"snd_basic_reject_method",
		"rcv_basic_reject_method",
		"rcv_basic_nack_method",

		"snd_heartbeat",

	};
	return statenames[state];
}


#define CUSTOM_WAIT_FOR_NOTHING        11111
#define CUSTOM_WAIT_FOR_HEARTBEAT      11112
#define CUSTOM_HEARTBEAT_TIMEOUT       22222
#define CUSTOM_SOCKET_CREATE_ERROR     33333
#define CUSTOM_SOCKET_CONNECT_ERROR    44444
#define CUSTOM_SOCKET_READ_ERROR       55555
#define CUSTOM_SOCKET_WRITE_ERROR      66666
#define CUSTOM_CONNECTION_CLOSE_NORMAL 77777

#define CUSTOM_RECV_CONTENT_HEADER     88888
#define CUSTOM_RECV_CONTENT_BODY       99999

const char *method_number_2_string( amqp_method_number_t method_number )
{
	uint16_t i = 0;
	struct {
		amqp_method_number_t mn;
		const char *ms;
	} method_array[] = {
		{ CUSTOM_WAIT_FOR_NOTHING,          "Nothing" },
		{ CUSTOM_WAIT_FOR_HEARTBEAT,        "WaitFor.Heartbeat" },
		{ CUSTOM_HEARTBEAT_TIMEOUT,         "Heartbeat.TimeOut" },
		{ CUSTOM_SOCKET_CREATE_ERROR,       "SocketCreate.Error" },
		{ CUSTOM_SOCKET_CONNECT_ERROR,      "SocketConnect.Failed" },
		{ CUSTOM_SOCKET_READ_ERROR,         "SocketRead.Error" },
		{ CUSTOM_SOCKET_WRITE_ERROR,        "SocketWrite.Error" },
		{ CUSTOM_CONNECTION_CLOSE_NORMAL,   "ConnectionClose.Normal" },

		{ CUSTOM_RECV_CONTENT_HEADER,       "Content.Header" },
		{ CUSTOM_RECV_CONTENT_BODY,         "Content.Body" },


		{ AMQP_CONNECTION_START_METHOD,     "Connection.Start" },
		{ AMQP_CONNECTION_START_OK_METHOD,  "Connection.Start-Ok" },
		{ AMQP_CONNECTION_TUNE_METHOD,      "Connection.Tune" },
		{ AMQP_CONNECTION_TUNE_OK_METHOD,   "Connection.Tune-Ok" },
		{ AMQP_CONNECTION_OPEN_METHOD,      "Connection.Open" },
		{ AMQP_CONNECTION_OPEN_OK_METHOD,   "Connection.Open-Ok" },
		{ AMQP_CONNECTION_CLOSE_METHOD,     "Connection.Close" },
		{ AMQP_CONNECTION_CLOSE_OK_METHOD,  "Connection.Close-Ok" },

		{ AMQP_CHANNEL_OPEN_METHOD,         "Channel.Open" },
		{ AMQP_CHANNEL_OPEN_OK_METHOD,      "Channel.Open-Ok" },
		{ AMQP_CHANNEL_CLOSE_METHOD,        "Channel.Close" },
		{ AMQP_CHANNEL_CLOSE_OK_METHOD,     "Channel.Close-Ok" },

		{ AMQP_CONFIRM_SELECT_METHOD,       "Confirm.Select"},
		{ AMQP_CONFIRM_SELECT_OK_METHOD,    "Confirm.Select-Ok"},

		{ AMQP_QUEUE_DECLARE_METHOD,        "Queue.Declare" },
		{ AMQP_QUEUE_DECLARE_OK_METHOD,     "Queue.Declare-Ok" },
		{ AMQP_QUEUE_BIND_METHOD,           "Queue.Bind" },
		{ AMQP_QUEUE_BIND_OK_METHOD,        "Queue.Bind-Ok" },
		{ AMQP_QUEUE_DELETE_METHOD,         "Queue.Delete" },
		{ AMQP_QUEUE_DELETE_OK_METHOD,      "Queue.Delete-Ok" },
		{ AMQP_QUEUE_UNBIND_METHOD,         "Queue.UnBind" },
		{ AMQP_QUEUE_UNBIND_OK_METHOD,      "Queue.UnBind-Ok" },
		{ AMQP_QUEUE_PURGE_METHOD,          "Queue.Purge" },
		{ AMQP_QUEUE_PURGE_OK_METHOD,       "Queue.Purge-Ok" },

		{ AMQP_BASIC_QOS_METHOD,            "Queue.Qos" },
		{ AMQP_BASIC_QOS_OK_METHOD,         "Queue.Qos-Ok" },
		{ AMQP_BASIC_CONSUME_METHOD,        "Basic.Consume" },
		{ AMQP_BASIC_CONSUME_OK_METHOD,     "Basic.Consume-Ok" },

		{ AMQP_BASIC_PUBLISH_METHOD,        "Basic.Publish" },
		{ AMQP_BASIC_DELIVER_METHOD,        "Basic.Deliver" },
		{ AMQP_BASIC_ACK_METHOD,            "Basic.Ack" },
		{ AMQP_BASIC_RETURN_METHOD,         "Basic.Return" },
		{ AMQP_BASIC_NACK_METHOD,           "Basic.Nack" },
		{ AMQP_BASIC_REJECT_METHOD,         "Basic.Reject" },
		{ AMQP_BASIC_CANCEL_METHOD,         "Basic.Cancel" },

		{ AMQP_EXCHANGE_DECLARE_METHOD,     "Exchange.Declare" },
		{ AMQP_EXCHANGE_DECLARE_OK_METHOD,  "Exchange.Declare-Ok" },
		{ AMQP_EXCHANGE_DELETE_METHOD,      "Exchange.Delete" },
		{ AMQP_EXCHANGE_DELETE_OK_METHOD,   "Exchange.Delete-Ok" },

	};

	for ( i=0; i< sizeof(method_array)/sizeof(method_array[0]); i++ )
	{
		if ( method_array[i].mn == method_number )
			return method_array[i].ms;
	}
	return "not match";
}

amqp_bytes_t make_sasl_response( amqp_pool_t *pool, amqp_sasl_method_enum sasl_method, char *login_user, char *login_pwd )
{
	amqp_bytes_t response;

	switch ( sasl_method )
	{
	case AMQP_SASL_METHOD_PLAIN:
		{
			size_t username_len = strlen(login_user);
			size_t password_len = strlen(login_pwd);
			char *response_buf;

			amqp_pool_alloc_bytes( pool, username_len + password_len + 2, &response );
			if ( response.bytes == NULL )
			{
				/* We never request a zero-length block, because of the +2
				above, so a NULL here really is ENOMEM. */
				return response;
			}

			response_buf = (char *)response.bytes;
			response_buf[0] = 0;
			memcpy(response_buf + 1, login_user, username_len);
			response_buf[username_len + 1] = 0;
			memcpy(response_buf + username_len + 2, login_pwd, password_len);			
		}
		break;
	default:
		amqp_abort("Invalid SASL method: %d", (int)sasl_method );
		break;
	}

	return response;
}


static void conn_set_state( amqp_connection_state_t conn, amqp_conn_state_enum next_state )
{
	amqp_conn_state_enum conn_state;

	if ( NULL == conn )
	{
		fprintf( stderr, "[MQ] conn_set_state: current conn is NULL! something is WRONG!!\n" );
		return;
	}
	if ( next_state < conn_init || next_state >= conn_max_state )
	{
		fprintf( stderr, "[MQ] conn_set_state: abnormal state!\n" );
		return;
	}

	conn_state = amqp_get_state( conn );

	if ( next_state != conn_state ) 
	{
		//fprintf( stderr, "[MQ][s:%d] conn_state CHANGE   %s ==> %s\n", amqp_get_sockfd(conn), state_text(conn_state), state_text(state) );
		amqp_set_state( conn, next_state );
	}

	return;
}

void fsm( amqp_connection_state_t conn );

static void event_handler( const int fd, const short which, void *arg )
{
	amqp_connection_state_t conn = (amqp_connection_state_t)arg;
	if ( NULL == conn )
	{
		fprintf( stderr, "[MQ] event_handler: conn is null! Error!\n" );
		return;
	}

	conn->ev_tri = which;

	/* sanity */
	if ( fd != amqp_get_sockfd(conn) )
	{
		fprintf( stderr, "[MQ] Catastrophic: event fd doesn't match conn fd! Error!\n" );
		return;
	}

	fsm( conn );

	/* wait for next event */
	return;
}


static int update_event( amqp_connection_state_t conn, const int new_event, int sec, int milli )
{
	struct timeval tv;
	struct timeval *p_tv = NULL;

	if ( NULL == conn )
	{
		fprintf( stderr, "[MQ] update_event: current conn is NULL! something is WRONG!!\n" );
		return 0;
	}

	if ( sec >= 0 || milli >= 0 )
	{
		tv.tv_sec = sec;
		tv.tv_usec = milli * 1000;
		p_tv = &tv;
	}
	else
	{
		p_tv = NULL;
	}

	if ( event_del( &conn->notify_event ) == -1 )
	{
		fprintf( stderr, "[MQ] update_event: event_del failed! Error!\n" );
		return 0;
	}

	event_assign( &conn->notify_event, conn->base, amqp_get_sockfd(conn), new_event, event_handler, (void *)conn );

	if ( event_add( &conn->notify_event, p_tv ) == -1 )
	{
		fprintf( stderr, "[MQ] update_event: event_add failed! Error!\n" );
		return 0;
	}

	return 1;
}

static void conn_set_disconnect_reason( amqp_connection_state_t conn, amqp_method_number_t expect_for, amqp_method_number_t received )
{
	fprintf( stderr, "[MQ][%s s:%d]      #### Expect (%s), but Get (%s)! Log for diagnose!! ####\n", 
		conn->tag, amqp_get_sockfd(conn), method_number_2_string(expect_for), method_number_2_string(received) );

	conn->expect_method = expect_for;
	conn->get_method = received;

	return;
}


#define AMQP_CUSTOM_TIMEOUT  ((int)2)   // seconds
#define AMQP_RW_TIMEOUT	     ((int)10)  // millisecond

// conn           -> connection related structure
// chan_num       -> AMQP channel in use
// method_to_wait -> response needed in current state
// next_state     -> next state to enter
static void wait_method_nonblock( amqp_connection_state_t conn, uint16_t chan_num, amqp_method_number_t method_to_wait, 
	amqp_conn_state_enum next_state, amqp_boolean_t *stop )
{
	amqp_method_t method;
	unsigned to_remove = EMPTY;
	uint16_t ev_RW = 0;

	switch ( amqp_simple_wait_method_nonblock( conn, chan_num, method_to_wait, &method ) )
	{
	case READ_DATA_RECEIVED:
		{
			fprintf( stderr, "[MQ][%s s:%d]  <-- recv %s\n", conn->tag, amqp_get_sockfd(conn), method_number_2_string(method_to_wait) );

			conn->hb_counter = 0;

			switch ( method_to_wait )
			{
				case AMQP_QUEUE_DECLARE_OK_METHOD: to_remove = Q_DECLARE; break;
				case AMQP_QUEUE_BIND_OK_METHOD:    to_remove = Q_BIND; break;
				case AMQP_BASIC_QOS_OK_METHOD:     to_remove = B_QOS; break;
				case AMQP_BASIC_CONSUME_OK_METHOD: to_remove = B_CONSUME; break;

				case AMQP_CHANNEL_CLOSE_OK_METHOD:
				case AMQP_CONNECTION_START_METHOD:
				case AMQP_CONNECTION_TUNE_METHOD:
				case AMQP_CONNECTION_OPEN_OK_METHOD: ev_RW = EV_WRITE; break;

				case AMQP_BASIC_DELIVER_METHOD: ev_RW = EV_READ; break;
				default: break;
			}

			conn->broker_flag = conn->broker_flag & ~to_remove;

			switch ( method_to_wait )
			{
			case AMQP_CONNECTION_START_METHOD:
				{
					amqp_connection_start_t *s = (amqp_connection_start_t *) method.decoded;
					if ( (s->version_major != AMQP_PROTOCOL_VERSION_MAJOR) || (s->version_minor != AMQP_PROTOCOL_VERSION_MINOR) ) 
					{
						fprintf( stderr, "[MQ][%s s:%d] AMQP_PROTOCOL_VERSION not support!!\n",
							conn->tag, amqp_get_sockfd(conn) );
						return;
					}

					/* TODO: check that our chosen SASL mechanism is in the list of
					acceptable mechanisms. Or even let the application choose from
					the list! */
				}
				break;
			case AMQP_CONNECTION_TUNE_METHOD:
				{
					int res = 0;
					int server_frame_max;
					uint16_t server_channel_max;
					uint16_t server_heartbeat;

					amqp_connection_tune_t *s = (amqp_connection_tune_t *) method.decoded;
					server_channel_max = s->channel_max;
					server_frame_max = s->frame_max;
					server_heartbeat = s->heartbeat;

					if (server_channel_max != 0 && server_channel_max < conn->channel_max)
						conn->channel_max = server_channel_max;

					if (server_frame_max != 0 && server_frame_max < conn->frame_max)
						conn->frame_max = server_frame_max;

					if (server_heartbeat != 0 && server_heartbeat < conn->heartbeat)
						conn->heartbeat = server_heartbeat;

					res = amqp_tune_connection( conn, conn->channel_max, conn->frame_max, conn->heartbeat );
					if ( res < 0 )
					{
						fprintf( stderr, "[MQ][%s s:%d] amqp_tune_connection failed!\n",
							conn->tag, amqp_get_sockfd(conn) );
						return;
					}
				}
				break;
			case AMQP_QUEUE_DECLARE_OK_METHOD:
				{
					amqp_queue_declare_ok_t *queue_declare_p = (amqp_queue_declare_ok_t *)method.decoded;
					memcpy( conn->queue, (char *)queue_declare_p->queue.bytes, queue_declare_p->queue.len );

					// 增加用户获取 RabbitMQ Server 创建的匿名 queue 名字和相关参数的回调
					if ( NULL != conn->anonymous_queue_declare_cb )
					{
						conn->anonymous_queue_declare_cb( conn, queue_declare_p->queue.bytes, queue_declare_p->queue.len );
					}
				}
				break;
			case AMQP_BASIC_DELIVER_METHOD:
				{
					// 原本 delivery_tag 是针对特定 channel 上的 Message ID 应该定义在与 channel 相关的结构中
					// 目前的实现不支持多 channel 共享 connection 故将 delivery_tag 定义在 conn 结构中
					amqp_basic_deliver_t *basic_deliver_p = (amqp_basic_deliver_t *)method.decoded;
					conn->cur_delivery_tag = basic_deliver_p->delivery_tag;

					conn->last_stable_state = next_state;
					conn->last_stable_ev_set = ev_RW;
					conn->last_stable_timeout_sec = 0;
					conn->last_stable_timeout_mil = 0;
				}
				break;
			case AMQP_BASIC_ACK_METHOD:
				{

#if !defined(WIN32) && !defined(_WIN32)
					int off = 0;
					struct timeval tv;
					char buf[64] = {0};

					gettimeofday( &tv, NULL );
					off = strftime( buf, sizeof(buf), "%d %b %H:%M:%S.", localtime( &tv.tv_sec ) );
					snprintf( buf + off, sizeof(buf) - off, "%03d", (int)tv.tv_usec/1000 );
					fprintf( stderr, "[MQ][s:%d] recv Basic.Ack => timestamp = %s\n", amqp_get_sockfd(conn), buf );
#endif

					// 支持 Publisher Confirm 机制
					if ( NULL != conn->publisher_confirm_cb )
					{
						conn->publisher_confirm_cb( conn, method.decoded, RT_ACK );
					}
				}
				break;
			case AMQP_BASIC_RETURN_METHOD:
				{
					amqp_basic_return_t *basic_return_p = (amqp_basic_return_t *)method.decoded;
					fprintf( stderr, "[MQ][%s s:%d]  <-- recv Channel.Return\n", conn->tag, amqp_get_sockfd(conn) );
					fprintf( stderr, "[MQ][%s s:%d]      ### reply_code: %d, reply_text: %.*s, exchange=%.*s, routing_key=%.*s\n",
						conn->tag, amqp_get_sockfd(conn),
						basic_return_p->reply_code, 
						(int)basic_return_p->reply_text.len, (char *)basic_return_p->reply_text.bytes,
						(int)basic_return_p->exchange.len, (char *)basic_return_p->exchange.bytes,
						(int)basic_return_p->routing_key.len, (char *)basic_return_p->routing_key.bytes );

					// 可以在此增加相应处理
				}
				break;
			case AMQP_CONFIRM_SELECT_OK_METHOD:
				{
					fprintf( stderr, "[MQ][%s s:%d]  <-- recv Confirm.Select-Ok\n",
						conn->tag, amqp_get_sockfd(conn) );
				}
				break;
			case AMQP_CONNECTION_OPEN_OK_METHOD:
			case AMQP_CHANNEL_CLOSE_OK_METHOD:
			case AMQP_CONNECTION_CLOSE_OK_METHOD:
			case AMQP_CHANNEL_OPEN_OK_METHOD:
			case AMQP_QUEUE_BIND_OK_METHOD:
			case AMQP_BASIC_QOS_OK_METHOD:
			case AMQP_BASIC_CONSUME_OK_METHOD:
				break;
			default:
				{
					fprintf( stderr, "[MQ][%s s:%d] Need to code something?! method_to_wait='%s'\n", 
						conn->tag, amqp_get_sockfd(conn), method_number_2_string(method_to_wait) );
				}
				break;
			}

			update_event( conn, ev_RW, 0, 0 );
			conn_set_state( conn, next_state );
			*stop = 1;
		}
		break;

	case READ_DATA_RECEIVED_CHANNEL_CLOSE:
		{
			amqp_channel_close_t *channel_close_p = (amqp_channel_close_t *)method.decoded;
			fprintf( stderr, "[MQ][%s s:%d]  <-- recv Channel.Close\n", conn->tag, amqp_get_sockfd(conn) );			
			fprintf( stderr, "[MQ][%s s:%d]      ### reply_code: %d, reply_text: %.*s\n",
				conn->tag, amqp_get_sockfd(conn),
				channel_close_p->reply_code, 
				(int)channel_close_p->reply_text.len, (char *)channel_close_p->reply_text.bytes );

			conn->hb_counter = 0;

			conn_set_disconnect_reason( conn, method_to_wait, AMQP_CHANNEL_CLOSE_METHOD );
			conn_set_state( conn, conn_snd_channel_close_rsp_method );
			*stop = 0;
		}
		break;

	case READ_DATA_RECEIVED_CONNECTION_CLOSE:
		{
			amqp_connection_close_t *connection_close_p = (amqp_connection_close_t *)method.decoded;
			fprintf( stderr, "[MQ][%s s:%d]  <-- recv Connection.Close\n", conn->tag, amqp_get_sockfd(conn) );		
			fprintf( stderr, "[MQ][%s s:%d]      ### reply_code: %d, reply_text: %.*s\n",
				conn->tag, amqp_get_sockfd(conn),
				connection_close_p->reply_code, 
				(int)connection_close_p->reply_text.len, (char *)connection_close_p->reply_text.bytes );			

			conn->hb_counter = 0;

			conn_set_disconnect_reason( conn, method_to_wait, AMQP_CONNECTION_CLOSE_METHOD );
			conn_set_state( conn, conn_snd_connection_close_rsp_method );
			*stop = 0;
		}
		break;

	case READ_DATA_RECEIVED_BASIC_NACK:
		{
			fprintf( stderr, "[MQ][%s s:%d]  <-- recv Basic.Nack\n", conn->tag, amqp_get_sockfd(conn) );

			conn->hb_counter = 0;

			// 支持 Publisher Confirm 机制
			if ( NULL != conn->publisher_confirm_cb )
			{
				conn->publisher_confirm_cb( conn, method.decoded, RT_NACK );
			}

			update_event( conn, 0, 0, 0 );
			conn_set_state( conn, conn_idle );
			*stop = 1;
		}
		break;

	case READ_DATA_RECEIVED_BASIC_REJECT:
		{
			fprintf( stderr, "[MQ][%s s:%d]  <-- recv Basic.Reject\n", conn->tag, amqp_get_sockfd(conn) );

			conn->hb_counter = 0;

			// 支持 Publisher Confirm 机制
			if ( NULL != conn->publisher_confirm_cb )
			{
				conn->publisher_confirm_cb( conn, method.decoded, RT_REJECT );
			}

			update_event( conn, 0, 0, 0 );
			conn_set_state( conn, conn_idle );
			*stop = 1;
		}
		break;

	case READ_DATA_RECEIVED_HEARTBEAT:
		{
			fprintf( stderr, "[MQ][%s s:%d]  <-- recv Heartbeat frame!\n", conn->tag, amqp_get_sockfd(conn) );

			conn->hb_counter = 0;

			if ( conn->identity == PRODUCER )
			{
				// 回到之前的状态
				if ( conn->last_stable_state == conn_idle )
				{
					update_event( conn, 0, 0, 0 );
					conn_set_state( conn, conn_idle );
				}
				else
				{
					update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
					conn_set_state( conn, conn_snd_heartbeat );
				}
			}
			else if ( conn->identity == CONSUMER )
			{
				// HACK: 这里采用的方式为接收到来自服务器的 heartbeat 帧后立即回复 heartbeat 帧到服务器
				// 因为服务器在 3 * heartbeat 协商时间内没有收到 heartbeat 帧时，会通过 RST 断开当前连接
				update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
				conn_set_state( conn, conn_snd_heartbeat );
			}

			*stop = 1;
		}
		break;

	case READ_NO_DATA_RECEIVED: 
		{
//  			fprintf( stderr, "[MQ][%s s:%d] wait for %s method [NO_DATA,WAIT,%dms]!\n", 
//  				conn->tag, amqp_get_sockfd(conn), method_number_2_string(method_to_wait), AMQP_RW_TIMEOUT );

			if ( amqp_heartbeat_enabled( conn ) )
			{
				if ( conn->hb_counter >= ( conn->heartbeat * 3 * ( 1000 / AMQP_RW_TIMEOUT ) ) )
				{
					fprintf( stderr, "[MQ][%s s:%d] recv nothing more than 3 * %d seconds!(Heartbeat Timeout), hb_counter=%d\n", 
						conn->tag, amqp_get_sockfd(conn), conn->heartbeat, conn->hb_counter );
					fprintf( stderr, "[MQ][%s s:%d] Maybe network broken or RabbitMQ fucked! Plz retry consuming!\n", 
						conn->tag, amqp_get_sockfd(conn) );

					conn_set_disconnect_reason( conn, method_to_wait, CUSTOM_HEARTBEAT_TIMEOUT );
					update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
					conn_set_state( conn, conn_close );
					*stop = 1;
				}
				else
				{
// 					int off = 0;
// 					time_t t;
// 					char buf[64] = {0};
// 
// 					t = time( NULL );
// 					off = strftime( buf, sizeof(buf), "%d %b %H:%M:%S", localtime( &t ) );
// 					fprintf( stderr, "[MQ][%s s:%d] hb_counter [%d]  timestamp = %s\n", 
// 						conn->tag, amqp_get_sockfd(conn), conn->hb_counter, buf );

					conn->hb_counter++;
					update_event( conn, EV_READ, 0, AMQP_RW_TIMEOUT );
					*stop = 1;
				}
			}
			else
			{
				fprintf( stderr, "[MQ][%s s:%d] recv nothing and set no heartbeat!\n", conn->tag, amqp_get_sockfd(conn) );
				update_event( conn, EV_READ, 0, AMQP_RW_TIMEOUT );
				*stop = 1;
			}
		}
		break;
	case READ_ERROR:
		{
			// NOTE: 在基于 HAProxy 构建 RabbitMQ Cluster 的情况下，即使构成 Cluster 的 node 均未运行，客户端仍旧可以
			//       成功建立 TCP 连接，但是在向 HAProxy 发送 Protocol.Header 后会收到 FIN 包，此时将会从这里直接执行
			//       conn_close 分支。
			fprintf( stderr, "[MQ][%s s:%d] wait_method_nonblock() ==> get READ_ERROR!\n", conn->tag, amqp_get_sockfd(conn) );

			conn_set_disconnect_reason( conn, method_to_wait, CUSTOM_SOCKET_READ_ERROR );
			update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
			conn_set_state( conn, conn_close );
			*stop = 1;
		}
		break;
	default:
		fprintf( stderr, "[MQ] wait_method_nonblock => should not be here!\n" );
		break;
	}
}

void fsm( amqp_connection_state_t conn )
{
	int stop = 0;

	if ( NULL == conn )
	{
		fprintf( stderr, "[MQ][%s] conn is NULL! Error!\n", conn->tag );
		return;
	}

	while ( !stop )
	{
		switch ( amqp_get_state( conn ) ) 
		{
		case conn_init:
			{
				struct timeval tv;

				int err_code = 0;
				int sockfd = -1;
				int ret = -1;

				err_code = amqp_open_socket_and_connect( conn->hostname, conn->port, &sockfd );
				switch( err_code )
				{
				case 0:
					{
						fprintf( stderr, "[MQ][%s][fsm:conn_init] create and connect success!\n", conn->tag );

						ret = amqp_set_sockfd( conn, sockfd );
						if ( 0 != ret )
						{
							fprintf( stderr, "[MQ][%s][fsm:conn_init] amqp_set_sockfd() failed! Out of memory!\n",
								conn->tag );
							return;
						}

						conn_set_state( conn, conn_connected );
						stop = 0;
					}
					break;
#ifdef WIN32
				case WSAEWOULDBLOCK:
				case WSAEINVAL:
				case WSAEINPROGRESS:
#else
				case EINPROGRESS:
#endif
					{
						fprintf( stderr, "[MQ][%s][fsm:conn_init] TCP 3-way handshake start! --> [%s:%d][s:%d]\n",
							conn->tag, conn->hostname, conn->port, sockfd );

						ret = amqp_set_sockfd( conn, sockfd );
						if ( 0 != ret )
						{
							fprintf( stderr, "[MQ][%s][fsm:conn_init] amqp_set_sockfd() failed! Out of memory!\n",
								conn->tag );
							return;
						}

						event_assign( &conn->notify_event, conn->base, amqp_get_sockfd(conn), EV_WRITE, event_handler, conn );
						tv.tv_sec = AMQP_CUSTOM_TIMEOUT;
						tv.tv_usec = 0;
						event_add( &conn->notify_event, &tv );

						conn_set_state( conn, conn_connecting );
						stop = 1;
					}
					break;

				default:  // sockfd == -1
					{
						fprintf( stderr, "[MQ][%s][fsm:conn_init] Connect failed! Socket related operation Failed!\n", conn->tag );

						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_CREATE_ERROR );

						//update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );

						event_assign( &conn->notify_event, conn->base, amqp_get_sockfd(conn), 0, event_handler, conn );
						tv.tv_sec = AMQP_CUSTOM_TIMEOUT;
						tv.tv_usec = 0;
						event_add( &conn->notify_event, &tv );

						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				}
			}
			break;

		case conn_connecting:
			{
// 				struct timeval tv;
				int reinit = 0;
				int error;
				unsigned int errsz = sizeof( error );

				fprintf( stderr, "[MQ][%s][fsm:conn_connecting] connecting on socket(%d)...\n", 
					conn->tag, amqp_get_sockfd(conn) );

				do 
				{
					if ( conn->ev_tri == EV_TIMEOUT )
					{
						conn->retry_cnt++;

						if ( conn->retry_cnt <= 3 ) // 底层自动重连 3 次
						{
							conn->conn_timeout = 0;

							fprintf( stderr, "[MQ][%s][fsm:conn_connecting] connect timeout(%d <= 3) on socket(%d)\n", 
								conn->tag, conn->retry_cnt, amqp_get_sockfd(conn) );

							update_event( conn, EV_WRITE, AMQP_CUSTOM_TIMEOUT, 0 );
							stop = 1;
						}
						else  // 底层 3 次重连后仍旧失败，则触发业务层回调函数
						{
							fprintf( stderr, "[MQ][%s][fsm:conn_connecting] connect timeout(%d > 3) on socket(%d)\n", 
								conn->tag, conn->retry_cnt, amqp_get_sockfd(conn) );

							conn->conn_timeout = 1;
							conn->retry_cnt = 0;

							conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_CONNECT_ERROR );
							update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
							conn_set_state( conn, conn_close );
							stop = 1;
						}

						break;
					}
					else
					{
// 						fprintf( stderr, "[MQ][%s][fsm:conn_connecting] not by EV_TIMEOUT on socket(%d)\n", 
// 							conn->tag, amqp_get_sockfd(conn) );

						if ( getsockopt( amqp_get_sockfd(conn), SOL_SOCKET, SO_ERROR, (void *)&error, &errsz ) == -1 ) {
							fprintf( stderr, "[MQ][%s][fsm:conn_connecting] getsockopt failed on socket(%d)\n", 
								conn->tag, amqp_get_sockfd(conn) );
							reinit = 1;
						}

						if ( 0 != error ) {
							fprintf( stderr, "[MQ][%s][fsm:conn_connecting] connect failed on socket(%d): %s\n", 
								conn->tag, amqp_get_sockfd(conn), strerror(error) );
							reinit = 1;
						}

						// 重置状态变量
						conn->conn_timeout = 0;
						conn->retry_cnt = 0;

						if ( 1 == reinit )
						{
							// 在 socket 出错时，则回到 conn_init 状态重新创建 socket 和 TCP 连接
							update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
							conn_set_state( conn, conn_init );
							stop = 1;
						}
						else
						{
							conn_set_state( conn, conn_connected );
							stop = 0;
						}

						break;
					}
				} while (0);
			}
			break;

		case conn_connected:
			{
				fprintf( stderr, "[MQ][%s][fsm:conn_connected] connect SUCCESS on socket(%d)\n", 
					conn->tag, amqp_get_sockfd(conn) );

				if ( NULL != conn->conn_success_cb )
				{
					conn->conn_success_cb( conn, (char *)"TCP connection to RabbitMQ success!" );
				}

				update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
				conn_set_state( conn, conn_snd_protocol_header );
				stop = 1;
			}
			break;

		case conn_idle:
			{
				MQ_ITEM *mq_item = NULL;

				switch( conn->ev_tri )
				{
				case EV_TIMEOUT:
					{
						switch ( conn->identity )
						{
						case PRODUCER:
							{
								conn->last_stable_state = conn_idle;
								conn->last_stable_ev_set = 0;
								conn->last_stable_timeout_sec = 0;
								conn->last_stable_timeout_mil = 0;

								// 全局 MQ 中获取 MQ_ITEM
								mq_item = mq_pop( conn->msgQ );
								if ( NULL != mq_item )
								{

#if !defined(WIN32) && !defined(_WIN32)
									int off = 0;
									struct timeval tv;
									char buf[64] = {0};

									gettimeofday( &tv, NULL );
									off = strftime( buf, sizeof(buf), "%d %b %H:%M:%S.", localtime( &tv.tv_sec ) );
									snprintf( buf + off, sizeof(buf) - off, "%03d", (int)tv.tv_usec/1000 );
									fprintf( stderr, "[MQ][s:%d] mq_pop => timestamp = %s\n", amqp_get_sockfd(conn), buf );
#endif

									if ( RPC_MODE == mq_item->rpc_mode )
									{
										conn->rpc_mode = RPC_MODE;
										conn->correlation_id = amqp_bytes_malloc_dup( mq_item->correlation_id );
										conn->reply_to = amqp_bytes_malloc_dup( mq_item->reply_to );
									}
									else
									{
										conn->rpc_mode = NOT_RPC_MODE;
										conn->correlation_id = amqp_empty_bytes;
										conn->reply_to = amqp_empty_bytes;
									}

									conn->msg_persistent = mq_item->msg_persistent;

									conn->exchange = amqp_bytes_malloc_dup( mq_item->exchange );
									conn->routingkey = amqp_bytes_malloc_dup( mq_item->routingkey );
									conn->content = amqp_bytes_malloc_dup( mq_item->content );

									fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][P] conn->exchange: %.*s\n", 
										conn->tag, amqp_get_sockfd(conn), conn->exchange.len, (char *)conn->exchange.bytes );
									fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][P] conn->routingkey: %.*s\n", 
										conn->tag, amqp_get_sockfd(conn), conn->routingkey.len, (char *)conn->routingkey.bytes );
									fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][P] conn->content: ...\n", conn->tag, amqp_get_sockfd(conn) );
// 									fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][P] conn->content: %.*s\n", 
// 										conn->tag, amqp_get_sockfd(conn), conn->content.len, (char *)conn->content.bytes );

									// 目前此处的判定都是不符合条件直接 return ，导致的结果就是 Producer 的退出
									// 后续可以考虑是否做策略调整
									if ( NULL == conn->content.bytes )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][P] content is NULL! Error!\n",
											conn->tag, amqp_get_sockfd(conn) );
										return;
									}

									// FIXED: 最终将判定条件修正为 exchange 和 routingkey 不能同时为 NULL
									// 因为
									// 1.当使用默认 exchange 时，exchange 的值可以为 NULL
									// 2.当使用 fanout 类型的 exchange 时，routingkey 的值可以为 NULL
									// 3.目前未发现两者同时为 NULL 的使用场景
									if ( NULL == conn->routingkey.bytes && NULL == conn->exchange.bytes )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][P] both routingkey and content are NULL! Error!\n",
											conn->tag, amqp_get_sockfd(conn) );
										return;
									}

									if ( RPC_MODE == conn->rpc_mode )
									{
										if ( NULL == conn->correlation_id.bytes )
										{
											fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][P] RPC_MODE but correlation_id is NULL! Error!\n",
												conn->tag, amqp_get_sockfd(conn) );
											return;
										}
									}

									mqi_free_all( mq_item );

									update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
									conn_set_state( conn, conn_snd_basic_publish_method );
								}
								else
								{
									if ( amqp_heartbeat_enabled( conn ) )
									{
// 										int off = 0;
// 										time_t t;
// 										char buf[64] = {0};
// 
// 										t = time( NULL );
// 										off = strftime( buf, sizeof(buf), "%d %b %H:%M:%S", localtime( &t ) );
// 										fprintf( stderr, "[MQ][%s s:%d] Producer hb_counter [%d]  timestamp = %s\n", 
// 											conn->tag, amqp_get_sockfd(conn), conn->hb_counter, buf );

										conn->hb_counter++;
										if ( conn->hb_counter > ( conn->heartbeat * ( 1000 / AMQP_RW_TIMEOUT ) ) )
										{
											update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
											conn_set_state( conn, conn_snd_heartbeat );
											conn->hb_counter = 0;
										}
										else
										{
											update_event( conn, 0, 0, AMQP_RW_TIMEOUT );
										}	
									}
									else
									{
										update_event( conn, 0, 0, AMQP_RW_TIMEOUT );
									}								
								}
								stop = 1;

							}
							break;
						case CONSUMER:
							{
								do 
								{
									if ( 0 == (conn->broker_flag & Q_STANDARD_ACTIONS) )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][C] Start to recv!\n", 
											conn->tag, amqp_get_sockfd(conn) );

										conn->last_stable_state = conn_rcv_basic_deliver_method;
										conn->last_stable_ev_set = EV_READ;
										conn->last_stable_timeout_sec = 0;
										conn->last_stable_timeout_mil = 0;

										update_event( conn, EV_READ, 0, 0 );
										conn_set_state( conn, conn_rcv_basic_deliver_method );
										break;
									}

									if ( conn->broker_flag & Q_DECLARE )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][C] Queue Declaring!\n",
											conn->tag, amqp_get_sockfd(conn) );
										// 执行 Queue.Declare 
										update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
										conn_set_state( conn, conn_snd_queue_declare_method );
										break;
									}

									if ( conn->broker_flag & Q_BIND )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][C] Queue Binding!\n",
											conn->tag, amqp_get_sockfd(conn) );
										// 执行 Queue.Bind
										update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
										conn_set_state( conn, conn_snd_queue_bind_method );
										break;
									}

									if ( conn->broker_flag & B_QOS )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][C] Basic QoS!\n",
											conn->tag, amqp_get_sockfd(conn) );
										// 执行 Basic.Qos
										update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
										conn_set_state( conn, conn_snd_basic_qos_method );
										break;
									}

									if ( conn->broker_flag & B_CONSUME )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][C] Basic Consuming!\n",
											conn->tag, amqp_get_sockfd(conn) );
										// 执行 Basic.Consume
										update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
										conn_set_state( conn, conn_snd_basic_consume_method );
										break;
									}
								} while ( 0 );

								stop = 1;
							}
							break;
						case MANAGER:  // 专门用于特定的动作
							{
								do 
								{
									if ( 0 == (conn->broker_flag & ONE_SHOT_ACTIONS) )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][M] All one-shot actions are done!"
											" Close current connection!\n", conn->tag, amqp_get_sockfd(conn) );
										// 执行完特定动作后关闭相应的 channel 和 connection
										conn_set_state( conn, conn_closing );
										break;
									}

									if ( conn->broker_flag & Q_DECLARE )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][M] Queue Declaring!\n", 
											conn->tag, amqp_get_sockfd(conn) );
										// 执行 Queue.Deleting 
										update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
										conn_set_state( conn, conn_snd_queue_declare_method );
										break;
									}


									if ( conn->broker_flag & Q_DELETE )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][M] Queue Deleting!\n", 
											conn->tag, amqp_get_sockfd(conn) );
										// 执行 Queue.Deleting 
										update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
										conn_set_state( conn, conn_snd_queue_delete_method );
										break;
									}

									if ( conn->broker_flag & Q_UNBIND )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][M] Queue UnBinding!\n", 
											conn->tag, amqp_get_sockfd(conn) );
										// 执行 Queue.UnBind
										update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
										conn_set_state( conn, conn_snd_queue_unbind_method );
										break;
									}

									if ( conn->broker_flag & Q_PURGE )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][M] Queue Purging!\n", 
											conn->tag, amqp_get_sockfd(conn) );
										// 执行 Queue.Purge
										update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
										conn_set_state( conn, conn_snd_queue_purge_method );
										break;
									}

									if ( conn->broker_flag & E_DECLARE )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][M] Exchange Declaring!\n", 
											conn->tag, amqp_get_sockfd(conn) );
										// 执行 Exchange.Declare
										update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
										conn_set_state( conn, conn_snd_exchange_declare_method );
										break;
									}

									if ( conn->broker_flag & E_DELETE )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][M] Exchange Deleting!\n", 
											conn->tag, amqp_get_sockfd(conn) );
										// 执行 Exchange.Delete
										update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
										conn_set_state( conn, conn_snd_exchange_delete_method );
										break;
									}

									if ( conn->broker_flag & B_GET )
									{
										fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][M] Basic Geting!\n", 
											conn->tag, amqp_get_sockfd(conn) );
										// 执行 Basic.Get
										update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
										conn_set_state( conn, conn_snd_basic_get_method );
										break;
									}									

								} while ( 0 );

								stop = 1;
							}
							break;
						default:
							fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][*] Should not be here!\n", 
								conn->tag, amqp_get_sockfd(conn) );
							break;
						}
					}
					break;
				default:
					{
						fprintf( stderr, "[MQ][%s s:%d][fsm:conn_idle][*] Weird! NOT Trigger by EV_TIMEOUT, but by %d\n", 
							conn->tag, amqp_get_sockfd(conn), conn->ev_tri );

						update_event( conn, 0, 0, 0 );
						conn_set_state( conn, conn_idle );
						stop = 1;
					}
					break;
				}
			}
			break;

		case conn_closing:
			{
				fprintf( stderr, "[MQ][%s s:%d][fsm:conn_closing]\n", conn->tag, amqp_get_sockfd(conn) );

				update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
				conn_set_state( conn, conn_snd_channel_close_method );
				stop = 1;
			}			
			break;

		case conn_close:
			{
				fprintf( stderr, "[MQ][%s s:%d][fsm:conn_close] In\n", conn->tag, amqp_get_sockfd(conn) );

				// NOTE: 此处通过增加延时，可以有效避免因各种类型错误进入 conn_close 后，再重新建立连接时造成的类似死循环问题
				// 2015-09-08 注：经验证此处不可采用 sleep 进行延时，因为会导致整个事件循环被拖慢，产生消息处理慢的效果
// #if defined(WIN32) || defined(_WIN32)
// 				Sleep( 2 * 1000 );
// #else
// 				sleep( 2 );
// #endif

				if ( 0 != conn->conn_timeout )
				{
					fprintf( stderr, "[MQ][%s s:%d][fsm:conn_close] Connection Timeout!\n", 
						conn->tag, amqp_get_sockfd(conn) );
				}
				else
				{
					fprintf( stderr, "[MQ][%s s:%d][fsm:conn_close] Connection Disconnect! Expect (%s)  Get (%s)\n", conn->tag, 
						amqp_get_sockfd(conn), method_number_2_string(conn->expect_method), method_number_2_string(conn->get_method) );
				}

				if ( NULL != conn->conn_disconnect_cb )
				{
					fprintf( stderr, "[MQ][%s s:%d][fsm:conn_close] @@ Enter Disconnect CallBack @@\n", conn->tag, amqp_get_sockfd(conn) );
					conn->conn_disconnect_cb( conn, method_number_2_string(conn->expect_method), method_number_2_string(conn->get_method) );
				}

				amqp_destroy_connection( conn );

				fprintf( stderr, "[MQ][%s s:%d][fsm:conn_close] Out\n", conn->tag, amqp_get_sockfd(conn) );

				stop = 1;
			}
			break;

		case conn_snd_channel_close_method:
			{
				char codestr[13] = {0};
				amqp_channel_close_t req;

				req.reply_code = AMQP_REPLY_SUCCESS;
				req.reply_text.bytes = codestr;
				req.reply_text.len = sprintf( codestr, "%d", AMQP_REPLY_SUCCESS );
				req.class_id = 0;
				req.method_id = 0;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_CHANNEL_CLOSE_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Channel.Close\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_READ, 0, 0 );
						conn_set_state( conn, conn_rcv_channel_close_rsp_method );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
			}
			break;

		case conn_rcv_channel_close_rsp_method:
			{
				wait_method_nonblock( conn, 1, AMQP_CHANNEL_CLOSE_OK_METHOD, conn_snd_connection_close_method, &stop );
			}
			break;

		case conn_snd_channel_close_rsp_method:
			{
				amqp_channel_close_ok_t method;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_CHANNEL_CLOSE_OK_METHOD, &method ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Channel.Close-Ok\n",
							conn->tag, amqp_get_sockfd(conn) );

						conn_set_state( conn, conn_snd_connection_close_method );
						stop = 0;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
			}
			break;

		case conn_snd_connection_close_rsp_method:
			{
				amqp_connection_close_ok_t method;

				fprintf( stderr, "[MQ][%s s:%d]  --> send Connection.Close-Ok\n",
					conn->tag, amqp_get_sockfd(conn) );

				switch ( amqp_send_method_nonblock( conn, 0, AMQP_CONNECTION_CLOSE_OK_METHOD, &method ) )
				{
				case WRITE_COMPLETE:
					{
// 						fprintf( stderr, "[MQ][%s s:%d]  --> send Connection.Close-Ok\n",
// 							conn->tag, amqp_get_sockfd(conn) );

						//conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_CONNECTION_CLOSE_NORMAL );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						//conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
			}
			break;

		case conn_snd_connection_close_method:
			{
				char codestr[13];
				amqp_connection_close_t req;

				req.reply_code = AMQP_REPLY_SUCCESS;
				req.reply_text.bytes = codestr;
				req.reply_text.len = sprintf(codestr, "%d", AMQP_REPLY_SUCCESS);
				req.class_id = 0;
				req.method_id = 0;

				switch ( amqp_send_method_nonblock(conn, 0, AMQP_CONNECTION_CLOSE_METHOD, &req) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Connection.Close\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_READ, 0, 0 );
						conn_set_state( conn, conn_rcv_connection_close_rsp_method );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
			}
			break;

		case conn_rcv_connection_close_rsp_method:
			{
				wait_method_nonblock( conn, 0, AMQP_CONNECTION_CLOSE_OK_METHOD, conn_close, &stop );
			}
			break;

		case conn_snd_protocol_header:
			{
				int status = 0;
				status = amqp_send_header( conn );
				if ( AMQP_STATUS_OK != status )
				{
					fprintf( stderr, "[MQ][%s s:%d]  --> send Protocol.Header failed! status = %d\n", 
						conn->tag, amqp_get_sockfd(conn), status );
					stop = 1;
				}
				else
				{
					fprintf( stderr, "[MQ][%s s:%d]  --> send Protocol.Header\n", conn->tag, amqp_get_sockfd(conn) );

					update_event( conn, EV_READ, 0, 0 );
					conn_set_state( conn, conn_rcv_connection_start_method );
					stop = 1;
				}
			}
			break;

		case conn_rcv_connection_start_method:
			{
				wait_method_nonblock( conn, 0, AMQP_CONNECTION_START_METHOD, conn_snd_connection_start_rsp_method, &stop );
			}
			break;

		case conn_snd_connection_start_rsp_method:
			{
				amqp_table_entry_t properties[3];
				amqp_connection_start_ok_t s;
				amqp_pool_t *channel_pool;
				amqp_bytes_t response_bytes;

				// 为 channel 分配后续使用的 memory pool，内部使用 frame_max 设定 page 大小
				channel_pool = amqp_get_or_create_channel_pool( conn, 0 );
				if ( NULL == channel_pool ) {
					fprintf( stderr, "[MQ][%s s:%d] amqp_get_or_create_channel_pool failed!\n",
						conn->tag, amqp_get_sockfd(conn) );
					return;
				}

				// 目前仅支持 PLAIN 方式的 SASL
				response_bytes = make_sasl_response( channel_pool, conn->sasl_method, conn->login_user, conn->login_pwd );
				if ( response_bytes.bytes == NULL )
				{
					fprintf( stderr, "[MQ][%s s:%d] make_sasl_response failed!\n", 
						conn->tag, amqp_get_sockfd(conn) );
					return;
				}

				properties[0].key = amqp_cstring_bytes("product");
				properties[0].value.kind = AMQP_FIELD_KIND_UTF8;
				properties[0].value.value.bytes	= amqp_cstring_bytes("rabbitmq-c");

				properties[1].key = amqp_cstring_bytes("information");
				properties[1].value.kind = AMQP_FIELD_KIND_UTF8;
				properties[1].value.value.bytes	= amqp_cstring_bytes("See https://github.com/alanxz/rabbitmq-c");

				properties[2].key = amqp_cstring_bytes("sunfei's blog");
				properties[2].value.kind = AMQP_FIELD_KIND_UTF8;
				properties[2].value.value.bytes	= amqp_cstring_bytes("See http://my.oschina.net/moooofly/blog");

				s.client_properties.num_entries = 3;
				s.client_properties.entries = properties;
				s.mechanism = sasl_method_name( conn->sasl_method );
				s.response = response_bytes;
				s.locale.bytes = (void *)"en_US";
				s.locale.len = 5;

				switch ( amqp_send_method_nonblock(conn, 0, AMQP_CONNECTION_START_OK_METHOD, &s) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Connection.Start-Ok\n", 
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_READ, 0, 0 );
						conn_set_state( conn, conn_rcv_connection_tune_method );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
				amqp_maybe_release_buffers( conn );
			}
			break;

		case conn_rcv_connection_tune_method:
			{
				wait_method_nonblock( conn, 0, AMQP_CONNECTION_TUNE_METHOD, conn_snd_connection_tune_rsp_method, &stop );
			}
			break;

		case conn_snd_connection_tune_rsp_method:
			{
				amqp_connection_tune_ok_t s;
				s.frame_max = conn->frame_max;
				s.channel_max = conn->channel_max;
				s.heartbeat = conn->heartbeat;

				switch ( amqp_send_method_nonblock(conn, 0, AMQP_CONNECTION_TUNE_OK_METHOD, &s) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Connection.Tune-Ok\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						conn_set_state( conn, conn_snd_connection_open_method );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
				amqp_maybe_release_buffers( conn );
			}
			break;

		case conn_snd_connection_open_method:
			{
				amqp_connection_open_t s;
				s.virtual_host = amqp_cstring_bytes( conn->vhost );
				s.capabilities.len = 0;
				s.capabilities.bytes = NULL;
				s.insist = 1;

				switch ( amqp_send_method_nonblock(conn, 0, AMQP_CONNECTION_OPEN_METHOD, &s) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Connection.Open\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_READ, 0, 0 );
						conn_set_state( conn, conn_rcv_connection_open_rsp_method );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;

		case conn_rcv_connection_open_rsp_method:
			{
				wait_method_nonblock( conn, 0, AMQP_CONNECTION_OPEN_OK_METHOD, conn_snd_channel_open_method, &stop );
			}
			break;

		case conn_snd_channel_open_method:
			{
				amqp_channel_open_t req;
				req.out_of_band = amqp_empty_bytes; // 带外数据

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_CHANNEL_OPEN_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Channel.Open\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_READ, 0, 0 );
						conn_set_state( conn, conn_rcv_channel_open_rsp_method );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;

		case conn_rcv_channel_open_rsp_method:
			{
				if ( ( PRODUCER == conn->identity ) && ( P_CONFIRM_SELECT & conn->broker_flag ) )
				{
					wait_method_nonblock( conn, 1, AMQP_CHANNEL_OPEN_OK_METHOD, conn_snd_channel_confirm_select_method, &stop );
				}
				else
				{
					wait_method_nonblock( conn, 1, AMQP_CHANNEL_OPEN_OK_METHOD, conn_idle, &stop );
				}
			}
			break;

		case conn_snd_channel_confirm_select_method:
			{
				amqp_confirm_select_t req;
				req.nowait = 0;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_CONFIRM_SELECT_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Confirm.Select\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_READ, 0, 0 );
						conn_set_state( conn, conn_rcv_channel_confirm_select_rsp_method );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;

		case conn_rcv_channel_confirm_select_rsp_method:
			{
				wait_method_nonblock( conn, 1, AMQP_CONFIRM_SELECT_OK_METHOD, conn_idle, &stop );
			}
			break;

		case conn_snd_queue_declare_method:
			{
				amqp_queue_declare_t req;

				memset( &req, 0, sizeof(req) );
				req.ticket = 0;
				req.queue = amqp_cstring_bytes( conn->queue );

				if ( conn->broker_flag & PASSIVE )
				{
					req.passive = 1;
				}
				if ( conn->broker_flag & DURABLE )
				{
					req.durable = 1;
				}
				if ( conn->broker_flag & EXCLUSIVE )
				{
					req.exclusive = 1;
				}
				if ( conn->broker_flag & AUTO_DELETE )
				{
					req.auto_delete = 1;
				}
				if ( conn->broker_flag & NOWAIT )
				{
					req.nowait = 1;
				}

				req.arguments = amqp_empty_table;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_QUEUE_DECLARE_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Queue.Declare\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_READ, 0, 0 );
						conn_set_state( conn, conn_rcv_queue_declare_rsp_method );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;

		case conn_rcv_queue_declare_rsp_method:
			{
				wait_method_nonblock( conn, 1, AMQP_QUEUE_DECLARE_OK_METHOD, conn_idle, &stop );
			}
			break;

		case conn_snd_queue_bind_method:
			{
				amqp_queue_bind_t req;
				req.ticket = 0;
				req.queue = amqp_cstring_bytes( conn->queue );
				req.exchange = conn->exchange;
				req.routing_key = conn->bindingkey;
				req.nowait = 0;
				req.arguments = amqp_empty_table;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_QUEUE_BIND_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Queue.Bind\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_READ, 0, 0 );
						conn_set_state( conn, conn_rcv_queue_bind_rsp_method );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;

		case conn_rcv_queue_bind_rsp_method:
			{
				wait_method_nonblock( conn, 1, AMQP_QUEUE_BIND_OK_METHOD, conn_idle, &stop );
			}
			break;

		case conn_snd_basic_qos_method:
			{
				amqp_basic_qos_t req;
				req.prefetch_size = 0;
				req.prefetch_count = conn->prefetch_count;
				req.global = 0;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_BASIC_QOS_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Basic.Qos\n", 
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_READ, 0, 0 );
						conn_set_state( conn, conn_rcv_basic_qos_rsp_method );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;

		case conn_rcv_basic_qos_rsp_method:
			{
				wait_method_nonblock( conn, 1, AMQP_BASIC_QOS_OK_METHOD, conn_idle, &stop );
			}
			break;

		case conn_snd_basic_consume_method:
			{
				amqp_basic_consume_t req;
				req.ticket = 0;
				req.queue = amqp_cstring_bytes( conn->queue );
				req.consumer_tag = amqp_cstring_bytes( conn->tag );
				req.no_local = 0;
				req.no_ack = conn->no_ack;
				req.exclusive = 0;
				req.nowait = 0;
				req.arguments = amqp_empty_table;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_BASIC_CONSUME_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Basic.Consume\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_READ, 0, 0 );
						conn_set_state( conn, conn_rcv_basic_consume_rsp_method );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;

		case conn_rcv_basic_consume_rsp_method:
			{
				wait_method_nonblock( conn, 1, AMQP_BASIC_CONSUME_OK_METHOD, conn_idle, &stop );
			}
			break;

		case conn_snd_basic_publish_method:
			{
				amqp_basic_publish_t m;

				m.exchange = conn->exchange;
				m.routing_key = conn->routingkey;
				m.mandatory = conn->mandatory;
				m.immediate = 0;  // 目前 RabbitMQ-3.2.4 不支持
				m.ticket = 0;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_BASIC_PUBLISH_METHOD, &m ) )
				{
				case WRITE_COMPLETE:
					{

#if !defined(WIN32) && !defined(_WIN32)
						int off = 0;
						struct timeval tv;
						char buf[64] = {0};

						gettimeofday( &tv, NULL );
						off = strftime( buf, sizeof(buf), "%d %b %H:%M:%S.", localtime( &tv.tv_sec ) );
						snprintf( buf + off, sizeof(buf) - off, "%03d", (int)tv.tv_usec/1000 );
						fprintf( stderr, "[MQ][s:%d] send Basic.Publish => timestamp = %s\n", amqp_get_sockfd(conn), buf );
#endif

						fprintf( stderr, "[MQ][%s s:%d]  --> send Basic.Publish\n", 
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						conn_set_state( conn, conn_snd_basic_content_header );
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
			}
			break;

		case conn_snd_basic_content_header:
			{
				amqp_frame_t f;
				amqp_basic_properties_t props;

				memset( &props, 0, sizeof(props) );

				// 仅在 RPC 模式下使用
				if ( RPC_MODE == conn->rpc_mode )
				{
					props.correlation_id = amqp_bytes_malloc_dup( conn->correlation_id );
					props._flags |= AMQP_BASIC_CORRELATION_ID_FLAG;
					amqp_bytes_free( conn->correlation_id );

					props.reply_to = amqp_bytes_malloc_dup( conn->reply_to );
					if ( 0 != props.reply_to.len )
					{
						props._flags |= AMQP_BASIC_REPLY_TO_FLAG;
					}
					amqp_bytes_free( conn->reply_to );

// 					fprintf( stderr, "[MQ][%s s:%d]  --> props.correlation_id.bytes=%s props.correlation_id.len=%d "
// 						"props.reply_to.bytes=%s props.reply_to.len=%d\n", 
// 						conn->tag, amqp_get_sockfd(conn), 
// 						props.correlation_id.bytes, props.correlation_id.len,
// 						props.reply_to.bytes, props.reply_to.len );
				}

				if ( MSG_PERSISTENT == conn->msg_persistent )
				{
					props.delivery_mode = 2; /* persistent delivery mode */
					props._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
				}
				else
				{
					props.delivery_mode = 0; /* persistent delivery mode */
				}

				switch ( conn->contentType )
				{
				case CT_PLAIN:
					{
						props.content_type = amqp_cstring_bytes("text/plain");
						props._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
					}
					break;
				case CT_JSON:
					{
						props.content_type = amqp_cstring_bytes("application/json");
						props._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
					}
					break;
				default:
					break;
				}

				f.frame_type = AMQP_FRAME_HEADER;
				f.channel = 1;
				f.payload.properties.class_id = AMQP_BASIC_CLASS;
				f.payload.properties.body_size =  conn->content.len;
				f.payload.properties.decoded = (void *) &props;

				switch ( amqp_send_frame_nonblock(conn, &f) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Content-Header frame!\n", 
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						conn_set_state( conn, conn_snd_basic_content_body );
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
			}
			break;

		case conn_snd_basic_content_body:
			{
				amqp_frame_t f;

				f.frame_type = AMQP_FRAME_BODY;
				f.channel = 1;
				f.payload.body_fragment.bytes = amqp_offset( conn->content.bytes, 0 );
				f.payload.body_fragment.len = conn->content.len;

				switch ( amqp_send_frame_nonblock(conn, &f) )
				{
				case WRITE_COMPLETE:
					{

#if !defined(WIN32) && !defined(_WIN32)
						int off = 0;
						struct timeval tv;
						char buf[64] = {0};

						gettimeofday( &tv, NULL );
						off = strftime( buf, sizeof(buf), "%d %b %H:%M:%S.", localtime( &tv.tv_sec ) );
						snprintf( buf + off, sizeof(buf) - off, "%03d", (int)tv.tv_usec/1000 );
						fprintf( stderr, "[MQ][s:%d] send Content-Body => timestamp = %s\n", amqp_get_sockfd(conn), buf );
#endif

						fprintf( stderr, "[MQ][%s s:%d]  --> send Content-Body frame!\n", 
							conn->tag, amqp_get_sockfd(conn) );

						amqp_bytes_free( conn->content );

						// 注意：mandatory 和 Publisher Confirm 机制可以一起使用但此处是分别处理的(其实应该一起使用)
						if ( 0 != conn->mandatory )
						{
							// 设置了 mandatory 需要检查是否收到 Basic.Return 
							update_event( conn, EV_READ, 0, AMQP_RW_TIMEOUT );
							conn_set_state( conn, conn_rcv_basic_return_method );
						}
						else if( P_CONFIRM_SELECT & conn->broker_flag )
						{
							update_event( conn, EV_READ, 0, AMQP_RW_TIMEOUT );
							// 这里无法同时对 Basci.Ack 和 Basic.Nack 进行处理
							// 故利用 Basic.Ack 的状态进行判定
							conn_set_state( conn, conn_rcv_basic_ack_method );

							conn->last_stable_state = conn_rcv_basic_ack_method;
							conn->last_stable_ev_set = EV_READ;
							conn->last_stable_timeout_sec = 0;
							conn->last_stable_timeout_mil = AMQP_RW_TIMEOUT;
						}
						else
						{
							update_event( conn, 0, 0, 0 );
							conn_set_state( conn, conn_idle );
						}
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
			}
			break;

		case conn_rcv_basic_deliver_method:
			{
				wait_method_nonblock( conn, 1, AMQP_BASIC_DELIVER_METHOD, conn_rcv_content_header, &stop );
			}
			break;

		case conn_rcv_content_header:
			{
				amqp_frame_t frame;

				switch ( amqp_simple_wait_content_nonblock( conn, 1, AMQP_FRAME_HEADER, &frame ) )
				{
				case READ_DATA_RECEIVED:
					{
						fprintf( stderr, "[MQ][%s s:%d]  <-- recv Content.Header frame!\n",
							conn->tag, amqp_get_sockfd(conn) );						

						// 目前该回调仅用于返回 correlation_id 和 replyto 字段内容
						if ( NULL != conn->header_props_cb )
						{
							amqp_basic_properties_t *props = (amqp_basic_properties_t *)frame.payload.properties.decoded;
							conn->header_props_cb( conn, props->correlation_id.bytes, props->correlation_id.len, props->reply_to.bytes, props->reply_to.len );
						}

						if ( 0 != frame.payload.properties.body_size )
						{
							update_event( conn, EV_READ, 0, 0 );
							conn_set_state( conn, conn_rcv_content_body );

							conn->last_stable_state = conn_rcv_content_body;
							conn->last_stable_ev_set = EV_READ;
							conn->last_stable_timeout_sec = 0;
							conn->last_stable_timeout_mil = 0;
						}
						else
						{
							// 处理有 HEADER 无 BODY 的情况（此时没有考虑 no_ack 的问题）
							fprintf( stderr, "[MQ][%s s:%d]  FOUND frame.payload.properties.body_size = 0 ! "
								"Are You Kidding Me!?\n", conn->tag, amqp_get_sockfd(conn) );

							update_event( conn, 0, 0, 0 );
							conn_set_state( conn, conn_idle );
						}
						stop = 1;
					}					
					break;

				case READ_DATA_RECEIVED_CHANNEL_CLOSE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  <-- recv Channel.Close\n", 
							conn->tag, amqp_get_sockfd(conn) );

						conn_set_disconnect_reason( conn, CUSTOM_RECV_CONTENT_HEADER, AMQP_CHANNEL_CLOSE_METHOD );
						conn_set_state( conn, conn_snd_channel_close_rsp_method );
					}
					break;

				case READ_NO_DATA_RECEIVED: 
					{
						update_event( conn, EV_READ, 0, 0 );
						stop = 1;
					}
					break;

				case READ_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_READ_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;

				default:
					break;
				}
			}
			break;

		case conn_rcv_content_body:
			{
				amqp_frame_t frame;

				switch ( amqp_simple_wait_content_nonblock( conn, 1, AMQP_FRAME_BODY, &frame ) )
				{
				case READ_DATA_RECEIVED:
					{
						response_type rsp_type = RT_NONE;
						fprintf( stderr, "[MQ][%s s:%d]  <-- recv Content.Body frame!\n", conn->tag, amqp_get_sockfd(conn) );

						// 上层通过回调获取 content body 的内容
						if ( NULL != conn->body_cb )
						{
							// HACK: 为了上层能够按照通常的方式获取数据 -- 这样会不会导致后面一个字节被篡改？
							char *body_tmp = (char *)frame.payload.body_fragment.bytes;
							body_tmp[frame.payload.body_fragment.len]='\0';

// 							fprintf( stderr, "[MQ][%s s:%d] body_fragment.bytes =>\n%s\n", conn->tag, amqp_get_sockfd(conn), body_tmp );
// 							fprintf( stderr, "[MQ][%s s:%d] body_fragment.len => %d\n", conn->tag, amqp_get_sockfd(conn), frame.payload.body_fragment.len );

							conn->body_cb( conn, body_tmp, frame.payload.body_fragment.len, &rsp_type );
						}

						if ( 0 == conn->no_ack )
						{
							// 进行手动ACK
							switch ( rsp_type )
							{
							case RT_ACK:
								conn_set_state( conn, conn_snd_basic_ack_method );
								break;
							case RT_REJECT:
								conn_set_state( conn, conn_snd_basic_reject_method );
								break;
							default:
								fprintf( stderr, "[MQ][%s s:%d] Set neither RT_ACK nor RT_REJECT! what do you want?!\n", 
									conn->tag, amqp_get_sockfd(conn) );
								break;
							}
							update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						}
						else
						{
							update_event( conn, 0, 0, 0 );
							conn_set_state( conn, conn_idle );
						}
						stop = 1;
					}					
					break;
				case READ_DATA_RECEIVED_CHANNEL_CLOSE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  <-- recv Channel.Close\n",
							conn->tag, amqp_get_sockfd(conn) );

						conn_set_disconnect_reason( conn, CUSTOM_RECV_CONTENT_BODY, AMQP_CHANNEL_CLOSE_METHOD );
						conn_set_state( conn, conn_snd_channel_close_rsp_method );
					}
					break;
				case READ_NO_DATA_RECEIVED: 
					{
						update_event( conn, EV_READ, 0, 0 );
						stop = 1;
					}
					break;
				case READ_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_READ_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
			}
			break;

		case conn_snd_basic_ack_method:
			{
				amqp_basic_ack_t m;
				m.delivery_tag = conn->cur_delivery_tag;
				m.multiple = 0;  // 需要进行上层控制

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_BASIC_ACK_METHOD, &m ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Basic.Ack\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, 0, 0, 0 );
						conn_set_state( conn, conn_idle );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;

		case conn_rcv_basic_ack_method:
			{
				wait_method_nonblock( conn, 1, AMQP_BASIC_ACK_METHOD, conn_idle, &stop );
			}
			break;

		case conn_snd_basic_reject_method:
			{
				amqp_basic_reject_t req;
				req.delivery_tag = conn->cur_delivery_tag;
				req.requeue = 0;  // 需要进行上层控制

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_BASIC_REJECT_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Basic.Reject\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, 0, 0, 0 );
						conn_set_state( conn, conn_idle );
						stop = 1;
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;

		case conn_rcv_basic_reject_method:
			{
				wait_method_nonblock( conn, 1, AMQP_BASIC_REJECT_METHOD, conn_idle, &stop );
			}
			break;

		case conn_rcv_basic_return_method:
			{
				wait_method_nonblock( conn, 1, AMQP_BASIC_RETURN_METHOD, conn_idle, &stop );
			}
			break;

		case conn_snd_queue_delete_method:
			{
				amqp_queue_delete_t req;
				req.ticket = 0;
				req.queue = amqp_cstring_bytes( conn->queue );
				req.if_unused = 1;
				req.if_empty = 1;
				req.nowait = 0;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_QUEUE_DELETE_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Queue.Delete\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						conn_set_state( conn, conn_rcv_queue_delete_rsp_method );
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;
		case conn_rcv_queue_delete_rsp_method:
			{
				wait_method_nonblock( conn, 1, AMQP_QUEUE_DELETE_OK_METHOD, conn_closing, &stop );
			}
			break;
		case conn_snd_queue_unbind_method:
			{
				amqp_queue_unbind_t req;
				req.ticket = 0;
				req.queue = amqp_cstring_bytes( conn->queue );
				req.exchange = conn->exchange;
				req.routing_key = conn->routingkey;
				req.arguments = amqp_empty_table;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_QUEUE_UNBIND_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Queue.UnBind\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						conn_set_state( conn, conn_rcv_queue_unbind_rsp_method );
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;
		case conn_rcv_queue_unbind_rsp_method:
			{
				wait_method_nonblock( conn, 1, AMQP_QUEUE_UNBIND_OK_METHOD, conn_closing, &stop );
			}
			break;
		case conn_snd_queue_purge_method:
			{
				amqp_queue_purge_t req;
				req.ticket = 0;
				req.queue = amqp_cstring_bytes( conn->queue );
				req.nowait = 0;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_QUEUE_PURGE_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Queue.Purge\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						conn_set_state( conn, conn_rcv_queue_purge_rsp_method );
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;
		case conn_rcv_queue_purge_rsp_method:
			{
				wait_method_nonblock( conn, 1, AMQP_QUEUE_PURGE_OK_METHOD, conn_closing, &stop );
			}
			break;

		case conn_snd_exchange_declare_method:
			{
				amqp_exchange_declare_t req;

				memset( &req, 0, sizeof(req) );
				req.ticket = 0;
				req.exchange = conn->exchange;
				req.type = amqp_cstring_bytes( conn->exchange_type );

				if ( conn->broker_flag & PASSIVE )
				{
					req.passive = 1;
				}
				if ( conn->broker_flag & DURABLE )
				{
					req.durable = 1;
				}
				if ( conn->broker_flag & AUTO_DELETE )
				{
					req.auto_delete = 1;
				}
				if ( conn->broker_flag & NOWAIT )
				{
					req.nowait = 1;
				}

				req.internal = 0;
				req.arguments = amqp_empty_table;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_EXCHANGE_DECLARE_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Exchange.Declare\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						conn_set_state( conn, conn_rcv_exchange_declare_rsp_method );
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;
		case conn_rcv_exchange_declare_rsp_method:
			{
				wait_method_nonblock( conn, 1, AMQP_EXCHANGE_DECLARE_OK_METHOD, conn_closing, &stop );
			}
			break;

		case conn_snd_exchange_delete_method:
			{
				amqp_exchange_delete_t req;
				req.ticket = 0;
				req.exchange = conn->exchange;
				req.if_unused = 1;
				req.nowait = 0;

				switch ( amqp_send_method_nonblock( conn, 1, AMQP_EXCHANGE_DELETE_METHOD, &req ) )
				{
				case WRITE_COMPLETE:
					{
						fprintf( stderr, "[MQ][%s s:%d]  --> send Exchange.Delete\n",
							conn->tag, amqp_get_sockfd(conn) );

						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						conn_set_state( conn, conn_rcv_exchange_delete_rsp_method );
					}
					break;
				case WRITE_INCOMPLETE:
				case WRITE_NO_DATA_SENDED:
					{
						update_event( conn, EV_WRITE, 0, AMQP_RW_TIMEOUT );
						stop = 1;
					}
					break;
				case WRITE_ERROR:
					{
						conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
						update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
						conn_set_state( conn, conn_close );
						stop = 1;
					}
					break;
				default:
					break;
				}
// 				amqp_maybe_release_buffers( conn );
			}
			break;
		case conn_rcv_exchange_delete_rsp_method:
			{
				wait_method_nonblock( conn, 1, AMQP_EXCHANGE_DELETE_OK_METHOD, conn_closing, &stop );
			}
			break;

		case conn_snd_basic_get_method:
			break;
		case conn_rcv_basic_get_rsp_method:
			break;

		case conn_snd_heartbeat:
			{
				int status = 0;
				amqp_frame_t heartbeat;
				heartbeat.channel = 0;
				heartbeat.frame_type = AMQP_FRAME_HEARTBEAT;

				// 发送 heartbeat 帧
				status = amqp_send_frame( conn, &heartbeat );
				if ( AMQP_STATUS_OK != status )
				{
					fprintf( stderr, "[MQ][%s s:%d]  --> send Heartbeat failed! status = %d(-9 <-> SOCKET_ERROR)\n",
						conn->tag, amqp_get_sockfd(conn), status );

					conn_set_disconnect_reason( conn, CUSTOM_WAIT_FOR_NOTHING, CUSTOM_SOCKET_WRITE_ERROR );
					update_event( conn, 0, AMQP_CUSTOM_TIMEOUT, 0 );
					conn_set_state( conn, conn_close );
					stop = 1;
				}
				else
				{
					fprintf(stderr, "[MQ][%s s:%d]  --> send Heartbeat frame!\n", conn->tag, amqp_get_sockfd(conn) );

					if ( conn->identity == PRODUCER )
					{
						if ( conn->last_stable_state == conn_idle )
						{
							update_event( conn, EV_READ, 0, AMQP_RW_TIMEOUT );
							conn_set_state( conn, conn_rcv_heartbeat );
						}
						else
						{
							// 返回到前一个稳态
							update_event( conn, conn->last_stable_ev_set, conn->last_stable_timeout_sec, conn->last_stable_timeout_mil );
							conn_set_state( conn, conn->last_stable_state );
						}
					}
					else if ( conn->identity == CONSUMER )
					{
						// 返回到前一个稳态
						update_event( conn, conn->last_stable_ev_set, conn->last_stable_timeout_sec, conn->last_stable_timeout_mil );
						conn_set_state( conn, conn->last_stable_state );
					}
					stop = 1;
				}
			}
			break;

		case conn_rcv_heartbeat:
			{
				wait_method_nonblock( conn, 1, CUSTOM_WAIT_FOR_HEARTBEAT, conn_idle, &stop );
			}
			break;

		case conn_max_state:
			break;
		}
// 		amqp_maybe_release_buffers( conn );
	}

	return;
}


