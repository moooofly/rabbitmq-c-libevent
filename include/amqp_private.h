/* vim:set ft=c ts=2 sw=2 sts=2 et cindent: */
#ifndef librabbitmq_amqp_private_h
#define librabbitmq_amqp_private_h

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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "amqp.h"
#include "amqp_framing.h"
#include <string.h>

#ifdef _WIN32
# include <Winsock2.h>
#else
# include <arpa/inet.h>
#endif

/* GCC attributes */
#if __GNUC__ > 2 | (__GNUC__ == 2 && __GNUC_MINOR__ > 4)
#define AMQP_NORETURN \
	__attribute__ ((__noreturn__))
#define AMQP_UNUSED \
	__attribute__ ((__unused__))
#else
#define AMQP_NORETURN
#define AMQP_UNUSED
#endif

#if __GNUC__ >= 4
#define AMQP_PRIVATE \
	__attribute__ ((visibility ("hidden")))
#else
#define AMQP_PRIVATE
#endif

char *amqp_os_error_string(int err);

#ifdef WITH_SSL
char *amqp_ssl_error_string(int err);
#endif

#include "amqp_socket.h"
#include "amqp_timer.h"

#define INITIAL_FRAME_POOL_PAGE_SIZE 65536
#define INITIAL_DECODING_POOL_PAGE_SIZE 131072
#define INITIAL_INBOUND_SOCK_BUFFER_SIZE 131072

/*
* Connection states: XXX FIX THIS
*
* - CONNECTION_STATE_INITIAL: The initial state, when we cannot be
*   sure if the next thing we will get is the first AMQP frame, or a
*   protocol header from the server.
*
* - CONNECTION_STATE_IDLE: The normal state between
*   frames. Connections may only be reconfigured, and the
*   connection's pools recycled, when in this state. Whenever we're
*   in this state, the inbound_buffer's bytes pointer must be NULL;
*   any other state, and it must point to a block of memory allocated
*   from the frame_pool.
*
* - CONNECTION_STATE_HEADER: Some bytes of an incoming frame have
*   been seen, but not a complete frame header's worth.
*
* - CONNECTION_STATE_BODY: A complete frame header has been seen, but
*   the frame is not yet complete. When it is completed, it will be
*   returned, and the connection will return to IDLE state.
*
*/
typedef enum amqp_connection_state_enum_ {
	CONNECTION_STATE_IDLE = 0,
	CONNECTION_STATE_INITIAL,
	CONNECTION_STATE_HEADER,
	CONNECTION_STATE_BODY
} amqp_connection_state_enum;

/* 7 bytes up front, then payload, then 1 byte footer */
#define HEADER_SIZE 7
#define FOOTER_SIZE 1

#define AMQP_PSEUDOFRAME_PROTOCOL_HEADER 'A'

typedef struct amqp_link_t_ {
	struct amqp_link_t_ *next;
	void *data;
} amqp_link_t;

#define POOL_TABLE_SIZE 16

typedef struct amqp_pool_table_entry_t_ {
	struct amqp_pool_table_entry_t_ *next;
	amqp_pool_t pool;       // 为指定 channel 分配的内存池
	amqp_channel_t channel; // 哪个 channel 使用了当前内存池
} amqp_pool_table_entry_t;

struct amqp_connection_state_t_ {
	// v4.0 new 将原来的 frame_pool 和 decoding_pool 等统一管理起来
	amqp_pool_table_entry_t *pool_table[POOL_TABLE_SIZE];

	amqp_connection_state_enum state;

	int channel_max;
/*
 * the maximum size of an frame.
 * The smallest this can be is 4096 (2^12)
 * The largest this can be is 2147483647 (2^31-1)
 * Unless you know what you're doing the recommended size is 131072 or 128KB (2^17)
 */
	int frame_max;     // 最大允许帧长
	int heartbeat;     // 标识设置的 heartbeat 时间值(非0代表使能)

	/* buffer for holding frame headers.  Allows us to delay allocating
	* the raw frame buffer until the type, channel, and size are all known
	*/
	// v4.0 new
	char header_buffer[HEADER_SIZE + 1];

	amqp_bytes_t inbound_buffer;  // 用于拼接完整帧数据的 buffer（循环将 sock_inbound_buffer 中数据拷贝到该 buffer 中，直到获取完整帧）
	size_t inbound_offset; // 已搬入到 inbound_buffer 中数据的偏移量
	size_t target_size;    // 期望收到的帧长度

	amqp_bytes_t outbound_buffer;  // 用于拼接要发送帧数据的 buffer
	size_t sock_outbound_offset;   // 待发送数据的起始偏移量
	size_t sock_outbound_limit;    // 待发送数据的长度

	// v4.0 new socket 相关函数指针
	amqp_socket_t *socket;

	amqp_boolean_t conn_timeout; // 表示是否连接超时
	int retry_cnt;               // 连接超时次数

	struct event_base *base;     // 当前连接使用的 event_base
	struct event notify_event;   // 监听事件结构

	short ev_tri;     // 当前触发回调的事件（EV_TIMEOUT|EV_READ|EV_WRITE|EV_SIGNAL|EV_PERSIST）

	short last_stable_ev_set;      // 前一个 conn 稳态中设置的可触发事件  
	int last_stable_timeout_sec;   // 前一个 conn 稳态中设置的超时时间(second)
	int last_stable_timeout_mil;   // 前一个 conn 稳态中设置的超时时间(millisecond)

	amqp_conn_state_enum cur_conn_state;    // 当前 conn 状态
	amqp_conn_state_enum last_stable_state; // 前一个 conn 稳态

	uint64_t cur_delivery_tag;       // 当前 conn 上指定 channel 中正在处理 message 的 id
	amqp_method_number_t expect_method; // 连接断开前希望收到的 method
	amqp_method_number_t get_method;    // 连接断开时实际收到的 method

	// ==== 自定义 connection 配置信息 ====

	// 权限控制
	char vhost[VHOST_LEN];
	char login_user[USER_LEN];
	char login_pwd[PWD_LEN];
	amqp_sasl_method_enum sasl_method;

	// 目标 RabbitMQ Server 地址
	char hostname[HOSTNAME_LEN]; // 可以是域名或 ip 地址
	uint16_t port;

	char queue[QUEUE_LEN];
	amqp_bytes_t exchange;
	amqp_bytes_t bindingkey; // Consumer 使用
	amqp_bytes_t routingkey; // Producer 使用
	amqp_boolean_t no_ack;   // 是否自动 Ack
	uint16_t prefetch_count; // Qos

	// 针对单条消息的配置（发送消息使用）
	amqp_bytes_t content;
	amqp_boolean_t msg_persistent;
	amqp_boolean_t rpc_mode;
	amqp_bytes_t correlation_id;
	amqp_bytes_t reply_to;

	amqp_boolean_t mandatory;       // 是否按照 mandatory 方式处理消息
	content_type contentType;       // 标识消息内容格式

	// 原本仅用作 consumer tag，现扩展为 consumer|producer|manager 都可使用
	char tag[TAG_LEN];

	// 仅在声明 exchange 的时候使用
	char exchange_type[EXCHANGE_TYPE_LEN];

	rabbitmq_identity identity;   // 标识当前 connection 的身份（Producer or Consumer）
	unsigned broker_flag;         // 标识动作和属性

	MQ *msgQ;  // 保存当前 conn 上待处理消息的队列

	ConnectionSucc_CB conn_success_cb;
	ConnectionFail_CB conn_disconnect_cb;

	ContenHeaderProps_CB header_props_cb;
	ContentBody_CB body_cb;

	PublisherConfirm_CB publisher_confirm_cb;
	AnonymousQueueDeclare_CB anonymous_queue_declare_cb;

	// ====  ====

	amqp_bytes_t sock_inbound_buffer; // 保存通过 recv 接收的网络数据的 buffer
	size_t sock_inbound_offset;       // 待读取数据的起始偏移量
	size_t sock_inbound_limit;        // 一次 recv 获取的数据量

	amqp_link_t *first_queued_frame;  // 待处理的帧列表的头
	amqp_link_t *last_queued_frame;   // 待处理的帧列表的尾

	amqp_rpc_reply_t most_recent_api_result;  // 保存最近一次请求的结果（可能是应答信息，也可能是错误信息）

	

	short hb_counter; // 用于控制由于网络断开或者服务器异常断电导致的“半打开”TCP问题

	// v4.0 new
	uint64_t next_recv_heartbeat;  // 下一次应收到 heartbeat 帧的时间戳
	uint64_t next_send_heartbeat;
};

amqp_pool_t *amqp_get_or_create_channel_pool(amqp_connection_state_t connection, amqp_channel_t channel);
amqp_pool_t *amqp_get_channel_pool(amqp_connection_state_t state, amqp_channel_t channel);

static inline amqp_boolean_t amqp_heartbeat_enabled(amqp_connection_state_t state)
{
	return (state->heartbeat > 0);
}

static inline uint64_t amqp_calc_next_send_heartbeat(amqp_connection_state_t state, uint64_t cur)
{
	return cur + ((uint64_t)state->heartbeat * AMQP_NS_PER_S);
}

static inline uint64_t amqp_calc_next_recv_heartbeat(amqp_connection_state_t state, uint64_t cur)
{
	return cur + ((uint64_t)state->heartbeat * 2 * AMQP_NS_PER_S);
}

int amqp_try_recv(amqp_connection_state_t state, uint64_t current_time);

static inline void *amqp_offset(void *data, size_t offset)
{
	return (char *)data + offset;
}

/* This macro defines the encoding and decoding functions associated with a
simple type. */

// 该宏定义了如下 4 个按字节存取数据的函数
#define DECLARE_CODEC_BASE_TYPE(bits, htonx, ntohx)                           \
	\
	static inline void amqp_e##bits(void *data, size_t offset, uint##bits##_t val) \
{									                                        \
	/* The AMQP data might be unaligned. So we encode and then copy the       \
result into place. */                                            \
	uint##bits##_t res = htonx(val);                                          \
	memcpy(amqp_offset(data, offset), &res, bits/8);                          \
}                                                                           \
	\
	static inline uint##bits##_t amqp_d##bits(void *data, size_t offset)        \
{                                                                           \
	/* The AMQP data might be unaligned.  So we copy the source value         \
into a variable and then decode it. */                           \
	uint##bits##_t val;                                                       \
	memcpy(&val, amqp_offset(data, offset), bits/8);                          \
	return ntohx(val);                                                        \
}                                                                           \
	\
	static inline int amqp_encode_##bits(amqp_bytes_t encoded, size_t *offset, uint##bits##_t input)  \
{                                                                           \
	size_t o = *offset;                                                       \
	if ((*offset = o + bits / 8) <= encoded.len) {                            \
	amqp_e##bits(encoded.bytes, o, input);                                  \
	return 1;                                                               \
	}                                                                         \
	else {                                                                    \
	return 0;                                                               \
}                                                                         \
}                                                                           \
	\
	static inline int amqp_decode_##bits(amqp_bytes_t encoded, size_t *offset, uint##bits##_t *output)  \
{                                                                           \
	size_t o = *offset;                                                       \
	if ((*offset = o + bits / 8) <= encoded.len) {                            \
	*output = amqp_d##bits(encoded.bytes, o);                               \
	return 1;                                                               \
	}                                                                         \
	else {                                                                    \
	return 0;                                                               \
}                                                                         \
}

/* Determine byte order */
#if defined(__GLIBC__)
# include <endian.h>
# if (__BYTE_ORDER == __LITTLE_ENDIAN)
#  define AMQP_LITTLE_ENDIAN
# elif (__BYTE_ORDER == __BIG_ENDIAN)
#  define AMQP_BIG_ENDIAN
# else
/* Don't define anything */
# endif
#elif defined(_BIG_ENDIAN) && !defined(_LITTLE_ENDIAN) ||                   \
	defined(__BIG_ENDIAN__) && !defined(__LITTLE_ENDIAN__)
# define AMQP_BIG_ENDIAN
#elif defined(_LITTLE_ENDIAN) && !defined(_BIG_ENDIAN) ||                   \
	defined(__LITTLE_ENDIAN__) && !defined(__BIG_ENDIAN__)
# define AMQP_LITTLE_ENDIAN
#elif defined(__hppa__) || defined(__HPPA__) || defined(__hppa) ||          \
	defined(_POWER) || defined(__powerpc__) || defined(__ppc___) ||       \
	defined(_MIPSEB) || defined(__s390__) ||                              \
	defined(__sparc) || defined(__sparc__)
# define AMQP_BIG_ENDIAN
#elif defined(__alpha__) || defined(__alpha) || defined(_M_ALPHA) ||        \
	defined(__amd64__) || defined(__x86_64__) || defined(_M_X64) ||       \
	defined(__ia64) || defined(__ia64__) || defined(_M_IA64) ||           \
	defined(__arm__) || defined(_M_ARM) ||                                \
	defined(__i386__) || defined(_M_IX86)
# define AMQP_LITTLE_ENDIAN
#else
/* Don't define anything */
#endif

#if defined(AMQP_LITTLE_ENDIAN)

#define DECLARE_XTOXLL(func)                        \
	static inline uint64_t func##ll(uint64_t val)     \
{                                                 \
union {                                         \
	uint64_t whole;                               \
	uint32_t halves[2];                           \
} u;                                            \
	uint32_t t;                                     \
	u.whole = val;                                  \
	t = u.halves[0];                                \
	u.halves[0] = func##l(u.halves[1]);             \
	u.halves[1] = func##l(t);                       \
	return u.whole;                                 \
}

#elif defined(AMQP_BIG_ENDIAN)

#define DECLARE_XTOXLL(func)                        \
	static inline uint64_t func##ll(uint64_t val)     \
{                                                 \
union {                                         \
	uint64_t whole;                               \
	uint32_t halves[2];                           \
} u;                                            \
	u.whole = val;                                  \
	u.halves[0] = func##l(u.halves[0]);             \
	u.halves[1] = func##l(u.halves[1]);             \
	return u.whole;                                 \
}

#else
# error Endianness not known
#endif

#ifndef HAVE_HTONLL
DECLARE_XTOXLL(hton)
DECLARE_XTOXLL(ntoh)
#endif

// 下面每一行宏定义相当于定义了4个相应函数
DECLARE_CODEC_BASE_TYPE(8, (uint8_t), (uint8_t))
DECLARE_CODEC_BASE_TYPE(16, htons, ntohs)
DECLARE_CODEC_BASE_TYPE(32, htonl, ntohl)
DECLARE_CODEC_BASE_TYPE(64, htonll, ntohll)

// 将由 input 指向的内容 encode 到 encoded 中 offset 指向的位置上
static inline int amqp_encode_bytes(amqp_bytes_t encoded, size_t *offset, amqp_bytes_t input)
{
	size_t o = *offset;
	if ((*offset = o + input.len) <= encoded.len) {
		memcpy(amqp_offset(encoded.bytes, o), input.bytes, input.len);
		return 1;
	} else {
		return 0;
	}
}

static inline int amqp_decode_bytes(amqp_bytes_t encoded, size_t *offset, amqp_bytes_t *output, size_t len)
{
	size_t o = *offset;
	if ((*offset = o + len) <= encoded.len) {
		output->bytes = amqp_offset(encoded.bytes, o);
		output->len = len;
		return 1;
	} else {
		return 0;
	}
}

AMQP_NORETURN
	void
	amqp_abort(const char *fmt, ...);

#endif
