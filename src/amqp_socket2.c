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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "amqp_private.h"
#include <stdio.h>
#include <errno.h>

#ifdef _WIN32
# ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
# endif
# include <Winsock2.h>
# include <ws2tcpip.h>
#else
# include <sys/types.h>      /* On older BSD this must come before net includes */
# include <netinet/in.h>
# include <netinet/tcp.h>
# include <sys/socket.h>
# include <netdb.h>
# include <sys/uio.h>
# include <fcntl.h>
# include <unistd.h>
#include <signal.h>
#endif

extern const char *method_number_2_string( amqp_method_number_t method_number );

int amqp_os_socket_init(void);
int amqp_os_socket_socket(int domain, int type, int protocol);
int amqp_os_socket_setsockopt(int sock, int level, int optname, const void *optval, size_t optlen);
int amqp_os_socket_setsockblock(int sock, int block);

int amqp_open_socket_and_connect( char const *hostname, int portnumber, int *client_sockfd )
{
	struct addrinfo hint;
	struct addrinfo *address_list;
	struct addrinfo *addr;
	char portnumber_string[33]; //?
	int sockfd = -1;
	int last_error = AMQP_STATUS_OK;
	int one = 1; /* for setsockopt */
	int fd_close = 1;

	last_error = amqp_os_socket_init();
	if (AMQP_STATUS_OK != last_error) {
		return last_error;
	}

	memset(&hint, 0, sizeof(hint));
	hint.ai_family = PF_UNSPEC; /* PF_INET or PF_INET6 */
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_protocol = IPPROTO_TCP;

	(void)sprintf(portnumber_string, "%d", portnumber);

	last_error = getaddrinfo(hostname, portnumber_string, &hint, &address_list);

	if ( 0 != last_error )
	{
		return -AMQP_STATUS_HOSTNAME_RESOLUTION_FAILED;
	}

	for (addr = address_list; addr; addr = addr->ai_next)
	{
		if (-1 != sockfd) {
			amqp_os_socket_close(sockfd);
			sockfd = -1;
		}

		sockfd = amqp_os_socket_socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
		if (-1 == sockfd)
		{
			last_error = AMQP_STATUS_SOCKET_ERROR;
			continue;
		}

#ifdef SO_NOSIGPIPE
		// SO_NOSIGPIPE 仅在 IOS 系统上有效
		if (0 != amqp_os_socket_setsockopt(sockfd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one))) {
			last_error = AMQP_STATUS_SOCKET_ERROR;
			continue;
		}
#endif /* SO_NOSIGPIPE */

		if (0 != amqp_os_socket_setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one))) {
			last_error = AMQP_STATUS_SOCKET_ERROR;
			continue;
		}

		if ( AMQP_STATUS_OK != amqp_os_socket_setsockblock(sockfd, 0) )
		{
			last_error = AMQP_STATUS_SOCKET_ERROR;
			continue;
		}

		if ( -1 == connect(sockfd, addr->ai_addr, addr->ai_addrlen) )
		{
			last_error = amqp_os_socket_error();
#ifdef WIN32			
			if ( last_error == WSAEWOULDBLOCK || 
				last_error == WSAEINVAL || 
				last_error == WSAEINPROGRESS )
			{
				fd_close = 0;
				break;
			}
#else
			if (last_error == EINPROGRESS) {
				fd_close = 0;
				break;
			}
#endif
		}
		else
		{
			last_error = AMQP_STATUS_OK;
			fd_close = 0;
			break;
		}		
	}

	freeaddrinfo( address_list );

	if ( fd_close != 0 ) {
		if (-1 != sockfd) {
			amqp_os_socket_close(sockfd);
			sockfd = -1;
		}
	}

#if !defined(WIN32) && !defined(_WIN32)
	if ( signal( SIGPIPE, SIG_IGN ) == SIG_ERR )
	{
		fprintf( stderr, "[MQ] set SIGPIPE handler failed! Warning!\n" );
	}
#endif

	*client_sockfd = sockfd;

	return last_error;
}

static try_read_result_enum wait_frame_inner_nonblock(amqp_connection_state_t state, amqp_frame_t *decoded_frame)
{
	int res;
	int last_error;

	while (1) {

		while ( amqp_data_in_buffer(state) ) {
			amqp_bytes_t buffer;
			buffer.len = state->sock_inbound_limit - state->sock_inbound_offset;
			buffer.bytes = ((char *)state->sock_inbound_buffer.bytes) + state->sock_inbound_offset;

			res = amqp_handle_input(state, buffer, decoded_frame);
			if (res < 0)
				return READ_ERROR;

			state->sock_inbound_offset += res;

			/* Complete frame was read. Return it. */
			if (decoded_frame->frame_type != 0)
				return READ_DATA_RECEIVED;
		}

		res = recv( amqp_socket_get_sockfd(state->socket), state->sock_inbound_buffer.bytes, state->sock_inbound_buffer.len, 0 );
		if ( res == 0 ) {
			fprintf( stderr, "[MQ][%s s:%d] wait_frame_inner_nonblock() ==> recv (0), this is FIN packet!\n", 
				state->tag, amqp_get_sockfd(state) );
			return READ_ERROR;
		}
		if ( res == -1 ) {
			last_error = amqp_os_socket_error();
			if ( last_error == EAGAIN || last_error == E_NET_WOULDBLOCK ) {
				return READ_NO_DATA_RECEIVED;
			}
			fprintf( stderr, "[MQ][%s s:%d] wait_frame_inner_nonblock ==> recv (-1), Error(%d) '%s'\n", 
				state->tag, amqp_get_sockfd(state), last_error, strerror(last_error) );
			return READ_ERROR;
		}

		state->sock_inbound_limit = res;
		state->sock_inbound_offset = 0;
	}
}

try_read_result_enum amqp_simple_wait_frame_nonblock( amqp_connection_state_t state, amqp_frame_t *decoded_frame )
{
	if (state->first_queued_frame != NULL) {
		amqp_frame_t *f = (amqp_frame_t *) state->first_queued_frame->data;
		state->first_queued_frame = state->first_queued_frame->next;

		if (state->first_queued_frame == NULL) {
			state->last_queued_frame = NULL;
		}
		*decoded_frame = *f;
		return READ_DATA_RECEIVED;
	} else {
		return wait_frame_inner_nonblock(state, decoded_frame);
	}
}

try_read_result_enum amqp_simple_wait_method_nonblock(amqp_connection_state_t state,
	amqp_channel_t expected_channel,
	amqp_method_number_t expected_method,
	amqp_method_t *output)
{
	amqp_frame_t frame;
	try_read_result_enum res = amqp_simple_wait_frame_nonblock( state, &frame );
	switch ( res )
	{
	case READ_DATA_RECEIVED:
		{
			do 
			{
				if ( frame.channel != expected_channel )
				{
					if ( frame.frame_type == AMQP_FRAME_HEARTBEAT )
					{
						// Hack: 告知状态机收到的是 Heartbeat 帧
						res = READ_DATA_RECEIVED_HEARTBEAT;
					}
					else if ( frame.frame_type == AMQP_FRAME_METHOD )
					{
						switch ( frame.payload.method.id ) 
						{
						case AMQP_CONNECTION_CLOSE_METHOD:
							{
								// Hack: 告知状态机收到的是 Connection.Close method 帧
								res = READ_DATA_RECEIVED_CONNECTION_CLOSE;
							}
							break;
						default:
							fprintf( stderr, "@@@@ Warning @@@ unknown frame method, method id 0x%08X\n", frame.payload.method.id );
							break;
						}
					}
					break;
				}

				if ( frame.frame_type != AMQP_FRAME_METHOD )
				{
					fprintf( stderr, "Expected 0x%08X method frame on channel %d, but got frame type %d", 
						expected_method, expected_channel, frame.frame_type);
					break;
				}

				if ( frame.payload.method.id != expected_method )
				{
					switch ( frame.payload.method.id ) 
					{
					case AMQP_CONNECTION_CLOSE_METHOD:
						{
							// Hack: 告知状态机收到的是 Connection.Close method 帧
							res = READ_DATA_RECEIVED_CONNECTION_CLOSE;
						}
						break;

					case AMQP_CHANNEL_CLOSE_METHOD:
						{
							// Hack: 告知状态机收到的是 Channel.Close method 帧
							res = READ_DATA_RECEIVED_CHANNEL_CLOSE;
						}
						break;

					case AMQP_BASIC_NACK_METHOD:
						{
							fprintf( stderr, "Expected method ID 0x%08X on channel %d, but got AMQP_BASIC_NACK_METHOD method!\n", 
								expected_method, expected_channel );
							fprintf( stderr, "Message is lost by RabbitMQ internal Error! Should do something to re-publish!\n" );

							// Hack: 告知状态机收到的是 Basic.Nack method 帧
							res = READ_DATA_RECEIVED_BASIC_NACK;
						}
						break;

					case AMQP_BASIC_REJECT_METHOD:
						{
							fprintf( stderr, "Expected method ID 0x%08X on channel %d, but got AMQP_BASIC_REJECT_METHOD method!\n", 
								expected_method, expected_channel );
							fprintf( stderr, "Message is lost by RabbitMQ internal Error! Should do something to re-publish!\n" );

							// Hack: 告知状态机收到的是 Basic.Reject method 帧
							res = READ_DATA_RECEIVED_BASIC_REJECT;
						}
						break;

					default:
						fprintf( stderr, "### Warning ### unknown frame method, method id 0x%08X\n", frame.payload.method.id );
						break;
					}
					break;
				}

			} while (0);
			*output = frame.payload.method;
		}
		break;

	case READ_NO_DATA_RECEIVED:
// 		fprintf( stderr, "[MQ][%s s:%d] Expected method %s, but Socket Read no Data!\n", 
// 			state->tag, amqp_get_sockfd(state), method_number_2_string(expected_method) );
		break;

	case READ_ERROR:
		fprintf( stderr, "[MQ][%s s:%d] Expect (%s), but Recv (SocketRead.Error)! (Check username and password first)\n", 
			state->tag, amqp_get_sockfd(state), method_number_2_string(expected_method) );
		break;

	default:
		fprintf( stderr, "[MQ] Should not be here! Error!\n" );
		break;
	}

	return res;
}

try_read_result_enum amqp_simple_wait_content_nonblock(amqp_connection_state_t state, 
	amqp_channel_t expected_channel, 
	uint8_t expected_frame_type, 
	amqp_frame_t *output )
{
	try_read_result_enum res = amqp_simple_wait_frame_nonblock( state, output );
	switch ( res )
	{
	case READ_DATA_RECEIVED:
		{
			if ( output->channel != expected_channel )
			{
				fprintf( stderr, "Expected CONTENT frame on channel %d, got frame on channel %d\n", expected_channel, output->channel);
				return READ_NO_DATA_RECEIVED;
			}

			if ( output->frame_type != expected_frame_type )
			{
				fprintf( stderr, "Expected 0x%08X CONTENT frame on channel %d, got frame type %d\n", 
					expected_frame_type, expected_channel, output->frame_type );
				output = NULL;
				return READ_NO_DATA_RECEIVED;
			}
		}
		break;
	case READ_NO_DATA_RECEIVED:
		break;
	case READ_ERROR:
		fprintf( stderr, "There are some error on current socket!\n" );
		break;
	default:
		fprintf( stderr, "Should not be here!\n" );
		break;
	}

	return res;
}

try_write_result_enum amqp_send_method_nonblock(amqp_connection_state_t state,
	amqp_channel_t channel,
	amqp_method_number_t id,
	void *decoded)
{
	amqp_frame_t frame;

	frame.frame_type = AMQP_FRAME_METHOD;
	frame.channel = channel;
	frame.payload.method.id = id;
	frame.payload.method.decoded = decoded;
	return amqp_send_frame_nonblock(state, &frame);
}


