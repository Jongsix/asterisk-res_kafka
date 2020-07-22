/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright (C) 2010 FIXME
 *
 * See http://www.asterisk.org for more information about
 * the Asterisk project. Please do not directly contact
 * any of the maintainers of this project for assistance;
 * the project provides a web site, mailing lists and IRC
 * channels for your use.
 *
 * This program is free software, distributed under the terms of
 * the GNU General Public License Version 2. See the LICENSE file
 * at the top of the source tree.
 */

/*! \file
 * \brief Kafka resource
 */

#ifndef _ASTERISK_RES_KAFKA_H
#define _ASTERISK_RES_KAFKA_H

#include "asterisk/json.h"

struct ast_kafka_pipe;

/*!
 * \brief Publish event to the specified pipe.
 * 
 * \details
 * Publish event to the specified pipe.
 * 
 * \note
 * 
 * \param pipe
 * \param key - Kafka message key, can be NULL
 * \param reason - event reason
 * \param payload
 * 
 * \return
 */
int ast_kafka_publish(struct ast_kafka_pipe *pipe, const char *key, 
			const char *reason, struct ast_json *payload);

/*!
 * \brief Send json message to the specified pipe.
 * 
 * \details
 * Send json message to the specified pipe.
 * 
 * \note
 * 
 * \param pipe
 * \param key - Kafka message key, can be NULL
 * \param json
 * \param reason - reason for message (added to message header) or NULL
 * 
 * \return
 */
int ast_kafka_send_json_message(struct ast_kafka_pipe *pipe, const char *key, 
				struct ast_json *json,
				const char *reason);

/*!
 * \brief Send raw message to the specified pipe.
 * 
 * \details
 * Send raw message to the specified pipe.
 * 
 * \note
 * 
 * \param pipe
 * \param key - Kafka message key, can be NULL
 * \param payoad
 * \param payload_size
 * \param reason - reason for message (added to message header) or NULL
 * 
 * \return
 */
int ast_kafka_send_raw_message(struct ast_kafka_pipe *pipe, const char *key, 
				const void *payload, size_t payload_size,
				const char *reason);
/*!
 * 
 */
struct ast_kafka_pipe *ast_kafka_get_pipe(const char *pipe_id, int force);

#endif /* _ASTERISK_RES_KAFKA_H */
