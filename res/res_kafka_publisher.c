/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright (C) 2020, Vedga
 *
 * Igor Nikolaev <igorn@ozon.ru>
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

/*!
 * \brief Distribute Asterisk events via Kafka
 *
 * This module distribute Asterisk events via Kafka
 * topics.
 *
 * \author Igor Nikolaev <igorn@ozon.ru>
 * \since 13.7.0
 */

/*** MODULEINFO
	<depend type="module">res_kafka</depend>
	<depend type="module">res_stasis_device_state</depend>
	<defaultenabled>no</defaultenabled>
	<support_level>extended</support_level>
 ***/

/*** DOCUMENTATION
 ***/

#include "asterisk.h"

ASTERISK_FILE_VERSION(__FILE__, "$Revision$")

#include "asterisk/res_kafka.h"

#include "asterisk/module.h"
//#include "asterisk/sorcery.h"
#include "asterisk/stasis.h"
#include "asterisk/devicestate.h"
#include "asterisk/stasis_app_device_state.h"
#include "asterisk/utils.h"

/* Fowrdwd local functions declaration */
static void device_state_cb(void *data, struct stasis_subscription *sub, struct stasis_message *message);
static int load_module(void);
static int unload_module(void);
static int reload_module(void);

/* Local variables */
static struct stasis_subscription *device_state_subscription;

static void device_state_cb(void *data, struct stasis_subscription *sub, struct stasis_message *message) {
        struct ast_device_state_message *payload;
        enum ast_device_state state;
        const char *device;
	RAII_VAR(struct ast_kafka_pipe *, pipe, NULL, ao2_cleanup);
//	char *s;
	
        if (stasis_message_type(message) != ast_device_state_message_type()) {
                return;
        }

        payload = stasis_message_data(message);
        state = payload->state;
        device = payload->device;

        if (ast_strlen_zero(device)) {
                return;
        }

        /* Ignore aggregate events */
        if (!payload->eid) {
                return;
        }

	if(NULL != (pipe = ast_kafka_get_pipe("pipe_1",0))) {
		RAII_VAR(struct ast_json *, json, stasis_app_device_state_to_json(device, state), ast_json_unref);
		ast_kafka_publish(pipe, device, "peerstatus", json);
	}
	
	ast_debug(3, "Device '%s' change state to %u '%s'.\n", device, state, ast_devstate_str(state));
	
//	s = ast_json_dump_string(json);
	
//	ast_debug(3, "Device '%s' change state to %u '%s'. Event: '%s'\n", device, state, ast_devstate_str(state), s);
	
//	ast_json_free(s);
}

static int load_module(void) {
	
	if(NULL == (device_state_subscription = stasis_subscribe(ast_device_state_topic_all(), 
									device_state_cb, NULL))) {
		return AST_MODULE_LOAD_DECLINE;
	}
	
	return AST_MODULE_LOAD_SUCCESS;
}

static int unload_module(void) {
	device_state_subscription = stasis_unsubscribe_and_join(device_state_subscription);
	
	return 0;
}

static int reload_module(void) {
	return 0;
}
	
AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_DEFAULT, "Kafka events producer",
	.support_level = AST_MODULE_SUPPORT_EXTENDED,
	.load = load_module,
	.unload = unload_module,
	.reload = reload_module,
	);
	