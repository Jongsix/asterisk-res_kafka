/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright (C) 2020, Vedga
 *
 * Igor Nikolaev <support@vedga.com>
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
 * \brief Kafka support module.
 *
 * This module expose Kafka library resources
 * based on their state.
 *
 * \author Igor Nikolaev <support@vedga.com>
 * \since 13.7.0
 */

/*** MODULEINFO
	<use type="external">rdkafka</use>
	<defaultenabled>no</defaultenabled>
	<support_level>extended</support_level>
 ***/

/*** DOCUMENTATION
	<configInfo name="res_kafka" language="en_US">
		<synopsis>Kafka Resource using rdkafka client library</synopsis>
		<configFile name="kafka.conf">
			<configObject name="cluster">
				<synopsis>Kafka cluster description</synopsis>
				<description><para>
					The <emphasis>Endpoint</emphasis> is the primary configuration object.
					It contains the core SIP related options only, endpoints are <emphasis>NOT</emphasis>
					dialable entries of their own. Communication with another SIP device is
					accomplished via Addresses of Record (AoRs) which have one or more
					contacts associated with them. Endpoints <emphasis>NOT</emphasis> configured to
					use a <literal>transport</literal> will default to first transport found
					in <filename>pjsip.conf</filename> that matches its type.
					</para>
					<para>Example: An Endpoint has been configured with no transport.
					When it comes time to call an AoR, PJSIP will find the
					first transport that matches the type. A SIP URI of <literal>sip:5000@[11::33]</literal>
					will use the first IPv6 transport and try to send the request.
					</para>
					<para>If the anonymous endpoint identifier is in use an endpoint with the name
					"anonymous@domain" will be searched for as a last resort. If this is not found
					it will fall back to searching for "anonymous". If neither endpoints are found
					the anonymous endpoint identifier will not return an endpoint and anonymous
					calling will not be possible.
					</para>
				</description>
				<configOption name="type">
					<synopsis>Must be of type 'cluster'</synopsis>
				</configOption>
				<configOption name="brokers" default="localhost">
					<synopsis>Bootstrap CSV list of brokers or host:port values</synopsis>
				</configOption>
				<configOption name="security_protocol" default="plaintext">
					<synopsis>Protocol used to communicate with brokers</synopsis>
					<description>
						<enumlist>
							<enum name="plaintext" />
							<enum name="ssl" />
							<enum name="sasl_plaintext" />
							<enum name="sasl_ssl" />
						</enumlist>
					</description>
				</configOption>
				<configOption name="sasl_mechanism" default="PLAIN">
					<synopsis>SASL mechanism to use for authentication</synopsis>
					<description>
						<enumlist>
							<enum name="PLAIN" />
							<enum name="GSSAPI" />
							<enum name="SCRAM-SHA-256" />
							<enum name="SCRAM-SHA-512" />
							<enum name="OAUTHBEARER" />
						</enumlist>
					</description>
				</configOption>
				<configOption name="sasl_username">
					<synopsis>SASL authentication username</synopsis>
				</configOption>
				<configOption name="sasl_password">
					<synopsis>SASL authentication password</synopsis>
				</configOption>
				<configOption name="client_id" default="asterisk">
					<synopsis>Client id for this connection</synopsis>
				</configOption>
			</configObject>
 
			<configObject name="producer">
				<synopsis>Kafka producer description</synopsis>
				<configOption name="type">
					<synopsis>Must be of type 'producer'</synopsis>
				</configOption>
				<configOption name="cluster">
					<synopsis>Cluster resource id</synopsis>
				</configOption>
				<configOption name="timeout" default="10000">
					<synopsis>Default service timeout, ms. Default 10000ms.</synopsis>
				</configOption>
				<configOption name="partition" default="-1">
					<synopsis>Producer's partition, less than zero if unassigned (default)</synopsis>
				</configOption>
				<configOption name="request_required_acks" default="-1">
					<synopsis>request.required.acks (default -1)</synopsis>
				</configOption>
				<configOption name="max_in_flight_requests_per_connection" default="1000000">
					<synopsis>max.in.flight.requests.per.connection (default 1000000)</synopsis>
				</configOption>
				<configOption name="message_send_max_retries" default="2">
					<synopsis>message.send.max.retries (default 2)</synopsis>
				</configOption>
				<configOption name="enable_idempotence" default="no">
					<synopsis>enable.idempotence (default no)</synopsis>
					<description>
						<enumlist>
							<enum name="no" />
							<enum name="yes" />
						</enumlist>
					</description>
				</configOption>
				<configOption name="transactional_id">
					<synopsis>transactional.id (default none)</synopsis>
				</configOption>
				<configOption name="retry_backoff_ms" default="100">
					<synopsis>The backoff time in milliseconds before retrying a protocol request.</synopsis>
					<description><para>
						The backoff time in milliseconds before retrying a protocol request.
						</para>
						<para>Kafka property: retry.backoff.ms</para>
						<para>Default value: 100</para>
					</description>
				</configOption>
				<configOption name="client_id">
					<synopsis>Client id (default from cluster configuration)</synopsis>
				</configOption>
				<configOption name="debug">
					<synopsis>Comma-separated list of debug contexts to enable</synopsis>
				</configOption>
			</configObject>

			<configObject name="consumer">
				<synopsis>Kafka consumer description</synopsis>
				<configOption name="type">
					<synopsis>Must be of type 'consumer'</synopsis>
				</configOption>
				<configOption name="cluster">
					<synopsis>Cluster resource id</synopsis>
				</configOption>
				<configOption name="group_id" default="asterisk">
					<synopsis>Client group id string. All clients sharing the same group_id belong to the same group. Default is 'asterisk'.</synopsis>
				</configOption>
				<configOption name="timeout" default="10000">
					<synopsis>Default service timeout, ms. Default 10000ms.</synopsis>
				</configOption>
				<configOption name="partition" default="-1">
					<synopsis>Consumer's partition, less than zero if unassigned (default)</synopsis>
				</configOption>
				<configOption name="enable_auto_commit" default="yes">
					<synopsis>Enable auto commit (default yes)</synopsis>
					<description>
						<enumlist>
							<enum name="no" />
							<enum name="yes" />
						</enumlist>
					</description>
				</configOption>
				<configOption name="auto_commit_interval" default="5000">
					<synopsis>Interval when consumer commited offset, ms. Default 5000ms.</synopsis>
				</configOption>
				<configOption name="isolation_level" default="read_committed">
					<synopsis>isolation.level (default 'read_committed')</synopsis>
					<description>
						<enumlist>
							<enum name="read_committed" />
							<enum name="read_uncommitted" />
						</enumlist>
					</description>
				</configOption>
				<configOption name="client_id">
					<synopsis>Client id (default from cluster configuration)</synopsis>
				</configOption>
				<configOption name="debug">
					<synopsis>Comma-separated list of debug contexts to enable</synopsis>
				</configOption>
			</configObject>

			<configObject name="topic">
				<synopsis>Kafka topic description</synopsis>
				<configOption name="type">
					<synopsis>Must be of type 'topic'</synopsis>
				</configOption>
				<configOption name="pipe">
					<synopsis>Asterisk messages pipe name</synopsis>
				</configOption>
				<configOption name="topic">
					<synopsis>Kafka topic name</synopsis>
				</configOption>
				<configOption name="producer">
					<synopsis>Producer resource id</synopsis>
				</configOption>
				<configOption name="consumer">
					<synopsis>Consumer resource id</synopsis>
				</configOption>
			</configObject>
		</configFile>
	</configInfo>
 ***/


#include "asterisk.h"

ASTERISK_FILE_VERSION(__FILE__, "$Revision$")

#include "asterisk/res_kafka.h"

#include "asterisk/module.h"
#include "asterisk/sorcery.h"
//#include "asterisk/taskprocessor.h"
#include "asterisk/astobj2.h"
#include "asterisk/linkedlists.h"
#include "asterisk/dlinkedlists.h"
#include "asterisk/cli.h"
#include "asterisk/stringfields.h"
#include "asterisk/utils.h"
#include "asterisk/lock.h"
/* define ast_config_AST_SYSTEM_NAME */
#include "asterisk/paths.h"


#include "librdkafka/rdkafka.h"

#include <unistd.h>
#include <string.h>

#define KAFKA_CONFIG_FILENAME "kafka.conf"

#define KAFKA_CLUSTER "cluster"
#define KAFKA_TOPIC "topic"
#define KAFKA_PRODUCER "producer"
#define KAFKA_CONSUMER "consumer"
#define KAFKA_ERRSTR_MAX_SIZE 80
#define TMP_BUF_SIZE 32

#define KAFKA_MONITOR_NO_EVENTS_SLEEP_US 500

//#define KAFKA_TASKPROCESSOR_MONITOR_ID "kafka/monitor"

/*! Buckets for pipe hash. Keep it prime! */
#define KAFKA_PIPE_BUCKETS 127

/*! Kafka cluster common parameters */
struct sorcery_kafka_cluster {
	SORCERY_OBJECT(defails);
	AST_DECLARE_STRING_FIELDS(
		/*! Initial (bootstrap) CSV list of brokers or host:port */
		AST_STRING_FIELD(brokers);
		/*! Security protocol used to communicate with broker */
		AST_STRING_FIELD(security_protocol);
		/*! SASL mechanism used to authenticate */
		AST_STRING_FIELD(sasl_mechanism);
		/*! SASL authentication username */
		AST_STRING_FIELD(sasl_username);
		/*! SASL authentication password */
		AST_STRING_FIELD(sasl_password);
		/*! Client identifier */
		AST_STRING_FIELD(client_id);
	);
};

/*! Kafka producer common parameters */
struct sorcery_kafka_producer {
	SORCERY_OBJECT(defails);
	AST_DECLARE_STRING_FIELDS(
		/*! Cluster resource id */
		AST_STRING_FIELD(cluster_id);
		/*! Client identifier */
		AST_STRING_FIELD(client_id);
		/*! transactional.id */
		AST_STRING_FIELD(transactional_id);
		/*! Comma separated contexts for debug */
		AST_STRING_FIELD(debug);
	);
	/*! Default service timeout, ms */
	unsigned int timeout_ms;
	/*! Producer's partition, less than zero mean is unassigned */
	int partition;
	/*! request.required.acks */
	int request_required_acks;
	/*! max.in.flight.requests.per.connection */
	unsigned int max_in_flight_requests_per_connection;
	/*! message.send.max.retries */
	unsigned int message_send_max_retries;
	/*! enable.idempotence */
	unsigned int enable_idempotence;
	/*! The backoff time in milliseconds before retrying a protocol request. */
	unsigned int retry_backoff_ms;
};

/*! Kafka consumer common parameters */
struct sorcery_kafka_consumer {
	SORCERY_OBJECT(defails);
	AST_DECLARE_STRING_FIELDS(
		/*! Cluster resource id */
		AST_STRING_FIELD(cluster_id);
		/*! Client identifier */
		AST_STRING_FIELD(client_id);
		/*! Client group id */
		AST_STRING_FIELD(group_id);
		/*! isolation.level */
		AST_STRING_FIELD(isolation_level);
		/*! Comma separated contexts for debug */
		AST_STRING_FIELD(debug);
	);
	/*! Default service timeout, ms */
	unsigned int timeout_ms;
	/*! Consumer's partition, less than zero mean is unassigned */
	int partition;
	/*! enable.auto.commit */
	unsigned int enable_auto_commit;
	/*! Automatic update offset interval, ms */
	unsigned int auto_commit_interval_ms;
};

/*! Kafka topic common parameters */
struct sorcery_kafka_topic {
	SORCERY_OBJECT(defails);
	AST_DECLARE_STRING_FIELDS(
		/*! Pipe id, used by Asterisk modules to produce or consume messages */
		AST_STRING_FIELD(pipe);
		/*! Kafka topic name */
		AST_STRING_FIELD(topic);
		/*! Producer resource id */
		AST_STRING_FIELD(producer_id);
		/*! Consumer resource id */
		AST_STRING_FIELD(consumer_id);
	);
};

/*! Internal representation of Kafka's producer or consumer service */
struct kafka_service {
	/*! Link to next service on the global services (producers or consumers) list */
	AST_RWDLLIST_ENTRY(kafka_service) link;
	/*! librdkafka producer's or consumer's handle */
	rd_kafka_t *rd_kafka;
	/*! Default service timeout, ms */
	unsigned int timeout_ms;
	/*! Service partition */
	int32_t partition;
	/*! Linked topics (must be accessed with lock producers or customers variable) */
	unsigned int topic_count;
	/*! Producer or consumer specific */
	union {
		/*! Producer specific */
		struct {
		} producer;
		/*! Consumer specific */
		struct {
			/*! Consumer message queue */
			rd_kafka_queue_t *queue;
		} consumer;
	} specific;
};

/*! Internal representation of Kafka's topic */
struct kafka_topic {
	/*! Link to next topic on the list in ast_kafka_pipe */
	AST_LIST_ENTRY(kafka_topic) link;
	/*! Link to the topic service (producer or consumer) */
	struct kafka_service *service;
	/*! librdkafka topic's handle */
	rd_kafka_topic_t *rd_kafka_topic;
#if 0
	/*! Producer or consumer specific */
	union {
		/*! Producer specific */
		struct {
		} producer;
		/*! Consumer specific */
		struct {
			/*! Need to call rd_kafka_consume_stop() */
			int consume_started;
		} consumer;
	} specific;
#endif
};

/*! Internal representation of message pipe */
struct ast_kafka_pipe {
	AST_DECLARE_STRING_FIELDS(
		/*! Pipe id, used by Asterisk modules to produce or consume messages */
		AST_STRING_FIELD(id);
	);
	/*! List of producer's topics for this pipe */
	AST_LIST_HEAD(/*producer_topics_s*/, kafka_topic) producer_topics;
	/*! List of consumer's topics for this pipe */
	AST_LIST_HEAD(/*consumer_topics_s*/, kafka_topic) consumer_topics;
};

/* Fowrdwd local functions declaration */

static int send_message(struct kafka_topic *topic, void *opaque_1, void *opaque_2, struct ast_kafka_pipe *pipe);

static char *handle_kafka_loopback(struct ast_cli_entry *e, int cmd, struct ast_cli_args *a);
static char *handle_kafka_show(struct ast_cli_entry *e, int cmd, struct ast_cli_args *a);
static char *complete_pipe_choice(const char *word);
static int show_pipes_cb(void *obj, void *arg, int flags);
static int cli_show_topic_cb(struct kafka_topic *topic, void *opaque_1, void *opaque_2, struct ast_kafka_pipe *pipe);


static void on_producer_created(const void *obj);
static void on_producer_updated(const void *obj);
static void on_producer_deleted(const void *obj);
static void on_producer_loaded(const char *type);


static void process_all_clusters(void);
static void process_cluster(const struct sorcery_kafka_cluster *cluster);
static int process_producer(const struct sorcery_kafka_cluster *sorcery_cluster, const struct sorcery_kafka_producer *sorcery_producer);
static int process_consumer(const struct sorcery_kafka_cluster *sorcery_cluster, const struct sorcery_kafka_consumer *sorcery_consumer);

static struct kafka_service *new_kafka_producer(const struct sorcery_kafka_cluster *sorcery_cluster, const struct sorcery_kafka_producer *sorcery_producer);
static void kafka_producer_destructor(void *obj);
static struct kafka_service *new_kafka_consumer(const struct sorcery_kafka_cluster *sorcery_cluster, const struct sorcery_kafka_consumer *sorcery_consumer);
static void kafka_consumer_destructor(void *obj);

static rd_kafka_conf_t *build_rdkafka_cluster_config(const struct sorcery_kafka_cluster *cluster);
static int service_add_property_int(rd_kafka_conf_t *config, 
					const char *property, int value, 
					const char *service_type,
					const char *service_id,
					const char *cluster_id);
static int service_add_property_uint(rd_kafka_conf_t *config, 
					const char *property, unsigned value, 
					const char *service_type,
					const char *service_id,
					const char *cluster_id);
static int service_add_property_string(rd_kafka_conf_t *config, 
					const char *property, const char *value,
					const char *service_type,
					const char *service_id,
					const char *cluster_id);


static int process_producer_topic(struct kafka_service *producer, const struct sorcery_kafka_topic *sorcery_topic);
static int process_consumer_topic(struct kafka_service *consumer, const struct sorcery_kafka_topic *sorcery_topic);

static struct kafka_topic *new_kafka_producer_topic(struct kafka_service *producer, const struct sorcery_kafka_topic *sorcery_topic);
static void kafka_producer_topic_destructor(void *obj);
static struct kafka_topic *new_kafka_consumer_topic(struct kafka_service *producer, const struct sorcery_kafka_topic *sorcery_topic);
static void kafka_consumer_topic_destructor(void *obj);

static int start_monitor_thread(void);
static void *monitor_thread_job(void *opaque);
static void on_producer_message_processed(rd_kafka_t *rd_kafka, const rd_kafka_message_t *message, void *opaque);
static void on_consumer_message_processed(rd_kafka_t *rd_kafka, const rd_kafka_message_t *message, void *opaque);


static int on_all_producers(int (*callback)(struct kafka_service *service, void *opaque), void *opaque, int lock, int wrlock);
static int on_all_consumers(int (*callback)(struct kafka_service *service, void *opaque), void *opaque, int lock, int wrlock);

static int on_all_producer_topics(struct ast_kafka_pipe *pipe, int (*callback)(struct kafka_topic *topic, void *opaque_1, void *opaque_2, struct ast_kafka_pipe *pipe), void *opaque_1, void *opaque_2);
static int on_all_consumer_topics(struct ast_kafka_pipe *pipe, int (*callback)(struct kafka_topic *topic, void *opaque_1, void *opaque_2, struct ast_kafka_pipe *pipe), void *opaque_1, void *opaque_2);
static struct kafka_service *rdkafka_topic_to_service(const rd_kafka_topic_t *rd_kafka_topic);
static struct kafka_topic *rdkafka_topic_to_topic(const rd_kafka_topic_t *rd_kafka_topic);

static int sorcery_object_register(const char *type, void *(*alloc)(const char *name),int (*apply)(const struct ast_sorcery *sorcery, void *obj));
static int sorcery_kafka_topic_apply_handler(const struct ast_sorcery *sorcery, void *obj);
static void *sorcery_kafka_topic_alloc(const char *name);
static void sorcery_kafka_topic_destructor(void *obj);
static int sorcery_kafka_producer_apply_handler(const struct ast_sorcery *sorcery, void *obj);
static void *sorcery_kafka_producer_alloc(const char *name);
static void sorcery_kafka_producer_destructor(void *obj);
static int sorcery_kafka_consumer_apply_handler(const struct ast_sorcery *sorcery, void *obj);
static void *sorcery_kafka_consumer_alloc(const char *name);
static void sorcery_kafka_consumer_destructor(void *obj);
static int sorcery_kafka_cluster_apply_handler(const struct ast_sorcery *sorcery, void *obj);
static void *sorcery_kafka_cluster_alloc(const char *name);
static void sorcery_kafka_cluster_destructor(void *obj);

static void *kafka_pipe_alloc(const char *pipe_id);
static void kafka_pipe_destructor(void *obj);

static struct ast_cli_entry kafka_cli[] = {
	AST_CLI_DEFINE(handle_kafka_show, "Show module data"),
	AST_CLI_DEFINE(handle_kafka_loopback, "Loopback test operations"),
};

static const struct ast_sorcery_observer producer_observers = {
	.created = on_producer_created,
	.updated = on_producer_updated,
	.deleted = on_producer_deleted,
	.loaded = on_producer_loaded,
};

/*! Protect access to the monitor variable */
AST_MUTEX_DEFINE_STATIC(monitor_lock);

/*! Module's monitor thread */
static pthread_t monitor;

/*! Module's taskprocessor */
//static struct ast_taskprocessor *kafka_tps;

/*! List of active producers */
static AST_RWLIST_HEAD(kafka_producers_head_t, kafka_service) producers;

/*! List of active consumers */
static AST_RWLIST_HEAD(kafka_consumers_head_t, kafka_service) consumers;

/*! Sorcery */
static struct ast_sorcery *kafka_sorcery;

/*! Defined pipes container */
static struct ao2_container *pipes;

int ast_kafka_publish(struct ast_kafka_pipe *pipe, const char *key, 
			const char *reason, struct ast_json *payload) {
	char eid_str[128];
	char pbx_uuid[AST_UUID_STR_LEN];
	RAII_VAR(struct ast_json *, json, NULL, ast_json_unref);
	
        if(ast_eid_is_empty(&ast_eid_default)) {
                ast_log(LOG_WARNING, "Entity ID is not set.\n");
	}
	
        ast_eid_to_str(eid_str, sizeof(eid_str), &ast_eid_default);
        ast_pbx_uuid_get(pbx_uuid, sizeof(pbx_uuid));

        json = ast_json_pack("{s: s, s: s, s: s, s:s, s: o}",
				"reason", reason,
				"eid", eid_str,
				"uuid", pbx_uuid,
				"sysname", ast_config_AST_SYSTEM_NAME,
				"payload", ast_json_ref(payload));
	
	return ast_kafka_send_json_message(pipe, key, json);
}


/*! Module API: Send json message to the pipe */
int ast_kafka_send_json_message(struct ast_kafka_pipe *pipe, const char *key, 
				struct ast_json *json) {
	int processed = 0;
	char *raw = ast_json_dump_string(json);
	
	if(raw) {
		ast_debug(3, "Message to send: '%s'", raw);
		
		processed = ast_kafka_send_raw_message(pipe, key, raw, strlen(raw));

		ast_json_free(raw);
	}
	
	return processed;
}

/*! Module API: Send raw message to the pipe */
int ast_kafka_send_raw_message(struct ast_kafka_pipe *pipe, const char *key, 
				const void *payload, size_t payload_size) {
	return on_all_producer_topics(pipe, send_message, (void*)payload, &payload_size);
}

static int send_message(struct kafka_topic *topic, void *opaque_1, void *opaque_2, struct ast_kafka_pipe *pipe) {
	void *payload = opaque_1;
	size_t payload_size = *(size_t *)opaque_2;

	if(RD_KAFKA_PARTITION_UA == topic->service->partition) {
		ast_debug(3, "Kafka pipe %s (%p) try to produce message on topic (%p) via unassigned partition, payload=%p, size=%lu\n", 
				pipe->id, pipe, topic->rd_kafka_topic, payload, payload_size);
	} else {
		ast_debug(3, "Kafka pipe %s (%p) try to produce message on topic (%p) via partition %d, payload=%p, size=%lu\n", 
				pipe->id, pipe, topic->rd_kafka_topic, topic->service->partition, payload, payload_size);
	}

	return rd_kafka_produce(topic->rd_kafka_topic, 
				topic->service->partition, 
				RD_KAFKA_MSG_F_COPY, 
				payload, payload_size,	/* payload and payload size*/
				NULL, 0,		/* key and key size */
				NULL);
}

/*! Module API: Get existing pipe or create new if not present */
struct ast_kafka_pipe *ast_kafka_get_pipe(const char *pipe_id, int force) {
	struct ast_kafka_pipe *pipe;

	ao2_lock(pipes);

	if((NULL == (pipe = ao2_find(pipes, pipe_id, OBJ_SEARCH_KEY|OBJ_NOLOCK))) && force) {
		/* Pipe with requested id not found */
		if(NULL != (pipe = kafka_pipe_alloc(pipe_id))) {
			/* Add new pipe to the container */
			ao2_link_flags(pipes, pipe, OBJ_NOLOCK);

			ast_debug(3, "New Kafka pipe %s (%p) added to the container\n", pipe->id, pipe);
		}
	}

	ao2_unlock(pipes);

	return pipe;
}

/*! Cli loppback command */
static char *handle_kafka_loopback(struct ast_cli_entry *e, int cmd, struct ast_cli_args *a) {
	static const char *option[] = {
		"pipe",
		NULL,
	};
	static const char *pipe_option[] = {
		"start",
		"stop",
		NULL,
	};

	switch(cmd) {
	case CLI_INIT:
		e->command = "kafka loopback";
		e->usage =
			"Usage: kafka loopback pipe <pipe_id>\n"
			"       Execute loopback test on specified pipe\n";
		return NULL;
	case CLI_GENERATE:
		switch(a->pos) {
		case 2:
			/* "kafka loopback" */
			return ast_cli_complete(a->word, option, a->n);
		case 3:
			/* "kafka loopback some"*/
			if(0 == strcasecmp(a->argv[2], option[0])) {
				/* "kafka loopback pipe"*/
				return complete_pipe_choice(a->word);
			}
			return NULL;
		case 4:
			if(0 == strcasecmp(a->argv[2], option[0])) {
				/* "kafka loopback pipe <pipe_id>"*/
				return ast_cli_complete(a->word, pipe_option, a->n);
			}
			return NULL;
		default:
			/* No completion choices */
			return NULL;
		}
	default:
		break;
	}

	if(5 == a->argc) {
		if(0 == strcasecmp(a->argv[2], option[0])) {
			RAII_VAR(struct ast_kafka_pipe *, pipe, ast_kafka_get_pipe(a->argv[3],0), ao2_cleanup);

			if(NULL == pipe) {
				ast_cli(a->fd, "Pipe '%s' not found\n", a->argv[3]);
			} else {
				int res = ast_kafka_send_raw_message(pipe, NULL, "PING LOOPBACK", 13);

				ast_cli(a->fd, "Execute %s on pipe %s with code %d\n", a->argv[4], a->argv[3], res);
			}

			return CLI_SUCCESS;
		}
	}

	return CLI_SHOWUSAGE;
}


/*! Cli show command */
static char *handle_kafka_show(struct ast_cli_entry *e, int cmd, struct ast_cli_args *a) {
	static const char *option[] = {
		"version",
		"pipes",
		"pipe",
		NULL,
	};
	static const char *pipe_option[] = {
		"services",
		"producers",
		"consumers",
		NULL,
	};

	switch(cmd) {
	case CLI_INIT:
		e->command = "kafka show";
		e->usage =
			"Usage: kafka show version|pipes|pipe\n"
			"       Show the version of librdkafka that res_kafka is running against\n";
		return NULL;
	case CLI_GENERATE:
		switch(a->pos) {
		case 2:
			/* "kafka show" */
			return ast_cli_complete(a->word, option, a->n);
		case 3:
			/* "kafka show some"*/
			if(0 == strcasecmp(a->argv[2], option[2])) {
				/* "kafka show pipe"*/
				return complete_pipe_choice(a->word);
			}
			return NULL;
		case 4:
			if(0 == strcasecmp(a->argv[2], option[2])) {
				/* "kafka show pipe <pipe_id>"*/
				return ast_cli_complete(a->word, pipe_option, a->n);
			}
			return NULL;
		default:
			/* No completion choices */
			return NULL;
		}
	default:
		break;
	}

	if(3 == a->argc) {
		if(0 == strcasecmp(a->argv[2], option[0])) {
			/* "kafka show version" */
			ast_cli(a->fd, "librdkafka version currently running against: %s\n", rd_kafka_version_str());
			return CLI_SUCCESS;
		}

		if(0 == strcasecmp(a->argv[2], option[1])) {
			/* "kafka show pipes" */
			ao2_callback(pipes, OBJ_NODATA, show_pipes_cb, a);
			return CLI_SUCCESS;
		}
		return CLI_SHOWUSAGE;
	}

	if(5 == a->argc) {
		if(0 == strcasecmp(a->argv[2], option[2])) {
			/* "kafka show pipe <pipe_id> services|consumers|producers" */
			RAII_VAR(struct ast_kafka_pipe *, pipe, ast_kafka_get_pipe(a->argv[3],0), ao2_cleanup);

			if(NULL == pipe) {
				ast_cli(a->fd, "Pipe '%s' not found\n", a->argv[3]);
			} else {
				if((0 == strcasecmp(a->argv[4], pipe_option[0])) || (0 == strcasecmp(a->argv[4], pipe_option[1]))) {
					/* Show services or producers */
					on_all_producer_topics(pipe, cli_show_topic_cb, a, (char*)pipe_option[1]);
				}

				if((0 == strcasecmp(a->argv[4], pipe_option[0])) || (0 == strcasecmp(a->argv[4], pipe_option[2]))) {
					/* Show services or consumers */
					on_all_consumer_topics(pipe, cli_show_topic_cb, a, (char*)pipe_option[2]);
				}
			}

			return CLI_SUCCESS;
		}
	}

	return CLI_SHOWUSAGE;
}


/*! Complete pipe names for cli */
static char *complete_pipe_choice(const char *word) {
	int wordlen = strlen(word);
	struct ao2_iterator i;
	void *obj;

	i = ao2_iterator_init(pipes, 0);
	while(NULL != (obj = ao2_iterator_next(&i))) {
		struct ast_kafka_pipe *pipe = obj;

		if(0 == strncasecmp(word, pipe->id, wordlen)) {
			ast_cli_completion_add(ast_strdup(pipe->id));
		}

		ao2_ref(obj, -1);
	}
	ao2_iterator_destroy(&i);

	return NULL;
}

/*! Show registered pipes */
static int show_pipes_cb(void *obj, void *arg, int flags) {
	struct ast_kafka_pipe *pipe = obj;
	struct ast_cli_args *a = arg;

	ast_cli(a->fd, "%s\n", pipe->id);

	return 0;
}

static int cli_show_topic_cb(struct kafka_topic *topic, void *opaque_1, void *opaque_2, struct ast_kafka_pipe *pipe) {
	struct ast_cli_args *a = opaque_1;
	const char *service_type = opaque_2;
	RAII_VAR(struct kafka_service *, service, rdkafka_topic_to_service(topic->rd_kafka_topic), ao2_cleanup);
	const struct rd_kafka_metadata *metadata;
	rd_kafka_resp_err_t response;

	ast_debug(3, "Kafka request %s (%p) for topic (%p)\n", service_type, service->rd_kafka, topic->rd_kafka_topic);

	if(RD_KAFKA_RESP_ERR_NO_ERROR == (response = rd_kafka_metadata(service->rd_kafka, 1, topic->rd_kafka_topic, &metadata, service->timeout_ms))) {
		int i;
//		ast_cli(a->fd, "broker id=%d '%s', total brokers %d, total topics %d\n", metadata->orig_broker_id, metadata->orig_broker_name, metadata->broker_cnt, metadata->topic_cnt);

		for(i = 0;i < metadata->topic_cnt;i++) {
			const rd_kafka_metadata_topic_t *topic_metadata = metadata->topics + i;

			ast_cli(a->fd, "On %s topic '%s' handled by broker %d '%s', total partitions %d, total brokers %d, status %s (%d)\n", 
				service_type, topic_metadata->topic, metadata->orig_broker_id, metadata->orig_broker_name,
				topic_metadata->partition_cnt,
				metadata->broker_cnt, rd_kafka_err2str(topic_metadata->err), topic_metadata->err);
		}

		rd_kafka_metadata_destroy(metadata);
	} else {
		ast_log(LOG_ERROR, "Kafka get metadata got error: %s\n", rd_kafka_err2str(response));
	}

	return 0;
}



static void on_producer_created(const void *obj) {
	const struct sorcery_kafka_producer *producer = obj;

	ast_debug(3, "on_producer_created %s (%p)\n", ast_sorcery_object_get_id(producer), producer);
}

static void on_producer_updated(const void *obj) {
	const struct sorcery_kafka_producer *producer = obj;

	ast_debug(3, "on_producer_updated %s (%p)\n", ast_sorcery_object_get_id(producer), producer);
}

static void on_producer_deleted(const void *obj) {
	const struct sorcery_kafka_producer *producer = obj;

	ast_debug(3, "on_producer_deleted %s (%p)\n", ast_sorcery_object_get_id(producer), producer);
}

static void on_producer_loaded(const char *type) {
	ast_debug(3, "on_producer_loaded %s\n", type);
}





/*! Process all defined in configuration file clusters */
static void process_all_clusters(void) {
	RAII_VAR(struct ao2_container *, clusters, 
		ast_sorcery_retrieve_by_fields(kafka_sorcery, KAFKA_CLUSTER, AST_RETRIEVE_FLAG_MULTIPLE | AST_RETRIEVE_FLAG_ALL, NULL), ao2_cleanup);

	if(clusters) {
		struct ao2_iterator i = ao2_iterator_init(clusters, 0);
		struct sorcery_kafka_cluster *cluster;

		while(NULL != (cluster = ao2_iterator_next(&i))) {
			process_cluster(cluster);

			ao2_ref(cluster, -1);
		}

		ao2_iterator_destroy(&i);
	}
}

/*! Process Kafka cluster */
static void process_cluster(const struct sorcery_kafka_cluster *cluster) {
	RAII_VAR(struct ast_variable *, filter, ast_variable_new("cluster", ast_sorcery_object_get_id(cluster), ""), ast_variables_destroy);
	RAII_VAR(struct ao2_container *, found, NULL, ao2_cleanup);

	ast_debug(3, "Kafka cluster at %s (%p)\n", ast_sorcery_object_get_id(cluster), cluster);

	if(NULL == (found = ast_sorcery_retrieve_by_fields(kafka_sorcery, KAFKA_PRODUCER, AST_RETRIEVE_FLAG_MULTIPLE, filter))) {
		ast_log(LOG_WARNING, "Unable to retrieve producers from cluster %s\n", ast_sorcery_object_get_id(cluster));
	} else {
		struct ao2_iterator i = ao2_iterator_init(found, 0);
		struct sorcery_kafka_producer *producer;

		while(NULL != (producer = ao2_iterator_next(&i))) {
			process_producer(cluster, producer);

			ao2_ref(producer, -1);
		}

		ao2_iterator_destroy(&i);

		/* Producers processing complete */
		ao2_cleanup(found);
		found = NULL;
	}

	if(NULL == (found = ast_sorcery_retrieve_by_fields(kafka_sorcery, KAFKA_CONSUMER, AST_RETRIEVE_FLAG_MULTIPLE, filter))) {
		ast_log(LOG_WARNING, "Unable to retrieve consumers from cluster %s\n", ast_sorcery_object_get_id(cluster));
	} else {
		struct ao2_iterator i = ao2_iterator_init(found, 0);
		struct sorcery_kafka_consumer *consumer;

		while(NULL != (consumer = ao2_iterator_next(&i))) {
			process_consumer(cluster, consumer);

			ao2_ref(consumer, -1);
		}

		ao2_iterator_destroy(&i);
	}
}

/*! Process Kafka producer at the cluster */
static int process_producer(const struct sorcery_kafka_cluster *sorcery_cluster, const struct sorcery_kafka_producer *sorcery_producer) {
	RAII_VAR(struct ast_variable *, filter, ast_variable_new("producer", ast_sorcery_object_get_id(sorcery_producer), ""), ast_variables_destroy);
	RAII_VAR(struct ao2_container *, found, NULL, ao2_cleanup);

	ast_debug(3, "Process Kafka producer %s on cluster %s\n", ast_sorcery_object_get_id(sorcery_producer), ast_sorcery_object_get_id(sorcery_cluster));

	if(NULL == (found = ast_sorcery_retrieve_by_fields(kafka_sorcery, KAFKA_TOPIC, AST_RETRIEVE_FLAG_MULTIPLE, filter))) {
		ast_log(LOG_WARNING, "Unable to retrieve topics from producer %s at cluster %s\n", ast_sorcery_object_get_id(sorcery_producer), ast_sorcery_object_get_id(sorcery_cluster));
	} else {
		if(ao2_container_count(found)) {
			/* Producers present */
			struct kafka_service *producer = new_kafka_producer(sorcery_cluster, sorcery_producer);

			if(NULL == producer) {
				return -1;
			} else {
				struct ao2_iterator i = ao2_iterator_init(found, 0);
				struct sorcery_kafka_topic *sorcery_topic;

				while(NULL != (sorcery_topic = ao2_iterator_next(&i))) {
					if(0 == process_producer_topic(producer, sorcery_topic)) {
						producer->topic_count++;
					}

					ao2_ref(sorcery_topic, -1);
				}

				ao2_iterator_destroy(&i);

				if(producer->topic_count) {
					AST_RWDLLIST_WRLOCK(&producers);

					/* Keep object reference count because list reference to it */
					AST_RWDLLIST_INSERT_TAIL(&producers, producer, link);

					AST_RWDLLIST_UNLOCK(&producers);

					/* Assert monitor thread is running */
					if(start_monitor_thread()) {
						ast_log(LOG_WARNING, "Unable to start monitoring thread.\n");
					}
				} else {
					/* This producer has no topics */
					ao2_ref(producer, -1);
				}
			}
		}
	}

	return 0;
}

/*! Process Kafka consumer at the cluster */
static int process_consumer(const struct sorcery_kafka_cluster *sorcery_cluster, const struct sorcery_kafka_consumer *sorcery_consumer) {
	RAII_VAR(struct ast_variable *, filter, ast_variable_new("consumer", ast_sorcery_object_get_id(sorcery_consumer), ""), ast_variables_destroy);
	RAII_VAR(struct ao2_container *, found, NULL, ao2_cleanup);

	ast_debug(3, "Process Kafka consumer %s on cluster %s\n", ast_sorcery_object_get_id(sorcery_consumer), ast_sorcery_object_get_id(sorcery_cluster));

	if(NULL == (found = ast_sorcery_retrieve_by_fields(kafka_sorcery, KAFKA_TOPIC, AST_RETRIEVE_FLAG_MULTIPLE, filter))) {
		ast_log(LOG_WARNING, "Unable to retrieve topics from consumer %s at cluster %s\n", ast_sorcery_object_get_id(sorcery_consumer), ast_sorcery_object_get_id(sorcery_cluster));
	} else {
		if(ao2_container_count(found)) {
			/* Consumers present */
			struct kafka_service *consumer = new_kafka_consumer(sorcery_cluster, sorcery_consumer);

			if(NULL == consumer) {
				return -1;
			} else {
				struct ao2_iterator i = ao2_iterator_init(found, 0);
				struct sorcery_kafka_topic *sorcery_topic;

				while(NULL != (sorcery_topic = ao2_iterator_next(&i))) {
					if(0 == process_consumer_topic(consumer, sorcery_topic)) {
						consumer->topic_count++;
					}

					ao2_ref(sorcery_topic, -1);
				}

				ao2_iterator_destroy(&i);

				if(consumer->topic_count) {
					AST_RWDLLIST_WRLOCK(&consumers);

					/* Keep object reference count because list reference to it */
					AST_RWDLLIST_INSERT_TAIL(&consumers, consumer, link);

					AST_RWDLLIST_UNLOCK(&consumers);

					/* Assert monitor thread is running */
					if(start_monitor_thread()) {
						ast_log(LOG_WARNING, "Unable to start monitoring thread.\n");
					}
				} else {
					/* This consumer has no topics */
					ao2_ref(consumer, -1);
				}
			}
		}
	}

	return 0;
}

/*! Create new producer service object */
static struct kafka_service *new_kafka_producer(const struct sorcery_kafka_cluster *sorcery_cluster, const struct sorcery_kafka_producer *sorcery_producer) {
	rd_kafka_conf_t *config = build_rdkafka_cluster_config(sorcery_cluster);
	struct kafka_service *producer;

	if(NULL == config) {
		return NULL;
	} else {
		/* Set producer-specific configuration options */
		static const char *service_type = "producer";
		const char *service_id = ast_sorcery_object_get_id(sorcery_producer);
		const char *cluster_id = ast_sorcery_object_get_id(sorcery_cluster);
		
		if(service_add_property_uint(config, "retry.backoff.ms", sorcery_producer->retry_backoff_ms, service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}

		if(service_add_property_int(config, "request.required.acks", sorcery_producer->request_required_acks, service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}
		
		if(service_add_property_uint(config, "message.send.max.retries", sorcery_producer->message_send_max_retries, service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}

		if(service_add_property_uint(config, "max.in.flight.requests.per.connection", sorcery_producer->max_in_flight_requests_per_connection, service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}

		if(service_add_property_string(config, "enable.idempotence", sorcery_producer->enable_idempotence ? "true" : "false", service_type, service_id, cluster_id)) {
			/* On current version librdkafka idempotence producers not supported yet */
			ast_log(LOG_WARNING, "Current version librdkafka (%s) not support dempotence producers.\n", rd_kafka_version_str());
		}
		
		if(service_add_property_string(config, "transactional.id", sorcery_producer->transactional_id, service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}

		if(service_add_property_string(config, "client.id", S_OR(sorcery_producer->client_id, sorcery_cluster->client_id), service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}		
		
		if(service_add_property_string(config, "debug", sorcery_producer->debug, service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}		
	}

	if(NULL == (producer = ao2_alloc(sizeof(*producer), kafka_producer_destructor))) {
		ast_log(LOG_ERROR, "Kafka cluster '%s': Unable to create producer '%s' because Out of memory\n", ast_sorcery_object_get_id(sorcery_cluster), ast_sorcery_object_get_id(sorcery_producer));
	} else {
		char *errstr = ast_alloca(KAFKA_ERRSTR_MAX_SIZE);

		producer->timeout_ms = sorcery_producer->timeout_ms;
		producer->partition = (sorcery_producer->partition < 0) ? RD_KAFKA_PARTITION_UA : sorcery_producer->partition;
		producer->topic_count = 0;

		/* Passed to on_producer_message_processed as opaque value */
		rd_kafka_conf_set_opaque(config, producer);

		/* Processed message callback */
		rd_kafka_conf_set_dr_msg_cb(config, on_producer_message_processed);

		/* Attempt to creare librdkafka producer */
		if(NULL == (producer->rd_kafka = rd_kafka_new(RD_KAFKA_PRODUCER, config, errstr, KAFKA_ERRSTR_MAX_SIZE))) {
			ast_log(LOG_ERROR, "Kafka cluster '%s': unable to create producer '%s' because %s\n", ast_sorcery_object_get_id(sorcery_cluster), ast_sorcery_object_get_id(sorcery_producer), errstr);
		} else {
			ast_debug(3, "Producer service '%s' (%p) for cluster '%s' have handle %p\n",
					ast_sorcery_object_get_id(sorcery_producer), producer,
					ast_sorcery_object_get_id(sorcery_cluster), producer->rd_kafka);

			return producer;
		}
	}

	rd_kafka_conf_destroy(config);

	ao2_cleanup(producer);

	return NULL;
}

static void kafka_producer_destructor(void *obj) {
	struct kafka_service *producer = obj;

	if(producer->rd_kafka) {
		ast_debug(3, "Destroy rd_kafka_t object %p on producer %p\n", producer->rd_kafka, producer);
		rd_kafka_destroy(producer->rd_kafka);
	}
}

/*! Create new consumer service object */
static struct kafka_service *new_kafka_consumer(const struct sorcery_kafka_cluster *sorcery_cluster, const struct sorcery_kafka_consumer *sorcery_consumer) {
	rd_kafka_conf_t *config = build_rdkafka_cluster_config(sorcery_cluster);
	struct kafka_service *consumer;

	if(NULL == config) {
		return NULL;
	} else {
		/* Set consumer-specific configuration options */
		static const char *service_type = "producer";
		const char *service_id = ast_sorcery_object_get_id(sorcery_consumer);
		const char *cluster_id = ast_sorcery_object_get_id(sorcery_cluster);

		if(service_add_property_string(config, "group.id", sorcery_consumer->group_id, service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}		

		if(service_add_property_string(config, "enable.auto.commit", sorcery_consumer->enable_auto_commit ? "true" : "false", service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}		

		if(service_add_property_uint(config, "auto.commit.interval.ms", sorcery_consumer->auto_commit_interval_ms, service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}		

		/* Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition. */
		if(service_add_property_string(config, "enable.partition.eof", "true", service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}		
		
		if(service_add_property_string(config, "client.id", S_OR(sorcery_consumer->client_id, sorcery_cluster->client_id), service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}		
		
		if(service_add_property_string(config, "debug", sorcery_consumer->debug, service_type, service_id, cluster_id)) {
			rd_kafka_conf_destroy(config);
			return NULL;
		}		
	}

	if(NULL == (consumer = ao2_alloc(sizeof(*consumer), kafka_consumer_destructor))) {
		ast_log(LOG_ERROR, "Kafka cluster '%s': Unable to create consumer '%s' because Out of memory\n", ast_sorcery_object_get_id(sorcery_cluster), ast_sorcery_object_get_id(sorcery_consumer));
	} else {
		char *errstr = ast_alloca(KAFKA_ERRSTR_MAX_SIZE);

		consumer->specific.consumer.queue = NULL;

		consumer->timeout_ms = sorcery_consumer->timeout_ms;
		consumer->partition = (sorcery_consumer->partition < 0) ? RD_KAFKA_PARTITION_UA : sorcery_consumer->partition;
		consumer->topic_count = 0;

		/* Passed to on_consumer_message_processed as opaque value */
		rd_kafka_conf_set_opaque(config, consumer);

		/* Processed message callback */
		rd_kafka_conf_set_dr_msg_cb(config, on_consumer_message_processed);

		/* Attempt to creare librdkafka consumer */
		if(NULL == (consumer->rd_kafka = rd_kafka_new(RD_KAFKA_CONSUMER, config, errstr, KAFKA_ERRSTR_MAX_SIZE))) {
			ast_log(LOG_ERROR, "Kafka cluster '%s': unable to create consumer '%s' because %s\n", ast_sorcery_object_get_id(sorcery_cluster), ast_sorcery_object_get_id(sorcery_consumer), errstr);
		} else {
			if(NULL == (consumer->specific.consumer.queue = rd_kafka_queue_new(consumer->rd_kafka))) {
				ast_log(LOG_ERROR, "Kafka cluster '%s': unable to create consumer '%s' queue because %s\n", ast_sorcery_object_get_id(sorcery_cluster), ast_sorcery_object_get_id(sorcery_consumer), errstr);
			} else {
				ast_debug(3, "Consumer service '%s' (%p) for cluster '%s' have handle %p\n",
						ast_sorcery_object_get_id(sorcery_consumer), consumer,
						ast_sorcery_object_get_id(sorcery_cluster), consumer->rd_kafka);

				return consumer;
			}

			rd_kafka_destroy(consumer->rd_kafka);
			consumer->rd_kafka = NULL;

			ao2_cleanup(consumer);

			return NULL;
		}
	}

	rd_kafka_conf_destroy(config);

	ao2_cleanup(consumer);

	return NULL;
}

static void kafka_consumer_destructor(void *obj) {
	struct kafka_service *consumer = obj;

	if(consumer->specific.consumer.queue) {
		rd_kafka_queue_destroy(consumer->specific.consumer.queue);
	}

	if(consumer->rd_kafka) {
		ast_debug(3, "Destroy rd_kafka_t object %p on consumer %p\n", consumer->rd_kafka, consumer);
		rd_kafka_destroy(consumer->rd_kafka);
	}
}



/*! Build common rdkafka cluster configuration */
static rd_kafka_conf_t *build_rdkafka_cluster_config(const struct sorcery_kafka_cluster *cluster) {
	rd_kafka_conf_t *config = rd_kafka_conf_new();
	char *errstr = ast_alloca(KAFKA_ERRSTR_MAX_SIZE);

	if(NULL == config) {
		ast_log(LOG_ERROR, "Kafka cluster %s: unable to create configuration object\n", ast_sorcery_object_get_id(cluster));
		return NULL;
	}

	if(RD_KAFKA_CONF_OK != rd_kafka_conf_set(config, "metadata.broker.list", cluster->brokers, errstr, KAFKA_ERRSTR_MAX_SIZE)) {
		ast_log(LOG_ERROR, "Kafka cluster %s: unable to set bootstrap brokers because %s\n", ast_sorcery_object_get_id(cluster), errstr);
		rd_kafka_conf_destroy(config);
		return NULL;
	}

	if(RD_KAFKA_CONF_OK != rd_kafka_conf_set(config, "security.protocol", cluster->security_protocol, errstr, KAFKA_ERRSTR_MAX_SIZE)) {
		ast_log(LOG_ERROR, "Kafka cluster %s: unable to set security protocol because %s\n", ast_sorcery_object_get_id(cluster), errstr);
		rd_kafka_conf_destroy(config);
		return NULL;
	}

	if(RD_KAFKA_CONF_OK != rd_kafka_conf_set(config, "sasl.mechanism", cluster->sasl_mechanism, errstr, KAFKA_ERRSTR_MAX_SIZE)) {
		ast_log(LOG_ERROR, "Kafka cluster %s: unable to set SASL mechanism because %s\n", ast_sorcery_object_get_id(cluster), errstr);
		rd_kafka_conf_destroy(config);
		return NULL;
	}

	if(RD_KAFKA_CONF_OK != rd_kafka_conf_set(config, "sasl.username", cluster->sasl_username, errstr, KAFKA_ERRSTR_MAX_SIZE)) {
		ast_log(LOG_ERROR, "Kafka cluster %s: unable to set SASL username because %s\n", ast_sorcery_object_get_id(cluster), errstr);
		rd_kafka_conf_destroy(config);
		return NULL;
	}

	if(RD_KAFKA_CONF_OK != rd_kafka_conf_set(config, "sasl.password", cluster->sasl_password, errstr, KAFKA_ERRSTR_MAX_SIZE)) {
		ast_log(LOG_ERROR, "Kafka cluster %s: unable to set SASL password because %s\n", ast_sorcery_object_get_id(cluster), errstr);
		rd_kafka_conf_destroy(config);
		return NULL;
	}

	return config;
}

/*! Add signed integer property value to the Kafka configuration */
static int service_add_property_int(rd_kafka_conf_t *config, 
					const char *property, int value, 
					const char *service_type,
					const char *service_id,
					const char *cluster_id) {
	char *tmp = ast_alloca(TMP_BUF_SIZE);
	
	snprintf(tmp, TMP_BUF_SIZE, "%d", value);

	return service_add_property_string(config, property, tmp, service_type, service_id, cluster_id);
}

/*! Add unsigned integer property value to the Kafka configuration */
static int service_add_property_uint(rd_kafka_conf_t *config, 
					const char *property, unsigned value, 
					const char *service_type,
					const char *service_id,
					const char *cluster_id) {
	char *tmp = ast_alloca(TMP_BUF_SIZE);
	
	snprintf(tmp, TMP_BUF_SIZE, "%u", value);

	return service_add_property_string(config, property, tmp, service_type, service_id, cluster_id);
}

/*! Add string property value to the Kafka configuration */
static int service_add_property_string(rd_kafka_conf_t *config, 
					const char *property, const char *value,
					const char *service_type,
					const char *service_id,
					const char *cluster_id) {
	if(value && strlen(value)) {
		char *errstr = ast_alloca(KAFKA_ERRSTR_MAX_SIZE);

		if(RD_KAFKA_CONF_OK == rd_kafka_conf_set(config, property, value, errstr, KAFKA_ERRSTR_MAX_SIZE)) {
			return 0;
		}

		ast_log(LOG_ERROR, "Kafka %s '%s' on cluster '%s': unable to set '%s' property because %s\n", 
			service_type, service_id, cluster_id, property, errstr);

		return -1;
	}

	return 0;
}

/*! Process Kafka producer related topic at the cluster */
static int process_producer_topic(struct kafka_service *producer, const struct sorcery_kafka_topic *sorcery_topic) {
#if 1
	RAII_VAR(struct kafka_topic *, topic, new_kafka_producer_topic(producer, sorcery_topic), ao2_cleanup);
	RAII_VAR(struct ast_kafka_pipe *, pipe, ast_kafka_get_pipe(sorcery_topic->pipe, 1), ao2_cleanup);

	if(pipe && topic) {
		AST_LIST_LOCK(&pipe->producer_topics);

		/* Add new topic to the specified pipe */
		AST_LIST_INSERT_TAIL(&pipe->producer_topics, topic, link);

		/* Object in the list */
		ao2_ref(topic, +1);

		AST_LIST_UNLOCK(&pipe->producer_topics);

		return 0;
	} else {
		ast_log(LOG_ERROR, "Unable to add producer topic '%s' to pipe '%s' - Out of memory\n", 
			ast_sorcery_object_get_id(sorcery_topic), sorcery_topic->pipe);

		return -1;
	}

#else
	struct kafka_topic *topic;
	rd_kafka_resp_err_t response;

	struct ast_kafka_pipe *pipe = ast_kafka_get_pipe(sorcery_topic->pipe, 1);
	ao2_cleanup(pipe);

	ast_debug(3, "Process Kafka topic %s for producer %p\n", ast_sorcery_object_get_id(sorcery_topic), producer);

	topic = new_kafka_producer_topic(producer, sorcery_topic);

	if(rd_kafka_produce(topic->rd_kafka_topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY, "test", 4, NULL, 0, NULL)) {
		ast_log(LOG_ERROR, "Unable to produce message producer topic '%s' because %s\n", ast_sorcery_object_get_id(sorcery_topic), rd_kafka_err2str(rd_kafka_last_error()));
	}

	if(RD_KAFKA_RESP_ERR_NO_ERROR == (response = rd_kafka_flush(producer->rd_kafka, 10000))) {
		ast_log(LOG_ERROR, "Kafka producer topic %s got error: %s\n", ast_sorcery_object_get_id(sorcery_topic), rd_kafka_err2str(response));
	}

	ao2_cleanup(topic);
#endif
}

/*! Process Kafka consumer related topic at the cluster */
static int process_consumer_topic(struct kafka_service *consumer, const struct sorcery_kafka_topic *sorcery_topic) {
	RAII_VAR(struct kafka_topic *, topic, new_kafka_consumer_topic(consumer, sorcery_topic), ao2_cleanup);
	RAII_VAR(struct ast_kafka_pipe *, pipe, ast_kafka_get_pipe(sorcery_topic->pipe, 1), ao2_cleanup);

	if(pipe && topic) {
		AST_LIST_LOCK(&pipe->consumer_topics);

		/* Add new topic to the specified pipe */
		AST_LIST_INSERT_TAIL(&pipe->consumer_topics, topic, link);

		/* Object in the list */
		ao2_ref(topic, +1);

		AST_LIST_UNLOCK(&pipe->consumer_topics);

		return 0;
	} else {
		ast_log(LOG_ERROR, "Unable to add consumer topic '%s' to pipe '%s' - Out of memory\n", 
			ast_sorcery_object_get_id(sorcery_topic), sorcery_topic->pipe);

		return -1;
	}
}




static struct kafka_topic *new_kafka_producer_topic(struct kafka_service *producer, const struct sorcery_kafka_topic *sorcery_topic) {
	struct kafka_topic *topic = ao2_alloc(sizeof(*topic), kafka_producer_topic_destructor);

	if(NULL == topic) {
		ast_log(LOG_ERROR, "Out of memory while create producer topic '%s'\n", ast_sorcery_object_get_id(sorcery_topic));
	} else {
		rd_kafka_topic_conf_t *config = rd_kafka_topic_conf_new();

		topic->rd_kafka_topic = NULL;

		if(NULL == config) {
			ast_log(LOG_ERROR, "Unable to create config for producer topic '%s'\n", ast_sorcery_object_get_id(sorcery_topic));
			ao2_ref(topic, -1);
			return NULL;
		}

		/* With topic's callbacks we want see the kafka_producer_topic structure reference */
		rd_kafka_topic_conf_set_opaque(config, topic);

		if(NULL == (topic->rd_kafka_topic = rd_kafka_topic_new(producer->rd_kafka, sorcery_topic->topic, config))) {
			ast_log(LOG_ERROR, "Unable to create producer topic '%s' because %s\n", ast_sorcery_object_get_id(sorcery_topic), rd_kafka_err2str(rd_kafka_last_error()));
			rd_kafka_topic_conf_destroy(config);

			/* This object not usable */
			ao2_ref(topic, -1);
			return NULL;
		}

		/* Topic handle created, link this object to the service */
		topic->service = producer;
		ao2_ref(producer, +1);
	}

	return topic;
}

static void kafka_producer_topic_destructor(void *obj) {
	struct kafka_topic *topic = obj;
	RAII_VAR(struct kafka_service *, producer, topic->service, ao2_cleanup);

	if(producer) {
		int last_topic;

		AST_RWDLLIST_WRLOCK(&producers);

		if(! --producer->topic_count) {
			last_topic = 1;

			/* Remove service from producers list */
			AST_DLLIST_REMOVE(&producers, producer, link);
		} else {
			last_topic = 0;
		}

		AST_RWDLLIST_UNLOCK(&producers);

		if(last_topic) {
			/* Wait for messages to be delivered */
			while(rd_kafka_outq_len(producer->rd_kafka) > 0) {
				ast_debug(3, "Producer %p have %u outstanding message(s), wait %ums\n", 
						producer, rd_kafka_outq_len(producer->rd_kafka), producer->timeout_ms);

				rd_kafka_poll(producer->rd_kafka, producer->timeout_ms);
			}

			/* No reference to this service */
			ao2_ref(producer, -1);
		}
	}

	if(topic->rd_kafka_topic) {
		ast_debug(3, "Destroy rd_kafka_topic_t object %p on producer's topic %p\n", topic->rd_kafka_topic, topic);
		rd_kafka_topic_destroy(topic->rd_kafka_topic);
	}
}


static struct kafka_topic *new_kafka_consumer_topic(struct kafka_service *consumer, const struct sorcery_kafka_topic *sorcery_topic) {
	struct kafka_topic *topic = ao2_alloc(sizeof(*topic), kafka_consumer_topic_destructor);

	if(NULL == topic) {
		ast_log(LOG_ERROR, "Out of memory while create consumer topic '%s'\n", ast_sorcery_object_get_id(sorcery_topic));
	} else {
		rd_kafka_topic_conf_t *config = rd_kafka_topic_conf_new();

		topic->rd_kafka_topic = NULL;

		if(NULL == config) {
			ast_log(LOG_ERROR, "Unable to create config for consumer topic '%s'\n", ast_sorcery_object_get_id(sorcery_topic));
			ao2_ref(topic, -1);
			return NULL;
		}

		/* With topic's callbacks we want see the kafka_producer_topic structure reference */
		rd_kafka_topic_conf_set_opaque(config, topic);

		if(NULL == (topic->rd_kafka_topic = rd_kafka_topic_new(consumer->rd_kafka, sorcery_topic->topic, config))) {
			ast_log(LOG_ERROR, "Unable to create consumer topic '%s' because %s\n", ast_sorcery_object_get_id(sorcery_topic), rd_kafka_err2str(rd_kafka_last_error()));
			rd_kafka_topic_conf_destroy(config);

			/* This object not usable */
			ao2_ref(topic, -1);
			return NULL;
		}

		/* Start consuming from end of kafka partition queue: next msg */
#if 1
		if(rd_kafka_consume_start_queue(topic->rd_kafka_topic, consumer->partition, RD_KAFKA_OFFSET_BEGINNING, consumer->specific.consumer.queue) < 0) {
#else
		if(rd_kafka_consume_start(topic->rd_kafka_topic, consumer->partition, RD_KAFKA_OFFSET_END) < 0) {
#endif
			ast_log(LOG_ERROR, "Unable to start consuming topic '%s' because %s\n", ast_sorcery_object_get_id(sorcery_topic), rd_kafka_err2str(rd_kafka_last_error()));

			/* Destroy librdkafka topic object */
			rd_kafka_topic_destroy(topic->rd_kafka_topic);
			topic->rd_kafka_topic = NULL;

			/* This object not usable */
			ao2_ref(topic, -1);
			return NULL;
		}

		/* Topic handle created, link this object to the service */
		topic->service = consumer;
		ao2_ref(consumer, +1);
	}

	return topic;
}

static void kafka_consumer_topic_destructor(void *obj) {
	struct kafka_topic *topic = obj;
	RAII_VAR(struct kafka_service *, consumer, topic->service, ao2_cleanup);

	if(consumer) {
		int last_topic;

		if(topic->rd_kafka_topic) {
			/* Stop consuming on this topic */
			rd_kafka_consume_stop(topic->rd_kafka_topic, consumer->partition);
		}

		AST_RWDLLIST_WRLOCK(&consumers);

		if(! --consumer->topic_count) {
			last_topic = 1;

			/* Remove service from consumers list */
			AST_DLLIST_REMOVE(&consumers, consumer, link);
		} else {
			last_topic = 0;
		}

		AST_RWDLLIST_UNLOCK(&consumers);

		if(last_topic) {
			/* Wait for messages to be delivered */
			while(rd_kafka_outq_len(consumer->rd_kafka) > 0) {
				ast_debug(3, "Consumer %p have %u outstanding message(s), wait %ums\n", 
						consumer, rd_kafka_outq_len(consumer->rd_kafka), consumer->timeout_ms);

				rd_kafka_poll(consumer->rd_kafka, consumer->timeout_ms);
			}

			/* No reference to this service */
			ao2_ref(consumer, -1);
		}
	}

	if(topic->rd_kafka_topic) {
		ast_debug(3, "Destroy rd_kafka_topic_t object %p on consumer's topic %p\n", topic->rd_kafka_topic, topic);
		rd_kafka_topic_destroy(topic->rd_kafka_topic);
	}
}



/*! Start monitor thread if possible */
static int start_monitor_thread(void) {
	int res = 0;

	ast_mutex_lock(&monitor_lock);

	if(AST_PTHREADT_NULL == monitor) {
		res = ast_pthread_create_detached_background(&monitor, NULL, monitor_thread_job, NULL);
	}

	ast_mutex_unlock(&monitor_lock);

	return res;
}

/*! Monitoring thread job */
static void *monitor_thread_job(void *opaque) {
	int active;

	ast_debug(3, "Monitor thread started.\n");

	do {
		int processed = 0;
		struct kafka_service *service;

		active = 0;

		/* Process producer services*/
		AST_RWDLLIST_RDLOCK(&producers);
		AST_DLLIST_TRAVERSE(&producers, service, link) {
			active++;

			/* Process producer events */
			processed += rd_kafka_poll(service->rd_kafka, 0);
		}
		AST_RWDLLIST_UNLOCK(&producers);

		AST_RWDLLIST_RDLOCK(&consumers);
		AST_DLLIST_TRAVERSE(&consumers, service, link) {
#if 0
			rd_kafka_message_t* msg = rd_kafka_consumer_poll(service->rd_kafka, 0);

			ast_debug(3, "Got consumer msg=%p\n", msg);

			if(RD_KAFKA_RESP_ERR_NO_ERROR == msg->err) {
				/* We got a message */
				ast_debug(3, "Got message!!!\n");

				rd_kafka_message_destroy(msg);
			} else {
				/* We got a status */
				ast_debug(3, "Got consumer status %d: %s\n", msg->err, rd_kafka_err2str(msg->err));

				rd_kafka_message_destroy(msg);
			}

			active++;
#else
#if 1
			rd_kafka_message_t *msg = rd_kafka_consume_queue(service->specific.consumer.queue, 0);

			if(NULL != msg) {
				if(RD_KAFKA_RESP_ERR_NO_ERROR == msg->err) {
					rd_kafka_resp_err_t response;

					ast_debug(3, "Got consumer message %d: %s\n", msg->err, rd_kafka_err2str(msg->err));

//					if(RD_KAFKA_RESP_ERR_NO_ERROR == (response = rd_kafka_commit_message(service->rd_kafka, msg, 0))) {
//					} else {
//						ast_debug(3, "Consumer message commit failed %d: %s\n", response, rd_kafka_err2str(response));
//					}
				} else {
					ast_debug(3, "Got consumer status %d: %s\n", msg->err, rd_kafka_err2str(msg->err));
				}

				processed++;
				rd_kafka_message_destroy(msg);
			}
#endif

			active++;

			/* Process consumer events */
			processed += rd_kafka_poll(service->rd_kafka, 0);
#endif
		}
		AST_RWDLLIST_UNLOCK(&consumers);

		if(active && !processed) {
			/* Some services exist, but no pending events yet */
			usleep(KAFKA_MONITOR_NO_EVENTS_SLEEP_US);
		}
	} while(active);

	ast_mutex_lock(&monitor_lock);

	if((AST_PTHREADT_NULL != monitor) && (AST_PTHREADT_STOP != monitor)) {
		/* Normal thread shudtown */
		monitor = AST_PTHREADT_NULL;
	}

	ast_mutex_unlock(&monitor_lock);

	ast_debug(3, "Monitor thread finished.\n");

	return NULL;
}

/*! Called by librdkafka when producer message processing complete */
static void on_producer_message_processed(rd_kafka_t *rd_kafka, const rd_kafka_message_t *message, void *opaque) {
	ast_debug(3,"on_producer_message_processed(%p, %p, %p). Status: %s\n", rd_kafka, message, opaque, rd_kafka_err2str(message->err));
}

/*! Called by librdkafka when consumer message processing complete */
static void on_consumer_message_processed(rd_kafka_t *rd_kafka, const rd_kafka_message_t *message, void *opaque) {
	ast_debug(3,"on_consumer_message_processed(%p, %p, %p)\n", rd_kafka, message, opaque);
}



/*! Execute callback on all producers */
static int on_all_producers(int (*callback)(struct kafka_service *service, void *opaque), void *opaque, int lock, int wrlock) {
	int status = 0;
	struct kafka_service *producer;

	if(lock) {
		if(wrlock) {
			AST_RWDLLIST_WRLOCK(&producers);
		} else {
			AST_RWDLLIST_RDLOCK(&producers);
		}
	}

	AST_DLLIST_TRAVERSE(&producers, producer, link) {
		status |= (*callback)(producer, opaque);

		if(status < 0) {
			/* Fatal error */
			break;
		}
	}

	if(lock) {
		AST_RWDLLIST_UNLOCK(&producers);
	}

	return status;
}

/*! Execute callback on all consumers */
static int on_all_consumers(int (*callback)(struct kafka_service *service, void *opaque), void *opaque, int lock, int wrlock) {
	int status = 0;
	struct kafka_service *consumer;

	if(lock) {
		if(wrlock) {
			AST_RWDLLIST_WRLOCK(&consumers);
		} else {
			AST_RWDLLIST_RDLOCK(&consumers);
		}
	}

	AST_DLLIST_TRAVERSE(&consumers, consumer, link) {
		status |= (*callback)(consumer, opaque);

		if(status < 0) {
			/* Fatal error */
			break;
		}
	}

	if(lock) {
		AST_RWDLLIST_UNLOCK(&consumers);
	}

	return status;
}

/*! Execute callback on all producer topics in the pipe */
static int on_all_producer_topics(struct ast_kafka_pipe *pipe, int (*callback)(struct kafka_topic *topic, void *opaque_1, void *opaque_2, struct ast_kafka_pipe *pipe), void *opaque_1, void *opaque_2) {
	int status = 0;
	struct kafka_topic *topic;

	AST_LIST_LOCK(&pipe->producer_topics);

	AST_LIST_TRAVERSE(&pipe->producer_topics, topic, link) {
		status |= (*callback)(topic, opaque_1, opaque_2, pipe);

		if(status < 0) {
			/* Fatal error */
			break;
		}
	}

	AST_LIST_UNLOCK(&pipe->producer_topics);

	return status;
}

/*! Execute callback on all consumer topics in the pipe */
static int on_all_consumer_topics(struct ast_kafka_pipe *pipe, int (*callback)(struct kafka_topic *topic, void *opaque_1, void *opaque_2, struct ast_kafka_pipe *pipe), void *opaque_1, void *opaque_2) {
	int status = 0;
	struct kafka_topic *topic;

	AST_LIST_LOCK(&pipe->consumer_topics);

	AST_LIST_TRAVERSE(&pipe->consumer_topics, topic, link) {
		status |= (*callback)(topic, opaque_1, opaque_2, pipe);

		if(status < 0) {
			/* Fatal error */
			break;
		}
	}

	AST_LIST_UNLOCK(&pipe->consumer_topics);

	return status;
}

/*! Fetch kafka_service object by rdkafka topic's handle */
static struct kafka_service *rdkafka_topic_to_service(const rd_kafka_topic_t *rd_kafka_topic) {
	RAII_VAR(struct kafka_topic *, topic, rdkafka_topic_to_topic(rd_kafka_topic), ao2_cleanup);
	struct kafka_service *service = topic->service;

	ast_assert(service != topic);

	ao2_ref(service, +1);

	return service;
}

/*! Fetch kafka_topic object by rdkafka topic's handle */
static struct kafka_topic *rdkafka_topic_to_topic(const rd_kafka_topic_t *rd_kafka_topic) {
	struct kafka_topic *topic = rd_kafka_topic_opaque(rd_kafka_topic);

	ast_assert(NULL != topic);

	ao2_ref(topic, +1);

	return topic;
}


/*! Common register sorcery object actions */
static int sorcery_object_register(const char *type, void *(*alloc)(const char *name),int (*apply)(const struct ast_sorcery *sorcery, void *obj)) {
	char *options = ast_alloca(80);

	sprintf(options, KAFKA_CONFIG_FILENAME ",criteria=type=%s", type);

	if(AST_SORCERY_APPLY_SUCCESS != ast_sorcery_apply_default(kafka_sorcery, type, "config", options)) {
		ast_log(LOG_NOTICE, "Failed to apply defaults for Kafka sorcery %s\n", type);
	}

	if(ast_sorcery_object_register(kafka_sorcery, type, alloc, NULL, apply)) {
		ast_log(LOG_ERROR, "Failed to register '%s' with Kafka sorcery.\n", type);
		return -1;
	}

	ast_sorcery_object_field_register(kafka_sorcery, type, "type", "", OPT_NOOP_T, 0, 0);

	return 0;
}


static int sorcery_kafka_topic_apply_handler(const struct ast_sorcery *sorcery, void *obj) {
	struct sorcery_kafka_topic *topic = obj;

	ast_debug(3, "Apply Kafka topic %s (%p)\n", ast_sorcery_object_get_id(topic), topic);

	return 0;
}

static void *sorcery_kafka_topic_alloc(const char *name) {
	struct sorcery_kafka_topic *topic = ast_sorcery_generic_alloc(sizeof(*topic), sorcery_kafka_topic_destructor);

	if(NULL == topic) {
		return NULL;
	}

	if(ast_string_field_init(topic, 64)) {
		ao2_cleanup(topic);
		return NULL;
	}

	ast_debug(3, "Allocated Kafka topic %s (%p)\n", name, topic);

	return topic;
}

static void sorcery_kafka_topic_destructor(void *obj) {
	struct sorcery_kafka_topic *topic = obj;

	ast_debug(3, "Destroyed Kafka topic %s (%p)\n", ast_sorcery_object_get_id(topic), topic);

	ast_string_field_free_memory(topic);
}

static int sorcery_kafka_producer_apply_handler(const struct ast_sorcery *sorcery, void *obj) {
	struct sorcery_kafka_producer *producer = obj;

	ast_debug(3, "Apply Kafka producer %s (%p)\n", ast_sorcery_object_get_id(producer), producer);

	return 0;
}

static void *sorcery_kafka_producer_alloc(const char *name) {
	struct sorcery_kafka_producer *producer = ast_sorcery_generic_alloc(sizeof(*producer), sorcery_kafka_producer_destructor);

	if(NULL == producer) {
		return NULL;
	}

	if(ast_string_field_init(producer, 64)) {
		ao2_cleanup(producer);
		return NULL;
	}

//	ast_debug(3, "Allocated Kafka producer %s (%p)\n", name, producer);

	return producer;
}

static void sorcery_kafka_producer_destructor(void *obj) {
	struct sorcery_kafka_producer *producer = obj;

//	ast_debug(3, "Destroyed Kafka producer %s (%p)\n", ast_sorcery_object_get_id(producer), producer);

	ast_string_field_free_memory(producer);
}

static int sorcery_kafka_consumer_apply_handler(const struct ast_sorcery *sorcery, void *obj) {
	struct sorcery_kafka_consumer *consumer = obj;

//	ast_debug(3, "Apply Kafka consumer %s (%p)\n", ast_sorcery_object_get_id(consumer), consumer);

	return 0;
}

static void *sorcery_kafka_consumer_alloc(const char *name) {
	struct sorcery_kafka_consumer *consumer = ast_sorcery_generic_alloc(sizeof(*consumer), sorcery_kafka_consumer_destructor);

	if(NULL == consumer) {
		return NULL;
	}

	if(ast_string_field_init(consumer, 64)) {
		ao2_cleanup(consumer);
		return NULL;
	}

//	ast_debug(3, "Allocated Kafka consumer %s (%p)\n", name, consumer);

	return consumer;
}

static void sorcery_kafka_consumer_destructor(void *obj) {
	struct sorcery_kafka_consumer *consumer = obj;

//	ast_debug(3, "Destroyed Kafka consumer %s (%p)\n", ast_sorcery_object_get_id(consumer), consumer);

	ast_string_field_free_memory(consumer);
}

static int sorcery_kafka_cluster_apply_handler(const struct ast_sorcery *sorcery, void *obj) {
	struct sorcery_kafka_cluster *cluster = obj;

	ast_debug(3, "Apply Kafka cluster %s (%p): brokers=%s client_id=%s\n", ast_sorcery_object_get_id(cluster), cluster, cluster->brokers, cluster->client_id);

#if 0
	if(NULL != producer) {
		rd_kafka_resp_err_t response;
		const struct rd_kafka_metadata *metadata;

		ast_debug(3, "Kafka cluster %s (%p) create producer %p\n", ast_sorcery_object_get_id(cluster), cluster, producer);

		if(RD_KAFKA_RESP_ERR_NO_ERROR == (response = rd_kafka_metadata(producer, 1, NULL, &metadata, 10000))) {
			rd_kafka_metadata_destroy(metadata);
		} else {
			ast_log(LOG_ERROR, "Kafka cluster %s get metadata got error: %s\n", ast_sorcery_object_get_id(cluster), rd_kafka_err2str(response));
		}

		rd_kafka_destroy(producer);
	}
#endif


	return 0;
}

static void *sorcery_kafka_cluster_alloc(const char *name) {
	struct sorcery_kafka_cluster *cluster = ast_sorcery_generic_alloc(sizeof(*cluster), sorcery_kafka_cluster_destructor);

	if(NULL == cluster) {
		return NULL;
	}

	if(ast_string_field_init(cluster, 64)) {
		ao2_cleanup(cluster);
		return NULL;
	}

//	ast_debug(3, "Allocated Kafka cluster %s (%p)\n", name, cluster);

	return cluster;
}

static void sorcery_kafka_cluster_destructor(void *obj) {
	struct sorcery_kafka_cluster *cluster = obj;

//	ast_debug(3, "Destroyed Kafka cluster %s (%p)\n", ast_sorcery_object_get_id(cluster), cluster);

	ast_string_field_free_memory(cluster);
}

/*! Create new pipe object with specified pipe name */
static void *kafka_pipe_alloc(const char *pipe_id) {
	struct ast_kafka_pipe *pipe = ao2_alloc(sizeof(*pipe), kafka_pipe_destructor);

	if(NULL == pipe) {
		return NULL;
	}

	AST_LIST_HEAD_INIT(&pipe->producer_topics);

	AST_LIST_HEAD_INIT(&pipe->consumer_topics);

	if(ast_string_field_init(pipe, 64)) {
		ao2_cleanup(pipe);
		return NULL;
	}

	ast_string_field_set(pipe, id, pipe_id);

	ast_debug(3, "Allocated Kafka pipe %s (%p)\n", pipe->id, pipe);

	return pipe;
}

/*! Pipe object destructor */
static void kafka_pipe_destructor(void *obj) {
	struct ast_kafka_pipe *pipe = obj;
	struct kafka_topic *topic;

	AST_LIST_LOCK(&pipe->consumer_topics);

	while(NULL != (topic = AST_LIST_REMOVE_HEAD(&pipe->consumer_topics, link))) {
		/* Release consumer object */
		ao2_ref(topic, -1);
	}

	AST_LIST_UNLOCK(&pipe->consumer_topics);

	AST_LIST_HEAD_DESTROY(&pipe->consumer_topics);

	AST_LIST_LOCK(&pipe->producer_topics);

	while(NULL != (topic = AST_LIST_REMOVE_HEAD(&pipe->producer_topics, link))) {
		/* Release producer object */
		ao2_ref(topic, -1);
	}

	AST_LIST_UNLOCK(&pipe->producer_topics);

	AST_LIST_HEAD_DESTROY(&pipe->producer_topics);

	ast_debug(3, "Destroyed Kafka pipe %s (%p)\n", pipe->id, pipe);

	ast_string_field_free_memory(pipe);
}

/*! Calculate hash */
AO2_STRING_FIELD_HASH_FN(ast_kafka_pipe, id)
/*! Compare pipes */
AO2_STRING_FIELD_CMP_FN(ast_kafka_pipe, id)

static int load_module(void) {
	/* Monitor thread not started yet */
	monitor = AST_PTHREADT_NULL;

	AST_RWDLLIST_HEAD_INIT(&producers);
	AST_RWDLLIST_HEAD_INIT(&consumers);

	if(NULL == (pipes = ao2_container_alloc_hash(AO2_ALLOC_OPT_LOCK_MUTEX, 0, KAFKA_PIPE_BUCKETS, ast_kafka_pipe_hash_fn, NULL, ast_kafka_pipe_cmp_fn))) {
		AST_RWDLLIST_HEAD_DESTROY(&producers);
		AST_RWDLLIST_HEAD_DESTROY(&consumers);
		return AST_MODULE_LOAD_DECLINE;
	}

//	if(NULL == (kafka_tps = ast_taskprocessor_get(KAFKA_TASKPROCESSOR_MONITOR_ID, TPS_REF_DEFAULT))) {
//		ast_log(LOG_ERROR, "Failed to create Kafka taskprocessor.\n");
//
//		ao2_cleanup(pipes);
//		pipes = NULL;
//		AST_RWDLLIST_HEAD_DESTROY(&producers);
//		AST_RWDLLIST_HEAD_DESTROY(&consumers);
//		return AST_MODULE_LOAD_DECLINE;
//	}

	if(NULL == (kafka_sorcery = ast_sorcery_open())) {
		ast_log(LOG_ERROR, "Failed to open Kafka sorcery.\n");

//		ast_taskprocessor_unreference(kafka_tps);
//		kafka_tps = NULL;
		ao2_cleanup(pipes);
		pipes = NULL;
		AST_RWDLLIST_HEAD_DESTROY(&producers);
		AST_RWDLLIST_HEAD_DESTROY(&consumers);
		return AST_MODULE_LOAD_DECLINE;
	}

	if(sorcery_object_register(KAFKA_CLUSTER, sorcery_kafka_cluster_alloc, sorcery_kafka_cluster_apply_handler)) {
		ast_sorcery_unref(kafka_sorcery);
		kafka_sorcery = NULL;
//		ast_taskprocessor_unreference(kafka_tps);
//		kafka_tps = NULL;
		ao2_cleanup(pipes);
		pipes = NULL;
		AST_RWDLLIST_HEAD_DESTROY(&producers);
		AST_RWDLLIST_HEAD_DESTROY(&consumers);
		return AST_MODULE_LOAD_DECLINE;
	}

	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "brokers", "localhost", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, brokers));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "security_protocol", "plaintext", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, security_protocol));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "sasl_mechanism", "PLAIN", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, sasl_mechanism));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "sasl_username", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, sasl_username));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "sasl_password", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, sasl_password));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "client_id", "asterisk", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, client_id));

	if(sorcery_object_register(KAFKA_TOPIC, sorcery_kafka_topic_alloc, sorcery_kafka_topic_apply_handler)) {
		ast_sorcery_unref(kafka_sorcery);
		kafka_sorcery = NULL;
//		ast_taskprocessor_unreference(kafka_tps);
//		kafka_tps = NULL;
		ao2_cleanup(pipes);
		pipes = NULL;
		AST_RWDLLIST_HEAD_DESTROY(&producers);
		AST_RWDLLIST_HEAD_DESTROY(&consumers);
		return AST_MODULE_LOAD_DECLINE;
	}

	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_TOPIC, "pipe", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_topic, pipe));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_TOPIC, "topic", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_topic, topic));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_TOPIC, "producer", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_topic, producer_id));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_TOPIC, "consumer", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_topic, consumer_id));

	if(sorcery_object_register(KAFKA_PRODUCER, sorcery_kafka_producer_alloc, sorcery_kafka_producer_apply_handler)) {
		ast_sorcery_unref(kafka_sorcery);
		kafka_sorcery = NULL;
//		ast_taskprocessor_unreference(kafka_tps);
//		kafka_tps = NULL;
		ao2_cleanup(pipes);
		pipes = NULL;
		AST_RWDLLIST_HEAD_DESTROY(&producers);
		AST_RWDLLIST_HEAD_DESTROY(&consumers);
		return AST_MODULE_LOAD_DECLINE;
	}

	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "cluster", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_producer, cluster_id));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "timeout", "10000", OPT_UINT_T, 0, FLDSET(struct sorcery_kafka_producer, timeout_ms));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "partition", "-1", OPT_INT_T, 0, FLDSET(struct sorcery_kafka_producer, partition));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "request_required_acks", "-1", OPT_INT_T, 0, FLDSET(struct sorcery_kafka_producer, request_required_acks));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "max_in_flight_requests_per_connection", "1000000", OPT_UINT_T, 0, FLDSET(struct sorcery_kafka_producer, max_in_flight_requests_per_connection));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "message_send_max_retries", "2", OPT_UINT_T, 0, FLDSET(struct sorcery_kafka_producer, message_send_max_retries));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "retry_backoff_ms", "100", OPT_UINT_T, 0, FLDSET(struct sorcery_kafka_producer, retry_backoff_ms));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "enable_idempotence", "no", OPT_BOOL_T, 1, FLDSET(struct sorcery_kafka_producer, enable_idempotence));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "transactional_id", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_producer, transactional_id));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "client_id", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_producer, client_id));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "debug", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_producer, debug));



	if(ast_sorcery_observer_add(kafka_sorcery, KAFKA_PRODUCER, &producer_observers)) {
		ast_log(LOG_ERROR, "Failed to register observer for '%s' with Kafka sorcery.\n", KAFKA_PRODUCER);
		ast_sorcery_unref(kafka_sorcery);
		kafka_sorcery = NULL;
//		ast_taskprocessor_unreference(kafka_tps);
//		kafka_tps = NULL;
		ao2_cleanup(pipes);
		pipes = NULL;
		AST_RWDLLIST_HEAD_DESTROY(&producers);
		AST_RWDLLIST_HEAD_DESTROY(&consumers);
		return AST_MODULE_LOAD_DECLINE;
	}



	if(sorcery_object_register(KAFKA_CONSUMER, sorcery_kafka_consumer_alloc, sorcery_kafka_consumer_apply_handler)) {
		ast_sorcery_unref(kafka_sorcery);
		kafka_sorcery = NULL;
//		ast_taskprocessor_unreference(kafka_tps);
//		kafka_tps = NULL;
		ao2_cleanup(pipes);
		pipes = NULL;
		AST_RWDLLIST_HEAD_DESTROY(&producers);
		AST_RWDLLIST_HEAD_DESTROY(&consumers);
		return AST_MODULE_LOAD_DECLINE;
	}

	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CONSUMER, "cluster", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_consumer, cluster_id));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CONSUMER, "group_id", "asterisk", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_consumer, group_id));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CONSUMER, "isolation_level", "read_committed", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_consumer, isolation_level));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CONSUMER, "timeout", "10000", OPT_UINT_T, 0, FLDSET(struct sorcery_kafka_consumer, timeout_ms));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CONSUMER, "partition", "-1", OPT_INT_T, 0, FLDSET(struct sorcery_kafka_consumer, partition));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CONSUMER, "enable_auto_commit", "yes", OPT_BOOL_T, 1, FLDSET(struct sorcery_kafka_consumer, enable_auto_commit));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CONSUMER, "auto_commit_interval", "5000", OPT_UINT_T, 0, FLDSET(struct sorcery_kafka_consumer, auto_commit_interval_ms));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CONSUMER, "client_id", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_consumer, client_id));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CONSUMER, "debug", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_consumer, debug));


	/* Load all registered objects */
	ast_sorcery_load(kafka_sorcery);

	/* Process all defined clusters */
	process_all_clusters();

	ast_cli_register_multiple(kafka_cli, ARRAY_LEN(kafka_cli));

	return AST_MODULE_LOAD_SUCCESS;
}

static int unload_module(void) {
	pthread_t active_monitor;

	/* When we remove pipes, it destroy all linked services */
	ao2_cleanup(pipes);
	pipes = NULL;

	ast_mutex_lock(&monitor_lock);

	active_monitor = monitor;

	/* Prevent monitor activation */
	monitor = AST_PTHREADT_STOP;

	ast_mutex_unlock(&monitor_lock);

	if((AST_PTHREADT_NULL != active_monitor) && (AST_PTHREADT_STOP != active_monitor)) {
		/* Need to stop monitor thread */
#if 0
		pthread_cancel(active_monitor);
		pthread_kill(active_monitor, SIGURG);
#endif
		pthread_join(active_monitor, NULL);
	}

	ast_sorcery_observer_remove(kafka_sorcery, KAFKA_PRODUCER, &producer_observers);

	ast_cli_unregister_multiple(kafka_cli, ARRAY_LEN(kafka_cli));

	ast_sorcery_unref(kafka_sorcery);
	kafka_sorcery = NULL;

//	ast_taskprocessor_unreference(kafka_tps);
//	kafka_tps = NULL;

	AST_RWDLLIST_HEAD_DESTROY(&producers);
	AST_RWDLLIST_HEAD_DESTROY(&consumers);

	return 0;
}

static int reload_module(void) {
	ast_sorcery_reload(kafka_sorcery);

	return 0;
}

AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_GLOBAL_SYMBOLS | AST_MODFLAG_LOAD_ORDER, "Kafka resources",
	.support_level = AST_MODULE_SUPPORT_EXTENDED,
	.load = load_module,
	.unload = unload_module,
	.reload = reload_module,
	.load_pri = AST_MODPRI_DEFAULT,
	);
