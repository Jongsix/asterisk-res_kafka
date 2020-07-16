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
				<configOption name="ssl" default="no">
					<synopsis>MQTT broker require SSL connection</synopsis>
					<description>
						<enumlist>
							<enum name="no" />
							<enum name="yes" />
						</enumlist>
					</description>
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
					<synopsis>Default service timeout, ms</synopsis>
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
				<configOption name="timeout" default="10000">
					<synopsis>Default service timeout, ms</synopsis>
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
#include "asterisk/astobj2.h"
#include "asterisk/linkedlists.h"
#include "asterisk/cli.h"
#include "asterisk/stringfields.h"
#include "asterisk/utils.h"

#include "librdkafka/rdkafka.h"

#define KAFKA_CONFIG_FILENAME "kafka.conf"

#define KAFKA_CLUSTER "cluster"
#define KAFKA_TOPIC "topic"
#define KAFKA_PRODUCER "producer"
#define KAFKA_CONSUMER "consumer"
#define KAFKA_ERRSTR_MAX_SIZE 80

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
	/*! Broker must use SSL connection */
	unsigned int ssl;
};

/*! Kafka producer common parameters */
struct sorcery_kafka_producer {
	SORCERY_OBJECT(defails);
	AST_DECLARE_STRING_FIELDS(
		/*! Cluster resource id */
		AST_STRING_FIELD(cluster_id);
	);
	/*! Default service timeout, ms */
	unsigned int timeout_ms;
};

/*! Kafka consumer common parameters */
struct sorcery_kafka_consumer {
	SORCERY_OBJECT(defails);
	AST_DECLARE_STRING_FIELDS(
		/*! Cluster resource id */
		AST_STRING_FIELD(cluster_id);
	);
	/*! Default service timeout, ms */
	unsigned int timeout_ms;
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
	/* librdkafka producer's or consumer's handle */
	rd_kafka_t *rd_kafka;
	/* Default service timeout, ms */
	unsigned int timeout_ms;
};

/*! Internal representation of Kafka's topic */
struct kafka_topic {
	/*! Link to next topic on the list in ast_kafka_pipe */
	AST_LIST_ENTRY(kafka_topic) link;
	/*! Link to the topic service (producer or consumer) */
	struct kafka_service *service;
	/*! librdkafka topic's handle */
	rd_kafka_topic_t *rd_kafka_topic;
};

/*! Internal representation of message pipe */
struct ast_kafka_pipe {
	AST_DECLARE_STRING_FIELDS(
		/*! Pipe id, used by Asterisk modules to produce or consume messages */
		AST_STRING_FIELD(id);
	);
	/*! List of producer's topics for this pipe */
	AST_LIST_HEAD(producer_topics_s, kafka_topic) producer_topics;
	/*! List of consumer's topics for this pipe */
	AST_LIST_HEAD(consumer_topics_s, kafka_topic) consumer_topics;
};



static char *handle_kafka_show(struct ast_cli_entry *e, int cmd, struct ast_cli_args *a);
static char *complete_pipe_choice(const char *word);
static int show_pipes_cb(void *obj, void *arg, int flags);
static int cli_show_topic_cb(struct kafka_topic *topic, void *opaqe_1, void *opaqe_2, struct ast_kafka_pipe *pipe);


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

static void process_producer_topic(struct kafka_service *producer, const struct sorcery_kafka_topic *sorcery_topic);
static void process_consumer_topic(struct kafka_service *consumer, const struct sorcery_kafka_topic *sorcery_topic);

static struct kafka_topic *new_kafka_producer_topic(struct kafka_service *producer, const struct sorcery_kafka_topic *sorcery_topic);
static void kafka_producer_topic_destructor(void *obj);
static struct kafka_topic *new_kafka_consumer_topic(struct kafka_service *producer, const struct sorcery_kafka_topic *sorcery_topic);
static void kafka_consumer_topic_destructor(void *obj);

static int on_all_producer_topics(struct ast_kafka_pipe *pipe, int (*callback)(struct kafka_topic *topic, void *opaqe_1, void *opaqe_2, struct ast_kafka_pipe *pipe), void *opaqe_1, void *opaqe_2);
static int on_all_consumer_topics(struct ast_kafka_pipe *pipe, int (*callback)(struct kafka_topic *topic, void *opaqe_1, void *opaqe_2, struct ast_kafka_pipe *pipe), void *opaqe_1, void *opaqe_2);
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
};

static const struct ast_sorcery_observer producer_observers = {
	.created = on_producer_created,
	.updated = on_producer_updated,
	.deleted = on_producer_deleted,
	.loaded = on_producer_loaded,
};

/*! Sorcery */
static struct ast_sorcery *kafka_sorcery;

/*! Defined pipes container */
static struct ao2_container *pipes;


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

static int cli_show_topic_cb(struct kafka_topic *topic, void *opaqe_1, void *opaqe_2, struct ast_kafka_pipe *pipe) {
	struct ast_cli_args *a = opaqe_1;
	const char *service_type = opaqe_2;
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

#if 0
static rd_kafka_t *kafka_get_producer(struct sorcery_kafka_cluster *cluster) {
	rd_kafka_conf_t *config = rd_kafka_conf_new();
	char *errstr = ast_alloca(KAFKA_ERRSTR_MAX_SIZE);
	rd_kafka_t *producer;

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

	producer = rd_kafka_new(RD_KAFKA_PRODUCER, config, errstr, KAFKA_ERRSTR_MAX_SIZE);

	if(NULL == producer) {
		ast_log(LOG_ERROR, "Kafka cluster %s: unable to create producer because %s\n", ast_sorcery_object_get_id(cluster), errstr);
		rd_kafka_conf_destroy(config);
	}

	return producer;
}
#endif


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
					process_producer_topic(producer, sorcery_topic);

					ao2_ref(sorcery_topic, -1);
				}

				ao2_iterator_destroy(&i);

				ao2_ref(producer, -1);
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
					process_consumer_topic(consumer, sorcery_topic);

					ao2_ref(sorcery_topic, -1);
				}

				ao2_iterator_destroy(&i);

				ao2_ref(consumer, -1);
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
	}

	if(NULL == (producer = ao2_alloc(sizeof(*producer), kafka_producer_destructor))) {
		ast_log(LOG_ERROR, "Kafka cluster '%s': Unable to create producer '%s' because Out of memory\n", ast_sorcery_object_get_id(sorcery_cluster), ast_sorcery_object_get_id(sorcery_producer));
	} else {
		char *errstr = ast_alloca(KAFKA_ERRSTR_MAX_SIZE);

		producer->timeout_ms = sorcery_producer->timeout_ms;

		/* Attempt to creare librdkafka producer */
		if(NULL == (producer->rd_kafka = rd_kafka_new(RD_KAFKA_PRODUCER, config, errstr, KAFKA_ERRSTR_MAX_SIZE))) {
			ast_log(LOG_ERROR, "Kafka cluster '%s': unable to create producer '%s' because %s\n", ast_sorcery_object_get_id(sorcery_cluster), ast_sorcery_object_get_id(sorcery_producer), errstr);
		} else {
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
	}

	if(NULL == (consumer = ao2_alloc(sizeof(*consumer), kafka_consumer_destructor))) {
		ast_log(LOG_ERROR, "Kafka cluster '%s': Unable to create consumer '%s' because Out of memory\n", ast_sorcery_object_get_id(sorcery_cluster), ast_sorcery_object_get_id(sorcery_consumer));
	} else {
		char *errstr = ast_alloca(KAFKA_ERRSTR_MAX_SIZE);

		consumer->timeout_ms = sorcery_consumer->timeout_ms;

		/* Attempt to creare librdkafka consumer */
		if(NULL == (consumer->rd_kafka = rd_kafka_new(RD_KAFKA_CONSUMER, config, errstr, KAFKA_ERRSTR_MAX_SIZE))) {
			ast_log(LOG_ERROR, "Kafka cluster '%s': unable to create consumer '%s' because %s\n", ast_sorcery_object_get_id(sorcery_cluster), ast_sorcery_object_get_id(sorcery_consumer), errstr);
		} else {
			return consumer;
		}
	}

	rd_kafka_conf_destroy(config);

	ao2_cleanup(consumer);

	return NULL;
}

static void kafka_consumer_destructor(void *obj) {
	struct kafka_service *consumer = obj;

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

/*! Process Kafka producer related topic at the cluster */
static void process_producer_topic(struct kafka_service *producer, const struct sorcery_kafka_topic *sorcery_topic) {
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
	} else {
		ast_log(LOG_ERROR, "Unable to add producer topic '%s' to pipe '%s' - Out of memory\n", 
			ast_sorcery_object_get_id(sorcery_topic), sorcery_topic->pipe);
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
static void process_consumer_topic(struct kafka_service *consumer, const struct sorcery_kafka_topic *sorcery_topic) {
	RAII_VAR(struct kafka_topic *, topic, new_kafka_consumer_topic(consumer, sorcery_topic), ao2_cleanup);
	RAII_VAR(struct ast_kafka_pipe *, pipe, ast_kafka_get_pipe(sorcery_topic->pipe, 1), ao2_cleanup);

	if(pipe && topic) {
		AST_LIST_LOCK(&pipe->consumer_topics);

		/* Add new topic to the specified pipe */
		AST_LIST_INSERT_TAIL(&pipe->consumer_topics, topic, link);

		/* Object in the list */
		ao2_ref(topic, +1);

		AST_LIST_UNLOCK(&pipe->consumer_topics);
	} else {
		ast_log(LOG_ERROR, "Unable to add consumer topic '%s' to pipe '%s' - Out of memory\n", 
			ast_sorcery_object_get_id(sorcery_topic), sorcery_topic->pipe);
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

	if(topic->rd_kafka_topic) {
		ast_debug(3, "Destroy rd_kafka_topic_t object %p on producer's topic %p\n", topic->rd_kafka_topic, topic);
		rd_kafka_topic_destroy(topic->rd_kafka_topic);
	}

	/* Release service object */
	ao2_cleanup(topic->service);
}


//!!!
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

		/* Topic handle created, link this object to the service */
		topic->service = consumer;
		ao2_ref(consumer, +1);
	}

	return topic;
}

static void kafka_consumer_topic_destructor(void *obj) {
	struct kafka_topic *topic = obj;

	if(topic->rd_kafka_topic) {
		ast_debug(3, "Destroy rd_kafka_topic_t object %p on consumer's topic %p\n", topic->rd_kafka_topic, topic);
		rd_kafka_topic_destroy(topic->rd_kafka_topic);
	}

	/* Release service object */
	ao2_cleanup(topic->service);
}

//!!!




/*! Execute callback on all producer topics in the pipe */
static int on_all_producer_topics(struct ast_kafka_pipe *pipe, int (*callback)(struct kafka_topic *topic, void *opaqe_1, void *opaqe_2, struct ast_kafka_pipe *pipe), void *opaqe_1, void *opaqe_2) {
	int status = 0;
	struct kafka_topic *topic;

	AST_LIST_LOCK(&pipe->producer_topics);

	AST_LIST_TRAVERSE(&pipe->producer_topics, topic, link) {
		status |= (*callback)(topic, opaqe_1, opaqe_2, pipe);

		if(status < 0) {
			/* Fatal error */
			break;
		}
	}

	AST_LIST_UNLOCK(&pipe->producer_topics);

	return status;
}

/*! Execute callback on all consumer topics in the pipe */
static int on_all_consumer_topics(struct ast_kafka_pipe *pipe, int (*callback)(struct kafka_topic *topic, void *opaqe_1, void *opaqe_2, struct ast_kafka_pipe *pipe), void *opaqe_1, void *opaqe_2) {
	int status = 0;
	struct kafka_topic *topic;

	AST_LIST_LOCK(&pipe->consumer_topics);

	AST_LIST_TRAVERSE(&pipe->consumer_topics, topic, link) {
		status |= (*callback)(topic, opaqe_1, opaqe_2, pipe);

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

	ast_debug(3, "Allocated Kafka producer %s (%p)\n", name, producer);

	return producer;
}

static void sorcery_kafka_producer_destructor(void *obj) {
	struct sorcery_kafka_producer *producer = obj;

	ast_debug(3, "Destroyed Kafka producer %s (%p)\n", ast_sorcery_object_get_id(producer), producer);

	ast_string_field_free_memory(producer);
}

static int sorcery_kafka_consumer_apply_handler(const struct ast_sorcery *sorcery, void *obj) {
	struct sorcery_kafka_consumer *consumer = obj;

	ast_debug(3, "Apply Kafka consumer %s (%p)\n", ast_sorcery_object_get_id(consumer), consumer);

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

	ast_debug(3, "Allocated Kafka consumer %s (%p)\n", name, consumer);

	return consumer;
}

static void sorcery_kafka_consumer_destructor(void *obj) {
	struct sorcery_kafka_consumer *consumer = obj;

	ast_debug(3, "Destroyed Kafka consumer %s (%p)\n", ast_sorcery_object_get_id(consumer), consumer);

	ast_string_field_free_memory(consumer);
}

static int sorcery_kafka_cluster_apply_handler(const struct ast_sorcery *sorcery, void *obj) {
	struct sorcery_kafka_cluster *cluster = obj;

//	rd_kafka_t *producer = kafka_get_producer(cluster);

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

	ast_debug(3, "Allocated Kafka cluster %s (%p)\n", name, cluster);

	return cluster;
}

static void sorcery_kafka_cluster_destructor(void *obj) {
	struct sorcery_kafka_cluster *cluster = obj;

	ast_debug(3, "Destroyed Kafka cluster %s (%p)\n", ast_sorcery_object_get_id(cluster), cluster);

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
		/* Release object */
		ao2_ref(topic, -1);
	}

	AST_LIST_UNLOCK(&pipe->consumer_topics);

	AST_LIST_HEAD_DESTROY(&pipe->consumer_topics);

	AST_LIST_LOCK(&pipe->producer_topics);

	while(NULL != (topic = AST_LIST_REMOVE_HEAD(&pipe->producer_topics, link))) {
		/* Release object */
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
	if(NULL == (pipes = ao2_container_alloc_hash(AO2_ALLOC_OPT_LOCK_MUTEX, 0, KAFKA_PIPE_BUCKETS, ast_kafka_pipe_hash_fn, NULL, ast_kafka_pipe_cmp_fn))) {
		return AST_MODULE_LOAD_DECLINE;
	}

	if(NULL == (kafka_sorcery = ast_sorcery_open())) {
		ast_log(LOG_ERROR, "Failed to open Kafka sorcery.\n");

		ao2_cleanup(pipes);
		pipes = NULL;
		return AST_MODULE_LOAD_DECLINE;
	}

	if(sorcery_object_register(KAFKA_CLUSTER, sorcery_kafka_cluster_alloc, sorcery_kafka_cluster_apply_handler)) {
		ast_sorcery_unref(kafka_sorcery);
		kafka_sorcery = NULL;
		ao2_cleanup(pipes);
		pipes = NULL;
		return AST_MODULE_LOAD_DECLINE;
	}

	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "brokers", "localhost", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, brokers));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "security_protocol", "plaintext", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, security_protocol));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "sasl_mechanism", "PLAIN", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, sasl_mechanism));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "sasl_username", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, sasl_username));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "sasl_password", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, sasl_password));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "client_id", "asterisk", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_cluster, client_id));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CLUSTER, "ssl", "no", OPT_BOOL_T, 1, FLDSET(struct sorcery_kafka_cluster, ssl));

	if(sorcery_object_register(KAFKA_TOPIC, sorcery_kafka_topic_alloc, sorcery_kafka_topic_apply_handler)) {
		ast_sorcery_unref(kafka_sorcery);
		kafka_sorcery = NULL;
		ao2_cleanup(pipes);
		pipes = NULL;
		return AST_MODULE_LOAD_DECLINE;
	}

	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_TOPIC, "pipe", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_topic, pipe));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_TOPIC, "topic", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_topic, topic));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_TOPIC, "producer", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_topic, producer_id));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_TOPIC, "consumer", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_topic, consumer_id));

	if(sorcery_object_register(KAFKA_PRODUCER, sorcery_kafka_producer_alloc, sorcery_kafka_producer_apply_handler)) {
		ast_sorcery_unref(kafka_sorcery);
		kafka_sorcery = NULL;
		ao2_cleanup(pipes);
		pipes = NULL;
		return AST_MODULE_LOAD_DECLINE;
	}

	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "cluster", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_producer, cluster_id));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_PRODUCER, "timeout", "10000", OPT_UINT_T, 0, FLDSET(struct sorcery_kafka_producer, timeout_ms));



	if(ast_sorcery_observer_add(kafka_sorcery, KAFKA_PRODUCER, &producer_observers)) {
		ast_log(LOG_ERROR, "Failed to register observer for '%s' with Kafka sorcery.\n", KAFKA_PRODUCER);
		ast_sorcery_unref(kafka_sorcery);
		kafka_sorcery = NULL;
		ao2_cleanup(pipes);
		pipes = NULL;
		return AST_MODULE_LOAD_DECLINE;
	}



	if(sorcery_object_register(KAFKA_CONSUMER, sorcery_kafka_consumer_alloc, sorcery_kafka_consumer_apply_handler)) {
		ast_sorcery_unref(kafka_sorcery);
		kafka_sorcery = NULL;
		ao2_cleanup(pipes);
		pipes = NULL;
		return AST_MODULE_LOAD_DECLINE;
	}

	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CONSUMER, "cluster", "", OPT_STRINGFIELD_T, 0, STRFLDSET(struct sorcery_kafka_consumer, cluster_id));
	ast_sorcery_object_field_register(kafka_sorcery, KAFKA_CONSUMER, "timeout", "10000", OPT_UINT_T, 0, FLDSET(struct sorcery_kafka_consumer, timeout_ms));


	/* Load all registered objects */
	ast_sorcery_load(kafka_sorcery);

	/* Process all defined clusters */
	process_all_clusters();




	ast_cli_register_multiple(kafka_cli, ARRAY_LEN(kafka_cli));

	return AST_MODULE_LOAD_SUCCESS;
}

static int unload_module(void) {
	ast_sorcery_observer_remove(kafka_sorcery, KAFKA_PRODUCER, &producer_observers);

	ast_cli_unregister_multiple(kafka_cli, ARRAY_LEN(kafka_cli));

	ast_sorcery_unref(kafka_sorcery);
	kafka_sorcery = NULL;

	ao2_cleanup(pipes);
	pipes = NULL;

	return 0;
}

static int reload_module(void) {
	ast_sorcery_reload(kafka_sorcery);

	return 0;
}

AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_DEFAULT, "Kafka resources",
	.support_level = AST_MODULE_SUPPORT_EXTENDED,
	.load = load_module,
	.unload = unload_module,
	.reload = reload_module,
	);
