import logging

from gcn_kafka import Consumer

from tom_alertstreams.alertstreams.alertstream import AlertStream


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class GCNClassicAlertStream(AlertStream):
    """

    Pre-requisite: visit gcn.nasa.gov and sign-up to get your client_id and
    client_secret.
    """
    # Upon __init__, the AlertStream base class creates instance properties from
    # the settings OPTIONS dictionary, converting the keys to lowercase.
    required_keys = ['GCN_CLASSIC_CLIENT_ID', 'GCN_CLASSIC_CLIENT_SECRET', 'TOPIC_HANDLERS']
    allowed_keys = ['GCN_CLASSIC_CLIENT_ID', 'GCN_CLASSIC_CLIENT_SECRET', 'TOPIC_HANDLERS', 'DOMAIN', 'CONFIG']

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # properties have been created from the OPTIONS dicttionary

    def listen(self):
        super().listen()

        consumer = Consumer(client_id=self.gcn_classic_client_id,
                            client_secret=self.gcn_classic_client_secret,
                            domain=self.domain,
                            config=self.config,
                            )

        consumer.subscribe(list(self.topic_handlers.keys()))

        # logger.debug(f'Here is a list of the available topics for {self.domain}')
        # for topic in consumer.list_topics().topics:
        #     logger.debug(f'topic: {topic}')

        # what is a cimpl.Message?, cimpl.KafkaError?
        # see https://docs.confluent.io/4.1.1/clients/confluent-kafka-python/index.html#message
        while True:
            for alert in consumer.consume():
                kafka_error = alert.error()  # cimpl.KafkaError
                if kafka_error is None:
                    # no error, so call the alert handler
                    topic = alert.topic()
                    try:
                        self.alert_handler[topic](alert)
                    except KeyError as err:
                        logger.error(f'alert from topic {topic} received but no handler defined. err: {err}')
                else:
                    logger.error(f'GCNClassicAlertStream KafkaError: {kafka_error.name()}: {kafka_error.str()}')
        consumer.close()


def alert_logger(alert):
    """Example alert handler for GCN Classic over Kafka

    This alert handler simply logs the topic and value of the cimpl.Message instance.

    See https://docs.confluent.io/4.1.1/clients/confluent-kafka-python/index.html#message
    for cimpl.Message details.
    """
    logger.info(f'gcn.alert_logger alert.topic(): {alert.topic()}')
    logger.info(f'gcn.alert_logger alert.value(): {alert.value()}')

