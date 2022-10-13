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
    required_keys = ['GCN_CLASSIC_CLIENT_ID', 'GCN_CLASSIC_CLIENT_SECRET', 'TOPIC_HANDLER']
    allowed_keys = ['GCN_CLASSIC_CLIENT_ID', 'GCN_CLASSIC_CLIENT_SECRET', 'TOPIC_HANDLER', 'DOMAIN', 'CONFIG']

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

        consumer.subscribe(list(self.topic_handler.keys()))

        # logger.debug(f'Here is a list of the available topics for {self.domain}')
        # for topic in consumer.list_topics().topics:
        #     logger.debug(f'topic: {topic}')

        while True:
            for message in consumer.consume():
                message_topic = message.topic()
                try:
                    self.topic_handler[message_topic](message)
                except KeyError as err:
                    logger.error(f'alert from topic {metadata.topic} received but no handler defined. err: {err}')

        consumer.close()
