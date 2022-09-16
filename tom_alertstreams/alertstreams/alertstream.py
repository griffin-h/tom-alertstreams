import abc
from  datetime import datetime, timezone
import logging
import os

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from gcn_kafka import Consumer

from hop import Stream
from hop.auth import Auth
from hop.models import JSONBlob
from hop.io import Metadata, StartPosition

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_default_alert_streams():
    """Return the AlertStreams configured in settings.py
    """
    return get_alert_streams(settings.ALERT_STREAMS)


def get_alert_streams(alert_stream_configs: list):
    """Return the AlertStreams configured in the given alert_stream_configs
    (a list of configuration dictionaries )

    Use get_default_alert_streams() if you want the AlertStreams configured in settings.py.
    """
    alert_streams = [] # build and return this list of AlertStream subclass instances
    for alert_stream_config in alert_stream_configs:
        if not alert_stream_config['OPTIONS'].get('ACTIVE', True):
            logger.debug(f'get_alert_streams - ignoring inactive stream: {alert_stream_config["NAME"]}')
            continue  # skip configs that are not active; default to ACTIVE
        try:
            klass = import_string(alert_stream_config['NAME'])
        except ImportError:
            msg = (
                f'The module (the value of the NAME key): {alert_stream_config["NAME"]} could not be imported. '
                f'Check your ALERT_STREAMS setting.'
            )
            raise ImproperlyConfigured(msg)

        alert_stream: AlertStream = klass(**alert_stream_config.get("OPTIONS", {}))
        alert_streams.append(alert_stream)

    return alert_streams



class AlertStream(abc.ABC):
    """Base class for specific alert streams like Hopskotch, GCNClassic, etc.

    * kwargs to __init__ is the OPTIONS dictionary defined in ALERT_STREAMS configuration
    dictionary (for example, see settings.py).
    * allowed_keys and required_keys should be defined as class properties in subclasses.
    * The allowed_keys are turned into instance properties in __init__.
    * Missing required_keys result in an ImproperlyConfigured Django exception.


    To implmement subclass:
    1. define allowed_key, required_keys as class variables
       <say what these do: used with OPTIONS dict in ALERT_STREAMS config dict>
    2. implement listen()
       this method probably doesn't return
    3. write your alert_handlers. which proably take and alert do something.
       the HopskotchAlertStreamClass defines a dictionary keyed by topic with
       callable values (i.e call this method with alerts from this topic). You
       may want that, too.
    4.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

        # filter the kwargs by allowed keys and add them as properties to AlertStream instance
        self.__dict__.update((k.lower(), v) for k, v in kwargs.items() if k in self.allowed_keys)

        missing_keys = set(self.required_keys) - set(kwargs.keys())
        if missing_keys:
            msg = (
                f'The following required keys are missing from the configuration OPTIONS of '
                f'{self._get_stream_classname()}: {list(missing_keys)} ; '
                f'These keys were found: {list(kwargs.keys())} ; '
                f'Check your ALERT_STREAMS setting.'
            )
            raise ImproperlyConfigured(msg)


    def _get_stream_classname(self) -> str:
        return type(self).__qualname__

    def get_stream_url(self) -> str:
        return self.url


    @abc.abstractmethod
    def listen(self):
        """Listen at the steam and dispatch alerts to handlers. Subclass extentions of
        this method is not expected to return.
        """
        pass


class HopskotchAlertStream(AlertStream):
    """
    """
    required_keys = ['URL', 'USERNAME', 'PASSWORD']
    allowed_keys = ['URL', 'USERNAME', 'PASSWORD', 'TOPICS']

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


    def _get_hop_authentication(self) -> Auth:
        """Get username and password and configure Hop client authentication

        Get SCIMMA_AUTH_USERNAME and SCIMMA_AUTH_PASSWORD from environment.
        """
        username = os.getenv('SCIMMA_AUTH_USERNAME', None)
        password = os.getenv('SCIMMA_AUTH_PASSWORD', None)

        if username is None or password is None:
            error_message = ('Supply SCiMMA Auth Hopskotch credentials on command line or '
                             'set SCIMMA_AUTH_USERNAME and SCIMMA_AUTH_PASSWORD environment variables.')
            logger.error(error_message)
            raise ImproperlyConfigured(error_message)

        return Auth(username, password)


    def get_stream_url(self) -> str:
        """For Hopskotch, topics are specified on the url. So, this
        method gets a base url (from super) and then adds topics to it.
        """
        base_stream_url =  super().get_stream_url()

        # if not present, add trailing slash to base_stream url
        # so, comma-separated topics can be appeneded.
        if base_stream_url[-1] != '/':
            base_stream_url += '/'

        # append comma-separated topics to base URL
        topics = ','.join(self.topics)  # 'topic1,topic2,topic3'
        hopskotch_stream_url = base_stream_url + topics

        logger.debug(f'HopskotchAlertStream.get_stream_url url: {hopskotch_stream_url}')
        return hopskotch_stream_url


    def get_stream(self, start_position=StartPosition.LATEST) -> Stream:
        hop_auth = Auth(self.username, self.password)

        stream = Stream(auth=hop_auth, start_at=start_position)
        return stream


    def listen(self):
        super().listen()

        # map from topic to alert parser/db-updater for that topic
        alert_handler = {
            #'gcn.circular': self._update_db_with_gcn_circular,
            #'tomtoolkit.test': self._update_db_with_hermes_alert,
            #'hermes.test': self._update_db_with_hermes_alert,
            'sys.heartbeat': self._heartbeat_handler
        }

        stream_url: str = self.get_stream_url()
        logger.debug(f'opening Hopskotch URL: {stream_url}')

        stream: Stream = self.get_stream()
        with stream.open(stream_url, 'r') as src:
            for alert, metadata in src.read(metadata=True):
                # type(gcn_circular) is <hop.models.GNCCircular>
                # type(metadata) is <hop.io.Metadata>
                try:
                    alert_handler[metadata.topic](alert, metadata)
                except KeyError as err:
                    logger.error(f'alert from topic {metadata.topic} received but no handler defined')
                    # TODO: should define a default handler for all unhandeled topics



    def _heartbeat_handler(self, heartbeat: JSONBlob,  metadata: Metadata):
        content: dict = heartbeat.content # see hop_client reatthedocs
        timestamp = datetime.fromtimestamp(content["timestamp"]/1e6, tz=timezone.utc)
        logging.info(f'{timestamp.isoformat()} heartbeat.content dict: {heartbeat.content}')
        logging.info(f'{timestamp.isoformat()} heartbeat JSONBlob: {heartbeat}')
        logging.info(f'{timestamp.isoformat()} metadata: {metadata}')




class GCNClassicAlertStream(AlertStream):
    """

    Pre-requisite: visit gcn.nasa.gov and sign-up.
    """
    required_keys = ['USERNAME', 'PASSWORD']
    allowed_keys = ['USERNAME', 'PASSWORD', 'TOPICS']

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def sample_code(self):
        # Connect as a consumer.
        # Warning: don't share the client secret with others.
        consumer = Consumer(client_id='get from settings',
                            client_secret='get from settings')
        # TODO: get client_id and client_secret from settings

        # Subscribe to topics and receive alerts
        # TODO: transfer these topics in to settings
        topics = [
            'gcn.classic.text.LVC_COUNTERPART',
            'gcn.classic.text.LVC_EARLY_WARNING',
            'gcn.classic.text.LVC_INITIAL',
            'gcn.classic.text.LVC_PRELIMINARY',
            'gcn.classic.text.LVC_RETRACTION',
            'gcn.classic.text.LVC_TEST',
            'gcn.classic.text.LVC_UPDATE'
        ]
        # TODO: set up ALERT_HANDLER dispatch dict and methods for these topics

        consumer.subscribe(topics)

        # TODO: move this into a base-class defined method
        while True:
            for message in consumer.consume():
                value = message.value()
                print(value)
