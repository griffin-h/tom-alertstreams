import abc
from  datetime import datetime, timezone
import logging
import os

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from gcn_kafka import Consumer


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


class HopskotchAlertStream(AlertStream):
    """
    """
    required_keys = ['URL', 'USERNAME', 'PASSWORD']
    allowed_keys = ['URL', 'USERNAME', 'PASSWORD', 'TOPICS']

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


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
