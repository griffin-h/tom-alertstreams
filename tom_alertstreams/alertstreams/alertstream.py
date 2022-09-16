import abc
import logging
import re

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


def get_alert_streams(alert_stream_configs):
    """Return the AlertStreams configured in the given alert_stream_configs dict.

    Use get_default_alert_streams() if you want the AlertStreams configured in settings.py.
    """
    alert_streams = [] # build and return this list of AlertStream subclass instances
    for alert_stream_config in alert_stream_configs:
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
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        # filter the kwargs by allowed keys and add them as properties to AlertStream instance
        self.__dict__.update((k.lower(), v) for k, v in kwargs.items() if k in self.allowed_keys)

        missing_keys = set(self.required_keys) - set(kwargs.keys())
        if missing_keys:
            msg = (
                f'The following required keys are missing from the configuration OPTIONS of '
                f'{type(self).__qualname__}: {list(missing_keys)} ; '
                f'These keys were found: {list(kwargs.keys())} ; '
                f'Check your ALERT_STREAMS setting.'
            )
            raise ImproperlyConfigured(msg)


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
