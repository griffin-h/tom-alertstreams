import abc
import logging

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_default_alert_streams():
    """Return the AlertStreams configured in settings.py
    """
    try:
        alert_streams = get_alert_streams(settings.ALERT_STREAMS)
    except AttributeError as err:
        raise ImproperlyConfigured(err)

    return alert_streams


def get_alert_streams(alert_stream_configs: list):
    """Return the AlertStreams configured in the given alert_stream_configs
    (a list of configuration dictionaries )

    Use get_default_alert_streams() if you want the AlertStreams configured in settings.py.
    """
    alert_streams = []  # build and return this list of AlertStream subclass instances
    for alert_stream_config in alert_stream_configs:
        if not alert_stream_config.get('ACTIVE', True):
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
       The HopskotchAlertStream.listen() method defines an 'alert_handlers' dictionary keyed by
       alert topic with callable values (i.e call this method with alerts from this topic).
       The GCNClassicAlertStream.listen() is another example.
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

        self.alert_handler = self._process_topic_handlers()

    def _get_stream_classname(self) -> str:
        return type(self).__qualname__

    def _process_topic_handlers(self):
        """ convert the TOPIC_HANDLERS values in to callable functions in
        the returned message_handler dictionary. (keyed by topic, value is callable)
        """
        alert_handler = {}
        for topic, callable_string in self.topic_handlers.items():
            # convert string from TOPIC_HANDLERS in to a callable function in
            # the message_handler dictionary (both keyed by topic string)
            alert_handler[topic] = import_string(callable_string)
        return alert_handler

    @abc.abstractmethod
    def listen(self):
        """Listen at the steam and dispatch alerts to handlers. Subclass extentions of
        this method are not expected to return.
        """
        pass


def alert_logger(alert):
    """Example alert handler
    """
    logger.info(f'Alert received {alert}')
