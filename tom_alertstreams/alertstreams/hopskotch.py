from datetime import datetime, timezone
import logging

from django.core.exceptions import ImproperlyConfigured

from hop import Stream
from hop.auth import Auth
from hop.models import JSONBlob
from hop.io import Metadata, StartPosition

from tom_alertstreams.alertstreams.alertstream import AlertStream

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class HopskotchAlertStream(AlertStream):
    """
    """
    required_keys = ['URL', 'USERNAME', 'PASSWORD', 'TOPIC_HANDLERS']
    allowed_keys = ['URL', 'USERNAME', 'PASSWORD', 'TOPIC_HANDLERS']

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # the following methods may fail if improperly configured.
        # So, do them now to catch any errors, before listen() is spawned in it's own Process.
        self.stream_url = self.get_stream_url()
        self.stream = self.get_stream()

    def get_stream_url(self) -> str:
        """For Hopskotch, topics are specified on the url. So, this
        method gets a base url (from super) and then adds topics to it.

        Hopskotch (hop.io) requires at least one topic to be specified.

        You might not need a method like this if your Kafka client provides
        alternative ways to subscribe to a topic. For example, the gcn_kafka.Consumer
        class provides a 'substribe([list of topics])' method. (see gcn.py).
        """
        logger.debug(f'HopskotchAlertStream.get_stream_url topics: {list(self.topic_handlers.keys())}')
        if self.topic_handlers == {}:
            msg = 'Hopskotch requires at least one topic to open the stream. Check ALERT_STREAMS in settings.py'
            raise ImproperlyConfigured(msg)

        base_stream_url = self.url

        # if not present, add trailing slash to base_stream url
        # so, comma-separated topics can be appeneded.
        if base_stream_url[-1] != '/':
            base_stream_url += '/'

        # append comma-separated topics to base URL
        topics = ','.join(list(self.topic_handlers.keys()))  # 'topic1,topic2,topic3'
        hopskotch_stream_url = base_stream_url + topics

        logger.debug(f'HopskotchAlertStream.get_stream_url url: {hopskotch_stream_url}')
        return hopskotch_stream_url

    def get_stream(self, start_position=StartPosition.LATEST) -> Stream:
        hop_auth = Auth(self.username, self.password)

        # TODO: allow StartPosition to be set from OPTIONS configuration dictionary
        stream = Stream(auth=hop_auth, start_at=start_position)
        return stream

    def listen(self):
        super().listen()

        # TODO: Provide example of making this a collections.defaultdict with a
        # default_factory which handles unexpected topics nicely.

        # TODO: alternatively, WARN upon OPTIONS['topics'] extries that don't have
        # handlers in the alert_handler. (i.e they've configured a topic subscription
        # without providing a handler for the topic. So, warn them).

        with self.stream.open(self.stream_url, 'r') as src:
            for alert, metadata in src.read(metadata=True):
                # type(gcn_circular) is <hop.models.GNCCircular>
                # type(metadata) is <hop.io.Metadata>
                try:
                    # TODO: should probably use *args, **kwargs to pass unknow number of arguments
                    self.alert_handler[metadata.topic](alert, metadata)
                except KeyError as err:
                    logger.error(f'alert from topic {metadata.topic} received but no handler defined. err: {err}')
                    # TODO: should define a default handler for all unhandeled topics


def heartbeat_handler(heartbeat: JSONBlob, metadata: Metadata):
    """Example alert handler for HopskotchAlertStream sys.heartbeat topic.

    Note that HopskotchAlertStream.listen() method knows that Hopskotch alerts come with
    both alert and metadata. So, the alert_handler methods have a signiture (taking both
    as arguments) specific to this stream.
    """
    content: dict = heartbeat.content  # see hop_client reatthedocs
    timestamp = datetime.fromtimestamp(content["timestamp"] / 1e6, tz=timezone.utc)
    if heartbeat.content['count'] % 300 == 0:
        # mod 300 just for convenience so as not to flood logger
        logging.info(f'{timestamp.isoformat()} heartbeat.content dict: {heartbeat.content}. metadata: {metadata}')
