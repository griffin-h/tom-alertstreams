import logging
from threading import Thread

from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand

from tom_alertstreams.alertstreams.alertstream import get_default_alert_streams


logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)

class Command(BaseCommand):
    help = 'Consume alerts from the alert streams configured in the settings.py ALERT_STREAMS'

    def handle(self, *args, **options):
        logger.debug(f'readstreams.Command.handle() args: {args}')
        logger.debug(f'readstreams.Command.handle() options: {options}')

        try:
            alert_streams = get_default_alert_streams()
        except ImproperlyConfigured as ex:
            logger.error(f'{ex.__class__.__name__}: Configure your alert streams in a settings.py ALERT_STREAM list: {ex}')
            exit(1)

        try:
            # listen to each alert_stream in it's own Thread (sort of at the same time)
            for alert_stream in alert_streams:
                t = Thread(target=alert_stream.listen, name=alert_stream._get_stream_classname())
                t.start()
                logger.info((f'read_streams {alert_stream._get_stream_classname()} TID={t.native_id} ; '
                             f'thread identifier={t.ident}'))
        except KeyboardInterrupt as msg:
            logger.info(f'read_streams handling KeyboardInterupt {msg}')

        logger.info('readstreams Command.handle() returning...')
