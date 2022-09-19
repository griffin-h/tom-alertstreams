import logging

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

from tom_alertstreams.alertstreams.alertstream import get_default_alert_streams


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Command(BaseCommand):
    help = 'Consume alerts from the alert streams configured in the settings.py ALERT_STREAMS'


    def handle(self, *args, **options):
        logger.debug(f'args: {args}')
        logger.debug(f'options: {options}')

    
        logger.info('read_streams finding alert_streams...')
        for alert_stream in get_default_alert_streams():
            logger.info(f'read_streams listening to alert_stream: {alert_stream._get_stream_classname()}')
            alert_stream.listen()  # does not return
            logger.warning(f'*** Returned from {alert_stream._get_stream_classname()}')

        return

