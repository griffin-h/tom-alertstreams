import logging
import multiprocessing

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

from tom_alertstreams.alertstreams.alertstream import get_default_alert_streams


logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)

class Command(BaseCommand):
    help = 'Consume alerts from the alert streams configured in the settings.py ALERT_STREAMS'


    def handle(self, *args, **options):
        logger.debug(f'read_streams.Command.handle() args: {args}')
        logger.debug(f'read_streams.Command.handle() options: {options}')

    
        for alert_stream in get_default_alert_streams():
            p = multiprocessing.Process(target=alert_stream.listen)
            p.start()  # PID not assigned until p.start()
            logger.info(f'read_streams {alert_stream._get_stream_classname()} PID={p.pid}')

        return

