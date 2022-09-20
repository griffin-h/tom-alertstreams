import logging
from multiprocessing import Process

from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

from tom_alertstreams.alertstreams.alertstream import get_default_alert_streams


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)

class Command(BaseCommand):
    help = 'Consume alerts from the alert streams configured in the settings.py ALERT_STREAMS'


    def handle(self, *args, **options):
        logger.debug(f'read_streams.Command.handle() args: {args}')
        logger.debug(f'read_streams.Command.handle() options: {options}')

        try:
            alert_streams = get_default_alert_streams()
        except ImproperlyConfigured as err:
            logger.error(err)
            exit(1)

        alert_stream_processes = []
        try:
            for alert_stream in alert_streams:
                p = Process(target=alert_stream.listen)
                alert_stream_processes.append(p)  # save Process instance for clean-up 
                p.start()  # PID not assigned until p.start()
                logger.info(f'read_streams {alert_stream._get_stream_classname()} PID={p.pid}')
        except KeyboardInterrupt as msg:
            logger.info(f'read_streams handling KeyboardInterupt: {msg}')
            for p in alert_stream_processes:
                if p.is_alive():
                    logger.info(f'read_streams Terminating {alert_stream._get_stream_classname()} PID={p.pid}')
                    p.terminate()
                else:
                    logger.info(f'read_streams {alert_stream._get_stream_classname()} PID={p.pid} exit code: {p.exitcode}')

        logger.info(f'read_streams.Command.handle() exiting.')
        return

