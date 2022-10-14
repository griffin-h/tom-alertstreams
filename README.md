# tom-alertstreams

`tom-alertstreams` is a reusable TOM Toolkit app for listening to kafka streams.

`tom-alertstreams` provides a management command, `readstreams`. There are no `urlpatterns`,
no Views, and no templates. The `readstreams` management command reads the settings.py `ALERT_STREAMS`
configuration and starts listening to each configured Kafka stream. It is not expected
to return, and is intended to run along side your TOM's server component. The configuration
(see below) tells `readstreams` what streams to access, what topics to listen to, and what to
to with messages that arrive on a given topic.

## Installation

1. Install the package into your TOM environment:
    ```bash
    pip install tom-alertstreams
   ```

2. In your project `settings.py`, add `tom_alertstreams` to your `INSTALLED_APPS` setting:

    ```python
    INSTALLED_APPS = [
        ...
        'tom_alertstreams',
    ]
    ```

At this point you can verify the installation by running `./manage.py` to list the available
management commands and see

   ```bash
   [tom_alertstreams]
       readstreams
   ```
in the output.

## Configuration

documentation coming.

## Alert Handling

documentation coming.
## Subclassing `AlertStream`

documentation coming.