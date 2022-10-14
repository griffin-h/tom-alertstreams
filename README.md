# tom-alertstreams

`tom-alertstreams` is a reusable TOM Toolkit app for listening to kafka streams.

`tom-alertstreams` provides a management command, `readstreams`. There are no `urlpatterns`,
no Views, and no templates. The `readstreams` management command reads the `settings.py` `ALERT_STREAMS`
configuration and starts listening to each configured Kafka stream. It is not expected
to return, and is intended to run along side your TOM's server component. The `ALERT_STREAMS`
configuration (see below) tells `readstreams` what streams to access, what topics to listen to,
and what to do with messages that arrive on a given topic.

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

Each Kafka stream that your TOM listens to (via `readstreams`) will have a configuration dictionary
in your `settings.py` `ALERT_STREAMS`. `ALERT_STREAMS` is a list of configuration dictionaries, one
dictionary for each Kafka stream. Here's an example `ALERT_STREAMS` configuration for two Kafka streams:
[SCiMMA Hopskotch](https://scimma.org/hopskotch.html) and
[GCN Classic over Kafka](https://gcn.nasa.gov/quickstart).

```python
ALERT_STREAMS = [
    {
        'ACTIVE': True,
        'NAME': 'tom_alertstreams.alertstreams.hopskotch.HopskotchAlertStream',
        'OPTIONS': {
            'URL': 'kafka://kafka.scimma.org/',
            'USERNAME': os.getenv('SCIMMA_AUTH_USERNAME', None),
            'PASSWORD': os.getenv('SCIMMA_AUTH_PASSWORD', None),
            'TOPIC_HANDLERS': {
                'sys.heartbeat': 'tom_alertstreams.alertstreams.hopskotch.heartbeat_handler',
                'tomtoolkit.test': 'tom_alertstreams.alertstreams.alertstream.alert_logger',
                'hermes.test': 'tom_alertstreams.alertstreams.alertstream.alert_logger',
            },
        },
    },
    {
        'ACTIVE': True,
        'NAME': 'tom_alertstreams.alertstreams.gcn.GCNClassicAlertStream',
        # The keys of the OPTIONS dictionary become (lower-case) properties of the AlertStream instance.
        'OPTIONS': {
            # see https://github.com/nasa-gcn/gcn-kafka-python#to-use for configuration details.
            'GCN_CLASSIC_CLIENT_ID': os.getenv('GCN_CLASSIC_CLIENT_ID', None),
            'GCN_CLASSIC_CLIENT_SECRET': os.getenv('GCN_CLASSIC_CLIENT_SECRET', None),
            'DOMAIN': 'gcn.nasa.gov',  # optional, defaults to 'gcn.nasa.gov'
            'CONFIG': {  # optional
                # 'group.id': 'tom_alertstreams - llindstrom@lco.global',
                # 'auto.offset.reset': 'earliest',
                # 'enable.auto.commit': False
            },
            'TOPIC_HANDLERS': {
                'gcn.classic.text.LVC_INITIAL': 'tom_alertstreams.alertstreams.alertstream.alert_logger',
                'gcn.classic.text.LVC_PRELIMINARY': 'tom_alertstreams.alertstreams.alertstream.alert_logger',
                'gcn.classic.text.LVC_RETRACTION': 'tom_alertstreams.alertstreams.alertstream.alert_logger',
            },
        },
    }
]
```

The configuration dictionary for each `AlertStream` subclass will contain these key-value pairs:
* `ACTIVE`: Boolean which tells `readstreams` to access this stream. Should be `True`, unless you want to
keep a configuration dictionary, but ignore the stream.
* `NAME`: The name of the `AlertStream` subclass that implements the interface to this Kafka stream. `tom_alertstreams`
will provide `AlertStream` subclasses for major astromical Kafka streams. See below for instructions on Subclassing
the `AlertStream` base class.
* `OPTIONS`: A dictionary of key-value pairs specific to the`AlertStream` subclass given by `NAME`. The doc string for
`AlertStream` subclass should document what is expected. Typically, a URL, authentication information, and a dictionary,
`TOPIC_HANDLERS`, will be required. See "Subclassing `AlertStream`" below.

## Alert Handling

documentation coming.
## Subclassing `AlertStream`

documentation coming.