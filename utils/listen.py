from __future__ import absolute_import, print_function

from contextlib import contextmanager
from datetime import datetime
import json
import logging
import os
import traceback
import uuid

import six
import tornado.ioloop

from mds_data_transform.constants import constants
from mds_data_transform.heartbeat import get_heartbeater
from mds_data_transform.kafka_logger import KafkaLoggingHandler
from mds_data_transform.kafkaclients import (
    get_kafka_consumer, get_kafka_producer)
from mds_data_transform.transform_L0 import Transformer as L0Transformer

try:
    from mds_data_transform.transform_grib import (
        Transformer as GribTransformer)
    from mds_data_transform.transform_nc import (
        Transformer as NcTransformer)
except ImportError:
    GribTransformer = None
    NcTransformer = None


def setup_loggers():
    """
    Returns two loggers. The first should be used for referenceable & quality
    logs suitable for an operational controler to diagnose issues. The second
    is for full information about the system (incl. DEBUG level messages).

    All messages published to the summary logger will also be published to
    the full log automatically.
    """
    summary_log = logging.getLogger('mds-summary')

    # A logger to publish full detail (e.g. traceback)
    # for diagnosis.
    full_log = logging.getLogger('mds-full')

    # If this logger hasn't already been setup, let's make it stream to stderr
    if not full_log.handlers:
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s ' +
                                      ' %(levelname)s ' +
                                      constants.COMPONENT_LISTENER +
                                      ' %(message)s')
        stream_handler.setFormatter(formatter)
        full_log.addHandler(stream_handler)
        full_log.setLevel(logging.DEBUG)

    # Log MDS-summary to both the kafka topic & stderr.
    if not summary_log.handlers:
        kafka_handler = KafkaLoggingHandler(get_kafka_producer(),
                                            topic="systemLog",
                                            default_extra=constants.
                                            extra_log_content_listener)

        summary_log.addHandler(kafka_handler)
        for handler in full_log.handlers:
            summary_log.addHandler(handler)

        # We set the log to the lowest common level,
        # but the kafka handler should not publish anything below
        # INFO.
        summary_log.setLevel(logging.DEBUG)
        kafka_handler.setLevel(logging.INFO)

@contextmanager
def log_if_fail(message, level=logging.ERROR, component='System'):
    try:
        yield
    except Exception as err:
        if not hasattr(err, '_logged'):
            # We get hold of the loggers defined by setup_loggers
            # *just in case* that function itself causes an exception.
            logger = logging.getLogger('mds-summary')
            logger.log(level, message, extra={'component': component})
            full_log = logging.getLogger('mds-full')
            full_log.log(level, '{}\n{}'.format(err, traceback.format_exc()))
            err._logged = True
        # Re-raise so that others can catch the problem if they want.
        raise


def incomingData_IncomingScData(message):
    """
    A handler for preparing L1 data from SC data.

    """
    with log_if_fail('An unhandled exception occurred'):
        logger, full_log = setup_loggers()

        input_fpath = message.get('srcFile', None)

        if input_fpath is None:
            logger.error('The srcFile information was not correctly provided '
                         'in the incomingData topic.')
            return

        # Turn input_fpath into a list if a string
        if isinstance(input_fpath, six.string_types):
            input_fpath = [input_fpath]

        for fname in input_fpath:
            if not os.path.exists(fname):
                logger.error("File does not exist: '{}'".format(fname))
                return

        # Get the family, stream & variant. e.g. family":"ACCESS",
        # "stream":"APS2", "variant":"ACCESS-G"
        file_md = message['fileNameMetadata']
        family = file_md['family']
        stream = file_md['stream']
        variant = file_md['variant']

        output_dir = '/data/mds/incoming/from-preprocessor'
        input_basename, input_extension = os.path.splitext(os.path.basename
                                                           (input_fpath[0]))
        target = os.path.join(output_dir,
                              family,
                              stream,
                              variant,
                              input_basename + '.nc')

        if input_fpath[0].endswith('.grib2') or (
                input_fpath[0].endswith('.grb2')):
            if GribTransformer is None:
                raise ValueError("GribTransformer not available.")
            transformer_cls = GribTransformer
        elif input_fpath[0].endswith('.nc') or (
                input_fpath[0].endswith('.nc4')):
            if NcTransformer is None:
                raise ValueError("NcTransformer not available.")
            transformer_cls = NcTransformer
            # Need change target file name
            target = target.replace('.nc*', '.grib2')
           # Need change target file name
            target = target.replace('.nc*', '.grib2')
        else:
            transformer_cls = L0Transformer

        logger.info('Transforming {} to {}'.format(input_fpath, target))
        t = transformer_cls()

        logger.debug('Loading from {}'.format(input_fpath))
        with log_if_fail('Failed to load the given file',
                         component='Content'):
            t.load(input_fpath)

        t.transform()

        logger.debug('Writing to {}'.format(target))
        with log_if_fail('Failed to write the target file.',
                         component='Content'):
            t.save(target)

        t.validate(target)
 
 
# Define a function that can be used by the listener to give
# handled tasks human friendly & unique names.
def _incomingData_IncomingScData_keyer(handler_fn, payload):
    fname = payload.get('srcFile', '')
    if not isinstance(fname, six.string_types):
        # Pick out the first filename.
        fname = fname[0]
    key = '{} {}'.format(os.path.basename(fname).replace('-', '_'),
                         str(uuid.uuid4()))
    return key


incomingData_IncomingScData._keyer = _incomingData_IncomingScData_keyer

def listen(consumer, handlers, executor, poolers=None):
    """
    Listen to the given consumer, and dispatch events to the
    dispatcher based on the handler dictionary.

    Parameters
    ----------
    consumer : an iterable
        An iterator, such as a kafka.KafkaConsumer, that yields messages
        to be handled. Messages should be json encoded in the form:

            {'version': <version>,
             'event': <event name>,
             'payload': <event payload>}

    handlers : dict
        A mapping of event name to even handling function, where the only
        argument to the handled function is the event payload. If a handling
        function has a ``_keyer`` attribute, the attribute will be called
        with the handler function and the broker message, and should return
        the key of the scheduled Future.

    executor : concurrent.futures.Executor, distributed.Client, or similar
        The executor to submit the handling function to. Ideally this
        would be a non-blocking executor. The only method used by the exector
        is ``submit``, and it MUST return a concurrent.futures.Future object.

    poolers : list or tuple
        Objects to allow consumer message to be pooled before being submitted
        as executor tasks. Typically, this will be used to batch multiple
        messages into a single task. For example, ACCESS produces 2 files "PA"
        and "PE", and *both* must exist before processing may commence -
        therefore it is possible to pool the message until both have arrived,
        and then submit a single task with both files to the executor.

    """
    # A container to hold all of the submitted jobs.
    # We periodically check to see if any of the jobs have been completed,
    # and if so, log and remove them from the list.
    jobs = []
    jobstats = {}

    logger, full_log = setup_loggers()

    if poolers is None:
        poolers = []

    def cleanup_jobs():
        # We don't do any work unless there are some jobs to report on.
        if jobs:
            with log_if_fail('Failed to log the queue status',
                             level=logging.WARNING):
                done = [job for job in jobs if job.done()]

                # Any jobs that aren't in the done list must still be
                # working. To avoid a race-condition we simply pick-off
                # all of the jobs that aren't in done.
                working = [job for job in jobs if job not in done]

                # Done tasks may include those that failed. We delegate
                # error handling to those jobs, but we still want to report
                # appropriate summary statistics, so filter them out.
                errored = [job for job in done if job.status == 'error']

                # The number of successful tasks is then all that are done,
                # minus those that errored.
                n_finished = len(done) - len(errored)

                logger.info('{} jobs in the queue ({} working, {} done, '
                            '{} errored)'.format(len(jobs), len(working),
                                                  n_finished, errored))

                for job in errored:
                    full_log.error("Job Error: '{}'".format(job))

                # Now that we have logged them, remove the completed jobs.
                for job in done:
                    jobs.remove(job)

    # Call cleanup_jobs every 60 seconds using the executor's IOLoop (running
    # in another thread).
    callback = tornado.ioloop.PeriodicCallback(cleanup_jobs, 1000 * 60,
                                               io_loop=executor.loop)
    callback.start()

    for message in consumer:
        incomingData = json.loads(message.value)
        version = incomingData.get('version', None)
        if version != 1:
            logger.exception('Unsupported incomingData schema version ({})'
                             ''.format(version))
        else:
            payload = incomingData.get('payload')

            pool_message = None
            for pooler in poolers:
                if pooler.part_of_pooling_set(payload):
                    if pooler.pool_ready(payload):
                        payload = pooler.pooled_payload(payload)
                    else:
                        pool_message = pooler.enqueue(payload) or True
                    break
            if pool_message:
                logger.info(pool_message)
                # This payload has been queued, so continue to the next
                # message.
                   continue

            event = incomingData.get('event')
            handler = handlers.get(event)
            if handler is None:
                logger.warning('Unhandled event {}.'.format(event))
            else:
                # We must maintain a reference to the returned Future until
                # the computation is complete, otherwise the job will be
                # terminated.
                full_log.debug('Handling event {} with payload {}'
                               ''.format(event, payload))

                if hasattr(handler, '_keyer'):
                    key = handler._keyer(handler, payload)
                else:
                    key = str(uuid.uuid4())

                logger.info('Submitted job {}'.format(key))
                r = executor.submit(handler, payload, key=key)
                jobs.append(r)
                jobstats[key] = {'start_time': datetime.utcnow()}

                # When its done, remove the result.
                def job_done(job):
                    logger.info('Job {} is done'.format(job.key))
                    time = datetime.utcnow() - jobstats[job.key]['start_time']
                    logger.info('Job {} took {}. '.format(job.key, time))
                    jobstats.pop(job.key, None)

                r.add_done_callback(job_done)

def register_heartbeat_callback(heartbeater, executor):
    """
    Register the heartbeat callback.

    :param heartbeater: The heartbeater object.
    :param executor: The executor io loop.
    """
    heartbeat = tornado.ioloop.PeriodicCallback(
        lambda: heartbeater.beat(constants.COMPONENT_LISTENER),
        1000 * constants.HEARTBEAT_INTERVAL_IN_SECONDS, io_loop=executor.loop)
    heartbeat.start()


def main():
    # We only need "distributed" to be installed on the listener's process,
    # not on the workers, so we lazily import Client.
    from distributed import Client

    from mds_data_transform.pooler import ACCESSPool, TCXPool

    logger, full_log = setup_loggers()
    heartbeater = get_heartbeater(get_kafka_producer())

    logger.info('Data transformer starting up',
                extra=constants.extra_log_system_listener)
    heartbeater.beat(constants.COMPONENT_LISTENER,
                     status='START', remarks='Starting up')

    dask_addr = os.environ.get('DISPATCHER_ADDRESS', 'tcp://localhost:8786')
    logger.debug('Connecting to dask dispatcher on {}'.format(dask_addr),
                 extra=constants.extra_log_system_listener)
    executor = Client(dask_addr)

    kafka_addr = os.environ.get("KAFKA_ADDRESS", 'localhost:9092')
    logger.debug('Connecting to kafka consumer on {}'.format(kafka_addr))
    consumer = get_kafka_consumer()

    logger.debug('Subscribing to incomingData topic')
    consumer.subscribe(['incomingData'])

    handlers = {'IncomingScData': incomingData_IncomingScData,
                'IncomingL1Data': incomingData_IncomingL1Data,
                }

    # Setup the heartbeat callback
    register_heartbeat_callback(heartbeater, executor)
    logger.debug('Startup complete. Listening for messages...')
    try:
        listen(consumer, handlers, executor,
               poolers=[ACCESSPool(), TCXPool()])
    except KeyboardInterrupt:
        # The program was stopped by the user. So close down
        # without a traceback.
        pass
    finally:
        logger.warning('Data transformer closing down',
                       extra=constants.extra_log_system_listener)
        heartbeater.beat(constants.COMPONENT_LISTENER,
                         status='STOP', remarks='Shutting down')


if __name__ == '__main__':
    main()


