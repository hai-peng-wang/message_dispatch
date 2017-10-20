

from collections import OrderedDict
from copy import deepcopy
import re


class ACCESSPool(object):
    """
    An object to pool individual file names until the full set is ready.
    For ACCESS, we await the "PA" and "PE" files (surface and model level
    data) to both be ready before we do any processing.
    """
    def __init__(self):
        # TODO: Add timeout for pool?

        self.queue = OrderedDict()
        # A pattern to match something like qwxbjva_pe003_2016120100_utc_fc.um
        self.pattern = (r'(?P<experiment_id>[a-z]{7})'
                        r'_'
                        r'(?P<stream>[a-z]{2})'
                        r'(?P<forecast_period>\d{3})'
                        r'_'
                        r'(?P<utc_timestamp>\d{10})_utc'
                        r'_'
                        r'([a-z]{2}).um')
        self._re = re.compile(self.pattern)

    def part_of_pooling_set(self, message):
        """Whether the given message is part of a pooling set."""
        fname = message.get('srcFile', '')
        return bool(self._re.search(fname))

    def payload_keys(self, payload):
        fname = payload.get('srcFile', '')
        match = self._re.search(fname)
        return {'experiment_id': match.group('experiment_id'),
                'utc_timestamp': match.group('utc_timestamp'),
                'forecast_period': match.group('forecast_period'),
                'stream':  match.group('stream')}

    def queue_key(self, keys):
        queue_id = ('{0[experiment_id]} {0[utc_timestamp]} '
                    '{0[forecast_period]}'.format(keys))
        return queue_id

    def pool_ready(self, payload):
        keys = self.payload_keys(payload)
        queue_id = self.queue_key(keys)
        q = self.queue.get(queue_id, {})
        queued_streams = set([keys['stream']] + list(q.keys()))
        return queued_streams == {'pa', 'pe'}

    def enqueue(self, payload):
        keys = self.payload_keys(payload)
        queue_id = self.queue_key(keys)
        q = self.queue.setdefault(queue_id, {})
        stream = keys['stream']
        q[stream] = payload
        message = ('Pooling {0[stream]} stream for T{0[forecast_period]} '
                   '{0[utc_timestamp]}'.format(keys))
        return message

    def pooled_payload(self, payload):
        new_payload = deepcopy(payload)
        keys = self.payload_keys(payload)
         queue_id = self.queue_key(keys)
        q = self.queue.get(queue_id, {})
        # The fnames are ordered based on the order they arrived.
        fnames = ([pload.get('srcFile') for pload in q.values()] +
                  [payload.get('srcFile')])
        new_payload['srcFile'] = sorted(fnames)
        return new_payload


class TCXPool(ACCESSPool):
    def __init__(self):
        super(TCXPool, self).__init__()
        # A pattern to match something like xboiha_pe005
        self.pattern = (r'(?P<experiment_id>[a-z]{6})'
                        r'_'
                        r'(?P<stream>(pa|pe))'
                        r'(?P<forecast_period>\d{3})')
        self._re = re.compile(self.pattern)

    def payload_keys(self, payload):
        fname = payload.get('srcFile', '')
        match = self._re.search(fname)
        return {'experiment_id': match.group('experiment_id'),
                'utc_timestamp': None,
                'forecast_period': match.group('forecast_period'),
                'stream':  match.group('stream')}
