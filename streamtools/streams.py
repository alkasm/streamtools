from redis import StrictRedis
from heapq import merge
from collections import namedtuple


StreamData = namedtuple('StreamData', ('id', 'key', 'data'))


class StreamsClient(StrictRedis):
    """A Redis client for Redis 5.0+ with Streams capability."""

    def __init__(self, *args, **kwargs):
        """Connects to Redis and checks the Streams API is available."""
        super().__init__(*args, **kwargs)
        # check Redis has the Streams API
        try:
            self.xread
            self.xadd
            self.xrange
            self.xrevrange
        except AttributeError as e:
            print('You must be using a version of the redis library with the',
                'same API as https://github.com/alkasm/redis-py for accessing',
                'the Streams datatype introduced in Redis 5.0+.')
            raise


class StreamsInteractor:
    """A wrapper to hold a Redis instance and makes queries iterable"""

    def __init__(self, redis):
        """Stores a Redis instance."""
        self.redis = redis

    @staticmethod
    def decode_id(stream_id_b):
        """Decodes an id: b'1500000000-1' -> '1500000000-1'."""
        return stream_id_b.decode('utf-8')

    @staticmethod
    def decode_data(data_b):
        """Parses encoded dictionaries into decoded dictionaries, e.g.:
        {b'field': b'value', ...} -> {'field': 'value', ...}
        """
        return {k.decode('utf-8'): v.decode('utf-8') for k, v in data_b.items()}

    @classmethod
    def decode_response(cls, key, response):
        id_b, data_b = response
        stream_id = cls.decode_id(id_b)
        stream_data = cls.decode_data(data_b)
        return StreamData(stream_id, key, stream_data)

    @classmethod
    def parse_responses(cls, key, responses):
        """Parses Redis responses into a list of StreamData objects."""
        for response in responses:
            yield cls.decode_response(key, response)

    def scan(self):
        _, keys = self.redis.scan(0)
        yield from keys

    def xread(self, count_=None, block_=None, **streams):
        """See help(self.redis.xread)."""
        data = {}
        stream_responses = self.redis.xread(count_, block_, **streams)
        if stream_responses:
            for key, responses in stream_responses.items():
                data[key] = self.parse_responses(key, responses)
        return data

    def xrange(self, key, start='-', finish='+', count=None):
        """See help(self.redis.xrange)"""
        responses = self.redis.xrange(key, start, finish, count)
        yield from self.parse_responses(key, responses)

    def xrevrange(self, key, start='+', finish='-', count=None):
        """See help(self.redis.xrevrange)"""
        responses = self.redis.xrevrange(key, start, finish, count)
        yield from self.parse_responses(key, responses)
        
    def xadd(self, stream_key, stream_id='*', **kwargs):
        """See help(self.redis.xadd)"""
        post_id = self.redis.xadd(stream_key, stream_id, **kwargs)
        return self.decode_id(post_id)


class StreamsReader(StreamsInteractor):
    """Iterate and parse values from Redis streams, ordered by id."""

    def __init__(self, redis, streams, block=5000, continuous=True):
        """Uses a StreamsReader but holds state while iterating through values from a stream."""
        super().__init__(redis)
        self.continuous = continuous
        self.streams = streams
        self.block = block

    def __iter__(self):
        while True:
            responses = self.xread(block_=self.block, **self.streams)
            if responses:
                for stream_data in merge(*responses.values(), key=lambda sd: sd.id):
                    self.streams[stream_data.key] = stream_data.id
                    yield stream_data
            elif not self.continuous:
                raise StopIteration

