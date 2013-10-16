'''Test the queue functionality'''

from common import TestQsome


class TestGeneral(TestQsome):
    '''Test general queue-y things'''
    def test_malformed(self):
        '''Enumerate a bunch of malformed requests'''
        self.assertMalformed(self.lua, [
            ('queue.resize', 0, 'queue'),              # No size
            ('queue.resize', 0, 'queue', 'foo'),       # Size not a number
            ('queue.resize', 0, 'queue', -2),          # Negative size
            ('queue.subqueues', 0)                     # No queue provided
        ])

    def test_subqueues(self):
        '''We should have access to the number of subqueues for a queue'''
        self.assertEqual(len(self.lua('queue.subqueues', 0, 'queue')), 1)
        self.lua('queue.resize', 0, 'queue', 2)
        self.assertEqual(len(self.lua('queue.subqueues', 0, 'queue')), 2)


class TestPut(TestQsome):
    '''Test putting jobs into a queue'''
    # For reference:
    #
    #   put(now, queue, jid, klass, hash, data, delay,
    #       [priority, p],
    #       [tags, t],
    #       [retries, r],
    #       [depends, '[...]'])
    def put(self, *args):
        '''Alias for self.lua('put', ...)'''
        return self.lua('put', *args)

    def test_malformed(self):
        '''Enumerate all the ways in which the input can be messed up'''
        self.assertMalformed(self.put, [
            (12345,),                                        # No worker
            (12345, 'worker'),                               # No queue provided
            (12345, 'worker', 'foo'),                        # No jid provided
            (12345, 'worker', 'foo', 'bar'),                 # No klass provided
            (12345, 'worker', 'foo', 'bar', 'whiz'),         # No hash provided
            (12345, 'worker', 'foo', 'bar', 'whiz', 5),      # No data provided
            (12345, 'worker', 'foo', 'bar', 'whiz', 'foo'),  # Malformed hash
            (12345, 'worker', 'foo', 'bar', 'whiz', 5,
                '{}'),                               # No delay provided
            (12345, 'worker', 'foo', 'bar', 'whiz', 5,
                '{]'),                               # Malformed data provided
            (12345, 'worker', 'foo', 'bar', 'whiz', 5,
                '{}', 'number'),                     # Malformed delay provided
            (12345, 'worker', 'foo', 'bar', 'whiz', 5, '{}', 1,
                'retries'),                          # Retries arg missing
            (12345, 'worker', 'foo', 'bar', 'whiz', 5, '{}', 1,
                'retries', 'foo'),                   # Retries arg not a number
            (12345, 'worker', 'foo', 'bar', 'whiz', 5, '{}', 1,
                'tags'),                             # Tags arg missing
            (12345, 'worker', 'foo', 'bar', 'whiz', 5, '{}', 1,
                'tags', '{]'),                       # Tags arg malformed
            (12345, 'worker', 'foo', 'bar', 'whiz', 5, '{}', 1,
                'priority'),                         # Priority arg missing
            (12345, 'worker', 'foo', 'bar', 'whiz', 5, '{}', 1,
                'priority', 'foo'),                  # Priority arg malformed
            (12345, 'worker', 'foo', 'bar', 'whiz', 5, '{}', 1,
                'depends'),                          # Depends arg missing
            (12345, 'worker', 'foo', 'bar', 'whiz', 5, '{}', 1,
                'depends', '{]')                     # Depends arg malformed
        ])

    def test_basic(self):
        '''We should be able to put and get jobs'''
        jid = self.lua(
            'put', 12345, 'worker', 'queue', 'jid', 'klass', 5, {}, 0)
        self.assertEqual(jid, 'jid')
        # Now we should be able to verify the data we get back
        data = self.lua('get', 12345, 'jid')
        data.pop('history')
        self.assertEqual(data, {
            'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {},
            'hash': 5,
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'queue',
            'remaining': 5,
            'retries': 5,
            'state': 'waiting',
            'tags': {},
            'tracked': False,
            'worker': u''
        })

    def test_data_as_array(self):
        '''We should be able to provide an array as data'''
        # In particular, an empty array should be acceptable, and /not/
        # transformed into a dictionary when it returns
        self.lua('put', 12345, 'worker', 'queue', 'jid', 'klass', 5, [], 0)
        self.assertEqual(self.lua('get', 12345, 'jid')['data'], '[]')

    def test_put_delay(self):
        '''When we put a job with a delay, it's reflected in its data'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', 5, {}, 1)
        self.assertEqual(self.lua('get', 0, 'jid')['state'], 'scheduled')
        # After the delay, we should be able to pop
        self.assertEqual(self.lua('pop', 0, 'queue', 'worker', 10), {})
        self.assertEqual(
            len(self.lua('pop', 2, 'queue', 'worker', 10)), 1)

    def test_put_retries(self):
        '''Reflects changes to 'retries' '''
        self.lua('put', 12345, 'worker', 'queue', 'jid',
                 'klass', 5, {}, 0, 'retries', 2)
        self.assertEqual(self.lua('get', 12345, 'jid')['retries'], 2)
        self.assertEqual(self.lua('get', 12345, 'jid')['remaining'], 2)

    def test_put_tags(self):
        '''When we put a job with tags, it's reflected in its data'''
        self.lua('put',
            12345, 'worker', 'queue', 'jid', 'klass', 5, {}, 0, 'tags', ['foo'])
        self.assertEqual(self.lua('get', 12345, 'jid')['tags'], ['foo'])

    def test_put_priority(self):
        '''When we put a job with priority, it's reflected in its data'''
        self.lua('put',
            12345, 'worker', 'queue', 'jid', 'klass', 5, {}, 0, 'priority', 1)
        self.assertEqual(self.lua('get', 12345, 'jid')['priority'], 1)

    def test_put_depends(self):
        '''Dependencies are reflected in job data'''
        self.lua('put', 12345, 'worker', 'queue', 'a', 'klass', 5, {}, 0)
        self.lua('put',
            12345, 'worker', 'queue', 'b', 'klass', 5, {}, 0, 'depends', ['a'])
        self.assertEqual(self.lua('get', 12345, 'a')['dependents'], ['b'])
        self.assertEqual(self.lua('get', 12345, 'b')['dependencies'], ['a'])
        self.assertEqual(self.lua('get', 12345, 'b')['state'], 'depends')

    def test_move(self):
        '''Move is described in terms of puts.'''
        self.lua(
            'put', 0, 'worker', 'queue', 'jid', 'klass', 5, {'foo': 'bar'}, 0)
        self.lua(
            'put', 0, 'worker', 'other', 'jid', 'klass', 5, {'foo': 'bar'}, 0)
        data = self.lua('get', 1, 'jid')
        data.pop('history')
        self.assertEqual(data, {
            'data': '{"foo": "bar"}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {},
            'hash': 5,
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'other',
            'remaining': 5,
            'retries': 5,
            'state': 'waiting',
            'tags': {},
            'tracked': False,
            'worker': u''})

    def test_move_update(self):
        '''When moving, ensure data's only changed when overridden'''
        for key, value, update in [
            ('priority', 1, 2),
            ('tags', ['foo'], ['bar']),
            ('retries', 2, 3)]:
            # First, when not overriding the value, it should stay the sam3
            # even after moving
            self.lua(
                'put', 0, 'worker', 'queue', key, 'klass', 5, {}, 0, key, value)
            self.lua('put', 0, 'worker', 'other', key, 'klass', 5, {}, 0)
            self.assertEqual(self.lua('get', 0, key)[key], value)
            # But if we override it, it should be updated
            self.lua(
                'put', 0, 'worker', 'queue', key,
                'klass', 5, {}, 0, key, update)
            self.assertEqual(self.lua('get', 0, key)[key], update)

        # Updating dependecies has to be special-cased a little bit. Without
        # overriding dependencies, they should be carried through the move
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', 5, {}, 0)
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', 5, {}, 0)
        self.lua('put',
            0, 'worker', 'queue', 'c', 'klass', 5, {}, 0, 'depends', ['a'])
        self.lua('put', 0, 'worker', 'other', 'c', 'klass', 5, {}, 0)
        self.assertEqual(self.lua('get', 0, 'a')['dependents'], ['c'])
        self.assertEqual(self.lua('get', 0, 'b')['dependents'], {})
        self.assertEqual(self.lua('get', 0, 'c')['dependencies'], ['a'])
        # But if we move and update depends, then it should correctly reflect
        self.lua('put',
            0, 'worker', 'queue', 'c', 'klass', 5, {}, 0, 'depends', ['b'])
        self.assertEqual(self.lua('get', 0, 'a')['dependents'], {})
        self.assertEqual(self.lua('get', 0, 'b')['dependents'], ['c'])
        self.assertEqual(self.lua('get', 0, 'c')['dependencies'], ['b'])


class TestPeek(TestQsome):
    '''Test peeking jobs'''
    # For reference:
    #
    #   QlessAPI.peek = function(now, queue, count)
    def test_malformed(self):
        '''Enumerate all the ways in which the input can be malformed'''
        self.assertMalformed(self.lua, [
            ('peek', 12345,),                     # No queue provided
            ('peek', 12345, 'foo'),               # No count provided
            ('peek', 12345, 'foo', 'number'),     # Count arg malformed
        ])

    def test_basic(self):
        '''Can peek at a single waiting job'''
        # No jobs for an empty queue
        self.assertEqual(self.lua('peek', 0, 'foo', 10), {})
        self.lua('put', 0, 'worker', 'foo', 'jid', 'klass', 5, {}, 0)
        # And now we should see a single job
        self.assertEqual(len(self.lua('peek', 1, 'foo', 10)), 1)
        # With several jobs in the queue, we should be able to see more
        self.lua('put', 2, 'worker', 'foo', 'jid2', 'klass', 5, {}, 0)
        self.assertEqual(
            [o['jid'] for o in self.lua('peek', 3, 'foo', 10)],
            ['jid'])


class TestPop(TestQsome):
    '''Test popping jobs'''
    # For reference:
    #
    #   QlessAPI.pop = function(now, queue, worker, count)
    def test_malformed(self):
        '''Enumerate all the ways this can be malformed'''
        self.assertMalformed(self.lua, [
            ('pop', 12345,),                              # No queue provided
            ('pop', 12345, 'queue'),                      # No worker provided
            ('pop', 12345, 'queue', 'worker'),            # No count provided
            ('pop', 12345, 'queue', 'worker', 'number'),  # Malformed count
        ])

    def test_concurrency(self):
        '''We have limitations on the number of jobs from the same queue'''
        for i in range(10):
            self.lua('put', i, 'worker', 'queue', i, 'klass', i, {}, 0)
        # We should only be able to pop a single job
        self.assertEqual(
            len(self.lua('pop', 10, 'queue', 'worker', 10)), 1)
        # If we bump the concurrency, we can see more
        self.lua('queue.config', 11, 'queue', 'concurrency', 5)
        self.assertEqual(
            len(self.lua('pop', 12, 'queue', 'worker', 10)), 4)

    def test_grow(self):
        '''We should be able to change the number of subqueues in a queue'''
        for i in range(10):
            self.lua('put', i, 'worker', 'queue', i, 'klass', i, {}, 0)
        # We should only be able to pop a single job
        self.assertEqual(
            len(self.lua('pop', 10, 'queue', 'worker', 10)), 1)
        # If we grow it, we'll be able to pop more
        self.lua('queue.resize', 11, 'queue', 5)
        self.assertEqual(
            len(self.lua('pop', 12, 'queue', 'worker', 10)), 4)

    def test_shrink(self):
        '''We should be able to decrease the concurrency'''
        for i in range(10):
            self.lua('put', i, 'worker', 'queue', i, 'klass', i, {}, 0)
        self.lua('queue.resize', 10, 'queue', 5)
        self.assertEqual(
            len(self.lua('pop', 11, 'queue', 'worker', 10)), 5)
        # If we shrink the number of subqueues, we shouldn't be able to pop
        # more jobs until we complete some.
        self.lua('queue.resize', 11, 'queue', 1)
        for i in range(5):
            self.assertEqual(
                len(self.lua('pop', 12, 'queue', 'worker', 10)), 0)
            self.lua('complete', 12, i, 'worker', 'queue', {})
        # With these jobs complete, we should now be able to pop one job
        self.assertEqual(
            len(self.lua('pop', 13, 'queue', 'worker', 10)), 1)
