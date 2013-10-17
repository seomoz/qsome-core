'''Test some basic functionality'''

from common import TestQsome


class TestBasic(TestQsome):
    '''Some basic API calls'''
    def test_multiget(self):
        '''We should be able to get many jobs at once'''
        for index in range(10):
            self.lua('put', 0, 'worker', 'queue', index, 'klass', 5, [], 0)
        results = self.lua('multiget', 1, *range(10))
        self.assertEqual(len(results), 10)

    def test_complete(self):
        '''We shoudl be able to complete jobs and advance them'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', 5, {}, 0)
        self.assertEqual(len(self.lua('pop', 1, 'queue', 'worker', 10)), 1)
        self.lua('complete', 2, 'jid', 'worker', 'queue', {}, 'next', 'foo')
        data = self.lua('get', 3, 'jid')
        self.assertEqual(data['state'], 'waiting')
