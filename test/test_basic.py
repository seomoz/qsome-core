'''Test some basic functionality'''

from common import TestQsome


class TestBasic(TestQsome):
    '''Some basic API calls'''
    def test_multiget(self):
        '''We should be able to get many jobs at once'''
        for index in range(10):
            self.lua('put', 0, 'queue', index, 'klass', 5, [], 0)
        results = self.lua('multiget', 1, *range(10))
        self.assertEqual(len(results), 10)
